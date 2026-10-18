package bft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

const (
	// maxAdmissionsPerRun bounds one catch-up run; further runs are scheduled while it remains necessary.
	maxAdmissionsPerRun = 16
	// runDeadline bounds one catch-up run (REST calls and verification).
	runDeadline = 20 * time.Second
	// failedFrontierRetry is the cooldown after a failed attempt to admit the next epoch: at most one
	// failed-frontier retry per second, whatever messages arrive meanwhile.
	failedFrontierRetry = time.Second
	// minRunInterval bounds how often a run (which reads the head from the store) may start without
	// the previous one having admitted anything.
	minRunInterval = 250 * time.Millisecond
)

// catchUp is the single background worker (one per process and network, not per claimed epoch) that
// extends the admitted trust base history. The message path never calls the BFT node: an
// unknown-epoch response only wakes this worker (non-blocking, coalesced) and is dropped.
// The worker resolves only the next missing epoch after the admitted head, verified against its
// predecessor, so a distant forged epoch claim can never cause a skip or an unbounded scan.
type catchUp struct {
	history IntakeTrustBases
	log     *logger.Logger
	// onAdmitted is called after each admission (membership refresh).
	onAdmitted func(ctx context.Context, epoch uint64)
	// onHead is called with the head read from the store; it reports whether the head had moved
	// without this worker (for example another HA node admitted an epoch).
	onHead func(ctx context.Context, head uint64) bool
	// onRun is called at the start of every run: it retries a failed membership publication whatever
	// the head and the live epoch are (the set is recomputed from the local history).
	onRun func(ctx context.Context)
	// onCaughtUp is called after a run that admitted at least one epoch (re-request the latest UC).
	onCaughtUp func(ctx context.Context)
	now        func() time.Time

	wake chan struct{}

	mu             sync.Mutex
	failedHead     uint64 // frontier: the head epoch whose successor could not be admitted
	failedUntil    time.Time
	lastStart      time.Time
	retryScheduled bool

	// instrumentation (hardware independent)
	runs       atomic.Uint64
	attempts   atomic.Uint64 // REST admission attempts
	admissions atomic.Uint64
}

func newCatchUp(history IntakeTrustBases, log *logger.Logger) *catchUp {
	return &catchUp{history: history, log: log, now: time.Now, wake: make(chan struct{}, 1)}
}

// Signal wakes the worker. It never blocks and coalesces: any number of signals while a run is
// pending or active cause at most one further run.
func (c *catchUp) Signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Run serves signals until the context ends.
func (c *catchUp) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
			c.runOnce(ctx)
		}
	}
}

// startAllowed spaces the starts of runs (each reads the head from the store). When refused it
// schedules one retry, so recovery does not depend on further messages arriving.
func (c *catchUp) startAllowed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.lastStart.IsZero() || now.Sub(c.lastStart) >= minRunInterval {
		c.lastStart = now
		return true
	}
	c.scheduleLocked(minRunInterval - now.Sub(c.lastStart))
	return false
}

// allowed applies the miss cooldown for the frontier (head generation, next epoch). When refused it
// schedules one retry at the earliest allowed time.
func (c *catchUp) allowed(head uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.failedHead != head || !now.Before(c.failedUntil) {
		return true
	}
	c.scheduleLocked(c.failedUntil.Sub(now))
	return false
}

func (c *catchUp) scheduleLocked(wait time.Duration) {
	if c.retryScheduled {
		return
	}
	c.retryScheduled = true
	time.AfterFunc(wait, func() {
		c.mu.Lock()
		c.retryScheduled = false
		c.mu.Unlock()
		c.Signal()
	})
}

func (c *catchUp) fail(head uint64) {
	c.mu.Lock()
	c.failedHead = head
	c.failedUntil = c.now().Add(failedFrontierRetry)
	c.mu.Unlock()
}

// RetryLater wakes the worker after d (coalesced with other scheduled retries): used when a local
// step (membership publication) failed and must be retried although nothing else will signal.
func (c *catchUp) RetryLater(d time.Duration) {
	c.mu.Lock()
	c.scheduleLocked(d)
	c.mu.Unlock()
}

func (c *catchUp) runOnce(parent context.Context) {
	if !c.startAllowed() {
		return
	}
	ctx, cancel := context.WithTimeout(parent, runDeadline)
	defer cancel()
	if c.onRun != nil {
		c.onRun(ctx)
	}
	admitted := 0
	advancedElsewhere := false
	for admitted < maxAdmissionsPerRun {
		head, err := c.history.Head(ctx)
		if err != nil {
			c.log.WithContext(ctx).Warn("catch-up: failed to read the admitted head", "error", err.Error())
			return
		}
		if c.onHead != nil && c.onHead(ctx, head) {
			advancedElsewhere = true
		}
		// a changed head (our own or another node's admission) is a different frontier: no cooldown
		if !c.allowed(head) {
			break
		}
		if admitted == 0 {
			c.runs.Add(1)
		}
		c.attempts.Add(1)
		epoch, err := c.history.AdmitNext(ctx)
		if err != nil {
			c.fail(head)
			c.log.WithContext(ctx).Debug("catch-up: next epoch not admitted", "head", head, "error", err.Error())
			break
		}
		admitted++
		c.admissions.Add(1)
		if c.onAdmitted != nil {
			c.onAdmitted(ctx, epoch)
		}
	}
	if admitted == maxAdmissionsPerRun {
		// the bound was reached, not the end of the history: schedule the next bounded run
		c.Signal()
	}
	if (admitted > 0 || advancedElsewhere) && c.onCaughtUp != nil {
		// the parent (client lifetime) context, not the run's: the handshake send is asynchronous and
		// returns before the dial and write complete, which a cancelled run context would abort
		c.onCaughtUp(parent)
	}
}
