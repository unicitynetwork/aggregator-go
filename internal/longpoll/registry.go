// Package longpoll is the waiter registry of the optional long-poll on inclusion-proof reads.
//
// It owns no proof logic. It provides what the handlers need to wait without losing a wake-up and
// without taking more than their share of the server (arch-long-poll revision 3, sections 5, 7, 8):
//
//   - a global reservation cap on admitted waits (parked and resuming), non-blocking, with
//     effective capacity zero when the active-work limit C is below two;
//   - a monotone generation with a check-and-arm under the registry lock, so a notification which
//     arrives while a request is evaluating is never lost, and many events coalesce into one
//     recheck per waiter;
//   - cancellation, timeouts against an absolute cutoff that unrelated wake-ups never extend;
//   - a bounded FIFO resume queue with an execution quota max(1, floor(C/4)) which always leaves
//     one active-work token for fresh requests;
//   - drains (role change: reopen afterwards; shutdown: permanent) and a join.
//
// Nothing here runs under a publisher, tracker or catalog lock: Notify is O(1) plus a
// non-blocking signal per armed waiter, and callers invoke it after releasing their own locks.
package longpoll

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Config sizes the registry.
type Config struct {
	// MaxWaiters bounds admitted waits (parked and resuming). Zero disables parking.
	MaxWaiters int
	// Concurrency is the server's active-work limit (CONCURRENCY_LIMIT, C).
	Concurrency int
	// ResumeQueueMax bounds the queued resumptions; zero means MaxWaiters (every admitted wait can
	// queue at most once, so the queue is bounded by the reservation anyway).
	ResumeQueueMax int
}

// Admission is the outcome of Admit. Shedding is never an error: the caller serves the ordinary
// immediate read (with a zero echo).
type Admission int

const (
	Admitted Admission = iota
	ShedCapacity
	ShedDraining
)

// Wake says why Wait returned.
type Wake int

const (
	// WakeChanged: the generation moved before the wait was armed (an event raced the evaluation).
	WakeChanged Wake = iota
	// WakeSignal: armed, then notified.
	WakeSignal
	WakeTimeout
	WakeCancel
	// WakeDrain: a role-change or shutdown drain; observe the current state and finish.
	WakeDrain
)

// Resume errors. A resume which fails is answered with the existing busy/deadline error.
var (
	ErrResumeQueueFull = errors.New("longpoll: resume queue full")
	ErrResumeTimeout   = errors.New("longpoll: resume deadline passed in the queue")
	ErrWaiterClosed    = errors.New("longpoll: waiter closed")
)

// Registry is safe for concurrent use.
type Registry struct {
	cfg        Config
	capacity   int
	maxRunning int

	mu        sync.Mutex
	gen       uint64
	epoch     uint64
	admitting bool
	shutdown  bool
	waiters   map[*Waiter]struct{}
	queue     []*ticket
	running   int
	idle      chan struct{} // closed while there are no waiters

	shedCapacity atomic.Uint64
	shedDraining atomic.Uint64
	wakes        atomic.Uint64
	notifies     atomic.Uint64

	// testHookArm runs in Wait before the arm check; tests use it as a barrier.
	testHookArm func()
}

// New builds a registry.
func New(cfg Config) *Registry {
	r := &Registry{cfg: cfg, admitting: true, waiters: map[*Waiter]struct{}{}, idle: make(chan struct{})}
	close(r.idle)
	r.capacity = cfg.MaxWaiters
	if r.capacity < 0 || cfg.Concurrency < 2 {
		// With C < 2 the headroom guarantee for fresh requests is unfulfillable: immediate mode.
		r.capacity = 0
	}
	r.maxRunning = cfg.Concurrency / 4
	if r.maxRunning < 1 {
		r.maxRunning = 1
	}
	if r.cfg.ResumeQueueMax <= 0 {
		r.cfg.ResumeQueueMax = r.capacity
	}
	return r
}

// Capacity is the effective global reservation cap.
func (r *Registry) Capacity() int { return r.capacity }

// MaxRunning is the quota of simultaneously running resumed reads.
func (r *Registry) MaxRunning() int { return r.maxRunning }

// Generation returns the current publication/status/lifecycle generation.
func (r *Registry) Generation() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen
}

// Notify records that something a waiter may care about changed and wakes the armed waiters to
// recheck. It never blocks and runs no proof work; call it after releasing publisher, tracker
// and catalog locks. Waiters which are not armed (evaluating or queued) see the new generation
// when they next arm, so the newest generation is never lost.
func (r *Registry) Notify() {
	r.notifies.Add(1)
	r.mu.Lock()
	r.gen++
	for w := range r.waiters {
		w.signalLocked()
	}
	r.mu.Unlock()
}

// Admit reserves a wait without blocking. The reservation covers the wait until Waiter.Close,
// queued resumptions included.
func (r *Registry) Admit() (*Waiter, Admission) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.admitting {
		r.shedDraining.Add(1)
		return nil, ShedDraining
	}
	if len(r.waiters) >= r.capacity {
		r.shedCapacity.Add(1)
		return nil, ShedCapacity
	}
	if len(r.waiters) == 0 {
		r.idle = make(chan struct{})
	}
	w := &Waiter{reg: r, epoch: r.epoch, wake: make(chan struct{}, 1)}
	r.waiters[w] = struct{}{}
	return w, Admitted
}

// Drain ends every current wait (they return WakeDrain) and refuses new ones. A role-change drain
// is followed by Reopen once the transition settled; a shutdown drain (permanent) is not.
func (r *Registry) Drain(permanent bool) {
	r.mu.Lock()
	r.admitting = false
	if permanent {
		r.shutdown = true
	}
	r.epoch++
	r.gen++
	for w := range r.waiters {
		w.signalLocked()
	}
	r.mu.Unlock()
}

// Reopen admits new waits again after a role-change drain. It does nothing after shutdown.
func (r *Registry) Reopen() {
	r.mu.Lock()
	if !r.shutdown {
		r.admitting = true
	}
	r.mu.Unlock()
}

// Join blocks until every admitted wait has been closed (the h2c/shutdown drain waits here before
// storage is closed), or ctx ends.
func (r *Registry) Join(ctx context.Context) error {
	r.mu.Lock()
	idle := r.idle
	r.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats is a point-in-time view for tests and metrics.
type Stats struct {
	Reserved     int // parked + queued + running
	Queued       int
	Running      int
	Generation   uint64
	ShedCapacity uint64
	ShedDraining uint64
	Wakes        uint64
	Notifies     uint64
}

func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{Reserved: len(r.waiters), Queued: len(r.queue), Running: r.running, Generation: r.gen,
		ShedCapacity: r.shedCapacity.Load(), ShedDraining: r.shedDraining.Load(), Wakes: r.wakes.Load(), Notifies: r.notifies.Load()}
}

// Waiter is one admitted wait.
type Waiter struct {
	reg    *Registry
	epoch  uint64
	wake   chan struct{}
	armed  bool
	closed bool
	ticket *ticket
}

// signalLocked wakes an armed waiter. Callers hold reg.mu.
func (w *Waiter) signalLocked() {
	if !w.armed {
		return
	}
	w.armed = false
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Generation is read before the evaluation whose outcome Wait will rely on.
func (w *Waiter) Generation() uint64 { return w.reg.Generation() }

// Wait suspends until a notification, the cutoff, cancellation or a drain. seen is the generation
// read before the last evaluation: if the registry has moved on, Wait returns WakeChanged at once
// (the evaluation may have missed the event); otherwise the wait is armed atomically under the
// registry lock. The cutoff is absolute and unrelated wake-ups never extend it.
func (w *Waiter) Wait(ctx context.Context, seen uint64, cutoff time.Time) Wake {
	r := w.reg
	if h := r.testHookArm; h != nil {
		h()
	}
	r.mu.Lock()
	if w.closed {
		r.mu.Unlock()
		return WakeCancel
	}
	if w.epoch != r.epoch {
		r.mu.Unlock()
		return WakeDrain
	}
	if r.gen != seen {
		r.mu.Unlock()
		return WakeChanged
	}
	w.armed = true
	select { // a stale token from an earlier cycle must not satisfy this one
	case <-w.wake:
	default:
	}
	r.mu.Unlock()

	d := time.Until(cutoff)
	if d <= 0 {
		w.disarm()
		return WakeTimeout
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-w.wake:
		r.wakes.Add(1)
		r.mu.Lock()
		drained := w.epoch != r.epoch
		r.mu.Unlock()
		if drained {
			return WakeDrain
		}
		return WakeSignal
	case <-timer.C:
		w.disarm()
		return WakeTimeout
	case <-ctx.Done():
		w.disarm()
		return WakeCancel
	}
}

func (w *Waiter) disarm() {
	w.reg.mu.Lock()
	w.armed = false
	w.reg.mu.Unlock()
}

// ticket is a queued resumption.
type ticket struct {
	w       *Waiter
	permit  *jsonrpc.Permit
	granted chan struct{}
	state   int // 0 queued, 1 granted, 2 cancelled, 3 finished; guarded by reg.mu
}

// Grant is a running resumed read. Done releases the active-work token and the quota slot; it is
// idempotent. Call it as soon as the read attempt finished, before building the response.
type Grant struct {
	r      *Registry
	permit *jsonrpc.Permit
	t      *ticket
}

// Done ends the resumed read.
func (g *Grant) Done() {
	g.r.mu.Lock()
	if g.t.state == 1 {
		g.t.state = 3
		g.r.running--
	}
	g.r.mu.Unlock()
	g.permit.Release()
	g.r.Schedule()
}

// Resume queues a recheck for active-work capacity and returns when it is granted, in FIFO
// order, within the resumed-read quota and leaving a token for fresh requests. The permit is
// reacquired by the scheduler. No registry, tracker or catalog lock may be held by the caller.
// A wait which cannot be served before until fails with ErrResumeTimeout (the handler answers
// with the existing busy/deadline error); a cancelled ctx fails with its error.
func (w *Waiter) Resume(ctx context.Context, p *jsonrpc.Permit, until time.Time) (*Grant, error) {
	r := w.reg
	r.mu.Lock()
	if w.closed {
		r.mu.Unlock()
		return nil, ErrWaiterClosed
	}
	if len(r.queue) >= r.cfg.ResumeQueueMax {
		r.mu.Unlock()
		return nil, ErrResumeQueueFull
	}
	t := &ticket{w: w, permit: p, granted: make(chan struct{})}
	w.ticket = t
	r.queue = append(r.queue, t)
	r.mu.Unlock()
	r.Schedule()

	var timeout <-chan time.Time
	if d := time.Until(until); d > 0 {
		tm := time.NewTimer(d)
		defer tm.Stop()
		timeout = tm.C
	} else {
		timeout = closedChan
	}
	select {
	case <-t.granted:
	case <-timeout:
		return nil, r.cancelTicket(t, ErrResumeTimeout)
	case <-ctx.Done():
		return nil, r.cancelTicket(t, ctx.Err())
	}
	r.mu.Lock()
	if t.state == 2 { // the permit was found closed while queued
		r.mu.Unlock()
		return nil, ErrWaiterClosed
	}
	r.mu.Unlock()
	return &Grant{r: r, permit: p, t: t}, nil
}

var closedChan = func() <-chan time.Time { c := make(chan time.Time); close(c); return c }()

// cancelTicket removes a queued ticket, or returns a grant which raced the cancellation.
func (r *Registry) cancelTicket(t *ticket, cause error) error {
	r.mu.Lock()
	switch t.state {
	case 0:
		t.state = 2
		for i, q := range r.queue {
			if q == t {
				r.queue = append(r.queue[:i], r.queue[i+1:]...)
				break
			}
		}
		t.w.ticket = nil
		r.mu.Unlock()
	case 1: // granted concurrently: hand the token and the quota slot back
		t.state = 3
		r.running--
		t.w.ticket = nil
		r.mu.Unlock()
		t.permit.Release()
		r.Schedule()
	default:
		r.mu.Unlock()
	}
	return cause
}

// Schedule grants queued resumptions while the quota and the headroom allow. The server calls it
// whenever an active-work token is released (jsonrpc.Server.SetReleaseHook).
func (r *Registry) Schedule() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.queue) > 0 && r.running < r.maxRunning {
		t := r.queue[0]
		if t.permit.Closed() { // the request ended while queued
			t.state = 2
			close(t.granted)
			r.queue = r.queue[1:]
			continue
		}
		if !t.permit.TryReacquire(1) {
			return
		}
		t.state = 1
		r.running++
		r.queue = r.queue[1:]
		close(t.granted)
	}
}

// Close releases the reservation and every other resource of the wait, exactly once. It does not
// touch the HTTP permit (its owner closes it).
func (w *Waiter) Close() {
	r := w.reg
	r.mu.Lock()
	if w.closed {
		r.mu.Unlock()
		return
	}
	w.closed = true
	w.armed = false
	var release *jsonrpc.Permit
	if t := w.ticket; t != nil {
		switch t.state {
		case 0:
			t.state = 2
			close(t.granted) // a Resume blocked on it returns ErrWaiterClosed
			for i, q := range r.queue {
				if q == t {
					r.queue = append(r.queue[:i], r.queue[i+1:]...)
					break
				}
			}
		case 1: // a granted resumption nobody finished
			t.state = 3
			r.running--
			release = t.permit
		}
		w.ticket = nil
	}
	delete(r.waiters, w)
	if len(r.waiters) == 0 {
		close(r.idle)
	}
	r.mu.Unlock()
	if release != nil {
		release.Release()
	}
	r.Schedule()
}
