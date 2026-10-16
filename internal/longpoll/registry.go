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
// Nothing here runs under a publisher, tracker or catalog lock: Notify is a counter bump plus a
// non-blocking signal per armed waiter (O(armed waiters) under the registry lock), and callers invoke it after releasing their own locks.
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
	// ErrPermitHeld: Resume was given a permit which still holds its token. A wait releases the
	// permit before it parks (Permit.Release); resuming without that is a caller bug.
	ErrPermitHeld = errors.New("longpoll: permit still held, release it before parking")
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
	queueLen  atomic.Int32  // len(queue), readable without the lock (Schedule fast path)
	idle      chan struct{} // closed while there are no waiters

	shedCapacity atomic.Uint64
	shedDraining atomic.Uint64
	wakes        atomic.Uint64
	notifies     atomic.Uint64
	enqueued     atomic.Uint64

	// testHookArm runs in Wait before the arm check; tests use it as a barrier.
	testHookArm func()
	// testHookDeliver runs in Resume between the grant and its hand-over to the caller.
	testHookDeliver func()
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
// recheck. It never blocks and runs no proof work (O(W) signals under the registry lock); call it after releasing publisher, tracker
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
	Enqueued     uint64 // resumptions ever queued
}

func (r *Registry) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{Reserved: len(r.waiters), Queued: len(r.queue), Running: r.running, Generation: r.gen,
		ShedCapacity: r.shedCapacity.Load(), ShedDraining: r.shedDraining.Load(), Wakes: r.wakes.Load(), Notifies: r.notifies.Load(), Enqueued: r.enqueued.Load()}
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
	// Termination outranks generation traffic: a request whose context ended or whose cutoff
	// passed must finish its wait even if publications keep arriving during every evaluation.
	if k, done := w.terminated(ctx, cutoff); done {
		return k
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

	timer := time.NewTimer(time.Until(cutoff))
	defer timer.Stop()
	select {
	case <-w.wake:
		r.wakes.Add(1)
		if k, done := w.terminated(ctx, cutoff); done {
			return k // a ready notification does not supersede a passed cutoff or a cancel
		}
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

// terminated reports a cancelled context or a passed cutoff (and disarms the waiter).
func (w *Waiter) terminated(ctx context.Context, cutoff time.Time) (Wake, bool) {
	if ctx.Err() != nil {
		w.disarm()
		return WakeCancel, true
	}
	if !time.Now().Before(cutoff) {
		w.disarm()
		return WakeTimeout, true
	}
	return 0, false
}

func (w *Waiter) disarm() {
	w.reg.mu.Lock()
	w.armed = false
	w.reg.mu.Unlock()
}

// Ticket states (guarded by Registry.mu). A resumption is queued, then either granted (the token
// and the quota slot are taken, delivery to the caller still pending) and delivered (running), or
// cancelled; finished is terminal. The token/quota are held exactly in granted and running.
const (
	ticketQueued = iota
	ticketGranted
	ticketCancelled
	ticketFinished
	ticketRunning
)

// ticket is a queued resumption.
type ticket struct {
	w       *Waiter
	permit  *jsonrpc.Permit
	ctx     context.Context
	until   time.Time
	granted chan struct{} // closed on grant or cancellation
	state   int
	cause   error // why a cancelled ticket was cancelled by the scheduler
}

// Grant is a running resumed read. It owns one slot of the resumed-read quota; the active-work
// token stays with the request's permit. Done gives the quota slot back, once: only the call that
// wins this grant's running->finished transition does anything, so a late or repeated Done of an
// old grant can never affect a later grant. Done does NOT release the permit: the response of the
// resumed request is built and written under its token, like a legacy request, and the token is
// released by the owner closing the permit when the request ends (or by the handler calling
// Permit.Release before it parks again). Call Done as soon as the read attempt finished.
type Grant struct {
	r *Registry
	t *ticket
}

// Done ends the resumed read's quota slot.
func (g *Grant) Done() {
	r := g.r
	r.mu.Lock()
	if g.t.state != ticketRunning {
		r.mu.Unlock()
		return
	}
	g.t.state = ticketFinished
	r.running--
	if w := g.t.w; w.closed { // the wait was closed while this read ran: its reservation ends with the work
		r.removeLocked(w)
	}
	r.mu.Unlock()
	r.Schedule()
}

// removeLocked drops a closed waiter's reservation.
func (r *Registry) removeLocked(w *Waiter) {
	if _, ok := r.waiters[w]; !ok {
		return
	}
	delete(r.waiters, w)
	if len(r.waiters) == 0 {
		close(r.idle)
	}
}

// Resume queues a recheck for active-work capacity and returns when it is granted, in FIFO
// order, within the resumed-read quota and leaving a token for fresh requests. The permit is
// reacquired by the scheduler. No registry, tracker or catalog lock may be held by the caller.
// A resumption whose ctx ended or whose deadline passed is never granted - checked before it is
// queued, by the scheduler, and again when the grant is handed over; it fails with ctx's error
// or ErrResumeTimeout (the handler answers with the existing busy/deadline error).
func (w *Waiter) Resume(ctx context.Context, p *jsonrpc.Permit, until time.Time) (*Grant, error) {
	r := w.reg
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !time.Now().Before(until) {
		return nil, ErrResumeTimeout
	}
	if p.Held() {
		return nil, ErrPermitHeld
	}
	r.mu.Lock()
	if w.closed {
		r.mu.Unlock()
		return nil, ErrWaiterClosed
	}
	if len(r.queue) >= r.cfg.ResumeQueueMax {
		r.mu.Unlock()
		return nil, ErrResumeQueueFull
	}
	t := &ticket{w: w, permit: p, ctx: ctx, until: until, granted: make(chan struct{})}
	w.ticket = t
	r.queue = append(r.queue, t)
	r.enqueued.Add(1)
	r.queueLen.Store(int32(len(r.queue)))
	r.mu.Unlock()
	r.Schedule()

	tm := time.NewTimer(time.Until(until))
	defer tm.Stop()
	select {
	case <-t.granted:
	case <-tm.C:
		return nil, r.abandon(t, ErrResumeTimeout)
	case <-ctx.Done():
		return nil, r.abandon(t, ctx.Err())
	}
	if h := r.testHookDeliver; h != nil {
		h()
	}
	// Hand-over: the grant counts only if it is still ours, the waiter is open and neither the
	// context nor the deadline ended in the meantime.
	r.mu.Lock()
	switch {
	case t.state == ticketGranted && !w.closed && ctx.Err() == nil && time.Now().Before(until):
		t.state = ticketRunning
		r.mu.Unlock()
		return &Grant{r: r, t: t}, nil
	case t.state == ticketGranted: // revoked before delivery: give token and quota back
		t.state = ticketFinished
		r.running--
		w.ticket = nil
		cause := ctx.Err()
		switch {
		case w.closed:
			cause = ErrWaiterClosed
		case cause == nil:
			cause = ErrResumeTimeout
		}
		r.mu.Unlock()
		p.Release()
		r.Schedule()
		return nil, cause
	default: // cancelled while queued (waiter closed, request ended, or scheduler found it stale)
		cause := t.cause
		if cause == nil {
			cause = ErrWaiterClosed
		}
		r.mu.Unlock()
		return nil, cause
	}
}

// abandon withdraws a queued ticket, or hands back a grant which raced the cancellation.
func (r *Registry) abandon(t *ticket, cause error) error {
	r.mu.Lock()
	switch t.state {
	case ticketQueued:
		t.state = ticketCancelled
		r.dequeueLocked(t)
		t.w.ticket = nil
		r.mu.Unlock()
	case ticketGranted: // granted concurrently, never delivered: hand the token and the quota back
		t.state = ticketFinished
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

func (r *Registry) dequeueLocked(t *ticket) {
	for i, q := range r.queue {
		if q == t {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			r.queueLen.Store(int32(len(r.queue)))
			return
		}
	}
}

// Schedule grants queued resumptions while the quota and the headroom allow. The server calls it
// whenever an active-work token is released (jsonrpc.Server.SetReleaseHook). Stale work - a
// finished request, a cancelled context, a passed deadline - is dropped, never granted.
func (r *Registry) Schedule() {
	if r.queueLen.Load() == 0 {
		return // nothing queued: legacy request completions do not contend on the registry lock
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() { r.queueLen.Store(int32(len(r.queue))) }()
	for len(r.queue) > 0 && r.running < r.maxRunning {
		t := r.queue[0]
		switch {
		case t.permit.Closed():
			t.cause = ErrWaiterClosed
		case t.ctx.Err() != nil:
			t.cause = t.ctx.Err()
		case !time.Now().Before(t.until):
			t.cause = ErrResumeTimeout
		}
		if t.cause != nil {
			t.state = ticketCancelled
			t.w.ticket = nil
			close(t.granted)
			r.queue = r.queue[1:]
			continue
		}
		if !t.permit.TryReacquire(1) {
			return
		}
		t.state = ticketGranted
		r.running++
		r.queue = r.queue[1:]
		close(t.granted)
	}
}

// Close releases the reservation and every other resource of the wait, exactly once. It does not
// touch the HTTP permit (its owner closes it). A queued resumption is cancelled and a granted but
// undelivered one is revoked (its Resume fails). A resumed read which is already running keeps
// its token, quota slot and the reservation (and Join keeps waiting) until its Grant.Done: its
// proof work is not stopped by closing.
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
	keep := false
	if t := w.ticket; t != nil {
		switch t.state {
		case ticketQueued:
			t.state = ticketCancelled
			t.cause = ErrWaiterClosed
			close(t.granted) // a Resume blocked on it returns ErrWaiterClosed
			r.dequeueLocked(t)
		case ticketGranted:
			// revoke: the grant was not delivered. Resume observes the closed waiter and fails;
			// the token and the quota slot go back here exactly once.
			t.state = ticketFinished
			r.running--
			release = t.permit
		case ticketRunning:
			keep = true
		}
		if !keep {
			w.ticket = nil
		}
	}
	if !keep {
		r.removeLocked(w)
	}
	r.mu.Unlock()
	if release != nil {
		release.Release()
	}
	r.Schedule()
}
