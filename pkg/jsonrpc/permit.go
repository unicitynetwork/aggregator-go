package jsonrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// slotPool is the non-blocking active-work limit of the server: CONCURRENCY_LIMIT tokens, one per
// HTTP request. It replaces a plain channel semaphore so that a token can be taken conditionally
// (leaving headroom for fresh requests) and so that a release can notify the long-poll resume
// scheduler.
type slotPool struct {
	mu        sync.Mutex
	limit     int
	active    int
	onRelease func()
}

// tryAcquire takes a token unless that would leave fewer than reserve tokens free.
func (p *slotPool) tryAcquire(reserve int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active >= p.limit-reserve {
		return false
	}
	p.active++
	return true
}

func (p *slotPool) release() {
	p.mu.Lock()
	p.active--
	hook := p.onRelease
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
}

// Permit is the owned, idempotent HTTP concurrency permit of one request (C07). It is held when
// created by the server. A long-poll wait releases it while parked (Release), asks to get it back
// through the resume scheduler (TryReacquire), and the server closes it when the request ends.
//
// Rules, each covered by tests:
//   - Release, Close and TryReacquire are idempotent and only ever move this permit's own token;
//     a permit can never release a token it does not hold, so no other request's token is stolen
//     and nothing is released twice.
//   - After Close the permit never holds a token again: a worker that outlives the response
//     (TimeoutMiddleware returns before its goroutine stops) cannot reacquire.
//   - While workers registered with AddWorker are still running, Close does not release a held
//     token; the last worker's Done does. Active-work accounting is therefore never released
//     before proof work actually stops.
type Permit struct {
	pool    *slotPool
	created time.Time // when the request was admitted: the start of its application deadline

	mu      sync.Mutex
	held    bool
	closed  bool
	owned   bool
	workers int

	waitedMs atomic.Uint64 // cumulative long-poll wait so far (the echo of a response the middleware writes)
}

// Owned reports whether the request opted in to permit ownership (a long-poll wait): only then
// does the permit outlive a response sent by the timeout middleware until its worker stops.
// Requests which did not opt in keep the legacy accounting exactly.
func (p *Permit) Owned() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.owned
}

// SetWaited records the cumulative wait of the request so far (milliseconds, rounded up). The
// timeout middleware echoes it in the error it writes for an opt-in request whose handler is
// still running.
func (p *Permit) SetWaited(ms uint64) { p.waitedMs.Store(ms) }

// Waited is the last recorded wait.
func (p *Permit) Waited() uint64 { return p.waitedMs.Load() }

// Created is when the request was admitted.
func (p *Permit) Created() time.Time { return p.created }

// Held reports whether the permit currently owns a token.
func (p *Permit) Held() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.held
}

// Closed reports whether the request has ended.
func (p *Permit) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Release gives the token back while the request parks. It reports whether a token was released.
// It must only be called when no proof work of this request is running.
func (p *Permit) Release() bool {
	p.mu.Lock()
	if !p.held {
		p.mu.Unlock()
		return false
	}
	p.held = false
	p.mu.Unlock()
	p.pool.release()
	return true
}

// TryReacquire takes a token again without blocking, leaving at least reserve tokens free for
// fresh requests. It fails on a closed permit, a permit which already holds its token, or a full
// pool.
func (p *Permit) TryReacquire(reserve int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.held {
		return false
	}
	if !p.pool.tryAcquire(reserve) {
		return false
	}
	p.held = true
	return true
}

// AddWorker registers a goroutine which uses the permit's token (proof work). Every AddWorker
// needs exactly one WorkerDone. It reports false on a closed permit.
func (p *Permit) AddWorker() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.workers++
	return true
}

// WorkerDone ends a worker. If the request already ended while the worker ran, the held token is
// released now.
func (p *Permit) WorkerDone() {
	p.mu.Lock()
	p.workers--
	release := p.closed && p.workers == 0 && p.held
	if release {
		p.held = false
	}
	p.mu.Unlock()
	if release {
		p.pool.release()
	}
}

// Close ends the request. A held token is released unless workers still run.
func (p *Permit) Close() {
	p.mu.Lock()
	p.closed = true
	release := p.workers == 0 && p.held
	if release {
		p.held = false
	}
	p.mu.Unlock()
	if release {
		p.pool.release()
	}
}

type permitKey struct{}

// PermitFromContext returns the request's permit; nil outside a JSON-RPC request.
func PermitFromContext(ctx context.Context) *Permit {
	p, _ := ctx.Value(permitKey{}).(*Permit)
	return p
}

// ContextWithPermit attaches a permit (tests and the server).
func ContextWithPermit(ctx context.Context, p *Permit) context.Context {
	return context.WithValue(ctx, permitKey{}, p)
}

// ConcurrencyLimit is the configured number of active HTTP/RPC tokens.
func (s *Server) ConcurrencyLimit() int { return s.pool.limit }

// ActiveRequests is the number of tokens in use.
func (s *Server) ActiveRequests() int {
	s.pool.mu.Lock()
	defer s.pool.mu.Unlock()
	return s.pool.active
}

// SetReleaseHook registers a function called after every token release, outside all locks of the
// pool. The long-poll resume scheduler uses it to grant queued resumptions. Call before serving.
func (s *Server) SetReleaseHook(f func()) {
	s.pool.mu.Lock()
	s.pool.onRelease = f
	s.pool.mu.Unlock()
}

// newPermit takes a token for a fresh request, or reports false when the server is saturated.
func (s *Server) newPermit() (*Permit, bool) {
	if !s.pool.tryAcquire(0) {
		return nil, false
	}
	return &Permit{pool: s.pool, held: true, created: time.Now()}, true
}

// OwnershipPredicate decides from the raw params whether a request of a method takes part in
// permit ownership. It must be cheap and must not fail.
type OwnershipPredicate func(params []byte) bool

// SetOwnershipPredicate registers the opt-in test of a method (the long-poll methods register
// "waitMs present"). Methods without one never opt in.
func (s *Server) SetOwnershipPredicate(method string, f OwnershipPredicate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.ownership == nil {
		s.ownership = map[string]OwnershipPredicate{}
	}
	s.ownership[method] = f
}

func (s *Server) markOwned(permit *Permit, req *Request) {
	s.mutex.RLock()
	f := s.ownership[req.Method]
	s.mutex.RUnlock()
	if f != nil && f(req.Params) {
		permit.mu.Lock()
		permit.owned = true
		permit.mu.Unlock()
	}
}

// AcquirePermit takes an active-work token outside an HTTP request (tests and embedders), or
// reports false when the server is saturated.
func (s *Server) AcquirePermit() (*Permit, bool) { return s.newPermit() }

// SetOptInLimits bounds the requests of an otherwise unlimited method which opt in (the method's
// ownership predicate holds): their complete request, params and ID are limited before the
// handler runs. Legacy requests of the method are not affected.
func (s *Server) SetOptInLimits(method string, l MethodLimits) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.optIn == nil {
		s.optIn = map[string]MethodLimits{}
	}
	s.optIn[method] = l
}

func (s *Server) optInLimitsFor(req *Request) (MethodLimits, bool) {
	s.mutex.RLock()
	l, hasLimits := s.optIn[req.Method]
	f := s.ownership[req.Method]
	s.mutex.RUnlock()
	if !hasLimits || f == nil || !f(req.Params) {
		return MethodLimits{}, false
	}
	return l, true
}

type workerCountKey struct{}

// ActiveWorkers is the number of method workers (TimeoutMiddleware goroutines of requests served
// by this server) which have not stopped yet - including workers whose response was already
// written. Shutdown joins them before dependencies close.
func (s *Server) ActiveWorkers() int { return int(s.workers.Load()) }
