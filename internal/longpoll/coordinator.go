package longpoll

import (
	"context"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Budget constants of revision 3 section 8. The request deadline includes the work before the
// wait; after the wait the server still needs: resume admission (up to ResumeReserve), one final
// read (up to FinalReadReserve) and writing the response (WriteReserve).
const (
	ResumeReserve    = 2 * time.Second
	FinalReadReserve = 5 * time.Second
	WriteReserve     = time.Second
	// TotalReserve is subtracted from the remaining request budget before any wait is granted.
	TotalReserve = ResumeReserve + FinalReadReserve + WriteReserve
)

// Result is one read attempt of a long-pollable method.
type Result struct {
	// Pending: the read is not final and waiting may help (every non-included state is pending).
	Pending bool
	// Value is the success value (may be nil when Err is set). Err is a handled error.
	Value interface{}
	Err   *jsonrpc.Error
}

// Eval performs one complete, normal read attempt: it must release every lease, slot and buffer
// it took before it returns (a parked wait retains no snapshot, record or partial response).
type Eval func(ctx context.Context) Result

// Coordinator runs long-poll waits on top of a Registry.
type Coordinator struct {
	Reg *Registry
	// MaxWait is LONG_POLL_MAX_WAIT.
	MaxWait time.Duration
	// Budget is the application deadline of a request (the JSON-RPC method budget), counted from
	// the moment the request was admitted; it bounds waits even with an unlimited transport timeout.
	Budget time.Duration
	// Reserve and ResumeReserve override TotalReserve and ResumeReserve (tests); zero uses them.
	Reserve, ResumeReserveOverride time.Duration
}

func (c *Coordinator) reserve() time.Duration {
	if c.Reserve > 0 {
		return c.Reserve
	}
	return TotalReserve
}

func (c *Coordinator) resumeReserve() time.Duration {
	if c.ResumeReserveOverride > 0 {
		return c.ResumeReserveOverride
	}
	return ResumeReserve
}

func busyDeadline() *jsonrpc.Error {
	return jsonrpc.NewDetailedError(jsonrpc.ConcurrencyLimitCode, "Concurrency limit exceeded", "busy", "deadline", true, nil)
}

type transportKey struct{}

// WithTransportDeadline records when the transport will cut the response (the HTTP write
// deadline, counted from the start of the request). The wait budget never extends it.
func WithTransportDeadline(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, transportKey{}, t)
}

// requestDeadline is the earliest of the application budget (counted from admission), the
// context deadline (the method middleware) and the transport deadline; ok=false if none is known.
func (c *Coordinator) requestDeadline(ctx context.Context, p *jsonrpc.Permit, now time.Time) (time.Time, bool) {
	var deadline time.Time
	consider := func(t time.Time) {
		if deadline.IsZero() || t.Before(deadline) {
			deadline = t
		}
	}
	if c.Budget > 0 {
		if p != nil {
			consider(p.Created().Add(c.Budget))
		} else {
			consider(now.Add(c.Budget))
		}
	}
	if d, ok := ctx.Deadline(); ok {
		consider(d)
	}
	if t, ok := ctx.Value(transportKey{}).(time.Time); ok {
		consider(t)
	}
	return deadline, !deadline.IsZero()
}

// effectiveWait clamps the requested wait to the configured maximum and to the remaining request
// budget minus the post-wait reserve; zero means "do not park".
func (c *Coordinator) effectiveWait(ctx context.Context, p *jsonrpc.Permit, requested uint64, now time.Time) time.Duration {
	if requested == 0 || c.MaxWait <= 0 {
		return 0
	}
	// convert after clamping: never overflow a time.Duration with a huge requested value
	wait := c.MaxWait
	if requested < uint64(c.MaxWait/time.Millisecond) {
		wait = time.Duration(requested) * time.Millisecond
	}
	if deadline, ok := c.requestDeadline(ctx, p, now); ok {
		if rem := deadline.Sub(now) - c.reserve(); rem < wait {
			wait = rem
		}
	}
	if wait < 0 {
		return 0
	}
	return wait
}

// Run serves a request which carried waitMs=requested. It reads once (a pending result is the
// only reason to wait), parks while every non-included state is pending, rechecks on wake-ups
// with the original absolute cutoff, and returns the final result with the cumulative wait.
// A zero wait, no remaining budget, zero/full capacity, a drain, or a request without an owned
// permit return the ordinary immediate read with a zero wait. A timeout is a final normal read.
//
// The active-work token is released only while parked and held again (resume queue) for every
// re-read; the one held when Run returns stays with the request until its response is written.
func (c *Coordinator) Run(ctx context.Context, method string, requested uint64, eval Eval) (Result, uint64) {
	return c.RunClass(ctx, method, requested, nil, eval)
}

type resourcesKey struct{}

// WithResources attaches the capacity granted to a resumed read to its context (RunClass does it
// for the re-read; methods read it with ResourcesFromContext).
func WithResources(ctx context.Context, r Resources) context.Context {
	return context.WithValue(ctx, resourcesKey{}, r)
}

// ResourcesFromContext returns the method capacity the scheduler granted to this resumed read
// (nil for a first read or a class-less wait). The read consumes it instead of acquiring again.
func ResourcesFromContext(ctx context.Context) Resources {
	r, _ := ctx.Value(resourcesKey{}).(Resources)
	return r
}

// RunClass is Run for a class of reads whose resumptions are admitted together with their method
// capacity (see Registry.NewClass): the re-read receives the admission through its context.
func (c *Coordinator) RunClass(ctx context.Context, method string, requested uint64, class *Class, eval Eval) (Result, uint64) {
	p := jsonrpc.PermitFromContext(ctx)
	start := time.Now()
	wait := c.effectiveWait(ctx, p, requested, start)

	immediate := func(outcome string) (Result, uint64) {
		metrics.LongPollRequestsTotal.WithLabelValues(method, outcome).Inc()
		return eval(ctx), 0
	}
	switch {
	case requested == 0:
		return immediate("immediate_zero")
	case wait <= 0 || p == nil || !p.Owned() || c.Reg == nil:
		return immediate("immediate_no_budget")
	}
	w, adm := c.Reg.Admit()
	switch adm {
	case ShedCapacity:
		return immediate("shed_capacity")
	case ShedDraining:
		return immediate("shed_draining")
	}
	defer w.Close()

	cutoff := start.Add(wait)
	hard, hasHard := c.requestDeadline(ctx, p, start)
	postResume := c.reserve() - c.resumeReserve() // final read + response write after the resume
	var (
		waited  time.Duration
		grant   *Grant
		final   bool // the next read is the last one (timeout, drain)
		outcome = "completed"
		parked  bool
		woken   bool // the last wake came from an event, not from the cutoff or a drain
	)
	defer func() {
		if grant != nil {
			grant.Done()
		}
	}()
	for {
		gen := w.Generation()
		evalCtx, resumed := ctx, grant != nil
		if grant != nil {
			if r := grant.Resources(); r != nil {
				evalCtx = WithResources(ctx, r)
			}
		}
		res := eval(evalCtx)
		if resumed && res.Err != nil && res.Err.Code == jsonrpc.ConcurrencyLimitCode {
			outcome = "busy_downstream" // admitted by the scheduler but rejected by the method itself
		}
		if grant != nil {
			grant.Done()
			grant = nil
			metrics.LongPollRechecksTotal.WithLabelValues(method).Inc()
			if woken {
				if n := c.Reg.LastNotify(); !n.IsZero() {
					metrics.LongPollPublicationToResponseSeconds.WithLabelValues(method).Observe(time.Since(n).Seconds())
				}
			}
		}
		if !res.Pending || final || ctx.Err() != nil {
			if !res.Pending && !parked {
				outcome = "immediate_final"
			}
			return c.finish(method, outcome, res, waited)
		}

		// The completed read is the final one if no waiting remains (a slow first read can use
		// up the window): never release and reacquire only to turn it into an overload error.
		if !time.Now().Before(cutoff) {
			return c.finish(method, "timeout", res, waited)
		}

		// park: give the active-work token back, retain nothing but the compact wait state
		p.Release()
		parked = true
		t0 := time.Now()
		k := w.Wait(ctx, gen, cutoff)
		waited += time.Since(t0)
		p.SetWaited(ceilMs(waited))
		woken = k == WakeSignal || k == WakeChanged
		switch k {
		case WakeSignal:
			metrics.LongPollWakesTotal.WithLabelValues("signal").Inc()
		case WakeChanged:
			metrics.LongPollWakesTotal.WithLabelValues("changed").Inc()
		case WakeTimeout:
			metrics.LongPollWakesTotal.WithLabelValues("timeout").Inc()
			final, outcome = true, "timeout"
		case WakeDrain:
			metrics.LongPollWakesTotal.WithLabelValues("drain").Inc()
			final, outcome = true, "drained"
		case WakeCancel:
			metrics.LongPollWakesTotal.WithLabelValues("cancel").Inc()
			return c.finish(method, "cancelled", Result{Err: jsonrpc.NewError(jsonrpc.InternalErrorCode, "Request cancelled", nil)}, waited)
		}
		// every resumption, also an early one, queues for at most the resume reserve and never
		// past the point that leaves the final read and the write their time
		qStart := time.Now()
		resumeUntil := qStart.Add(c.resumeReserve())
		if hasHard && resumeUntil.After(hard.Add(-postResume)) {
			resumeUntil = hard.Add(-postResume)
		}
		g, err := w.ResumeClass(ctx, p, resumeUntil, class)
		metrics.LongPollResumeQueueSeconds.Observe(time.Since(qStart).Seconds())
		if err != nil {
			if ctx.Err() != nil {
				return c.finish(method, "cancelled", Result{Err: jsonrpc.NewError(jsonrpc.InternalErrorCode, "Request cancelled", nil)}, waited)
			}
			// Overloaded: the existing retryable busy/deadline error, not a new long-poll error.
			return c.finish(method, "busy_admission_deadline", Result{Err: busyDeadline()}, waited)
		}
		grant = g
	}
}

func (c *Coordinator) finish(method, outcome string, res Result, waited time.Duration) (Result, uint64) {
	ms := ceilMs(waited)
	metrics.LongPollRequestsTotal.WithLabelValues(method, outcome).Inc()
	if outcome != "immediate_final" {
		metrics.LongPollWaitSeconds.WithLabelValues(method).Observe(waited.Seconds())
	}
	return res, ms
}

func ceilMs(d time.Duration) uint64 { return uint64((d + time.Millisecond - 1) / time.Millisecond) }
