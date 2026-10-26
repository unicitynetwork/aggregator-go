package longpoll

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// rig serves one method behind a real jsonrpc.Server (permit, ownership predicate, release hook)
// whose handler runs the coordinator.
type rig struct {
	srv   *jsonrpc.Server
	reg   *Registry
	co    *Coordinator
	evals atomic.Int32
	// ready flips a read from pending to included
	ready atomic.Bool
}

func newRig(t *testing.T, waiters, c int, mutate func(*Coordinator)) *rig {
	t.Helper()
	srv := newServer(t, c)
	reg := New(Config{MaxWaiters: waiters, Concurrency: c})
	srv.SetReleaseHook(reg.Schedule)
	srv.SetOwnershipPredicate("m", func(p []byte) bool { return strings.Contains(string(p), "wait") })
	co := &Coordinator{Reg: reg, MaxWait: 5 * time.Second, Budget: 30 * time.Second, Reserve: 100 * time.Millisecond, ResumeReserveOverride: 200 * time.Millisecond}
	if mutate != nil {
		mutate(co)
	}
	return &rig{srv: srv, reg: reg, co: co}
}

type call struct {
	waited uint64
	res    Result
	held   bool // the request's token when Run returned
	active int  // active requests while parked (sampled by the eval of the first call)
}

// serve registers the handler: wait is the requested waitMs; eval is pending until ready.
func (r *rig) handler(wait uint64, out chan<- call, eval Eval) {
	if eval == nil {
		eval = func(context.Context) Result {
			r.evals.Add(1)
			if r.ready.Load() {
				return Result{Value: "included"}
			}
			return Result{Pending: true, Value: "pending"}
		}
	}
	r.srv.RegisterMethod("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *jsonrpc.Error) {
		res, waited := r.co.Run(ctx, "test", wait, eval)
		out <- call{waited: waited, res: res, held: jsonrpc.PermitFromContext(ctx).Held()}
		return "done", nil
	})
}

func (r *rig) post(ctx context.Context, params string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m","params":`+params+`}`)).WithContext(ctx)
	r.srv.ServeHTTP(rec, req)
	return rec
}

func TestRun_ImmediateModes(t *testing.T) {
	for name, tc := range map[string]struct {
		waiters, c int
		wait       uint64
		params     string
		mutate     func(*Coordinator)
		drain      bool
	}{
		"waitMs zero":          {8, 8, 0, `{"waitMs":0}`, nil, false},
		"cap zero":             {0, 8, 1000, `{"waitMs":1000}`, nil, false},
		"C=1 immediate mode":   {8, 1, 1000, `{"waitMs":1000}`, nil, false},
		"no budget":            {8, 8, 1000, `{"waitMs":1000}`, func(c *Coordinator) { c.Budget = 50 * time.Millisecond }, false},
		"max wait zero":        {8, 8, 1000, `{"waitMs":1000}`, func(c *Coordinator) { c.MaxWait = 0 }, false},
		"draining":             {8, 8, 1000, `{"waitMs":1000}`, nil, true},
		"not an opt-in params": {8, 8, 1000, `{}`, nil, false}, // predicate false: the permit is not owned
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, tc.waiters, tc.c, tc.mutate)
			if tc.drain {
				r.reg.Drain(true)
			}
			out := make(chan call, 1)
			r.handler(tc.wait, out, nil)
			start := time.Now()
			r.post(context.Background(), tc.params)
			c := <-out
			require.Less(t, time.Since(start), 500*time.Millisecond, "served at once")
			require.Zero(t, c.waited)
			require.True(t, c.res.Pending, "the ordinary immediate read is returned as is")
			require.EqualValues(t, 1, r.evals.Load())
			require.Zero(t, r.reg.Stats().Reserved)
			require.Zero(t, r.srv.ActiveRequests())
		})
	}
}

func TestRun_NonPendingFirstReadNeverParks(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	r.ready.Store(true)
	out := make(chan call, 1)
	r.handler(5000, out, nil)
	r.post(context.Background(), `{"waitMs":5000}`)
	c := <-out
	require.Equal(t, "included", c.res.Value)
	require.Zero(t, c.waited)
	require.True(t, c.held)
	require.EqualValues(t, 1, r.evals.Load())
}

func TestRun_ParksReleasesTokenAndCompletesOnNotify(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	out := make(chan call, 1)
	var activeWhileParked atomic.Int32
	activeWhileParked.Store(-1)
	r.handler(5000, out, func(context.Context) Result {
		r.evals.Add(1)
		if r.ready.Load() {
			return Result{Value: "included"}
		}
		return Result{Pending: true}
	})
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":5000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 && r.srv.ActiveRequests() == 0 }, 2*time.Second, time.Millisecond,
		"while parked the request holds a reservation but no active-work token")
	activeWhileParked.Store(int32(r.srv.ActiveRequests()))
	time.Sleep(30 * time.Millisecond)
	r.ready.Store(true)
	r.reg.Notify()
	<-done
	c := <-out
	require.Equal(t, "included", c.res.Value)
	require.GreaterOrEqual(t, c.waited, uint64(30), "waitedMs is the cumulative wait, rounded up")
	require.Less(t, c.waited, uint64(2000))
	require.True(t, c.held, "the token is back for the final read and the response")
	require.EqualValues(t, 2, r.evals.Load())
	require.Zero(t, r.reg.Stats().Reserved)
	require.Zero(t, r.srv.ActiveRequests(), "released after the response")
	require.Zero(t, r.reg.Stats().Running)
}

func TestRun_TimeoutIsAFinalNormalReadAndUnrelatedEventsDoNotExtendIt(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	out := make(chan call, 1)
	r.handler(250, out, nil)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				r.reg.Notify() // unrelated: the read stays pending
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	start := time.Now()
	r.post(context.Background(), `{"waitMs":250}`)
	close(stop)
	c := <-out
	el := time.Since(start)
	require.GreaterOrEqual(t, el, 240*time.Millisecond)
	require.Less(t, el, 900*time.Millisecond, "the cutoff is absolute")
	require.True(t, c.res.Pending, "a timeout returns the ordinary pending outcome, not a new error")
	require.InDelta(t, 250, float64(c.waited), 80)
	require.Greater(t, r.evals.Load(), int32(2), "rechecks happened")
	require.Zero(t, r.srv.ActiveRequests())
	require.Zero(t, r.reg.Stats().Reserved)
}

func TestRun_CancelWhileParkedCleansUp(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	out := make(chan call, 1)
	r.handler(5000, out, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.post(ctx, `{"waitMs":5000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	cancel()
	<-done
	c := <-out
	require.NotNil(t, c.res.Err, "a disconnected client needs no response; the handler just unwinds")
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 0 && r.srv.ActiveRequests() == 0 }, time.Second, time.Millisecond)
}

func TestRun_DrainEndsTheWaitWithAFinalRead(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	out := make(chan call, 1)
	r.handler(5000, out, nil)
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":5000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	r.reg.Drain(false)
	<-done
	c := <-out
	require.True(t, c.res.Pending, "drain returns the current ordinary outcome")
	require.EqualValues(t, 2, r.evals.Load())
	r.reg.Reopen()
	require.Zero(t, r.reg.Stats().Reserved)
}

// Cap 1: the second request is shed (immediate, zero echo), not queued or failed.
func TestRun_CapFullShedsImmediately(t *testing.T) {
	r := newRig(t, 1, 8, nil)
	out := make(chan call, 2)
	r.handler(5000, out, nil)
	go r.post(context.Background(), `{"waitMs":5000}`)
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	start := time.Now()
	r.post(context.Background(), `{"waitMs":5000}`)
	c := <-out
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Zero(t, c.waited)
	require.True(t, c.res.Pending)
	require.EqualValues(t, 1, r.reg.Stats().ShedCapacity)
	r.ready.Store(true)
	r.reg.Notify()
	<-out
}

// C=2: a parked wait leaves both tokens to fresh requests; the headroom token is never taken by a
// resumed read while a fresh request holds the other one.
func TestRun_SmallCKeepsHeadroomForFreshRequests(t *testing.T) {
	r := newRig(t, 8, 2, nil)
	out := make(chan call, 1)
	r.handler(5000, out, nil)
	r.srv.RegisterMethod("fresh", func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) { return "ok", nil })
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":5000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 && r.srv.ActiveRequests() == 0 }, 2*time.Second, time.Millisecond)
	for i := 0; i < 5; i++ { // two fresh requests fit while one wait is parked
		rec := httptest.NewRecorder()
		r.srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"fresh"}`)))
		require.Contains(t, rec.Body.String(), `"result":"ok"`)
	}
	hold1, _ := r.srv.AcquirePermit() // a fresh request occupies one of the two tokens
	r.ready.Store(true)
	r.reg.Notify()
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 1, r.srv.ActiveRequests(), "the resumed read waits instead of taking the last token")
	hold1.Close() // the release hook schedules the resumption
	<-done
	c := <-out
	require.Equal(t, "included", c.res.Value)
}

// A resumption which cannot get capacity before its deadline fails with the existing retryable
// busy/deadline error (the handler adds the echo), never blocks.
func TestRun_ResumeQueueTimeoutIsBusyDeadline(t *testing.T) {
	r := newRig(t, 8, 4, nil) // quota 1, headroom 1
	out := make(chan call, 1)
	r.handler(150, out, nil)
	hold := make([]*jsonrpc.Permit, 0, 3)
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":150}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	for i := 0; i < 3; i++ { // fresh load fills every token the resumption may take
		p, ok := r.srv.AcquirePermit()
		require.True(t, ok)
		hold = append(hold, p)
	}
	<-done
	c := <-out
	require.NotNil(t, c.res.Err)
	require.Equal(t, jsonrpc.ConcurrencyLimitCode, c.res.Err.Code)
	data, _ := json.Marshal(c.res.Err)
	require.Contains(t, string(data), `"reason":"busy"`)
	require.Contains(t, string(data), `"detail":"deadline"`)
	for _, p := range hold {
		p.Close()
	}
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 0 }, time.Second, time.Millisecond)
}

func TestEffectiveWait_ClampsWithoutOverflow(t *testing.T) {
	co := &Coordinator{MaxWait: 20 * time.Second, Budget: 30 * time.Second}
	now := time.Now()
	ctx := context.Background()
	require.Equal(t, time.Duration(0), co.effectiveWait(ctx, nil, 0, now))
	require.Equal(t, 5*time.Second, co.effectiveWait(ctx, nil, 5000, now))
	require.Equal(t, 20*time.Second, co.effectiveWait(ctx, nil, 9007199254740991, now), "2^53-1 ms clamps before any duration conversion")
	require.Equal(t, 20*time.Second, co.effectiveWait(ctx, nil, 1<<62, now))
	// remaining budget minus the 8s reserve bounds it: 15s left -> 7s
	short, cancel := context.WithDeadline(ctx, now.Add(15*time.Second))
	defer cancel()
	require.Equal(t, 7*time.Second, co.effectiveWait(short, nil, 20000, now).Round(100*time.Millisecond))
	// nothing remains after the reserve: do not park
	tiny, cancel2 := context.WithDeadline(ctx, now.Add(8*time.Second))
	defer cancel2()
	require.Zero(t, co.effectiveWait(tiny, nil, 20000, now))
}

// The ownership of a permit across the timeout middleware: a wait whose worker outlives the
// middleware deadline keeps the token until the worker stops, and a worker parked without a token
// cannot take one afterwards.
func TestRun_TimeoutMiddlewareWorkerKeepsTheTokenUntilItStops(t *testing.T) {
	r := newRig(t, 8, 4, nil)
	lg, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	r.srv.AddMiddleware(jsonrpc.TimeoutMiddleware(150*time.Millisecond, lg))
	release := make(chan struct{})
	var ran atomic.Bool
	r.srv.RegisterMethod("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *jsonrpc.Error) {
		res, _ := r.co.Run(ctx, "test", 0+1000, func(context.Context) Result {
			ran.Store(true)
			<-release // proof work that outlives the response
			return Result{Value: "late"}
		})
		_ = res
		return "late", nil
	})
	rec := r.post(context.Background(), `{"waitMs":1000}`)
	require.Contains(t, rec.Body.String(), "Request timeout")
	require.True(t, ran.Load())
	require.Equal(t, 1, r.srv.ActiveRequests(), "proof work still runs: the token stays accounted")
	close(release)
	require.Eventually(t, func() bool { return r.srv.ActiveRequests() == 0 && r.reg.Stats().Reserved == 0 }, time.Second, time.Millisecond)
}

// The transport's write deadline, counted from the real start of the request, bounds the wait
// together with the application budget: after a slow request body too little time remains.
func TestRun_TransportDeadlineBoundsTheWait(t *testing.T) {
	co := &Coordinator{MaxWait: 20 * time.Second, Budget: 30 * time.Second}
	now := time.Now()
	// WriteTimeout 9s, 1.2s already spent: 7.8s left < the 8s reserve -> do not park
	ctx := WithTransportDeadline(t.Context(), now.Add(7800*time.Millisecond))
	require.Zero(t, co.effectiveWait(ctx, nil, 500, now))
	// plenty of transport time: the other bounds apply
	ctx = WithTransportDeadline(t.Context(), now.Add(40*time.Second))
	require.Equal(t, 500*time.Millisecond, co.effectiveWait(ctx, nil, 500, now))
	// the earliest of transport / middleware / application deadline wins
	ctx = WithTransportDeadline(t.Context(), now.Add(12*time.Second))
	short, cancel := context.WithDeadline(ctx, now.Add(20*time.Second))
	defer cancel()
	require.Equal(t, 4*time.Second, co.effectiveWait(short, nil, 20000, now).Round(10*time.Millisecond))
}

// A first read which used up the window (and the resume reserve) is the final read: the completed
// pending result is returned, the token is not released and no overload error is made up.
func TestRun_SlowFirstReadIsTheFinalReadNotAnOverload(t *testing.T) {
	r := newRig(t, 8, 8, nil)
	out := make(chan call, 1)
	r.handler(1, out, func(context.Context) Result {
		r.evals.Add(1)
		time.Sleep(80 * time.Millisecond) // longer than wait (1ms) + resume reserve (200ms test value is smaller than needed? use more)
		return Result{Pending: true, Value: "pending"}
	})
	r.co.ResumeReserveOverride = 20 * time.Millisecond
	r.post(context.Background(), `{"waitMs":1}`)
	c := <-out
	require.Nil(t, c.res.Err, "no busy/deadline error")
	require.True(t, c.res.Pending)
	require.True(t, c.held, "the token was never given up")
	require.EqualValues(t, 1, r.evals.Load())
	require.Zero(t, r.reg.Stats().Running)
}

// An early wake queues for its re-read until its own wait deadline plus the resume reserve, not
// for the fixed reserve from the wake.
func TestRun_EarlyWakeResumeQueuesUntilTheWaitDeadline(t *testing.T) {
	r := newRig(t, 8, 4, func(c *Coordinator) { c.ResumeReserveOverride = 50 * time.Millisecond })
	out := make(chan call, 1)
	r.handler(600, out, nil)
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":600}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 && r.srv.ActiveRequests() == 0 }, 2*time.Second, time.Millisecond)
	var hold []*jsonrpc.Permit
	for i := 0; i < 3; i++ { // saturate: the resumption cannot be granted
		p, ok := r.srv.AcquirePermit()
		require.True(t, ok)
		hold = append(hold, p)
	}
	start := time.Now()
	r.reg.Notify() // an immediate notification, 600ms before the cutoff
	<-done
	require.GreaterOrEqual(t, time.Since(start), 550*time.Millisecond, "queued past the fixed 50ms reserve, until cutoff + reserve")
	require.Less(t, time.Since(start), 1500*time.Millisecond)
	c := <-out
	require.NotNil(t, c.res.Err)
	require.Equal(t, jsonrpc.ConcurrencyLimitCode, c.res.Err.Code)
	for _, p := range hold {
		p.Close()
	}
}

// A woken waiter whose re-read slot frees up later than the resume reserve is still served.
func TestRun_EarlyWakeIsServedWhenCapacityFreesLate(t *testing.T) {
	r := newRig(t, 8, 4, func(c *Coordinator) { c.ResumeReserveOverride = 50 * time.Millisecond })
	out := make(chan call, 1)
	r.handler(2000, out, nil)
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":2000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 && r.srv.ActiveRequests() == 0 }, 2*time.Second, time.Millisecond)
	var hold []*jsonrpc.Permit
	for i := 0; i < 3; i++ {
		p, ok := r.srv.AcquirePermit()
		require.True(t, ok)
		hold = append(hold, p)
	}
	r.ready.Store(true)
	r.reg.Notify()
	time.Sleep(300 * time.Millisecond) // six times the resume reserve
	for _, p := range hold {
		p.Close()
	}
	<-done
	c := <-out
	require.Nil(t, c.res.Err)
	require.False(t, c.res.Pending)
}

// Drain(false) gives every waiter its ordinary final read even when the queue is longer than
// the resume reserve.
func TestRun_DrainGivesEveryWaiterItsFinalRead(t *testing.T) {
	r := newRig(t, 8, 4, func(c *Coordinator) { c.ResumeReserveOverride = 50 * time.Millisecond })
	out := make(chan call, 1)
	r.handler(5000, out, nil)
	done := make(chan struct{})
	go func() { r.post(context.Background(), `{"waitMs":5000}`); close(done) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 && r.srv.ActiveRequests() == 0 }, 2*time.Second, time.Millisecond)
	var hold []*jsonrpc.Permit
	for i := 0; i < 3; i++ {
		p, ok := r.srv.AcquirePermit()
		require.True(t, ok)
		hold = append(hold, p)
	}
	r.reg.Drain(false)
	time.Sleep(300 * time.Millisecond)
	for _, p := range hold {
		p.Close()
	}
	<-done
	c := <-out
	require.Nil(t, c.res.Err, "no busy answer for a drained waiter")
	require.True(t, c.res.Pending)
}
