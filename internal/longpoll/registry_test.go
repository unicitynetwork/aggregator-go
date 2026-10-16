package longpoll

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func newServer(t *testing.T, c int) *jsonrpc.Server {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	return jsonrpc.NewServer(log, c)
}

// newPair is a registry wired to a server of the same concurrency, as in production.
func newPair(t *testing.T, waiters, c int) (*Registry, *jsonrpc.Server) {
	t.Helper()
	srv := newServer(t, c)
	reg := New(Config{MaxWaiters: waiters, Concurrency: c})
	srv.SetReleaseHook(reg.Schedule)
	return reg, srv
}

func TestAdmit_CapacityZeroOneFullAndSmallC(t *testing.T) {
	for _, tc := range []struct {
		name            string
		waiters, c, cap int
	}{
		{"cap0", 0, 100, 0},
		{"C1-immediate-mode", 10, 1, 0},
		{"C0", 10, 0, 0},
		{"C2", 10, 2, 10},
		{"cap1", 1, 100, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := New(Config{MaxWaiters: tc.waiters, Concurrency: tc.c})
			require.Equal(t, tc.cap, r.Capacity())
			var ws []*Waiter
			for i := 0; i < tc.cap; i++ {
				w, a := r.Admit()
				require.Equal(t, Admitted, a)
				ws = append(ws, w)
			}
			w, a := r.Admit()
			require.Nil(t, w)
			require.Equal(t, ShedCapacity, a, "full is a shed, never an error")
			require.Equal(t, tc.cap, r.Stats().Reserved)
			for _, w := range ws {
				w.Close()
				w.Close()
			}
			require.Zero(t, r.Stats().Reserved)
			if tc.cap > 0 {
				_, a = r.Admit()
				require.Equal(t, Admitted, a, "reservation released on close")
			}
		})
	}
}

func TestRunningQuota(t *testing.T) {
	for c, want := range map[int]int{2: 1, 3: 1, 4: 1, 7: 1, 8: 2, 100: 25, 1000: 250} {
		require.Equal(t, want, New(Config{MaxWaiters: 1, Concurrency: c}).MaxRunning(), c)
	}
}

// --- wake races (T4) ---

func TestWait_PublicationBeforeRegisterIsSeenByTheEvaluator(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	r.Notify()
	w, _ := r.Admit()
	defer w.Close()
	seen := w.Generation() // the read after the event already reflects it
	require.Equal(t, WakeTimeout, w.Wait(context.Background(), seen, time.Now().Add(20*time.Millisecond)))
}

func TestWait_PublicationBetweenGenerationReadAndArmIsNotLost(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	seen := w.Generation()
	// the event lands after the evaluator's read, before the wait is armed
	r.testHookArm = func() { r.Notify() }
	require.Equal(t, WakeChanged, w.Wait(context.Background(), seen, time.Now().Add(time.Minute)))
	r.testHookArm = nil
	// ...and the next evaluation (new generation) can wait normally
	require.Equal(t, WakeTimeout, w.Wait(context.Background(), w.Generation(), time.Now().Add(20*time.Millisecond)))
}

func TestWait_PublicationDuringEvaluationStaysDirty(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	seen := w.Generation()
	for i := 0; i < 1000; i++ { // many events while the evaluator is slow coalesce
		r.Notify()
	}
	require.Equal(t, WakeChanged, w.Wait(context.Background(), seen, time.Now().Add(time.Minute)))
	// the newest generation must not disappear: reading it now and waiting yields no spurious wake
	seen = w.Generation()
	require.Equal(t, WakeTimeout, w.Wait(context.Background(), seen, time.Now().Add(20*time.Millisecond)))
}

func TestWait_NotifyAfterArmWakes(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	seen := w.Generation()
	got := make(chan Wake, 1)
	go func() { got <- w.Wait(context.Background(), seen, time.Now().Add(time.Minute)) }()
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return w.armed
	}, time.Second, time.Millisecond)
	r.Notify()
	require.Equal(t, WakeSignal, <-got)
}

func TestWait_NotifyBetweenWakeAndRearmIsNotLost(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	for round := 0; round < 200; round++ {
		seen := w.Generation()
		got := make(chan Wake, 1)
		go func() { got <- w.Wait(context.Background(), seen, time.Now().Add(time.Minute)) }()
		// the event races the arm from another goroutine: either WakeChanged or WakeSignal, never a hang
		go r.Notify()
		select {
		case k := <-got:
			require.Contains(t, []Wake{WakeChanged, WakeSignal}, k)
		case <-time.After(5 * time.Second):
			t.Fatal("lost wake-up")
		}
	}
}

func TestWait_UnrelatedEventsNeverExtendTheCutoff(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				r.Notify()
				time.Sleep(time.Millisecond)
			}
		}
	}()
	defer close(stop)
	start := time.Now()
	cutoff := start.Add(150 * time.Millisecond)
	rechecks := 0
	for {
		seen := w.Generation()
		if k := w.Wait(context.Background(), seen, cutoff); k == WakeTimeout {
			break
		}
		rechecks++
	}
	require.Less(t, time.Since(start), 400*time.Millisecond)
	require.NotZero(t, rechecks)
}

func TestWait_CancelRemovesParkedWaiter(t *testing.T) {
	r := New(Config{MaxWaiters: 1, Concurrency: 8})
	w, _ := r.Admit()
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan Wake, 1)
	seen := w.Generation()
	go func() { got <- w.Wait(ctx, seen, time.Now().Add(time.Minute)) }()
	require.Eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return w.armed }, time.Second, time.Millisecond)
	cancel()
	require.Equal(t, WakeCancel, <-got)
	r.mu.Lock()
	require.False(t, w.armed)
	r.mu.Unlock()
	w.Close()
	require.Zero(t, r.Stats().Reserved)
	require.Equal(t, WakeCancel, w.Wait(context.Background(), r.Generation(), time.Now().Add(time.Minute)), "closed waiter does not park")
}

func TestWait_PastCutoffTimesOutWithoutParking(t *testing.T) {
	r := New(Config{MaxWaiters: 1, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	require.Equal(t, WakeTimeout, w.Wait(context.Background(), w.Generation(), time.Now().Add(-time.Second)))
	r.mu.Lock()
	require.False(t, w.armed)
	r.mu.Unlock()
}

func TestNotify_DoesNotBlockWithManyWaitersAndRunsNoWork(t *testing.T) {
	r := New(Config{MaxWaiters: 2000, Concurrency: 8})
	var ws []*Waiter
	for i := 0; i < 2000; i++ {
		w, _ := r.Admit()
		ws = append(ws, w)
		seen := w.Generation()
		go w.Wait(context.Background(), seen, time.Now().Add(time.Minute))
	}
	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		n := 0
		for w := range r.waiters {
			if w.armed {
				n++
			}
		}
		return n == 2000
	}, 5*time.Second, 5*time.Millisecond)
	done := make(chan struct{})
	go func() { r.Notify(); r.Notify(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Notify blocked")
	}
	for _, w := range ws {
		w.Close()
	}
}

// --- drains and join ---

func TestDrain_RoleChangeWakesAllAndReopens(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	w1, _ := r.Admit()
	w2, _ := r.Admit()
	got := make(chan Wake, 2)
	for _, w := range []*Waiter{w1, w2} {
		w := w
		seen := w.Generation()
		go func() { got <- w.Wait(context.Background(), seen, time.Now().Add(time.Minute)) }()
	}
	require.Eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return w1.armed && w2.armed }, time.Second, time.Millisecond)
	r.Drain(false)
	require.Equal(t, WakeDrain, <-got)
	require.Equal(t, WakeDrain, <-got)
	// a wait which registered before the drain also ends when it only now tries to wait
	require.Equal(t, WakeDrain, w1.Wait(context.Background(), r.Generation(), time.Now().Add(time.Minute)))
	_, a := r.Admit()
	require.Equal(t, ShedDraining, a)
	r.Reopen()
	w3, a := r.Admit()
	require.Equal(t, Admitted, a, "role-change drains reopen")
	require.Equal(t, WakeTimeout, w3.Wait(context.Background(), w3.Generation(), time.Now().Add(10*time.Millisecond)), "new waits are unaffected by the old drain")
	w1.Close()
	w2.Close()
	w3.Close()
}

func TestDrain_ShutdownIsPermanentAndJoinWaitsForWaiters(t *testing.T) {
	r := New(Config{MaxWaiters: 4, Concurrency: 8})
	require.NoError(t, r.Join(context.Background()), "nothing to join")
	w, _ := r.Admit()
	r.Drain(true)
	r.Reopen()
	_, a := r.Admit()
	require.Equal(t, ShedDraining, a)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Join(ctx), context.DeadlineExceeded, "a live wait blocks the join")
	joined := make(chan error, 1)
	go func() { joined <- r.Join(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	w.Close()
	require.NoError(t, <-joined)
}

// --- resume queue (T7 parts) ---

func holder(t *testing.T, srv *jsonrpc.Server) *jsonrpc.Permit {
	t.Helper()
	p, ok := srv.AcquirePermit()
	require.True(t, ok)
	return p
}

func TestResume_FIFOWithinQuotaAndHeadroom(t *testing.T) {
	reg, srv := newPair(t, 10, 4) // quota 1, headroom 1
	var order []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	var grants []*Grant
	permits := make([]*jsonrpc.Permit, 5)
	ws := make([]*Waiter, 5)
	for i := range permits {
		permits[i] = holder(t, srv)
		if i == 4 {
			break
		}
		permits[i].Release() // parked
		ws[i], _ = reg.Admit()
	}
	for i := 0; i < 4; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := ws[i].Resume(context.Background(), permits[i], time.Now().Add(10*time.Second))
			require.NoError(t, err)
			mu.Lock()
			order = append(order, i)
			grants = append(grants, g)
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			g.Done()
			permits[i].Release() // the request ended: the response was written, the token goes back
		}()
		require.Eventually(t, func() bool { return reg.Stats().Enqueued > uint64(i) }, time.Second, time.Millisecond)
	}
	wg.Wait()
	require.Equal(t, []int{0, 1, 2, 3}, order, "FIFO")
	require.Zero(t, reg.Stats().Running)
	require.Zero(t, reg.Stats().Queued)
	for _, p := range permits {
		p.Close()
	}
	require.Zero(t, srv.ActiveRequests())
}

func TestResume_NeverTakesTheLastTokenFromFreshRequests(t *testing.T) {
	reg, srv := newPair(t, 4, 2)
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	fresh := holder(t, srv) // takes the second token: no headroom left for a resume
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := make(chan error, 1)
	go func() {
		g, err := w.Resume(ctx, p, time.Now().Add(time.Second))
		if err == nil {
			g.Done()
		}
		got <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, reg.Stats().Queued, "queued while fresh work holds the headroom")
	fresh.Close() // the release hook schedules the queued resume
	require.NoError(t, <-got)
	_, ok := srv.AcquirePermit()
	require.True(t, ok, "a fresh request still gets its token")
	p.Close()
	w.Close()
}

func TestResume_FreshRequestsKeepImmediateAdmission(t *testing.T) {
	reg, srv := newPair(t, 8, 2)
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	g, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
	require.NoError(t, err)
	f, ok := srv.AcquirePermit()
	require.True(t, ok, "one token stays free for fresh work while a resumed read runs")
	_, ok = srv.AcquirePermit()
	require.False(t, ok, "and saturation is the existing immediate rejection")
	f.Close()
	g.Done()
	g.Done()
	p.Close()
	w.Close()
	require.Zero(t, srv.ActiveRequests())
}

func TestResume_QueueDeadlineCancelAndFull(t *testing.T) {
	reg := New(Config{MaxWaiters: 4, Concurrency: 8, ResumeQueueMax: 1})
	srv := newServer(t, 8)
	srv.SetReleaseHook(reg.Schedule)
	// occupy the quota (2) and all tokens but the headroom
	var gs []*Grant
	var ps []*jsonrpc.Permit
	for i := 0; i < 2; i++ {
		p := holder(t, srv)
		p.Release()
		w, _ := reg.Admit()
		g, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
		require.NoError(t, err)
		gs = append(gs, g)
		ps = append(ps, p)
	}
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	_, err := w.Resume(context.Background(), p, time.Now().Add(30*time.Millisecond))
	require.ErrorIs(t, err, ErrResumeTimeout)
	require.Zero(t, reg.Stats().Queued, "timed-out ticket left the queue")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := w.Resume(ctx, p, time.Now().Add(time.Minute)); done <- err }()
	require.Eventually(t, func() bool { return reg.Stats().Queued == 1 }, time.Second, time.Millisecond)
	w2, _ := reg.Admit()
	p2 := holder(t, srv)
	p2.Release()
	_, err = w2.Resume(context.Background(), p2, time.Now().Add(time.Minute))
	require.ErrorIs(t, err, ErrResumeQueueFull, "queue is bounded")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, reg.Stats().Queued)

	for _, g := range gs {
		g.Done()
	}
	for _, p := range append(ps, p, p2) {
		p.Close()
	}
	w.Close()
	w2.Close()
	require.Zero(t, srv.ActiveRequests())
	require.Zero(t, reg.Stats().Running)
}

func TestResume_ClosedPermitInQueueAndWaiterClose(t *testing.T) {
	reg, srv := newPair(t, 4, 4) // quota 1
	p1 := holder(t, srv)
	p1.Release()
	w1, _ := reg.Admit()
	g1, err := w1.Resume(context.Background(), p1, time.Now().Add(time.Second))
	require.NoError(t, err)

	p2 := holder(t, srv)
	p2.Release()
	w2, _ := reg.Admit()
	done := make(chan error, 1)
	go func() { _, err := w2.Resume(context.Background(), p2, time.Now().Add(time.Minute)); done <- err }()
	require.Eventually(t, func() bool { return reg.Stats().Queued == 1 }, time.Second, time.Millisecond)
	p2.Close() // the request ended (client gone, middleware timeout) while queued
	g1.Done()  // frees the quota; the scheduler drops the dead ticket
	require.ErrorIs(t, <-done, ErrWaiterClosed)
	p1.Release() // p1's request ends
	require.Zero(t, srv.ActiveRequests(), "the finished resume and the dead ticket hold nothing")
	require.False(t, p2.TryReacquire(0))

	// closing a waiter whose resumed read is still running does not stop that work: token, quota
	// slot and the reservation stay accounted until the grant is done
	p3 := holder(t, srv)
	p3.Release()
	w3, _ := reg.Admit()
	g3, err := w3.Resume(context.Background(), p3, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, reg.Stats().Running)
	reserved := reg.Stats().Reserved
	w3.Close()
	require.Equal(t, 1, reg.Stats().Running)
	require.True(t, p3.Held())
	require.Equal(t, reserved, reg.Stats().Reserved, "the reservation lives until the work stops")
	g3.Done()
	require.Zero(t, reg.Stats().Running)
	require.True(t, p3.Held(), "the token goes back when the request ends, not with the quota slot")
	require.Equal(t, reserved-1, reg.Stats().Reserved)
	for _, p := range []*jsonrpc.Permit{p1, p3} {
		p.Close()
	}
	w1.Close()
	w2.Close()
	require.Zero(t, srv.ActiveRequests())
}

func TestWaiterClose_WhileResumeBlockedReturns(t *testing.T) {
	reg, srv := newPair(t, 4, 4)
	hold, _ := srv.AcquirePermit()
	_ = hold
	p1 := holder(t, srv)
	p1.Release()
	w1, _ := reg.Admit()
	g1, _ := w1.Resume(context.Background(), p1, time.Now().Add(time.Second))
	p2 := holder(t, srv)
	p2.Release()
	w2, _ := reg.Admit()
	done := make(chan error, 1)
	go func() { _, err := w2.Resume(context.Background(), p2, time.Now().Add(time.Minute)); done <- err }()
	require.Eventually(t, func() bool { return reg.Stats().Queued == 1 }, time.Second, time.Millisecond)
	w2.Close()
	require.ErrorIs(t, <-done, ErrWaiterClosed)
	g1.Done()
	for _, p := range []*jsonrpc.Permit{hold, p1, p2} {
		p.Close()
	}
	w1.Close()
	require.Zero(t, srv.ActiveRequests())
}

// Many waiters through park / notify / resume / timeout / cancel leave nothing behind.
func TestChurn_NoLeaksAndCountsReturnToBaseline(t *testing.T) {
	reg, srv := newPair(t, 64, 16)
	base := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, ok := srv.AcquirePermit()
			if !ok {
				return
			}
			defer p.Close()
			w, a := reg.Admit()
			if a != Admitted {
				return
			}
			defer w.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5+i%20)*time.Millisecond)
			defer cancel()
			cutoff := time.Now().Add(time.Duration(i%30) * time.Millisecond)
			for {
				seen := w.Generation()
				p.Release()
				k := w.Wait(ctx, seen, cutoff)
				g, err := w.Resume(context.Background(), p, time.Now().Add(2*time.Second))
				if err != nil {
					return
				}
				g.Done()
				if k == WakeTimeout || k == WakeCancel || k == WakeDrain {
					return
				}
			}
		}()
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				reg.Notify()
				time.Sleep(time.Millisecond)
			}
		}
	}()
	wg.Wait()
	close(stop)
	require.Eventually(t, func() bool {
		s := reg.Stats()
		return s.Reserved == 0 && s.Queued == 0 && s.Running == 0 && srv.ActiveRequests() == 0
	}, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= base+2 }, 2*time.Second, 10*time.Millisecond)
}

// --- architect findings on A1 ---

// An old grant's Done, called again after the same waiter/permit resumed as a newer grant, must
// not release the newer grant's token or quota slot.
func TestGrantDone_OldGrantNeverReleasesALaterGrant(t *testing.T) {
	reg, srv := newPair(t, 4, 8)
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	g1, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
	require.NoError(t, err)
	g1.Done()
	require.True(t, p.Held(), "Done gives back the quota only: the token stays with the request")
	p.Release() // parks again
	g2, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.True(t, p.Held())
	require.Equal(t, 1, reg.Stats().Running)

	g1.Done() // e.g. a deferred cleanup of the first attempt
	g1.Done()
	require.True(t, p.Held(), "the later grant keeps its active token")
	require.Equal(t, 1, srv.ActiveRequests())
	require.Equal(t, 1, reg.Stats().Running, "and its quota slot")

	g2.Done()
	g2.Done()
	require.Zero(t, reg.Stats().Running)
	require.True(t, p.Held(), "released only when the request ends")
	p.Close()
	require.Zero(t, srv.ActiveRequests())
	w.Close()
}

// The resumed response is built and written under the request's token (rev3 section 7): after the
// final resumed read the request still counts against the concurrency limit until ServeHTTP
// returns.
func TestResumedResponseHoldsTheTokenUntilTheRequestEnds(t *testing.T) {
	reg, srv := newPair(t, 4, 8)
	var during, afterDone int
	srv.RegisterMethod("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *jsonrpc.Error) {
		p := jsonrpc.PermitFromContext(ctx)
		w, a := reg.Admit()
		require.Equal(t, Admitted, a)
		defer w.Close()
		p.Release() // parked
		g, err := w.Resume(ctx, p, time.Now().Add(time.Second))
		require.NoError(t, err)
		during = srv.ActiveRequests()
		g.Done() // the read attempt is over; the response is still to be built and written
		afterDone = srv.ActiveRequests()
		return "ok", nil
	})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`)))
	require.Contains(t, rec.Body.String(), `"result":"ok"`)
	require.Equal(t, 1, during)
	require.Equal(t, 1, afterDone, "Done does not release the token")
	require.Zero(t, srv.ActiveRequests(), "the owner's Close releases it after the write")
}

// A permit which was not released before Resume would block the FIFO head for everyone else.
func TestResume_RejectsAHeldPermitAndDoesNotBlockTheQueue(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	held := holder(t, srv)
	wa, _ := reg.Admit()
	_, err := wa.Resume(context.Background(), held, time.Now().Add(time.Second))
	require.ErrorIs(t, err, ErrPermitHeld)
	require.Zero(t, reg.Stats().Queued)

	pb := holder(t, srv)
	pb.Release()
	wb, _ := reg.Admit()
	g, err := wb.Resume(context.Background(), pb, time.Now().Add(time.Second))
	require.NoError(t, err)
	g.Done()
	held.Close()
	pb.Close()
	wa.Close()
	wb.Close()
}

// Generation traffic cannot supersede a passed cutoff or a cancelled context.
func TestWait_StaleGenerationDoesNotOutliveCutoffOrCancel(t *testing.T) {
	r := New(Config{MaxWaiters: 2, Concurrency: 8})
	w, _ := r.Admit()
	defer w.Close()
	past := time.Now().Add(-time.Second)
	for i := 0; i < 100; i++ {
		seen := w.Generation()
		r.Notify()
		require.Equal(t, WakeTimeout, w.Wait(context.Background(), seen, past))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	seen := w.Generation()
	r.Notify()
	require.Equal(t, WakeCancel, w.Wait(ctx, seen, time.Now().Add(time.Minute)))

	// a notification which is ready when the cutoff passes does not extend the wait either
	seen = w.Generation()
	got := make(chan Wake, 1)
	cutoff := time.Now().Add(60 * time.Millisecond)
	go func() { got <- w.Wait(context.Background(), seen, cutoff) }()
	require.Eventually(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return w.armed }, time.Second, time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	require.Equal(t, WakeTimeout, <-got)
}

// An expired or cancelled resumption is rejected before it is queued, never granted by the
// scheduler, and revalidated at the hand-over.
func TestResume_ExpiredOrCancelledIsNeverGranted(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	for i := 0; i < 100; i++ { // capacity is free: a naive select would grant about half
		_, err := w.Resume(context.Background(), p, time.Now().Add(-time.Millisecond))
		require.ErrorIs(t, err, ErrResumeTimeout)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = w.Resume(ctx, p, time.Now().Add(time.Minute))
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Zero(t, reg.Stats().Running)
	require.Zero(t, reg.Stats().Queued)
	require.False(t, p.Held())

	// cancelled / expired while queued behind the quota: the scheduler drops it when capacity frees
	one, srv1 := newPair(t, 8, 4) // quota 1
	pa, pb := holder(t, srv1), holder(t, srv1)
	pa.Release()
	pb.Release()
	wa, _ := one.Admit()
	wb, _ := one.Admit()
	ga, err := wa.Resume(context.Background(), pa, time.Now().Add(time.Minute))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := wb.Resume(ctx, pb, time.Now().Add(time.Minute)); done <- err }()
	require.Eventually(t, func() bool { return one.Stats().Queued == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	ga.Done()
	require.Zero(t, one.Stats().Running, "the cancelled ticket was not granted when the quota freed")
	require.False(t, pb.Held())

	// the context ends between the grant and its delivery: the grant is revoked, not delivered
	ctx2, cancel2 := context.WithCancel(context.Background())
	one.testHookDeliver = func() { cancel2() }
	_, err = wb.Resume(ctx2, pb, time.Now().Add(time.Minute))
	one.testHookDeliver = nil
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, one.Stats().Running)
	require.False(t, pb.Held())
	for _, x := range []*jsonrpc.Permit{pa, pb} {
		x.Close()
	}
	wa.Close()
	wb.Close()
}

// Close racing a granted-but-undelivered resumption: Resume fails, nothing runs after the
// reservation disappeared, token and quota return exactly once.
func TestResume_CloseBetweenGrantAndDeliveryRevokes(t *testing.T) {
	for i := 0; i < 20; i++ {
		reg, srv := newPair(t, 8, 8)
		p := holder(t, srv)
		p.Release()
		w, _ := reg.Admit()
		reg.testHookDeliver = func() { w.Close() } // after the scheduler granted, before delivery
		g, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
		require.ErrorIs(t, err, ErrWaiterClosed)
		require.Nil(t, g)
		st := reg.Stats()
		require.Zero(t, st.Running)
		require.Zero(t, st.Reserved)
		require.False(t, p.Held())
		require.NoError(t, reg.Join(context.Background()))
		p.Close()
		require.Zero(t, srv.ActiveRequests())
	}
}

// Join accounts for a resumed read which is still running after its waiter was closed.
func TestJoin_WaitsForRunningResumedWorkAfterClose(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	p := holder(t, srv)
	p.Release()
	w, _ := reg.Admit()
	g, err := w.Resume(context.Background(), p, time.Now().Add(time.Second))
	require.NoError(t, err)
	w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, reg.Join(ctx), context.DeadlineExceeded)
	g.Done()
	require.NoError(t, reg.Join(context.Background()))
	p.Close()
}
