package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// F1 ruling: resumed batch reads are admitted by the scheduler together with their HTTP permit,
// a batch slot and a pin, leaving one of each to fresh requests; the read consumes that grant.

func TestBatchTryAdmit_LeavesCapacityForFreshRequestsAndRollsBack(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits()) // B = P = 8
	ids := e.addRecords(2, 1000)
	e.finalize([]byte("pv"))
	require.Equal(t, 8, cap(e.svc.slots))

	var granted []longpoll.Resources
	for i := 0; i < 7; i++ { // B-1 = P-1 = 7
		r, ok := e.svc.TryAdmit()
		require.True(t, ok, i)
		granted = append(granted, r)
	}
	_, ok := e.svc.TryAdmit()
	require.False(t, ok, "the eighth would take the last slot/pin of the fresh requests")
	require.Equal(t, 7, len(e.svc.slots))

	// a fresh request still runs: one slot and one pin remain
	resp, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
	require.Nil(t, berr)
	require.NotNil(t, resp)

	// releasing returns both; idempotent
	granted[0].Release()
	granted[0].Release()
	require.Equal(t, 6, len(e.svc.slots))
	again, ok := e.svc.TryAdmit()
	require.True(t, ok)
	granted[0] = again
	for _, r := range granted {
		r.Release()
	}
	require.Zero(t, len(e.svc.slots))
	require.Zero(t, e.catalog.Stats().ActivePins)
	require.Zero(t, e.catalog.Stats().ReservedPins, "released admissions leave no pin reservation behind")

	// complete rollback: slots free but pins occupied by leases -> nothing is held
	var leases []*proofview.Lease
	for i := 0; i < 7; i++ {
		l, err := e.catalog.AcquireLatest(context.Background())
		require.NoError(t, err)
		leases = append(leases, l)
	}
	_, ok = e.svc.TryAdmit()
	require.False(t, ok, "pin capacity is the limiting resource")
	require.Zero(t, len(e.svc.slots), "the slot taken first was rolled back")
	for _, l := range leases {
		l.Release()
	}
	// and the other way round: pins free, slots occupied
	for i := 0; i < 7; i++ {
		e.svc.slots <- struct{}{}
	}
	_, ok = e.svc.TryAdmit()
	require.False(t, ok)
	_, reserveOK := e.catalog.TryReservePin(0)
	require.True(t, reserveOK, "no pin reservation leaked from the failed admission")
	require.Equal(t, 1, e.catalog.Stats().ReservedPins, "only the probe above")
}

// The read uses the slot and the pin it was granted; it does not go through a second admission
// (which would find the pool full) and returns them when done.
func TestBatchResumedReadConsumesItsGrant(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(2, 1000)
	e.finalize([]byte("pv"))
	var held []longpoll.Resources
	for i := 0; i < 7; i++ {
		r, ok := e.svc.TryAdmit()
		require.True(t, ok)
		held = append(held, r)
	}
	// two more fresh readers fill the remaining slot and pin
	l, err := e.catalog.AcquireLatest(context.Background())
	require.NoError(t, err)
	e.svc.slots <- struct{}{}
	_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
	require.NotNil(t, berr, "a fresh read finds everything occupied")
	require.Equal(t, "busy", berr.Reason)

	// the granted read goes through although the pool is full
	ctx := longpolWithResources(context.Background(), held[0])
	resp, berr := e.svc.GetInclusionProofsV1(ctx, e.request(ids[:1], nil))
	require.Nil(t, berr)
	require.NotNil(t, resp)
	require.Equal(t, 7, len(e.svc.slots), "its slot went back; the other grants are untouched")

	held[0].Release() // nothing left to give back
	require.Equal(t, 7, len(e.svc.slots))
	for _, r := range held[1:] {
		r.Release()
	}
	<-e.svc.slots
	l.Release()
	require.Zero(t, len(e.svc.slots))
	require.Zero(t, e.catalog.Stats().ActivePins)
	require.Zero(t, e.catalog.Stats().ReservedPins, "the grant's pin was converted and released, the unused ones given back")
	_, ok := e.catalog.TryReservePin(0)
	require.True(t, ok)
}

func longpolWithResources(ctx context.Context, r longpoll.Resources) context.Context {
	return longpoll.WithResources(ctx, r)
}

// With fewer than two batch slots or pins parking is disabled: the ordinary immediate outcome
// with a zero echo, never a wait that could not leave capacity to fresh requests.
func TestLongPollBatch_ParkingDisabledBelowTwoSlotsOrPins(t *testing.T) {
	for name, mutate := range map[string]func(*batchEnv, *proofview.Limits){
		"B=1": func(e *batchEnv, _ *proofview.Limits) { e.svc.slots = make(chan struct{}, 1) },
		"P=1": nil,
	} {
		limits := proofview.DefaultLimits()
		if name == "P=1" {
			limits.MaxConcurrentPins = 1
		}
		e := newBatchEnv(t, limits)
		if mutate != nil {
			mutate(e, &limits)
		}
		ids := e.addRecords(2, 1000)
		e.finalize([]byte("pv1"))
		later := e.addRecords(1, 1000)
		reg := longpoll.New(longpoll.Config{MaxWaiters: 8, Concurrency: 100})
		co := &longpoll.Coordinator{Reg: reg, MaxWait: 5 * time.Second, Budget: 30 * time.Second}
		e.svc.longPoll = co
		url := goldenGateway(t, &AggregatorService{}, e.svc.HandleRPC, co)
		start := time.Now()
		body := goldenPost(t, url, fmt.Sprintf(`{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proofs.v1","params":{"stateIds":[%s],"waitMs":3000}}`, batchIDsJSON(ids[0], later[0])))
		require.Less(t, time.Since(start), time.Second, name)
		require.Contains(t, body, `"detail":"state_pending"`, name)
		require.Contains(t, body, `"waitedMs":0}`, name)
		require.Zero(t, reg.Stats().Reserved, name)
	}
}

func outcomeCount(outcome string) float64 {
	return testutil.ToFloat64(metrics.LongPollRequestsTotal.WithLabelValues("get_inclusion_proofs.v1", outcome))
}

type burstRig struct {
	*lpBatchRig
	co *longpoll.Coordinator
}

// B = P = 8 with production budgets: a wake burst of parked batch waiters completes without a
// single busy rejection, never runs more than B-1 resumed reads, and fresh traffic keeps working.
func TestLongPollBatch_WakeBurstWithProductionBatchConcurrency(t *testing.T) {
	const waiters = 96
	r := newLPBatchRig(t, 1024, 100)                                        // C=100: global resume quota 25, class cap min(25, 7, 7) = 7
	r.e.svc.longPoll.Reserve, r.e.svc.longPoll.ResumeReserveOverride = 0, 0 // production reserves
	r.e.svc.longPoll.MaxWait = 20 * time.Second
	base := map[string]float64{}
	for _, o := range []string{"busy_admission_deadline", "busy_downstream", "completed"} {
		base[o] = outcomeCount(o)
	}
	req := append([]api.StateID{r.ids[0]}, r.later[:15]...) // L = 16

	results := make(chan string, waiters)
	for i := 0; i < waiters; i++ {
		go func() { results <- r.call(t, req, `,"waitMs":15000`) }()
		if i%4 == 3 { // arrive no faster than the batch admission reads
			time.Sleep(15 * time.Millisecond)
		}
	}
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == waiters }, 30*time.Second, 10*time.Millisecond)

	// sample occupancy through the burst
	var peakSlots, peakPins, peakRunning atomic.Int64
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, p := range []struct {
				v *atomic.Int64
				n int64
			}{{&peakSlots, int64(len(r.e.svc.slots))}, {&peakPins, int64(r.e.catalog.Stats().ActivePins)}, {&peakRunning, int64(r.reg.Stats().Running)}} {
				for {
					o := p.v.Load()
					if p.n <= o || p.v.CompareAndSwap(o, p.n) {
						break
					}
				}
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	// fresh batch traffic during the burst
	var freshOK, freshTotal atomic.Int64
	freshDone := make(chan struct{})
	go func() {
		defer close(freshDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			body := r.call(t, r.ids[:1], "")
			freshTotal.Add(1)
			if strings.Contains(body, `"result"`) {
				freshOK.Add(1)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	r.publish() // the stalled publisher resumes: wake all
	var ok, busy int
	for i := 0; i < waiters; i++ {
		select {
		case body := <-results:
			switch {
			case strings.Contains(body, `"result"`):
				ok++
			case strings.Contains(body, `"busy"`):
				busy++
			default:
				t.Fatalf("unexpected response %s", body)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("only %d of %d answered", i, waiters)
		}
	}
	close(stop)
	<-freshDone
	sampler.Wait()

	require.Equal(t, waiters, ok, "every woken waiter was served; %d busy", busy)
	require.Zero(t, busy)
	require.Zero(t, outcomeCount("busy_downstream")-base["busy_downstream"], "no read admitted by the scheduler was rejected downstream")
	require.Zero(t, outcomeCount("busy_admission_deadline")-base["busy_admission_deadline"])
	require.LessOrEqual(t, peakRunning.Load(), int64(7), "resumed batch reads are capped at min(C/4, B-1, P-1)")
	require.LessOrEqual(t, peakSlots.Load(), int64(8))
	require.LessOrEqual(t, peakPins.Load(), int64(8))
	require.Positive(t, freshTotal.Load())
	require.Equal(t, freshTotal.Load(), freshOK.Load(), "fresh batch traffic kept its slot and pin during the burst (%d requests)", freshTotal.Load())

	require.Eventually(t, func() bool {
		st := r.reg.Stats()
		return st.Reserved == 0 && st.Queued == 0 && st.Running == 0 && len(r.e.svc.slots) == 0 && r.e.catalog.Stats().ActivePins == 0 && r.e.catalog.Stats().ReservedPins == 0
	}, 5*time.Second, 5*time.Millisecond, "complete resource cleanup")
	_, canReserve := r.e.catalog.TryReservePin(0)
	require.True(t, canReserve)
}

// Queueing is deadline-bounded: when the burst cannot be served within the resume reserve, the
// excess gets the busy/deadline outcome from ADMISSION (distinguishable from a downstream busy),
// resources are all returned, and nothing waits past the shared deadline.
func TestLongPollBatch_ResumeQueueIsDeadlineBounded(t *testing.T) {
	const waiters = 40
	r := newLPBatchRig(t, 1024, 100)
	r.e.svc.longPoll.Reserve = 100 * time.Millisecond
	r.e.svc.longPoll.ResumeReserveOverride = 150 * time.Millisecond
	req := append([]api.StateID{r.ids[0]}, r.later[:3]...)
	admBefore, downBefore := outcomeCount("busy_admission_deadline"), outcomeCount("busy_downstream")

	results := make(chan string, waiters)
	for i := 0; i < waiters; i++ {
		go func() { results <- r.call(t, req, `,"waitMs":10000`) }()
		if i%4 == 3 {
			time.Sleep(15 * time.Millisecond)
		}
	}
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == waiters }, 30*time.Second, 10*time.Millisecond)
	r.e.recs.delay = 100 * time.Millisecond // every re-read now takes 100 ms: 40 reads / 7 at a time >> 150 ms

	start := time.Now()
	r.publish()
	var ok, busy int
	for i := 0; i < waiters; i++ {
		body := <-results
		switch {
		case strings.Contains(body, `"result"`):
			ok++
		case strings.Contains(body, `"detail":"deadline"`) && strings.Contains(body, `"waitedMs":`):
			busy++
		default:
			t.Fatalf("unexpected response %s", body)
		}
	}
	require.Less(t, time.Since(start), 3*time.Second, "bounded by the resume reserve, not by the whole queue")
	require.Positive(t, ok, "the first reads inside the reserve are served")
	require.Positive(t, busy, "the excess is cut at the shared deadline")
	require.Equal(t, waiters, ok+busy)
	require.EqualValues(t, busy, outcomeCount("busy_admission_deadline")-admBefore, "admission expiry is counted as such")
	require.Zero(t, outcomeCount("busy_downstream")-downBefore, "and is not a downstream rejection")

	require.Eventually(t, func() bool {
		st := r.reg.Stats()
		return st.Reserved == 0 && st.Queued == 0 && st.Running == 0 && len(r.e.svc.slots) == 0 && r.e.catalog.Stats().ActivePins == 0 && r.e.catalog.Stats().ReservedPins == 0
	}, 5*time.Second, 5*time.Millisecond)
}

// Architect review of #231: the headroom check of a resumption must be atomic with ALL fresh slot
// acquisitions. Interleaving 1: a fresh read holds the seventh slot (and has not taken its pin
// yet) - the resumption must see one free slot and stay queued. Interleaving 2: the fresh read
// starts exactly between the resumption's headroom check and its slot take - it must wait for the
// critical section, so the resumption never consumes the last free slot.
func TestBatchHeadroomIsAtomicWithFreshAdmission(t *testing.T) {
	newEnv := func(t *testing.T) (*batchEnv, []api.StateID) {
		e := newBatchEnv(t, proofview.DefaultLimits()) // B = P = 8
		ids := e.addRecords(2, 1000)
		e.finalize([]byte("pv"))
		for i := 0; i < 6; i++ { // six slots occupied by other reads
			e.svc.slots <- struct{}{}
		}
		return e, ids
	}

	t.Run("fresh read holds the seventh slot before its pin", func(t *testing.T) {
		e, ids := newEnv(t)
		holding, release := make(chan struct{}), make(chan struct{})
		e.svc.testHookFreshSlot = func() { close(holding); <-release }
		done := make(chan *BatchError, 1)
		go func() {
			_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
			done <- err
		}()
		<-holding
		require.Equal(t, 7, len(e.svc.slots))
		_, ok := e.svc.TryAdmit()
		require.False(t, ok, "one free slot is left: the resumption must stay queued")
		require.Equal(t, 7, len(e.svc.slots), "and took nothing")
		_, reserved := e.catalog.TryReservePin(7)
		require.True(t, reserved, "no pin reservation leaked either")
		close(release)
		require.Nil(t, <-done)
		r, ok := e.svc.TryAdmit()
		require.True(t, ok, "admitted once the fresh read finished")
		r.Release()
	})

	t.Run("fresh read arrives between the check and the take", func(t *testing.T) {
		e, ids := newEnv(t)
		freshStarted := make(chan struct{})
		freshDone := make(chan *BatchError, 1)
		var slotsAtFreshStart atomic.Int32
		e.svc.testHookAdmitChecked = func() {
			go func() {
				close(freshStarted)
				_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
				freshDone <- err
			}()
			<-freshStarted
			time.Sleep(50 * time.Millisecond) // give the fresh read every chance to run ahead
			slotsAtFreshStart.Store(int32(len(e.svc.slots)))
		}
		hold := make(chan struct{})
		e.svc.testHookFreshSlot = func() { <-hold } // a fresh read that got its slot stays visible
		r, ok := e.svc.TryAdmit()
		close(hold)
		require.True(t, ok, "two slots were free when the resumption was admitted")
		require.EqualValues(t, 6, slotsAtFreshStart.Load(), "the fresh read could not take a slot inside the resumption's critical section")
		require.Nil(t, <-freshDone, "the fresh read got the last slot afterwards: headroom intact")
		require.Equal(t, 7, len(e.svc.slots), "six + the resumed one; the fresh read has finished")
		r.Release()
	})

	t.Run("fresh requests keep immediate rejection when full", func(t *testing.T) {
		e, ids := newEnv(t)
		e.svc.slots <- struct{}{}
		e.svc.slots <- struct{}{}
		start := time.Now()
		_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
		require.NotNil(t, berr)
		require.Equal(t, "busy", berr.Reason)
		require.Less(t, time.Since(start), 500*time.Millisecond)
	})
}
