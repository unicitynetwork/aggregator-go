package proofstatus

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func sid(n int) api.StateID {
	b := make([]byte, 32)
	b[0], b[1], b[2] = byte(n), byte(n>>8), byte(n>>16)
	return api.StateID(b)
}

type clock struct {
	mu  sync.Mutex
	now time.Time
	ref uint64
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Ref() uint64    { c.mu.Lock(); defer c.mu.Unlock(); return c.ref }
func (c *clock) advance(d time.Duration, ref uint64) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.ref = ref
	c.mu.Unlock()
}

func newTracker(c *clock, capacity int, ttl time.Duration) *Tracker {
	return New(Config{ExpiredCapacity: capacity, ExpiredTTL: ttl, ReferenceTime: c.Ref, Now: c.Now})
}

func TestStatuses(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)

	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(1)))
	tr.MarkPending(sid(1), 200)
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(1)))

	tr.ClearPending(sid(1)) // finalized: the proof answers, the hint is no longer needed
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(1)))

	tr.MarkPending(sid(2), 200)
	tr.MarkExpired(sid(2)) // dropped by a round
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(2)))
	p, e := tr.Counts()
	require.Equal(t, 0, p)
	require.Equal(t, 1, e)

	// re-admission of an expired request makes it pending again
	tr.MarkPending(sid(2), 300)
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(2)))
}

// Resubmitting a still-pending request is idempotent: one mark, the later deadline wins.
func TestResubmissionIsIdempotent(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	for i := 0; i < 5; i++ {
		tr.MarkPending(sid(1), 150)
	}
	tr.MarkPending(sid(1), 400)
	tr.MarkPending(sid(1), 120) // an earlier deadline does not shorten it
	p, _ := tr.Counts()
	require.Equal(t, 1, p)
	c.advance(time.Second, 200) // past 150 but before 400
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(1)))
	c.advance(time.Second, 400)
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))
}

func TestPassedDeadlineIsExpiredEvenIfNoRoundDroppedIt(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	tr.MarkPending(sid(1), 110)
	tr.MarkPending(sid(2), 500)
	c.advance(time.Second, 110) // the timeout is exclusive: reference time >= deadline
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(2)))
}

func TestExpiredMemoryIsBoundedByCountAndTTL(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 100, time.Minute)
	evBefore := testutil.ToFloat64(metrics.ProofStatusExpiredEvictions)
	for i := 0; i < 1000; i++ {
		tr.MarkExpired(sid(i))
		c.advance(time.Millisecond, 100)
	}
	_, e := tr.Counts()
	require.LessOrEqual(t, e, 100, "count cap")
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(0)), "oldest evicted")
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(999)))
	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.ProofStatusExpiredEvictions)-evBefore, float64(900))

	c.advance(2*time.Minute, 100) // TTL
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(999)))
	tr.MarkExpired(sid(5000)) // trimming runs on insertion too
	_, e = tr.Counts()
	require.LessOrEqual(t, e, 2)
}

func TestResetForgetsPending(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	tr.MarkPending(sid(1), 200)
	tr.MarkExpired(sid(2))
	tr.Reset()
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(1)))
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(2)), "the expired memory is separate")
}

func TestConcurrentUse(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 64, time.Minute)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				id := sid(g*10000 + i)
				tr.MarkPending(id, 1000)
				_ = tr.Status(id)
				if i%3 == 0 {
					tr.MarkExpired(id)
				} else {
					tr.ClearPending(id)
				}
			}
		}(g)
	}
	wg.Wait()
	p, e := tr.Counts()
	require.Zero(t, p)
	require.LessOrEqual(t, e, 64)
}

// Lookup cost does not depend on the number of pending entries: the work of a Status call
// (heap sweep steps, map probes) is counted, not timed, for a small and a 100x larger set.
func TestLookupWorkDoesNotGrowWithPendingCount(t *testing.T) {
	work := func(n int) (perLookupHeapOps float64, heapLen int) {
		c := &clock{now: time.Unix(1000, 0), ref: 100}
		tr := newTracker(c, 10, time.Minute)
		for i := 0; i < n; i++ {
			tr.MarkPending(sid(i), uint64(1<<40+i)) // none due
		}
		before := tr.sweepSteps()
		const lookups = 1000
		for i := 0; i < lookups; i++ {
			require.Equal(t, api.ProofStatusPending, tr.Status(sid(i%n)))
		}
		return float64(tr.sweepSteps()-before) / lookups, tr.HeapLen()
	}
	small, hs := work(500)
	large, hl := work(50000)
	require.Equal(t, 500, hs)
	require.Equal(t, 50000, hl)
	require.Zero(t, small, "no deadline is due: a lookup pops nothing")
	require.Zero(t, large, "and that does not depend on how many entries are pending")

	// when deadlines are due, the sweep pays once per expired entry, not once per lookup
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 1<<20, time.Minute)
	for i := 0; i < 5000; i++ {
		tr.MarkPending(sid(i), 150)
	}
	c.advance(time.Second, 200)
	before := tr.sweepSteps()
	for i := 0; i < 5000; i++ {
		_ = tr.Status(sid(i))
	}
	require.EqualValues(t, 5000, tr.sweepSteps()-before, "each deadline is processed exactly once in total")
	require.EqualValues(t, 0, tr.HeapLen())
}

// The deadline heap indexes only live pending entries: finalization/expiry of a request
// removes its node, so memory is O(pending), not O(admission rate x deadline) (review major 1).
func TestHeapStaysBoundedByPendingUnderChurn(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	maxHeap := 0
	for i := 0; i < 20000; i++ {
		id := sid(i)
		tr.MarkPending(id, 1<<62) // far-future client-chosen deadline
		tr.ClearFinalized(id)     // certified right away
		if i%3 == 0 {
			tr.MarkPending(id, 1<<61)
			tr.MarkExpired(id)
		}
		if n := tr.HeapLen(); n > maxHeap {
			maxHeap = n
		}
	}
	require.LessOrEqual(t, maxHeap, 2, "heap must not accumulate finalized/expired requests")
	p, _ := tr.Counts()
	require.Zero(t, p)
	require.Zero(t, tr.HeapLen())

	// steady state: heap == pending (+ none)
	for i := 0; i < 1000; i++ {
		tr.MarkPending(sid(i), uint64(1<<40+i))
	}
	require.Equal(t, 1000, tr.HeapLen())
	for i := 0; i < 1000; i += 2 {
		tr.ClearPending(sid(i))
	}
	require.Equal(t, 500, tr.HeapLen())
}

func TestNoDeadlineRequestsNeverCostAHeapNode(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	for i := 0; i < 1000; i++ {
		tr.MarkPending(sid(i), math.MaxUint64)
	}
	require.Zero(t, tr.HeapLen())
	p, _ := tr.Counts()
	require.Equal(t, 1000, p)
}

// A request a round pulled is decided by the round: the deadline sweep must not call it
// expired while it is being certified (review major 2).
func TestInRoundRequestIsNotSweptByDeadline(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	tr.MarkPending(sid(1), 150)
	tr.MarkPending(sid(2), 150)
	tr.MarkInRound(sid(1), 150) // pulled into a round pinned at 100 < 150, leaf accepted

	c.advance(time.Second, 200) // the certified round's UC moves the reference time past the deadline
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(1)), "being certified: pending, never expired")
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(2)), "still queued: its drop is guaranteed")

	tr.ClearFinalized(sid(1))
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(1)))
	p, e := tr.Counts()
	require.Zero(t, p)
	require.Equal(t, 1, e)

	// requests replayed from the queue after a restart enter as in-round
	tr.MarkInRound(sid(3), 150)
	c.advance(time.Second, 300)
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(3)))
	// a round which gives requests back makes them sweepable again
	tr.ReturnToQueue(sid(3))
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(3)))
}

// Finalization also forgets an expired record: a certified state was not dropped.
func TestFinalizationClearsExpiredRecord(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	tr.MarkExpired(sid(1))
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))
	tr.ClearFinalized(sid(1))
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(1)))
	// ClearPending alone does not touch the expired memory
	tr.MarkExpired(sid(2))
	tr.ClearPending(sid(2))
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(2)))
}

func TestPendingEntryCap(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := New(Config{MaxPending: 100, ReferenceTime: c.Ref, Now: c.Now})
	var added int
	for i := 0; i < 150; i++ {
		if tr.MarkPending(sid(i), 1<<40) {
			added++
		}
	}
	require.Equal(t, 100, added)
	p, _ := tr.Counts()
	require.Equal(t, 100, p)
	require.Equal(t, api.ProofStatusUnknown, tr.Status(sid(149)), "untracked beyond the cap")
	tr.ClearFinalized(sid(0))
	require.True(t, tr.MarkPending(sid(149), 1<<40), "room again once entries leave")
}
