package proofstatus

import (
	"fmt"
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

func TestLookupDoesNotScale(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	for i := 0; i < 50000; i++ {
		tr.MarkPending(sid(i), 1<<40)
	}
	start := time.Now()
	for i := 0; i < 50000; i++ {
		require.Equal(t, api.ProofStatusPending, tr.Status(sid(i)))
	}
	require.Less(t, time.Since(start), 2*time.Second, fmt.Sprintf("50k lookups took %s", time.Since(start)))
}
