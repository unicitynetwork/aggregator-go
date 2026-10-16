package proofstatus

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func notifyTracker(t *testing.T) (*Tracker, *clock, *atomic.Int32) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	var n atomic.Int32
	tr.SetNotify(func() {
		n.Add(1)
		// the callback runs outside the tracker lock: calling back in must not deadlock
		_ = tr.Status(sid(999))
	})
	return tr, c, &n
}

func TestNotify_DropSweepResetAndWithdrawnAdmission(t *testing.T) {
	tr, c, n := notifyTracker(t)

	tr.MarkPending(sid(1), 200)
	require.Zero(t, n.Load(), "a new admission is not an event for waiters")

	// a due queued sweep wakes even though no client looked the ID up
	c.advance(time.Second, 300)
	require.Equal(t, 1, tr.Sweep())
	require.EqualValues(t, 1, n.Load())
	require.Equal(t, 0, tr.Sweep(), "nothing more to sweep")
	require.EqualValues(t, 1, n.Load(), "no change, no signal")
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))

	// an actual drop by a round
	tr.MarkPending(sid(2), 0)
	tr.MarkInRound(sid(2), 0)
	before := n.Load()
	tr.MarkExpired(sid(2))
	require.Equal(t, before+1, n.Load())

	// a withdrawn admission (the queue refused it): only when a mark was really removed
	tr.MarkPending(sid(3), 1000)
	before = n.Load()
	tr.ClearPending(sid(4)) // nothing pending under this ID
	require.Equal(t, before, n.Load())
	tr.ClearPending(sid(3))
	require.Equal(t, before+1, n.Load())

	// demotion
	before = n.Load()
	tr.Reset()
	require.Equal(t, before+1, n.Load())
}

// Finalization is reported by the finalizing path after publication, not by the tracker.
func TestNotify_FinalizationClearDoesNotSignal(t *testing.T) {
	tr, _, n := notifyTracker(t)
	tr.MarkPending(sid(1), 1000)
	tr.ClearFinalized(sid(1))
	require.Zero(t, n.Load())
}

// In-round entries are never expired by wall time or by a later reference time.
func TestNotify_InRoundEntryIsNotSweptAndStaysPending(t *testing.T) {
	tr, c, n := notifyTracker(t)
	tr.MarkPending(sid(1), 200)
	tr.MarkInRound(sid(1), 200)
	c.advance(time.Hour, 1_000_000)
	require.Equal(t, 0, tr.Sweep())
	require.Zero(t, n.Load())
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(1)))
	require.Zero(t, n.Load())

	// it returns to the sweep once the round abandons it: the overdue entry expires AT that
	// boundary and the waiters are woken, no further Sweep or lookup is needed
	tr.ReturnToQueue(sid(1))
	require.EqualValues(t, 1, n.Load())
	require.Equal(t, 0, tr.Sweep(), "nothing left for a later sweep")
}

// A lookup or admission which sweeps tells the other waiters too.
func TestNotify_LookupAndAdmissionSweepsSignal(t *testing.T) {
	tr, c, n := notifyTracker(t)
	tr.MarkPending(sid(1), 200)
	tr.MarkPending(sid(2), 250)
	c.advance(time.Second, 220)
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))
	require.EqualValues(t, 1, n.Load())
	c.advance(time.Second, 300)
	tr.MarkPending(sid(3), 400) // sweeps sid(2)
	require.EqualValues(t, 2, n.Load())
}

// Resubmission after an expiry clears the old expiry and is pending again (and signals nothing
// by itself: waiters that saw expired have already ended).
func TestNotify_ResubmissionClearsOldExpiry(t *testing.T) {
	tr, c, _ := notifyTracker(t)
	tr.MarkPending(sid(1), 200)
	c.advance(time.Second, 300)
	require.Equal(t, 1, tr.Sweep())
	require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))
	require.True(t, tr.MarkPending(sid(1), 5000))
	require.Equal(t, api.ProofStatusPending, tr.Status(sid(1)))
}

// End to end with the registry (T5): a parked waiter on a queued request wakes when the local
// reference time passes its deadline, with no poll, and then reads expired.
func TestNotify_ParkedWaiterWakesOnDueSweepWithoutAPoll(t *testing.T) {
	c := &clock{now: time.Unix(1000, 0), ref: 100}
	tr := newTracker(c, 10, time.Minute)
	reg := longpoll.New(longpoll.Config{MaxWaiters: 4, Concurrency: 8})
	tr.SetNotify(reg.Notify)
	tr.MarkPending(sid(1), 200)

	w, _ := reg.Admit()
	defer w.Close()
	status := func() api.ProofStatus { // what the evaluator reads without sweeping first
		tr.mu.Lock()
		defer tr.mu.Unlock()
		return tr.statusLocked(sid(1).String())
	}
	seen := w.Generation()
	require.Equal(t, api.ProofStatusPending, status())
	woke := make(chan longpoll.Wake, 1)
	go func() { woke <- w.Wait(t.Context(), seen, time.Now().Add(time.Minute)) }()
	time.Sleep(20 * time.Millisecond)

	c.advance(time.Second, 300) // the local reference time advances: the round manager sweeps
	tr.Sweep()
	require.Contains(t, []longpoll.Wake{longpoll.WakeSignal, longpoll.WakeChanged}, <-woke)
	require.Equal(t, api.ProofStatusExpired, status())
}

func TestNotify_AbandonedRoundReturnsOverdueEntriesWithAWake(t *testing.T) {
	for name, back := range map[string]func(*Tracker){
		"ReturnToQueue":    func(tr *Tracker) { tr.ReturnToQueue(sid(1)) },
		"ReturnAllToQueue": func(tr *Tracker) { tr.ReturnAllToQueue() },
	} {
		t.Run(name, func(t *testing.T) {
			tr, c, n := notifyTracker(t)
			tr.MarkPending(sid(1), 200)
			tr.MarkInRound(sid(1), 200)
			c.advance(time.Second, 300) // reference time passes the deadline while in the round
			require.Equal(t, 0, tr.Sweep(), "the in-round exemption holds")
			require.Zero(t, n.Load())

			back(tr)
			require.EqualValues(t, 1, n.Load(), "returned overdue: expired and signalled without a lookup")
			require.Equal(t, api.ProofStatusExpired, tr.Status(sid(1)))

			// a not yet overdue entry returns quietly
			tr.MarkPending(sid(2), 10_000)
			tr.MarkInRound(sid(2), 10_000)
			before := n.Load()
			back2 := back
			back2(tr)
			require.Equal(t, before, n.Load())
			require.Equal(t, api.ProofStatusPending, tr.Status(sid(2)))
		})
	}
}
