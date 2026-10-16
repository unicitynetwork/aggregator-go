// Package proofstatus remembers, per node, which state IDs were admitted and are not yet
// in a published certified state (pending) and which were recently dropped because their
// effective timeout passed (expired). It backs the unauthenticated status hints of the
// inclusion proof APIs. All lookups are O(1) in-memory map reads; memory is bounded
// (pending by the admission queue and by deadlines, expired by a count and a TTL cap).
//
// The sets are per node and volatile: a node that did not admit a request, or one which
// restarted, answers unknown for it.
package proofstatus

import (
	"container/heap"
	"math"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Defaults of the recent-expired memory.
const (
	DefaultExpiredCapacity = 100_000
	DefaultExpiredTTL      = 10 * time.Minute
	// DefaultMaxPending is the hard cap of pending entries. Beyond it new requests are not
	// tracked (their status stays unknown) and the rejection is counted.
	DefaultMaxPending = 1 << 20
)

// Config bounds the tracker.
type Config struct {
	// ExpiredCapacity caps the remembered expired state IDs (oldest evicted first).
	ExpiredCapacity int
	// ExpiredTTL caps how long an expired state ID is remembered.
	ExpiredTTL time.Duration
	// MaxPending caps the pending entries (memory bound against floods of admitted requests,
	// including ones with a far-future deadline).
	MaxPending int
	// ReferenceTime returns the aggregator's current reference time (seconds): the time base
	// of request deadlines. A pending request whose deadline has passed is answered as
	// expired, and swept into the expired set, even if no round has dropped it yet.
	ReferenceTime func() uint64
	// Now is the wall clock (tests).
	Now func() time.Time
}

// pendEntry is the pending record of one state ID.
type pendEntry struct {
	id       string
	deadline uint64
	// inRound: a round has pulled the request, so it either becomes a leaf or is dropped by
	// the round (reported through MarkExpired). Such entries are never swept by deadline: a
	// leaf accepted under a round's pinned reference time is being certified, however far the
	// reference time has advanced since.
	inRound bool
	heapIdx int // position in the deadline heap, -1 when not a member
}

// deadlineHeap is an indexed min-heap of the queued (not yet pulled) requests with a finite
// deadline. Its size is bounded by the number of pending entries: every removal of a pending
// entry removes its heap node in O(log n).
type deadlineHeap []*pendEntry

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].deadline < h[j].deadline }
func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx, h[j].heapIdx = i, j
}
func (h *deadlineHeap) Push(x interface{}) {
	e := x.(*pendEntry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *deadlineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.heapIdx = -1
	*h = old[:n-1]
	return e
}

type expiredEntry struct {
	id string
	at time.Time
}

// Tracker is safe for concurrent use.
type Tracker struct {
	cfg Config

	mu       sync.Mutex
	pending  map[string]*pendEntry
	deadline deadlineHeap

	expired map[string]time.Time
	ring    []expiredEntry // FIFO of expired insertions (may hold stale entries)
	head    int

	// sweepCount counts deadline entries processed by the sweep (a hardware independent
	// work metric for tests).
	sweepCount uint64

	// notify is called, outside the tracker lock, after a change which a long-poll waiter may
	// be waiting for: an actual drop or sweep into the expired memory, a reset, a withdrawn
	// admission mark. Finalization (ClearFinalized) does not call it: the finalizing path
	// notifies once, after publication and the proof-ready update, so that a wake never
	// precedes the serving boundary.
	notify func()
}

// SetNotify registers the change callback (call before use). It must not block or call back
// into the tracker.
func (t *Tracker) SetNotify(f func()) {
	t.mu.Lock()
	t.notify = f
	t.mu.Unlock()
}

// fire runs the callback captured under the lock, after it was released.
func fire(n func(), changed bool) {
	if changed && n != nil {
		n()
	}
}

// Sweep expires the queued requests whose deadline has passed without waiting for a lookup, and
// reports how many were moved. The round manager calls it when the local reference time
// advances, so a long-poll waiter learns about an expiry although no client polls. In-round
// entries are never swept.
func (t *Tracker) Sweep() int {
	t.mu.Lock()
	n := t.sweepLocked()
	cb := t.notify
	t.mu.Unlock()
	fire(cb, n > 0)
	return n
}

func (t *Tracker) sweepSteps() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sweepCount
}

// New creates a tracker.
func New(cfg Config) *Tracker {
	if cfg.ExpiredCapacity <= 0 {
		cfg.ExpiredCapacity = DefaultExpiredCapacity
	}
	if cfg.ExpiredTTL <= 0 {
		cfg.ExpiredTTL = DefaultExpiredTTL
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = DefaultMaxPending
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Tracker{cfg: cfg, pending: make(map[string]*pendEntry), expired: make(map[string]time.Time)}
}

func (t *Tracker) refTime() (uint64, bool) {
	if t.cfg.ReferenceTime == nil {
		return 0, false
	}
	return t.cfg.ReferenceTime(), true
}

// SetReferenceTime sets the reference-time source (call before use).
func (t *Tracker) SetReferenceTime(f func() uint64) {
	t.mu.Lock()
	t.cfg.ReferenceTime = f
	t.mu.Unlock()
}

// MarkPending records an admitted (queued) request with its effective deadline
// (reference-time seconds). Re-admitting a pending ID (resubmission of the same request) is
// idempotent and keeps the later deadline. It reports whether the ID was newly marked
// pending; false also when the entry cap is reached (the request is then untracked).
// An ID which was recently expired and is admitted again is pending again.
func (t *Tracker) MarkPending(id api.StateID, deadline uint64) (added bool) {
	t.mu.Lock()
	added, swept := t.markPendingLocked(id, deadline)
	cb := t.notify
	t.mu.Unlock()
	fire(cb, swept > 0)
	return added
}

func (t *Tracker) markPendingLocked(id api.StateID, deadline uint64) (added bool, swept int) {
	key := id.String()
	swept = t.sweepLocked()
	delete(t.expired, key)
	if e, ok := t.pending[key]; ok {
		if deadline > e.deadline {
			e.deadline = deadline
			if e.heapIdx >= 0 {
				heap.Fix(&t.deadline, e.heapIdx)
			}
		}
		return false, swept
	}
	if len(t.pending) >= t.cfg.MaxPending {
		metrics.ProofStatusPendingRejected.Inc()
		return false, swept
	}
	e := &pendEntry{id: key, deadline: deadline, heapIdx: -1}
	t.pending[key] = e
	t.pushLocked(e)
	t.gaugesLocked()
	return true, swept
}

// MarkInRound records that a round pulled the request (also for requests replayed from the
// queue after a restart, which were never admitted here). From then on its fate is decided by
// the round: it is cleared on finalization or reported through MarkExpired, and the deadline
// sweep leaves it alone.
func (t *Tracker) MarkInRound(id api.StateID, deadline uint64) {
	key := id.String()
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.pending[key]
	if !ok {
		delete(t.expired, key)
		if len(t.pending) >= t.cfg.MaxPending {
			metrics.ProofStatusPendingRejected.Inc()
			return
		}
		e = &pendEntry{id: key, deadline: deadline, heapIdx: -1}
		t.pending[key] = e
	}
	e.inRound = true
	t.removeFromHeapLocked(e)
	t.gaugesLocked()
}

func (t *Tracker) pushLocked(e *pendEntry) {
	if e.inRound || e.deadline == math.MaxUint64 {
		return // never expires by deadline: not worth a heap node
	}
	heap.Push(&t.deadline, e)
}

func (t *Tracker) removeFromHeapLocked(e *pendEntry) {
	if e.heapIdx >= 0 {
		heap.Remove(&t.deadline, e.heapIdx)
	}
}

func (t *Tracker) deleteLocked(key string) {
	if e, ok := t.pending[key]; ok {
		t.removeFromHeapLocked(e)
		delete(t.pending, key)
	}
}

// ClearPending removes the pending marks (a request which could not be queued, or was
// rejected). It never touches the expired memory.
func (t *Tracker) ClearPending(ids ...api.StateID) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	removed := false
	for _, id := range ids {
		key := id.String()
		if _, ok := t.pending[key]; ok {
			removed = true
		}
		t.deleteLocked(key)
	}
	t.gaugesLocked()
	cb := t.notify
	t.mu.Unlock()
	fire(cb, removed) // a withdrawn admission: a waiter now sees unknown
}

// ReturnToQueue marks requests which a round pulled but did not keep (an abandoned round) as
// queued again, so the deadline sweep covers them once more.
func (t *Tracker) ReturnToQueue(ids ...api.StateID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		if e, ok := t.pending[id.String()]; ok && e.inRound {
			e.inRound = false
			t.pushLocked(e)
		}
	}
}

// ReturnAllToQueue is ReturnToQueue for every pulled request: the node abandoned its rounds
// and precollected state, but the requests are still in the shared queue and will be pulled
// again, so they stay pending (and are swept by deadline again) rather than becoming unknown.
func (t *Tracker) ReturnAllToQueue() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.pending {
		if e.inRound {
			e.inRound = false
			t.pushLocked(e)
		}
	}
}

// ClearFinalized removes the pending marks of states which were certified and forgets any
// expired record of them (a state which is certified was not dropped).
func (t *Tracker) ClearFinalized(ids ...api.StateID) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		key := id.String()
		t.deleteLocked(key)
		delete(t.expired, key)
	}
	t.gaugesLocked()
}

// Reset forgets all pending marks (the node stops being the one answering: deactivation).
func (t *Tracker) Reset() {
	t.mu.Lock()
	t.pending = make(map[string]*pendEntry)
	t.deadline = nil
	t.gaugesLocked()
	cb := t.notify
	t.mu.Unlock()
	fire(cb, true)
}

// HeapLen returns the size of the deadline heap (diagnostics, tests).
func (t *Tracker) HeapLen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.deadline)
}

// MarkExpired records state IDs dropped because their effective timeout passed.
func (t *Tracker) MarkExpired(ids ...api.StateID) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	now := t.cfg.Now()
	for _, id := range ids {
		key := id.String()
		t.deleteLocked(key)
		t.addExpiredLocked(key, now)
	}
	t.gaugesLocked()
	cb := t.notify
	t.mu.Unlock()
	fire(cb, true)
}

func (t *Tracker) addExpiredLocked(key string, now time.Time) {
	if _, ok := t.expired[key]; !ok {
		t.ring = append(t.ring, expiredEntry{id: key, at: now})
	}
	t.expired[key] = now
	t.trimLocked(now)
}

// trimLocked enforces the count and TTL caps, oldest first.
func (t *Tracker) trimLocked(now time.Time) {
	for t.head < len(t.ring) {
		e := t.ring[t.head]
		cur, live := t.expired[e.id]
		stale := !live || !cur.Equal(e.at)
		if !stale && len(t.expired) <= t.cfg.ExpiredCapacity && now.Sub(e.at) <= t.cfg.ExpiredTTL {
			break
		}
		if !stale {
			delete(t.expired, e.id)
			metrics.ProofStatusExpiredEvictions.Inc()
		}
		t.ring[t.head] = expiredEntry{}
		t.head++
	}
	if t.head > 1024 && t.head*2 > len(t.ring) { // compact the consumed prefix
		t.ring = append([]expiredEntry(nil), t.ring[t.head:]...)
		t.head = 0
	}
}

// sweepLocked moves queued requests whose deadline passed into the expired memory. Only
// requests no round has pulled are swept: for those the next round pins a reference time that
// is at least the current one, so the deadline guarantees the drop. Requests already in a
// round are decided by the round.
func (t *Tracker) sweepLocked() (swept int) {
	ref, ok := t.refTime()
	if !ok {
		return 0
	}
	now := t.cfg.Now()
	for len(t.deadline) > 0 && t.deadline[0].deadline <= ref {
		e := heap.Pop(&t.deadline).(*pendEntry)
		t.sweepCount++
		swept++
		delete(t.pending, e.id)
		t.addExpiredLocked(e.id, now)
	}
	t.trimLocked(now)
	return swept
}

// Status returns the hint for a state ID which is not included: pending, expired or
// unknown. O(1) apart from amortized sweeping of passed deadlines.
func (t *Tracker) Status(id api.StateID) api.ProofStatus {
	key := id.String()
	t.mu.Lock()
	swept := t.sweepLocked()
	st := t.statusLocked(key)
	cb := t.notify
	t.mu.Unlock()
	fire(cb, swept > 0) // other waiters learn about the expiry this lookup found
	return st
}

func (t *Tracker) statusLocked(key string) api.ProofStatus {
	if _, ok := t.pending[key]; ok {
		return api.ProofStatusPending
	}
	if at, ok := t.expired[key]; ok {
		if t.cfg.Now().Sub(at) <= t.cfg.ExpiredTTL {
			return api.ProofStatusExpired
		}
		delete(t.expired, key)
	}
	return api.ProofStatusUnknown
}

// Counts returns the sizes of the pending and expired sets (diagnostics, tests).
func (t *Tracker) Counts() (pending, expired int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending), len(t.expired)
}

func (t *Tracker) gaugesLocked() {
	metrics.ProofStatusPending.Set(float64(len(t.pending)))
	metrics.ProofStatusExpired.Set(float64(len(t.expired)))
}
