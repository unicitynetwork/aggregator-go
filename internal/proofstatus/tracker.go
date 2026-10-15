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
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Defaults of the recent-expired memory.
const (
	DefaultExpiredCapacity = 100_000
	DefaultExpiredTTL      = 10 * time.Minute
)

// Config bounds the tracker.
type Config struct {
	// ExpiredCapacity caps the remembered expired state IDs (oldest evicted first).
	ExpiredCapacity int
	// ExpiredTTL caps how long an expired state ID is remembered.
	ExpiredTTL time.Duration
	// ReferenceTime returns the aggregator's current reference time (seconds): the time base
	// of request deadlines. A pending request whose deadline has passed is answered as
	// expired, and swept into the expired set, even if no round has dropped it yet.
	ReferenceTime func() uint64
	// Now is the wall clock (tests).
	Now func() time.Time
}

type deadlineEntry struct {
	id       string
	deadline uint64
}

type deadlineHeap []deadlineEntry

func (h deadlineHeap) Len() int            { return len(h) }
func (h deadlineHeap) Less(i, j int) bool  { return h[i].deadline < h[j].deadline }
func (h deadlineHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *deadlineHeap) Push(x interface{}) { *h = append(*h, x.(deadlineEntry)) }
func (h *deadlineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
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
	pending  map[string]uint64 // state ID -> effective deadline
	deadline deadlineHeap      // lazy: entries whose map value changed or vanished are skipped

	expired map[string]time.Time
	ring    []expiredEntry // FIFO of expired insertions (may hold stale entries)
	head    int
}

// New creates a tracker.
func New(cfg Config) *Tracker {
	if cfg.ExpiredCapacity <= 0 {
		cfg.ExpiredCapacity = DefaultExpiredCapacity
	}
	if cfg.ExpiredTTL <= 0 {
		cfg.ExpiredTTL = DefaultExpiredTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Tracker{cfg: cfg, pending: make(map[string]uint64), expired: make(map[string]time.Time)}
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

// MarkPending records an admitted request with its effective deadline (reference-time
// seconds). Re-admitting a pending ID (resubmission of the same request) is idempotent
// and keeps the later deadline. It reports whether the ID was newly marked pending. An ID which was recently expired and is admitted again
// is pending again.
func (t *Tracker) MarkPending(id api.StateID, deadline uint64) (added bool) {
	key := id.String()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweepLocked()
	delete(t.expired, key)
	old, was := t.pending[key]
	if was && old >= deadline {
		return false
	}
	t.pending[key] = deadline
	heap.Push(&t.deadline, deadlineEntry{id: key, deadline: deadline})
	t.gaugesLocked()
	return !was
}

// ClearPending removes the pending marks (finalization, rejection, failed admission).
// It never touches the expired memory.
func (t *Tracker) ClearPending(ids ...api.StateID) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		delete(t.pending, id.String())
	}
	t.gaugesLocked()
}

// Reset forgets all pending marks (the node stops being the one answering: deactivation).
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = make(map[string]uint64)
	t.deadline = nil
	t.gaugesLocked()
}

// MarkExpired records state IDs dropped because their effective timeout passed.
func (t *Tracker) MarkExpired(ids ...api.StateID) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.cfg.Now()
	for _, id := range ids {
		key := id.String()
		delete(t.pending, key)
		t.addExpiredLocked(key, now)
	}
	t.gaugesLocked()
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

// sweepLocked moves pending requests whose deadline passed into the expired memory.
func (t *Tracker) sweepLocked() {
	ref, ok := t.refTime()
	if !ok {
		return
	}
	now := t.cfg.Now()
	for len(t.deadline) > 0 && t.deadline[0].deadline <= ref {
		e := heap.Pop(&t.deadline).(deadlineEntry)
		if cur, ok := t.pending[e.id]; ok && cur == e.deadline {
			delete(t.pending, e.id)
			t.addExpiredLocked(e.id, now)
		}
	}
	t.trimLocked(now)
}

// Status returns the hint for a state ID which is not included: pending, expired or
// unknown. O(1) apart from amortized sweeping of passed deadlines.
func (t *Tracker) Status(id api.StateID) api.ProofStatus {
	key := id.String()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweepLocked()
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
