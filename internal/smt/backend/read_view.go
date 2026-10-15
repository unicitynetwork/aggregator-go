package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ErrReadViewClosed is returned by a ReadView used after Close.
var ErrReadViewClosed = errors.New("read view closed")

// ErrReadViewUnsupported is returned by backends that cannot retain a read view
// of a finalized state (for example a store without a read snapshotter).
var ErrReadViewUnsupported = errors.New("backend does not support retained read views")

// ReadView is a backend-neutral, read-only handle to one finalized SMT state.
// It exposes no writes, commits or forks. Implementations must:
//
//   - serve every call from the state captured at retention time, regardless of
//     later commits, tombstones or compactions, and never fall back to live
//     backend state;
//   - be safe for concurrent use by any number of goroutines;
//   - make Close idempotent and wait for (or otherwise safely outlast) calls in
//     flight; calls after Close return ErrReadViewClosed. The proofview catalog
//     reference-counts handles so Close is only reached once no lease remains.
type ReadView interface {
	// RootHash returns a copy of the raw 32-byte root of the retained state.
	RootHash() []byte
	// GetInclusionCert builds the v2 inclusion certificate for the raw key
	// against the retained root. It honors ctx cancellation where the backend
	// can observe it. A key with no leaf in the retained state returns an error
	// wrapping ErrPublishedProofLeafNotFound for every backend; any other error
	// is an operational failure.
	GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error)
	// RetainedBytes is a conservative estimate of the incremental storage this
	// view keeps alive beyond the live state, including shared nodes counted
	// once. It may grow after retention (a view is charged for what its
	// successor replaced) and is never negative.
	RetainedBytes() uint64
	// Close releases the view. It is safe to call more than once.
	Close() error
}

// ReadViewRetainer is implemented by backends that can retain read views.
type ReadViewRetainer interface {
	// RetainReadView captures the backend's current committed state as a
	// ReadView. The caller must ensure that state is finalized (it holds the
	// same synchronization that orders commits and finalization); staging or
	// proposal snapshots can never be retained.
	RetainReadView(ctx context.Context) (ReadView, error)
}

// RetainReadView captures the current committed in-memory state. Consecutive
// calls on one backend form a lineage used for retention accounting: the nodes
// a call introduced are the memory that earlier views keep alive.
func (b *MemoryBackend) RetainReadView(ctx context.Context) (ReadView, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("memory SMT backend not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.retainMu.Lock()
	defer b.retainMu.Unlock()
	view := b.tree.FreezeReadView(b.lastSeq)
	b.lastSeq = view.Seq()
	return b.lineage.add(view), nil
}

// lineage accounts retained memory across the views of one backend. Each frozen
// view contributes a delta: the bytes of nodes it introduced, which replaced
// nodes held by earlier views. A surviving view is charged the deltas from
// itself (exclusive) up to and including the next surviving view's, or to the
// head if it is the last survivor, so shared nodes are counted once, the
// charge stays with surviving views when intermediate handles close, and
// closing a handle never loses nodes an older pinned view still holds.
type lineage struct {
	mu      sync.Mutex
	entries []*lineageEntry // oldest first

	// visited counts the lineage entries examined by retained-byte queries (observability
	// for the scaling measurements: each query scans from the oldest entry).
	visited atomic.Int64
}

// LineageStats reports the number of lineage entries currently kept (including dead
// interior entries awaiting trimming) and the cumulative number of entries examined by
// RetainedBytes queries. It is observability for the scaling measurements.
func (b *MemoryBackend) LineageStats() (entries int, visited int64) {
	b.lineage.mu.Lock()
	defer b.lineage.mu.Unlock()
	return len(b.lineage.entries), b.lineage.visited.Load()
}

type lineageEntry struct {
	delta uint64
	alive bool
}

func (l *lineage) add(view *smt.ReadView) *memoryReadView {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := &lineageEntry{delta: view.Introduced(), alive: true}
	l.entries = append(l.entries, e)
	return &memoryReadView{view: view, lin: l, entry: e}
}

// retained returns the incremental bytes charged to e.
func (l *lineage) retained(e *lineageEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !e.alive {
		return 0
	}
	total := uint64(smt.ReadViewOverheadBytes)
	after := false
	for _, x := range l.entries {
		l.visited.Add(1)
		if x == e {
			after = true
			continue
		}
		if !after {
			continue
		}
		total += x.delta
		if x.alive {
			break
		}
	}
	return total
}

// close marks e dead and drops the dead prefix, whose deltas nothing is
// charged for (no surviving view is older).
func (l *lineage) close(e *lineageEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.alive = false
	i := 0
	for i < len(l.entries) && !l.entries[i].alive {
		i++
	}
	l.entries = append([]*lineageEntry(nil), l.entries[i:]...)
}

type memoryReadView struct {
	// mu makes Close wait for in-flight reads. Reads share it; the tree itself
	// needs no lock.
	mu     sync.RWMutex
	view   *smt.ReadView
	lin    *lineage
	entry  *lineageEntry
	closed bool
}

func (v *memoryReadView) RootHash() []byte {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil
	}
	return v.view.RootHashRaw()
}

func (v *memoryReadView) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrReadViewClosed
	}
	cert, err := v.view.GetInclusionCert(key)
	if errors.Is(err, smt.ErrLeafNotFound) {
		// the contract of ReadView: an absent leaf is ErrPublishedProofLeafNotFound
		return nil, fmt.Errorf("%w: %v", ErrPublishedProofLeafNotFound, err)
	}
	return cert, err
}

func (v *memoryReadView) RetainedBytes() uint64 {
	return v.lin.retained(v.entry)
}

func (v *memoryReadView) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	// Drop the tree so retired views do not pin old nodes.
	v.view.Release()
	v.lin.close(v.entry)
	return nil
}

var (
	_ ReadView         = (*memoryReadView)(nil)
	_ ReadViewRetainer = (*MemoryBackend)(nil)
)
