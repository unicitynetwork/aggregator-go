package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"

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

// ErrNonInclusionUnsupported is returned when a view cannot produce non-inclusion
// certificates. It is a capability failure, never an absence.
var ErrNonInclusionUnsupported = errors.New("read view does not support non-inclusion certificates")

// NonInclusionReadView is the optional capability of a ReadView to build non-inclusion
// certificates from its retained state. GetNonInclusionCert returns the certificate when the
// key is absent from the retained root, an error wrapping api.ErrStateIncluded when the
// key's leaf is present, and any other error (storage, cancellation, closed view,
// corrupt or missing nodes) as an operational failure: a failure is never converted into an
// absence.
type NonInclusionReadView interface {
	GetNonInclusionCert(ctx context.Context, key []byte) (*api.NonInclusionCert, error)
}

// EmptyStateReadView is the optional capability of a ReadView to state, explicitly, that the
// retained state is the empty tree (no leaf). It is the fact a certified empty snapshot is
// reconciled with; it is never inferred from the bytes of the root hash.
type EmptyStateReadView interface {
	IsEmptyState() bool
}

// IsEmptyState reports whether the view explicitly declares the empty tree. A view without the
// capability is not empty as far as callers can tell.
func IsEmptyState(rv ReadView) bool {
	e, ok := rv.(EmptyStateReadView)
	return ok && e.IsEmptyState()
}

// NonInclusionCert builds the non-inclusion certificate through the view's capability.
func NonInclusionCert(ctx context.Context, rv ReadView, key []byte) (*api.NonInclusionCert, error) {
	ni, ok := rv.(NonInclusionReadView)
	if !ok {
		return nil, ErrNonInclusionUnsupported
	}
	return ni.GetNonInclusionCert(ctx, key)
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
//
// Every operation is O(1): entries keep the cumulative delta through
// themselves and are linked to their alive neighbors, so the charge of a view
// is a difference of two cumulative counters, closing unlinks the entry (its
// interval is thereby inherited by the alive predecessor) and the total over
// all alive views telescopes to alive*overhead + head - first.cum.
type lineage struct {
	mu    sync.Mutex
	total uint64 // cumulative delta of every entry ever added
	first *lineageEntry
	last  *lineageEntry
	alive int
	// visits counts the entries touched by accounting operations: the entry itself and the
	// neighbors read or relinked. Tests assert it is a constant per operation, independent of
	// the number of alive entries.
	visits uint64
}

type lineageEntry struct {
	cum        uint64 // lineage.total after this entry's delta was added
	alive      bool
	prev, next *lineageEntry // alive neighbors, oldest first
}

func (l *lineage) add(view *smt.ReadView) *memoryReadView {
	e := l.addDelta(view.Introduced())
	return &memoryReadView{view: view, lin: l, entry: e}
}

// addDelta appends an entry which introduced "delta" bytes.
func (l *lineage) addDelta(delta uint64) *lineageEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.visits++
	l.total += delta
	e := &lineageEntry{cum: l.total, alive: true, prev: l.last}
	if l.last != nil {
		l.visits++
		l.last.next = e
	} else {
		l.first = e
	}
	l.last = e
	l.alive++
	return e
}

// retained returns the incremental bytes charged to e.
func (l *lineage) retained(e *lineageEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.visits++
	if !e.alive {
		return 0
	}
	end := l.total
	if e.next != nil {
		l.visits++
		end = e.next.cum
	}
	return smt.ReadViewOverheadBytes + end - e.cum
}

// totalRetained is the sum of retained over all alive entries.
func (l *lineage) totalRetained() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.visits++
	if l.alive == 0 {
		return 0
	}
	return uint64(l.alive)*smt.ReadViewOverheadBytes + l.total - l.first.cum
}

// close marks e dead and unlinks it; its interval is inherited by the alive
// predecessor (nothing is charged if there is none). Idempotent.
func (l *lineage) close(e *lineageEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.visits++
	if !e.alive {
		return
	}
	e.alive = false
	if e.prev != nil {
		l.visits++
		e.prev.next = e.next
	} else {
		l.first = e.next
	}
	if e.next != nil {
		l.visits++
		e.next.prev = e.prev
	} else {
		l.last = e.prev
	}
	e.prev, e.next = nil, nil
	l.alive--
}

// RetainedBytesTotal is the sum of RetainedBytes over all open views of the
// backend, in O(1). Views which have left the accounting (closed, or retired
// through AccountingRetirer) are not included.
func (b *MemoryBackend) RetainedBytesTotal() uint64 { return b.lineage.totalRetained() }

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

func (v *memoryReadView) GetNonInclusionCert(ctx context.Context, key []byte) (*api.NonInclusionCert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrReadViewClosed
	}
	cert, err := v.view.GetNonInclusionCert(key)
	if errors.Is(err, smt.ErrNonInclusionUnsupported) {
		return nil, fmt.Errorf("%w: %v", ErrNonInclusionUnsupported, err)
	}
	return cert, err
}

func (v *memoryReadView) IsEmptyState() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return !v.closed && v.view.IsEmpty()
}

func (v *memoryReadView) RetainedBytes() uint64 {
	return v.lin.retained(v.entry)
}

// RetireAccounting removes the view from the retention accounting ahead of Close
// (the interval it covered is inherited by its predecessor). The view stays
// readable until Close. O(1), idempotent, safe to call under a catalog lock.
func (v *memoryReadView) RetireAccounting() { v.lin.close(v.entry) }

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

// RetentionAccountant is implemented by backends which can total the retained
// bytes of all their open views in O(1) (the sum of the views' RetainedBytes).
type RetentionAccountant interface {
	RetainedBytesTotal() uint64
}

// AccountingRetirer is implemented by views which can leave the retention
// accounting before they are closed: closing may have to wait for reads in
// flight and so cannot run under a lock, whereas retiring is O(1).
type AccountingRetirer interface {
	RetireAccounting()
}

var (
	_ NonInclusionReadView = (*memoryReadView)(nil)
	_ EmptyStateReadView   = (*memoryReadView)(nil)
	_ ReadView             = (*memoryReadView)(nil)
	_ ReadViewRetainer     = (*MemoryBackend)(nil)
	_ RetentionAccountant  = (*MemoryBackend)(nil)
	_ AccountingRetirer    = (*memoryReadView)(nil)
)
