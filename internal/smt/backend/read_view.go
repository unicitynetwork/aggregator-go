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
	// can observe it.
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
// calls on one backend form a lineage: each call charges the nodes it
// introduced to the previous view as the memory that view retains.
func (b *MemoryBackend) RetainReadView(ctx context.Context) (ReadView, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("memory SMT backend not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.retainMu.Lock()
	defer b.retainMu.Unlock()
	var prev *smt.ReadView
	if b.lastRetained != nil {
		prev = b.lastRetained.view
	}
	v := &memoryReadView{view: b.tree.FreezeReadView(prev)}
	b.lastRetained = v
	return v, nil
}

type memoryReadView struct {
	// mu makes Close wait for in-flight reads. Reads share it; the tree itself
	// needs no lock.
	mu     sync.RWMutex
	view   *smt.ReadView
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
	return v.view.GetInclusionCert(key)
}

func (v *memoryReadView) RetainedBytes() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return 0
	}
	return v.view.RetainedBytes()
}

func (v *memoryReadView) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	// Drop the tree so retired views do not pin old nodes. The root hash,
	// sequence and byte counter stay valid as the lineage anchor.
	v.view.Release()
	return nil
}

var (
	_ ReadView         = (*memoryReadView)(nil)
	_ ReadViewRetainer = (*MemoryBackend)(nil)
)
