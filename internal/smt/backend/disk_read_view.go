package backend

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RetainReadView captures the current committed disk SMT state as a ReadView
// backed by its own pinned store read snapshot. The caller must hold the same
// synchronization that orders commits and finalization so the committed state
// is the finalized one.
//
// The view reads only through its snapshot: later commits, tombstones and
// compactions cannot change what it serves and it never consults the live
// store. Stores without read snapshots or write-volume accounting cannot retain
// views (ErrReadViewUnsupported): without accounting the retention budget could
// not be honored.
//
// The surviving snapshot-holding views form a lineage ordered by capture: a
// view is charged the (conservatively estimated) bytes written between its
// capture and the capture of the next surviving view, or until now for the
// newest survivor. When a view closes, the interval it covered is inherited by
// its predecessor (which still pins that history), so the total charge of the
// survivors is always everything written since the oldest one, counted once.
func (b *DiskBackend) RetainReadView(ctx context.Context) (ReadView, error) {
	if b == nil || b.tree == nil || b.store == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshotter, ok := b.store.(storage.ReadSnapshotter)
	if !ok {
		return nil, ErrReadViewUnsupported
	}
	volume, ok := b.store.(storage.WriteVolumeReporter)
	if !ok {
		return nil, ErrReadViewUnsupported
	}

	b.retainMu.Lock()
	defer b.retainMu.Unlock()
	if b.retainClosed {
		return nil, ErrReadViewClosed
	}
	// The write volume is read before the snapshot is taken: a commit in
	// between only makes the charge larger.
	startVolume := volume.WrittenBytes()
	root, reader, closeSnapshot, err := b.tree.RetainReadSnapshot(func() (storage.ReadStore, func(), error) {
		return snapshotter.NewReadSnapshot()
	})
	if err != nil {
		return nil, err
	}
	v := &diskReadView{
		root:         root,
		reader:       reader,
		closeReader:  closeSnapshot,
		volume:       volume,
		openedAt:     time.Now(),
		owner:        b,
		hasSnapshot:  reader != nil,
		lineageStart: startVolume,
	}
	if reader != nil {
		metrics.SMTDiskProofSnapshotsActive.Inc()
		// a view without a snapshot pins nothing and takes no part in the lineage
		b.retained = append(b.retained, v)
	}
	return v, nil
}

// closeRetainedViews closes every outstanding retained view (backend shutdown)
// and refuses new ones. Each view waits for its in-flight reads.
func (b *DiskBackend) closeRetainedViews() {
	b.retainMu.Lock()
	b.retainClosed = true
	views := slices.Clone(b.retained)
	b.retainMu.Unlock()
	for _, v := range views {
		_ = v.Close()
	}
}

func (b *DiskBackend) forgetRetainedView(v *diskReadView) {
	b.retainMu.Lock()
	b.retained = slices.DeleteFunc(b.retained, func(o *diskReadView) bool { return o == v })
	b.retainMu.Unlock()
}

// lineageEnd is the write volume where the charge of v ends: the capture of
// the next surviving view, or the current volume for the newest survivor.
func (b *DiskBackend) lineageEnd(v *diskReadView, current uint64) uint64 {
	b.retainMu.Lock()
	defer b.retainMu.Unlock()
	i := slices.Index(b.retained, v)
	if i < 0 || i+1 >= len(b.retained) {
		return current
	}
	return b.retained[i+1].lineageStart
}

type diskReadView struct {
	root        disk.Hash
	reader      storage.ReadStore
	closeReader func()
	hasSnapshot bool
	volume      storage.WriteVolumeReporter
	openedAt    time.Time
	owner       *DiskBackend

	// mu lets Close wait for reads in flight; reads share it.
	mu     sync.RWMutex
	closed bool

	lineageStart uint64
}

func (v *diskReadView) RootHash() []byte {
	return hashBytes(v.root)
}

func (v *diskReadView) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrReadViewClosed
	}
	if v.root == disk.EmptyRootHash() {
		return nil, ErrPublishedProofLeafNotFound
	}
	cert, err := persist.BuildInclusionCert(v.root, v.reader, key)
	if err != nil {
		if errors.Is(err, persist.ErrProofLeafNotFound) {
			return nil, ErrPublishedProofLeafNotFound
		}
		return nil, err
	}
	return cert, nil
}

// RetainedBytes is the estimated storage written between this view's capture
// and its successor's (or now), the versions the snapshot keeps alive.
func (v *diskReadView) RetainedBytes() uint64 {
	v.mu.RLock()
	closed := v.closed
	v.mu.RUnlock()
	if closed || !v.hasSnapshot {
		return 0
	}
	end := v.owner.lineageEnd(v, v.volume.WrittenBytes())
	if end < v.lineageStart {
		return 0
	}
	return end - v.lineageStart
}

func (v *diskReadView) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	closeReader := v.closeReader
	v.closeReader = nil
	v.reader = nil
	v.mu.Unlock()
	if closeReader != nil {
		closeReader()
		metrics.SMTDiskProofSnapshotsActive.Dec()
		metrics.SMTDiskProofSnapshotHoldDuration.Observe(time.Since(v.openedAt).Seconds())
	}
	if v.owner != nil {
		v.owner.forgetRetainedView(v)
	}
	return nil
}

var (
	_ ReadView         = (*diskReadView)(nil)
	_ ReadViewRetainer = (*DiskBackend)(nil)
)
