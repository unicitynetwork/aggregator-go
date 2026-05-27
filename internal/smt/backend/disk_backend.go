package backend

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type DiskBackend struct {
	store storage.Store
	tree  *persist.Tree

	mu              sync.RWMutex
	lastCommitStats persist.CommitStats
}

func NewDiskBackend(store storage.Store, opts persist.Options) (*DiskBackend, error) {
	tree, err := persist.OpenWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	return &DiskBackend{
		store: store,
		tree:  tree,
	}, nil
}

func (b *DiskBackend) KeyLength() int {
	return api.StateTreeKeyLengthBits
}

func (b *DiskBackend) RootHashRaw(context.Context) ([]byte, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	return hashBytes(b.tree.RootHash()), nil
}

func (b *DiskBackend) CommittedState(context.Context) (CommittedState, error) {
	if b == nil || b.store == nil {
		return CommittedState{}, fmt.Errorf("disk SMT backend not initialized")
	}
	state, err := b.store.CommittedState()
	if err != nil {
		return CommittedState{}, err
	}
	return CommittedState{
		BlockNumber: cloneBigInt(state.BlockNumber),
		RootHash:    hashBytes(state.RootHash),
	}, nil
}

func (b *DiskBackend) CreateSnapshot(context.Context) (Snapshot, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	snapshot, err := b.tree.CreateSnapshot()
	if err != nil {
		return nil, err
	}
	return &DiskSnapshot{
		snapshot: snapshot,
		target:   b,
	}, nil
}

func (b *DiskBackend) GetInclusionCert(_ context.Context, key []byte) (*api.InclusionCert, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	return b.tree.GetInclusionCert(key)
}

func (b *DiskBackend) Stats(ctx context.Context) BackendStats {
	root, _ := b.RootHashRaw(ctx)
	if b == nil {
		return BackendStats{RootHash: root}
	}
	b.mu.RLock()
	lastCommitStats := b.lastCommitStats
	b.mu.RUnlock()

	raw := map[string]any{
		"last_commit": lastCommitStats,
	}
	if b.store != nil {
		raw["store_counters"] = b.store.Counters()
		raw["store_metrics"] = b.store.Metrics()
	}
	if b.tree != nil {
		raw["node_cache"] = b.tree.NodeCacheStats()
	}
	return BackendStats{
		RootHash: root,
		Raw:      raw,
	}
}

func (b *DiskBackend) Close() error {
	if b == nil || b.store == nil {
		return nil
	}
	return b.store.Close()
}

type DiskSnapshot struct {
	snapshot *persist.Snapshot
	target   *DiskBackend
}

func (s *DiskSnapshot) AddLeavesClassified(_ context.Context, inputs []LeafInput) (BatchApplyResult, error) {
	if s == nil || s.snapshot == nil {
		return BatchApplyResult{}, fmt.Errorf("disk SMT snapshot not initialized")
	}
	result, err := s.snapshot.AddLeaves(toDiskLeafInputs(inputs))
	return fromDiskApplyResult(result), err
}

func (s *DiskSnapshot) RootHashRaw(context.Context) ([]byte, error) {
	if s == nil || s.snapshot == nil {
		return nil, fmt.Errorf("disk SMT snapshot not initialized")
	}
	return hashBytes(s.snapshot.RootHash()), nil
}

func (s *DiskSnapshot) Fork(context.Context) (Snapshot, error) {
	if s == nil || s.snapshot == nil {
		return nil, fmt.Errorf("disk SMT snapshot not initialized")
	}
	child, err := s.snapshot.Fork()
	if err != nil {
		return nil, err
	}
	return &DiskSnapshot{
		snapshot: child,
		target:   s.target,
	}, nil
}

func (s *DiskSnapshot) SetCommitTarget(_ context.Context, target Backend) error {
	if s == nil || s.snapshot == nil {
		return fmt.Errorf("disk SMT snapshot not initialized")
	}
	diskTarget, ok := target.(*DiskBackend)
	if !ok {
		return fmt.Errorf("disk snapshot commit target must be *DiskBackend, got %T", target)
	}
	if diskTarget == nil || diskTarget.tree == nil {
		return fmt.Errorf("disk snapshot commit target is not initialized")
	}
	if err := s.snapshot.SetCommitTarget(diskTarget.tree); err != nil {
		return err
	}
	s.target = diskTarget
	return nil
}

func (s *DiskSnapshot) Commit(_ context.Context, meta CommitMetadata) error {
	if s == nil || s.snapshot == nil || s.target == nil {
		return fmt.Errorf("disk SMT snapshot not initialized")
	}
	root := hashBytes(s.snapshot.RootHash())
	if meta.RootHash != nil && !bytes.Equal(root, meta.RootHash) {
		return fmt.Errorf("disk SMT snapshot root %x does not match expected root %x", root, meta.RootHash)
	}
	if err := s.snapshot.Commit(meta.BlockNumber); err != nil {
		return err
	}
	s.target.mu.Lock()
	s.target.lastCommitStats = s.snapshot.LastCommitStats()
	s.target.mu.Unlock()
	return nil
}

func (s *DiskSnapshot) Discard(context.Context) {
	if s != nil && s.snapshot != nil {
		s.snapshot.Discard()
	}
}

func toDiskLeafInputs(inputs []LeafInput) []disk.LeafInput {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]disk.LeafInput, len(inputs))
	for i, input := range inputs {
		out[i] = disk.LeafInput{
			Key:   cloneBytes(input.Key),
			Value: cloneBytes(input.Value),
		}
	}
	return out
}

func fromDiskApplyResult(result disk.ApplyResult) BatchApplyResult {
	rejected := make([]RejectedLeaf, len(result.Rejected))
	for i, item := range result.Rejected {
		rejected[i] = RejectedLeaf{
			Index:  item.Index,
			Reason: fromDiskRejectReason(item.Reason),
			Err:    item.Err,
		}
	}
	return BatchApplyResult{
		OldRoot:          hashBytes(result.OldRoot),
		CandidateRoot:    hashBytes(result.CandidateRoot),
		AcceptedIndexes:  append([]int(nil), result.AcceptedIndexes...),
		DuplicateIndexes: append([]int(nil), result.DuplicateIndexes...),
		Rejected:         rejected,
		Stats: BatchApplyStats{
			MaterializedNodes: result.Stats.MaterializedNodes,
			NodeReads:         result.Stats.NodeReads,
			OverlayEntries:    result.Stats.OverlayEntries,
			OverlayBytes:      result.Stats.OverlayBytes,
		},
	}
}

func fromDiskRejectReason(reason disk.RejectReason) RejectReason {
	switch reason {
	case disk.RejectDuplicateCommitted:
		return RejectDuplicateCommitted
	case disk.RejectLeafModification:
		return RejectLeafModification
	case disk.RejectInvalidKey:
		return RejectInvalidKey
	case disk.RejectInternal:
		return RejectInternal
	default:
		return RejectInternal
	}
}

func hashBytes(hash disk.Hash) []byte {
	out := make([]byte, len(hash))
	copy(out, hash[:])
	return out
}
