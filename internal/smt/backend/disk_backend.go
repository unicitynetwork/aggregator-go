package backend

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type DiskBackend struct {
	store storage.Store
	tree  *persist.Tree

	mu              sync.Mutex
	lastCommitStats persist.CommitStats
	snapshotWarning sync.Once

	proofViewMu sync.RWMutex
	proofView   *diskProofView
}

func NewDiskBackend(store storage.Store, opts persist.Options) (*DiskBackend, error) {
	tree, err := persist.OpenWithOptions(store, opts)
	if err != nil {
		return nil, err
	}
	backend := &DiskBackend{
		store: store,
		tree:  tree,
	}
	if err := backend.RefreshPublishedProofView(context.Background(), nil); err != nil {
		return nil, err
	}
	return backend, nil
}

func (b *DiskBackend) KeyLength() int {
	return api.StateTreeKeyLengthBits
}

func (b *DiskBackend) IsDiskBackedSMT() bool {
	return true
}

func (b *DiskBackend) RootHashRaw(context.Context) ([]byte, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return hashBytes(b.tree.RootHash()), nil
}

func (b *DiskBackend) CommittedState(context.Context) (CommittedState, error) {
	if b == nil || b.store == nil {
		return CommittedState{}, fmt.Errorf("disk SMT backend not initialized")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
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
	b.mu.Lock()
	defer b.mu.Unlock()
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
	snapshotter, ok := b.store.(storage.ReadSnapshotter)
	if !ok {
		b.snapshotWarning.Do(func() {
			slog.Warn("Disk SMT backend store does not implement read snapshots; inclusion proofs use live store reads",
				"component", "smt_backend",
				"backend", "disk",
			)
		})
		return b.tree.GetInclusionCert(key)
	}
	openSnapshot := func() (storage.ReadStore, func(), error) {
		reader, closeSnapshot, err := snapshotter.NewReadSnapshot()
		if err != nil {
			return nil, nil, err
		}
		metrics.SMTDiskProofSnapshotsActive.Inc()
		start := time.Now()
		return reader, func() {
			if closeSnapshot != nil {
				closeSnapshot()
			}
			metrics.SMTDiskProofSnapshotsActive.Dec()
			metrics.SMTDiskProofSnapshotHoldDuration.Observe(time.Since(start).Seconds())
		}, nil
	}
	return b.tree.GetInclusionCertWithReadSnapshot(key, openSnapshot)
}

func (b *DiskBackend) GetPublishedInclusionCert(_ context.Context, key []byte) ([]byte, *api.InclusionCert, error) {
	if b == nil {
		return nil, nil, fmt.Errorf("disk SMT backend not initialized")
	}
	b.proofViewMu.RLock()
	defer b.proofViewMu.RUnlock()

	if b.proofView == nil {
		return nil, nil, fmt.Errorf("disk SMT proof view is not initialized")
	}
	if b.proofView.root == disk.EmptyRootHash() {
		return hashBytes(b.proofView.root), nil, fmt.Errorf("disk SMT published proof view is empty")
	}
	cert, err := persist.BuildInclusionCert(b.proofView.root, b.proofView.reader, key)
	if err != nil {
		return nil, nil, err
	}
	return hashBytes(b.proofView.root), cert, nil
}

func (b *DiskBackend) GetInclusionCerts(_ context.Context, keys [][]byte) ([]*api.InclusionCert, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("disk SMT backend not initialized")
	}
	snapshotter, ok := b.store.(storage.ReadSnapshotter)
	if !ok {
		b.snapshotWarning.Do(func() {
			slog.Warn("Disk SMT backend store does not implement read snapshots; inclusion proofs use live store reads",
				"component", "smt_backend",
				"backend", "disk",
			)
		})
		certs := make([]*api.InclusionCert, len(keys))
		for i, key := range keys {
			cert, err := b.tree.GetInclusionCert(key)
			if err != nil {
				return nil, err
			}
			certs[i] = cert
		}
		return certs, nil
	}
	openSnapshot := func() (storage.ReadStore, func(), error) {
		reader, closeSnapshot, err := snapshotter.NewReadSnapshot()
		if err != nil {
			return nil, nil, err
		}
		metrics.SMTDiskProofSnapshotsActive.Inc()
		start := time.Now()
		return reader, func() {
			if closeSnapshot != nil {
				closeSnapshot()
			}
			metrics.SMTDiskProofSnapshotsActive.Dec()
			metrics.SMTDiskProofSnapshotHoldDuration.Observe(time.Since(start).Seconds())
		}, nil
	}
	return b.tree.GetInclusionCertsWithReadSnapshot(keys, openSnapshot)
}

func (b *DiskBackend) StorePrecomputedProofResponses(_ context.Context, responses []PrecomputedProofResponse) error {
	if len(responses) == 0 {
		return nil
	}
	proofStore, ok := b.store.(storage.ProofResponseStore)
	if !ok {
		return nil
	}
	writes := make([]storage.ProofResponseWrite, len(responses))
	for i, response := range responses {
		if response.Response == nil {
			return fmt.Errorf("nil precomputed proof response at index %d", i)
		}
		wire, err := bfttypes.Cbor.Marshal(response.Response)
		if err != nil {
			return fmt.Errorf("marshal precomputed proof response %d: %w", i, err)
		}
		writes[i] = storage.ProofResponseWrite{
			StateID:  response.StateID,
			Response: wire,
		}
	}
	return proofStore.StoreProofResponses(writes)
}

func (b *DiskBackend) GetPrecomputedProofResponse(_ context.Context, stateID api.StateID) (*api.GetInclusionProofResponseV2, bool, error) {
	proofStore, ok := b.store.(storage.ProofResponseStore)
	if !ok {
		return nil, false, nil
	}
	wire, ok, err := proofStore.GetProofResponse(stateID)
	if err != nil || !ok {
		return nil, ok, err
	}
	var response api.GetInclusionProofResponseV2
	if err := bfttypes.Cbor.Unmarshal(wire, &response); err != nil {
		return nil, false, fmt.Errorf("decode precomputed proof response: %w", err)
	}
	return &response, true, nil
}

func (b *DiskBackend) Stats(ctx context.Context) BackendStats {
	root, _ := b.RootHashRaw(ctx)
	if b == nil {
		return BackendStats{RootHash: root}
	}
	b.mu.Lock()
	lastCommitStats := b.lastCommitStats
	b.mu.Unlock()

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
	b.proofViewMu.Lock()
	if b.proofView != nil {
		b.proofView.close()
		b.proofView = nil
	}
	b.proofViewMu.Unlock()
	return b.store.Close()
}

func (b *DiskBackend) RefreshPublishedProofView(_ context.Context, expectedRoot []byte) error {
	if b == nil || b.tree == nil {
		return fmt.Errorf("disk SMT backend not initialized")
	}
	b.mu.Lock()
	root := b.tree.RootHash()
	if expectedRoot != nil && !bytes.Equal(hashBytes(root), expectedRoot) {
		b.mu.Unlock()
		return fmt.Errorf("refusing to publish proof view: disk SMT root %s does not match expected finalized root %s",
			api.HexBytes(hashBytes(root)).String(), api.HexBytes(expectedRoot).String())
	}
	view, err := b.prepareProofViewLocked(root)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	old := b.swapProofView(view)
	b.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

func (b *DiskBackend) prepareProofViewLocked(root disk.Hash) (*diskProofView, error) {
	view := &diskProofView{root: root}
	if root == disk.EmptyRootHash() {
		return view, nil
	}
	snapshotter, ok := b.store.(storage.ReadSnapshotter)
	if !ok {
		return nil, fmt.Errorf("disk SMT backend store does not implement read snapshots")
	}
	reader, closeSnapshot, err := snapshotter.NewReadSnapshot()
	if err != nil {
		return nil, err
	}
	metrics.SMTDiskProofSnapshotsActive.Inc()
	view.reader = reader
	view.closeSnapshot = closeSnapshot
	view.openedAt = time.Now()
	return view, nil
}

func (b *DiskBackend) publishProofView(view *diskProofView) {
	old := b.swapProofView(view)
	if old != nil {
		old.close()
	}
}

func (b *DiskBackend) swapProofView(view *diskProofView) *diskProofView {
	b.proofViewMu.Lock()
	old := b.proofView
	b.proofView = view
	b.proofViewMu.Unlock()
	return old
}

type diskProofView struct {
	root          disk.Hash
	reader        storage.ReadStore
	closeSnapshot func()
	openedAt      time.Time
	once          sync.Once
}

func (v *diskProofView) close() {
	if v == nil {
		return
	}
	v.once.Do(func() {
		if v.closeSnapshot != nil {
			v.closeSnapshot()
			metrics.SMTDiskProofSnapshotsActive.Dec()
			metrics.SMTDiskProofSnapshotHoldDuration.Observe(time.Since(v.openedAt).Seconds())
		}
	})
}

type preparedDiskProofView struct {
	target *DiskBackend
	view   *diskProofView
}

func (p *preparedDiskProofView) Publish(context.Context) error {
	if p == nil || p.target == nil || p.view == nil {
		return fmt.Errorf("disk SMT prepared proof view is not initialized")
	}
	// Refuse to publish if the disk advanced since prepare; that would roll the served view backward.
	p.target.mu.Lock()
	current := p.target.tree.RootHash()
	if current != p.view.root {
		p.target.mu.Unlock()
		return fmt.Errorf("refusing to publish prepared proof view: disk SMT root advanced to %x since the view was prepared at %x", current, p.view.root)
	}
	old := p.target.swapProofView(p.view)
	p.view = nil
	p.target.mu.Unlock()
	if old != nil {
		old.close()
	}
	return nil
}

func (p *preparedDiskProofView) Discard(context.Context) {
	if p != nil && p.view != nil {
		p.view.close()
		p.view = nil
	}
}

type DiskSnapshot struct {
	snapshot *persist.Snapshot
	target   *DiskBackend
}

func (s *DiskSnapshot) AddLeavesClassified(_ context.Context, inputs []LeafInput) (BatchApplyResult, error) {
	if s == nil || s.snapshot == nil || s.target == nil {
		return BatchApplyResult{}, fmt.Errorf("disk SMT snapshot not initialized")
	}
	s.target.mu.Lock()
	defer s.target.mu.Unlock()
	result, err := s.snapshot.AddLeaves(toDiskLeafInputs(inputs))
	if err != nil {
		return BatchApplyResult{}, err
	}
	return fromDiskApplyResult(result)
}

func (s *DiskSnapshot) RootHashRaw(context.Context) ([]byte, error) {
	if s == nil || s.snapshot == nil {
		return nil, fmt.Errorf("disk SMT snapshot not initialized")
	}
	return hashBytes(s.snapshot.RootHash()), nil
}

func (s *DiskSnapshot) Fork(context.Context) (Snapshot, error) {
	if s == nil || s.snapshot == nil || s.target == nil {
		return nil, fmt.Errorf("disk SMT snapshot not initialized")
	}
	s.target.mu.Lock()
	defer s.target.mu.Unlock()
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
	diskTarget.mu.Lock()
	defer diskTarget.mu.Unlock()
	if err := s.snapshot.SetCommitTarget(diskTarget.tree); err != nil {
		return err
	}
	s.target = diskTarget
	return nil
}

func (s *DiskSnapshot) Commit(_ context.Context, meta CommitMetadata) error {
	_, err := s.commit(meta, false)
	return err
}

func (s *DiskSnapshot) CommitAndPrepareProofView(_ context.Context, meta CommitMetadata) (PreparedProofView, error) {
	return s.commit(meta, true)
}

func (s *DiskSnapshot) commit(meta CommitMetadata, prepareProofView bool) (PreparedProofView, error) {
	if s == nil || s.snapshot == nil || s.target == nil {
		return nil, fmt.Errorf("disk SMT snapshot not initialized")
	}
	s.target.mu.Lock()
	defer s.target.mu.Unlock()
	root := hashBytes(s.snapshot.RootHash())
	if meta.RootHash != nil && !bytes.Equal(root, meta.RootHash) {
		return nil, fmt.Errorf("disk SMT snapshot root %x does not match expected root %x", root, meta.RootHash)
	}
	if err := s.snapshot.Commit(meta.BlockNumber); err != nil {
		return nil, err
	}
	s.target.lastCommitStats = s.snapshot.LastCommitStats()
	if !prepareProofView {
		return nil, nil
	}
	view, err := s.target.prepareProofViewLocked(s.snapshot.RootHash())
	if err != nil {
		return nil, err
	}
	return &preparedDiskProofView{target: s.target, view: view}, nil
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

func fromDiskApplyResult(result disk.ApplyResult) (BatchApplyResult, error) {
	rejected := make([]RejectedLeaf, len(result.Rejected))
	for i, item := range result.Rejected {
		if item.Index < 0 {
			return BatchApplyResult{}, fmt.Errorf("disk SMT backend: invalid rejected leaf index %d: %w", item.Index, item.Err)
		}
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
	}, nil
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
