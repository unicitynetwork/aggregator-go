package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type Backend interface {
	KeyLength() int
	RootHashRaw(ctx context.Context) ([]byte, error)
	CommittedState(ctx context.Context) (CommittedState, error)
	CreateSnapshot(ctx context.Context) (Snapshot, error)
	GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error)
	Stats(ctx context.Context) BackendStats
	Close() error
}

type PrecomputedProofResponse struct {
	StateID  api.StateID
	Response *api.GetInclusionProofResponseV2
}

type PrecomputedProofReader interface {
	GetPrecomputedProofResponse(ctx context.Context, stateID api.StateID) (*api.GetInclusionProofResponseV2, bool, error)
}

type PrecomputedProofWriter interface {
	StorePrecomputedProofResponses(ctx context.Context, responses []PrecomputedProofResponse) error
}

type BatchInclusionCertBackend interface {
	GetInclusionCerts(ctx context.Context, keys [][]byte) ([]*api.InclusionCert, error)
}

var ErrPublishedProofRootChanged = errors.New("published proof root changed")
var ErrPublishedProofLeafNotFound = errors.New("published proof leaf not found")

type PublishedProofReader interface {
	PublishedRoot(ctx context.Context) ([]byte, error)
	GetPublishedInclusionCertAtRoot(ctx context.Context, expectedRoot []byte, key []byte) (*api.InclusionCert, error)
}

type PreparedProofView interface {
	Publish(ctx context.Context) error
	Discard(ctx context.Context)
}

type ProofViewPreparingSnapshot interface {
	CommitAndPrepareProofView(ctx context.Context, meta CommitMetadata) (PreparedProofView, error)
}

type CommitTimingProvider interface {
	LastCommitTiming() CommitTiming
}

type CommitTiming struct {
	CollectDuration     time.Duration
	TombstoneDuration   time.Duration
	BatchBuildDuration  time.Duration
	RootHashDuration    time.Duration
	EngineWriteDuration time.Duration
	CacheUpdateDuration time.Duration
	NodeWrites          int
	NodeDeletes         int
}

type ProofViewPublisher interface {
	// RefreshPublishedProofView publishes the backend's current committed root as
	// the served proof view. expectedRoot, when non-nil, must equal the backend's
	// current root or publication is refused; this forces callers to prove the
	// root they publish corresponds to finalized history. Pass nil only when the
	// committed state is already known to be finalized (startup verify, open).
	RefreshPublishedProofView(ctx context.Context, expectedRoot []byte) error
}

type DiskBacked interface {
	IsDiskBackedSMT() bool
}

type Snapshot interface {
	AddLeavesClassified(ctx context.Context, leaves []LeafInput) (BatchApplyResult, error)
	RootHashRaw(ctx context.Context) ([]byte, error)
	Fork(ctx context.Context) (Snapshot, error)
	SetCommitTarget(ctx context.Context, target Backend) error
	Commit(ctx context.Context, meta CommitMetadata) error
	Discard(ctx context.Context)
}

type LeafInput struct {
	Key   []byte
	Value []byte
}

type BatchApplyResult struct {
	// OldRoot is the backend root when this snapshot was created. It is not
	// advanced by multiple AddLeavesClassified calls on the same snapshot.
	OldRoot       []byte
	CandidateRoot []byte

	AcceptedIndexes  []int
	DuplicateIndexes []int
	Rejected         []RejectedLeaf

	Stats         BatchApplyStats
	WitnessHandle any
}

func (r BatchApplyResult) ValidateAllAccepted(expected int) error {
	if len(r.Rejected) > 0 {
		return fmt.Errorf("rejected %d leaves", len(r.Rejected))
	}
	if len(r.DuplicateIndexes) > 0 {
		return fmt.Errorf("duplicate %d leaves", len(r.DuplicateIndexes))
	}
	if len(r.AcceptedIndexes) != expected {
		return fmt.Errorf("accepted %d leaves, expected %d", len(r.AcceptedIndexes), expected)
	}
	return nil
}

type RejectedLeaf struct {
	Index  int
	Reason RejectReason
	Err    error
}

type RejectReason string

const (
	RejectDuplicateInBatch   RejectReason = "duplicate_in_batch"
	RejectDuplicateCommitted RejectReason = "duplicate_committed"
	RejectLeafModification   RejectReason = "leaf_modification"
	RejectWrongShard         RejectReason = "wrong_shard"
	RejectInvalidKey         RejectReason = "invalid_key"
	RejectInvalidValue       RejectReason = "invalid_value"
	RejectInternal           RejectReason = "internal"
)

type BatchApplyStats struct {
	MaterializedNodes int
	NodeReads         int
	BatchReads        int
	OverlayEntries    int
	OverlayBytes      int64
}

type BackendStats struct {
	RootHash []byte
	// Raw is for in-process backend diagnostics. Metrics and debug endpoints
	// should extract typed fields explicitly instead of JSON-serializing it.
	Raw map[string]any
}

type CommittedState struct {
	BlockNumber *api.BigInt
	RootHash    []byte
}

type CommitMetadata struct {
	BlockNumber *api.BigInt
	RootHash    []byte
}

type MemoryBackend struct {
	tree        *smt.ThreadSafeSMT
	blockNumber *api.BigInt
}

func NewMemoryBackend(tree *smt.ThreadSafeSMT) *MemoryBackend {
	return &MemoryBackend{tree: tree}
}

func (b *MemoryBackend) KeyLength() int {
	if b == nil || b.tree == nil {
		return 0
	}
	return b.tree.GetKeyLength()
}

func (b *MemoryBackend) RootHashRaw(context.Context) ([]byte, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("memory SMT backend not initialized")
	}
	return cloneBytes(b.tree.GetRootHashRaw()), nil
}

func (b *MemoryBackend) CommittedState(ctx context.Context) (CommittedState, error) {
	root, err := b.RootHashRaw(ctx)
	if err != nil {
		return CommittedState{}, err
	}
	return CommittedState{
		BlockNumber: cloneBigInt(b.blockNumber),
		RootHash:    root,
	}, nil
}

func (b *MemoryBackend) CreateSnapshot(ctx context.Context) (Snapshot, error) {
	root, err := b.RootHashRaw(ctx)
	if err != nil {
		return nil, err
	}
	return &MemorySnapshot{
		snapshot: b.tree.CreateSnapshot(),
		target:   b,
		baseRoot: root,
	}, nil
}

func (b *MemoryBackend) GetInclusionCert(_ context.Context, key []byte) (*api.InclusionCert, error) {
	if b == nil || b.tree == nil {
		return nil, fmt.Errorf("memory SMT backend not initialized")
	}
	return b.tree.GetInclusionCert(key)
}

func (b *MemoryBackend) Stats(ctx context.Context) BackendStats {
	root, _ := b.RootHashRaw(ctx)
	raw := map[string]any{}
	if b != nil && b.tree != nil {
		raw = b.tree.GetStats()
	}
	return BackendStats{
		RootHash: root,
		Raw:      raw,
	}
}

func (b *MemoryBackend) Close() error {
	return nil
}

func (b *MemoryBackend) legacyTree() *smt.ThreadSafeSMT {
	if b == nil {
		return nil
	}
	return b.tree
}

func (b *MemoryBackend) setCommittedBlockNumber(blockNumber *api.BigInt) {
	if blockNumber == nil {
		return
	}
	b.blockNumber = cloneBigInt(blockNumber)
}

func (b *MemoryBackend) SetCommittedBlockNumber(blockNumber *api.BigInt) {
	b.setCommittedBlockNumber(blockNumber)
}

type MemorySnapshot struct {
	snapshot *smt.ThreadSafeSmtSnapshot
	target   *MemoryBackend
	baseRoot []byte
	closed   bool
}

func (s *MemorySnapshot) AddLeavesClassified(ctx context.Context, inputs []LeafInput) (BatchApplyResult, error) {
	if s == nil || s.snapshot == nil || s.target == nil {
		return BatchApplyResult{}, fmt.Errorf("memory SMT snapshot not initialized")
	}
	if s.closed {
		return BatchApplyResult{}, fmt.Errorf("memory SMT snapshot is closed")
	}
	result := BatchApplyResult{
		OldRoot:          cloneBytes(s.baseRoot),
		AcceptedIndexes:  make([]int, 0, len(inputs)),
		DuplicateIndexes: make([]int, 0),
		Rejected:         make([]RejectedLeaf, 0),
	}
	if len(inputs) == 0 {
		root, err := s.RootHashRaw(ctx)
		if err != nil {
			return result, err
		}
		result.CandidateRoot = root
		return result, nil
	}

	keyLength := s.target.KeyLength()
	leaves := make([]*smt.Leaf, 0, len(inputs))
	inputIndexes := make([]int, 0, len(inputs))
	seen := make(map[string]int, len(inputs))

	for i, input := range inputs {
		key := cloneBytes(input.Key)
		if _, ok := seen[string(key)]; ok {
			result.DuplicateIndexes = append(result.DuplicateIndexes, i)
			continue
		}
		seen[string(key)] = i

		path, err := api.FixedBytesToPath(key, keyLength)
		if err != nil {
			result.Rejected = append(result.Rejected, RejectedLeaf{
				Index:  i,
				Reason: RejectInvalidKey,
				Err:    err,
			})
			continue
		}

		leaves = append(leaves, smt.NewLeaf(path, cloneBytes(input.Value)))
		inputIndexes = append(inputIndexes, i)
	}

	classified := s.snapshot.AddLeavesClassified(leaves)
	for _, idx := range classified.AddedIndexes {
		inputIdx := inputIndexes[idx]
		result.AcceptedIndexes = append(result.AcceptedIndexes, inputIdx)
	}
	for _, idx := range classified.DuplicateIndexes {
		inputIdx := inputIndexes[idx]
		result.DuplicateIndexes = append(result.DuplicateIndexes, inputIdx)
	}
	for _, rejected := range classified.Rejected {
		inputIdx := inputIndexes[rejected.Index]
		result.Rejected = append(result.Rejected, RejectedLeaf{
			Index:  inputIdx,
			Reason: classifyRejectReason(rejected.Err),
			Err:    rejected.Err,
		})
	}

	root, err := s.RootHashRaw(ctx)
	if err != nil {
		return result, err
	}
	result.CandidateRoot = root
	return result, nil
}

func (s *MemorySnapshot) RootHashRaw(context.Context) ([]byte, error) {
	if s == nil || s.snapshot == nil {
		return nil, fmt.Errorf("memory SMT snapshot not initialized")
	}
	return cloneBytes(s.snapshot.GetRootHashRaw()), nil
}

func (s *MemorySnapshot) Fork(ctx context.Context) (Snapshot, error) {
	if s == nil || s.snapshot == nil {
		return nil, fmt.Errorf("memory SMT snapshot not initialized")
	}
	if s.closed {
		return nil, fmt.Errorf("memory SMT snapshot is closed")
	}
	root, err := s.RootHashRaw(ctx)
	if err != nil {
		return nil, err
	}
	// Memory snapshots can represent deeper copy-on-write fork chains, while
	// disk snapshots support one uncommitted parent overlay. Callers should
	// follow the shared one-parent precollector invariant for both backends.
	return &MemorySnapshot{
		snapshot: s.snapshot.CreateSnapshot(),
		target:   s.target,
		baseRoot: root,
	}, nil
}

func (s *MemorySnapshot) SetCommitTarget(ctx context.Context, target Backend) error {
	if s == nil || s.snapshot == nil {
		return fmt.Errorf("memory SMT snapshot not initialized")
	}
	if s.closed {
		return fmt.Errorf("memory SMT snapshot is closed")
	}
	memoryTarget, ok := target.(*MemoryBackend)
	if !ok {
		return fmt.Errorf("memory snapshot commit target must be *MemoryBackend, got %T", target)
	}
	legacyTarget := memoryTarget.legacyTree()
	if legacyTarget == nil {
		return fmt.Errorf("memory snapshot commit target is not initialized")
	}
	targetRoot, err := memoryTarget.RootHashRaw(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(targetRoot, s.baseRoot) {
		return fmt.Errorf("memory snapshot commit target root %x does not match snapshot base root %x", targetRoot, s.baseRoot)
	}
	s.snapshot.SetCommitTarget(legacyTarget)
	s.target = memoryTarget
	return nil
}

func (s *MemorySnapshot) Commit(ctx context.Context, meta CommitMetadata) error {
	if s == nil || s.snapshot == nil || s.target == nil || s.target.legacyTree() == nil {
		return fmt.Errorf("memory SMT snapshot not initialized")
	}
	if s.closed {
		return fmt.Errorf("memory SMT snapshot is closed")
	}
	targetRoot, err := s.target.RootHashRaw(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(targetRoot, s.baseRoot) {
		return fmt.Errorf("memory snapshot commit base root %x does not match current root %x", s.baseRoot, targetRoot)
	}
	root := s.snapshot.GetRootHashRaw()
	if meta.RootHash != nil && !bytes.Equal(root, meta.RootHash) {
		return fmt.Errorf("memory SMT snapshot root %x does not match expected root %x", root, meta.RootHash)
	}
	s.snapshot.Commit(s.target.legacyTree())
	s.target.setCommittedBlockNumber(meta.BlockNumber)
	s.closed = true
	return nil
}

// Discard releases snapshot resources. The legacy in-memory snapshot has
// nothing to release.
func (s *MemorySnapshot) Discard(context.Context) {
	if s != nil {
		s.closed = true
	}
}

func classifyRejectReason(err error) RejectReason {
	switch {
	case errors.Is(err, smt.ErrDuplicateLeaf):
		return RejectDuplicateCommitted
	case errors.Is(err, smt.ErrLeafModification):
		return RejectLeafModification
	case errors.Is(err, smt.ErrWrongShard):
		return RejectWrongShard
	case errors.Is(err, smt.ErrKeyLength):
		return RejectInvalidKey
	case errors.Is(err, smt.ErrInvalidChildHashLength):
		return RejectInvalidValue
	default:
		return RejectInternal
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneBigInt(in *api.BigInt) *api.BigInt {
	if in == nil {
		return nil
	}
	out := api.NewBigInt(nil)
	out.Set(in.Int)
	return out
}
