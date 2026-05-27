package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestMemoryBackendRootMatchesLegacySMT(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}

	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)

	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), result.OldRoot)
	require.Equal(t, []int{0, 1, 2}, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Zero(t, result.Stats.OverlayEntries)

	expectedRoot := legacyRoot(t, inputs)
	require.Equal(t, expectedRoot, result.CandidateRoot)

	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(7), RootHash: expectedRoot}))
	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.BlockNumber.Uint64())
	require.Equal(t, expectedRoot, state.RootHash)

	secondSnapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, secondSnapshot.Commit(ctx, CommitMetadata{}))
	state, err = backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.BlockNumber.Uint64())
}

func TestMemoryBackendClassifiesDuplicatesAndInvalidKeys(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)

	duplicate := testLeafInput(1, 11)
	result, err := snapshot.AddLeavesClassified(ctx, []LeafInput{
		duplicate,
		duplicate,
		{Key: []byte{1, 2, 3}, Value: []byte("bad-key")},
	})
	require.NoError(t, err)

	require.Equal(t, []int{0}, result.AcceptedIndexes)
	require.Equal(t, []int{1}, result.DuplicateIndexes)
	require.Len(t, result.Rejected, 1)
	require.Equal(t, 2, result.Rejected[0].Index)
	require.Equal(t, RejectInvalidKey, result.Rejected[0].Reason)
}

func TestMemoryBackendEmptyBatch(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)

	result, err := snapshot.AddLeavesClassified(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), result.OldRoot)
	require.Equal(t, emptyRoot(t), result.CandidateRoot)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Zero(t, result.Stats.OverlayEntries)
}

func TestMemorySnapshotCommitRejectsRootMismatch(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)

	result, err := snapshot.AddLeavesClassified(ctx, []LeafInput{testLeafInput(1, 11)})
	require.NoError(t, err)
	wrongRoot := cloneBytes(result.CandidateRoot)
	wrongRoot[0] ^= 0xff

	require.ErrorContains(t, snapshot.Commit(ctx, CommitMetadata{RootHash: wrongRoot}), "does not match expected root")
	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), actualRoot)
}

func TestMemorySnapshotForkCommitChain(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))

	first, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	firstLeaf := testLeafInput(1, 11)
	_, err = first.AddLeavesClassified(ctx, []LeafInput{firstLeaf})
	require.NoError(t, err)

	second, err := first.Fork(ctx)
	require.NoError(t, err)
	secondLeaf := testLeafInput(2, 22)
	_, err = second.AddLeavesClassified(ctx, []LeafInput{secondLeaf})
	require.NoError(t, err)

	require.NoError(t, first.Commit(ctx, CommitMetadata{}))
	require.NoError(t, second.SetCommitTarget(ctx, backend))
	require.NoError(t, second.Commit(ctx, CommitMetadata{}))

	expectedRoot := legacyRoot(t, []LeafInput{firstLeaf, secondLeaf})
	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}

func TestMemorySnapshotLifecycleAndStaleRootGuards(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))

	parent, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	_, err = parent.AddLeavesClassified(ctx, []LeafInput{testLeafInput(1, 11)})
	require.NoError(t, err)

	child, err := parent.Fork(ctx)
	require.NoError(t, err)
	require.ErrorContains(t, child.SetCommitTarget(ctx, backend), "does not match snapshot base root")

	require.NoError(t, parent.Commit(ctx, CommitMetadata{}))
	require.ErrorContains(t, parent.Commit(ctx, CommitMetadata{}), "snapshot is closed")
	_, err = parent.Fork(ctx)
	require.ErrorContains(t, err, "snapshot is closed")

	require.NoError(t, child.SetCommitTarget(ctx, backend))
	require.NoError(t, child.Commit(ctx, CommitMetadata{}))
	require.ErrorContains(t, child.Commit(ctx, CommitMetadata{}), "snapshot is closed")
}

func TestMemorySnapshotStaleSiblingCommitFails(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))

	first, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	_, err = first.AddLeavesClassified(ctx, []LeafInput{testLeafInput(1, 11)})
	require.NoError(t, err)

	second, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	_, err = second.AddLeavesClassified(ctx, []LeafInput{testLeafInput(2, 22)})
	require.NoError(t, err)

	require.NoError(t, first.Commit(ctx, CommitMetadata{}))
	require.ErrorContains(t, second.Commit(ctx, CommitMetadata{}), "base root")
}

func TestMemorySnapshotSetCommitTargetRejectsNonMemoryBackend(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)

	require.Error(t, snapshot.SetCommitTarget(ctx, fakeBackend{}))
}

func TestBatchApplyResultValidateAllAccepted(t *testing.T) {
	require.NoError(t, BatchApplyResult{AcceptedIndexes: []int{0, 1}}.ValidateAllAccepted(2))
	require.Error(t, BatchApplyResult{Rejected: []RejectedLeaf{{Index: 0}}}.ValidateAllAccepted(1))
	require.Error(t, BatchApplyResult{DuplicateIndexes: []int{0}}.ValidateAllAccepted(1))
	require.Error(t, BatchApplyResult{AcceptedIndexes: []int{0}}.ValidateAllAccepted(2))
}

func testLeafInput(keyByte byte, valueByte byte) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[len(key)-1] = keyByte
	value := make([]byte, 32)
	value[len(value)-1] = valueByte
	return LeafInput{Key: key, Value: value}
}

func legacyRoot(t *testing.T, inputs []LeafInput) []byte {
	t.Helper()
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	leaves := make([]*smt.Leaf, len(inputs))
	for i, input := range inputs {
		path, err := api.FixedBytesToPath(input.Key, api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		leaves[i] = smt.NewLeaf(path, input.Value)
	}
	require.NoError(t, tree.AddLeaves(leaves))
	return tree.GetRootHashRaw()
}

func emptyRoot(t *testing.T) []byte {
	t.Helper()
	return smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits).GetRootHashRaw()
}

type fakeBackend struct{}

func (fakeBackend) KeyLength() int { return api.StateTreeKeyLengthBits }

func (fakeBackend) RootHashRaw(context.Context) ([]byte, error) { return nil, nil }

func (fakeBackend) CommittedState(context.Context) (CommittedState, error) {
	return CommittedState{}, nil
}

func (fakeBackend) CreateSnapshot(context.Context) (Snapshot, error) { return nil, nil }

func (fakeBackend) GetInclusionCert(context.Context, []byte) (*api.InclusionCert, error) {
	return nil, nil
}

func (fakeBackend) Stats(context.Context) BackendStats { return BackendStats{} }

func (fakeBackend) Close() error { return nil }
