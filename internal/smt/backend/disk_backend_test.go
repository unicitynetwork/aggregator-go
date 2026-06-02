//go:build rocksdb

package backend

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	rocksstore "github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskBackendRootMatchesLegacySMTAfterReopen(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	expectedRoot := legacyRoot(t, inputs)

	dir := t.TempDir()
	store, err := rocksstore.Open(dir, rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, diskBackendTestOptions())
	require.NoError(t, err)

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), result.OldRoot)
	require.Equal(t, []int{0, 1, 2}, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, expectedRoot, result.CandidateRoot)

	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(7), RootHash: expectedRoot}))
	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.BlockNumber.Uint64())
	require.Equal(t, expectedRoot, state.RootHash)
	require.NoError(t, backend.Close())

	reopenedStore, err := rocksstore.Open(dir, rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	reopenedBackend, err := NewDiskBackend(reopenedStore, diskBackendTestOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, reopenedBackend.Close()) }()

	reopenedRoot, err := reopenedBackend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, reopenedRoot)
	reopenedState, err := reopenedBackend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), reopenedState.BlockNumber.Uint64())
	require.Equal(t, expectedRoot, reopenedState.RootHash)
}

func TestDiskBackendClassifiesDuplicatesInvalidKeysAndModifications(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	duplicate := testLeafInput(1, 11)
	inBatchModification := testLeafInput(1, 12)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []LeafInput{
		duplicate,
		inBatchModification,
		{Key: []byte{1, 2, 3}, Value: []byte("bad-key")},
	})
	require.NoError(t, err)
	require.Equal(t, []int{0}, result.AcceptedIndexes)
	require.Equal(t, []int{1}, result.DuplicateIndexes)
	require.Len(t, result.Rejected, 1)
	require.Equal(t, 2, result.Rejected[0].Index)
	require.Equal(t, RejectInvalidKey, result.Rejected[0].Reason)
	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(1), RootHash: result.CandidateRoot}))

	second, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err = second.AddLeavesClassified(ctx, []LeafInput{duplicate})
	require.NoError(t, err)
	require.Empty(t, result.AcceptedIndexes)
	require.Equal(t, []int{0}, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	second.Discard(ctx)

	third, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	modified := testLeafInput(1, 12)
	result, err = third.AddLeavesClassified(ctx, []LeafInput{modified})
	require.NoError(t, err)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Len(t, result.Rejected, 1)
	require.Equal(t, 0, result.Rejected[0].Index)
	require.Equal(t, RejectLeafModification, result.Rejected[0].Reason)
	third.Discard(ctx)
}

func TestDiskBackendRandomizedBatchParityWithLegacySMT(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	rng := rand.New(rand.NewSource(42))
	allInputs := make([]LeafInput, 0, 200)
	for batch := uint64(0); batch < 5; batch++ {
		inputs := make([]LeafInput, 40)
		for i := range inputs {
			inputs[i] = randomLeafInput(rng, batch*1000+uint64(i))
		}
		allInputs = append(allInputs, inputs...)

		snapshot, err := backend.CreateSnapshot(ctx)
		require.NoError(t, err)
		result, err := snapshot.AddLeavesClassified(ctx, inputs)
		require.NoError(t, err)
		require.NoError(t, result.ValidateAllAccepted(len(inputs)))

		expectedRoot := legacyRoot(t, allInputs)
		require.Equal(t, expectedRoot, result.CandidateRoot)
		require.NoError(t, snapshot.Commit(ctx, CommitMetadata{
			BlockNumber: api.NewBigIntFromUint64(batch + 1),
			RootHash:    expectedRoot,
		}))

		actualRoot, err := backend.RootHashRaw(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedRoot, actualRoot)
	}
}

func TestDiskBackendEmptyBatch(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), result.OldRoot)
	require.Equal(t, emptyRoot(t), result.CandidateRoot)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Zero(t, result.Stats.MaterializedNodes)
	require.Zero(t, result.Stats.OverlayEntries)
	snapshot.Discard(ctx)
}

func TestDiskSnapshotCommitRejectsRootMismatch(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

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

func TestDiskBackendInclusionCertMatchesMemoryAfterCommit(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	expectedRoot := legacyRoot(t, inputs)
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(1), RootHash: result.CandidateRoot}))

	cert, err := backend.GetInclusionCert(ctx, inputs[1].Key)
	require.NoError(t, err)
	require.NoError(t, cert.Verify(inputs[1].Key, inputs[1].Value, expectedRoot, api.InclusionProofV2HashAlgorithm))

	memoryCert := legacyCert(t, inputs, inputs[1].Key)
	require.Equal(t, memoryCert.Bitmap, cert.Bitmap)
	require.Equal(t, memoryCert.Siblings, cert.Siblings)
}

func TestDiskSnapshotForkCommitChain(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	first, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	firstLeaf := testLeafInput(1, 11)
	firstResult, err := first.AddLeavesClassified(ctx, []LeafInput{firstLeaf})
	require.NoError(t, err)

	second, err := first.Fork(ctx)
	require.NoError(t, err)
	secondLeaf := testLeafInput(2, 22)
	secondResult, err := second.AddLeavesClassified(ctx, []LeafInput{secondLeaf})
	require.NoError(t, err)

	require.NoError(t, first.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(1), RootHash: firstResult.CandidateRoot}))
	require.NoError(t, second.SetCommitTarget(ctx, backend))
	expectedRoot := legacyRoot(t, []LeafInput{firstLeaf, secondLeaf})
	require.Equal(t, expectedRoot, secondResult.CandidateRoot)
	require.NoError(t, second.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(2), RootHash: expectedRoot}))

	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}

func TestDiskSnapshotLifecycleAndStaleRootGuards(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

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

func TestDiskSnapshotStaleSiblingCommitFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

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

func TestDiskSnapshotSetCommitTargetRejectsNonDiskBackend(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.ErrorContains(t, snapshot.SetCommitTarget(ctx, fakeBackend{}), "must be *DiskBackend")
}

func TestDiskBackendStatsExposeRootAndStoreSignals(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackend(t)
	defer func() { require.NoError(t, backend.Close()) }()

	stats := backend.Stats(ctx)
	require.Equal(t, emptyRoot(t), stats.RootHash)
	require.Contains(t, stats.Raw, "store_counters")
	require.Contains(t, stats.Raw, "store_metrics")
	require.Contains(t, stats.Raw, "node_cache")
	require.Contains(t, stats.Raw, "last_commit")
}

func newTestDiskBackend(t *testing.T) *DiskBackend {
	t.Helper()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, diskBackendTestOptions())
	require.NoError(t, err)
	return backend
}

func diskBackendTestOptions() persist.Options {
	return persist.DefaultOptions()
}

func randomLeafInput(rng *rand.Rand, id uint64) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	value := make([]byte, 32)
	_, _ = rng.Read(key)
	_, _ = rng.Read(value)
	for i := 0; i < 8; i++ {
		key[len(key)-1-i] = byte(id >> (8 * i))
	}
	return LeafInput{Key: key, Value: value}
}

func legacyCert(t *testing.T, inputs []LeafInput, key []byte) *api.InclusionCert {
	t.Helper()
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	leaves := make([]*smt.Leaf, len(inputs))
	for i, input := range inputs {
		path, err := api.FixedBytesToPath(input.Key, api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		leaves[i] = smt.NewLeaf(path, input.Value)
	}
	require.NoError(t, tree.AddLeaves(leaves))
	cert, err := tree.GetInclusionCert(key)
	require.NoError(t, err)
	return cert
}
