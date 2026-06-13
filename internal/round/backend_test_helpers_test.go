package round

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func testMemoryBackend(tree *smt.ThreadSafeSMT) smtbackend.Backend {
	return smtbackend.NewMemoryBackend(tree)
}

func testBackendSnapshot(t testing.TB, ctx context.Context, backend smtbackend.Backend) smtbackend.Snapshot {
	t.Helper()
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	return snapshot
}

func testRMSnapshot(t testing.TB, ctx context.Context, rm *RoundManager) smtbackend.Snapshot {
	t.Helper()
	return testBackendSnapshot(t, ctx, rm.smtBackend)
}

func testSnapshotRootRaw(t testing.TB, ctx context.Context, snapshot smtbackend.Snapshot) []byte {
	t.Helper()
	root, err := snapshot.RootHashRaw(ctx)
	require.NoError(t, err)
	return root
}

func testSnapshotRootHex(t testing.TB, ctx context.Context, snapshot smtbackend.Snapshot) string {
	t.Helper()
	return api.HexBytes(testSnapshotRootRaw(t, ctx, snapshot)).String()
}

func testSMTRawRootHex(tree *smt.ThreadSafeSMT) string {
	return api.HexBytes(tree.GetRootHashRaw()).String()
}

func testCommitSnapshot(t testing.TB, ctx context.Context, snapshot smtbackend.Snapshot) {
	t.Helper()
	require.NoError(t, snapshot.Commit(ctx, smtbackend.CommitMetadata{}))
}

func testSetSnapshotCommitTarget(t testing.TB, ctx context.Context, snapshot smtbackend.Snapshot, backend smtbackend.Backend) {
	t.Helper()
	require.NoError(t, snapshot.SetCommitTarget(ctx, backend))
}

func testLeafInputFromLegacyLeaf(t testing.TB, leaf *smt.Leaf) smtbackend.LeafInput {
	t.Helper()
	key, err := api.PathToFixedBytes(leaf.Path, api.StateTreeKeyLengthBits)
	require.NoError(t, err)
	return smtbackend.LeafInput{
		Key:   key,
		Value: leaf.Value,
	}
}

func testLeafInputsFromLegacyLeaves(t testing.TB, leaves []*smt.Leaf) []smtbackend.LeafInput {
	t.Helper()
	inputs := make([]smtbackend.LeafInput, len(leaves))
	for i, leaf := range leaves {
		inputs[i] = testLeafInputFromLegacyLeaf(t, leaf)
	}
	return inputs
}

func testAddLegacyLeavesToBackendSnapshot(t testing.TB, ctx context.Context, snapshot smtbackend.Snapshot, leaves []*smt.Leaf) {
	t.Helper()
	inputs := make([]smtbackend.LeafInput, len(leaves))
	for i, leaf := range leaves {
		inputs[i] = testLeafInputFromLegacyLeaf(t, leaf)
	}
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.Empty(t, result.Rejected)
}
