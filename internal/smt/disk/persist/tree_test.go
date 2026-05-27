package persist

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	diskstore "github.com/unicitynetwork/aggregator-go/internal/smt/disk/store"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestSnapshotFirstCommitAndReopenParity(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{
		leafInput(k1, v1),
		leafInput(k2, v2),
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
	), result.CandidateRoot)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))
	require.Equal(t, result.CandidateRoot, tree.RootHash())

	requireNodeExists(t, store, disk.RootNodeKey())
	root := treeRootNode(t, store)
	leftKey, _, _, err := childNodeKey(disk.PrefixBits{}, 0, root.Path, 0)
	require.NoError(t, err)
	rightKey, _, _, err := childNodeKey(disk.PrefixBits{}, 0, root.Path, 1)
	require.NoError(t, err)
	requireNodeExists(t, store, leftKey)
	requireNodeExists(t, store, rightKey)
	require.NoError(t, store.Close())

	reopenedStore := openTestStore(t, dir)
	defer reopenedStore.Close()
	reopenedTree := openPersistTree(t, reopenedStore)
	require.Equal(t, result.CandidateRoot, reopenedTree.RootHash())
}

func TestOpenDefaultsUseProductionMaterialization(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()

	tree, err := OpenWithOptions(store, Options{})
	require.NoError(t, err)
	require.Equal(t, MaterializeParallel, tree.materializeMode)
	require.Equal(t, FrontierReadPoint, tree.frontierReadMode)
	require.Equal(t, -1, tree.nodeCacheDepth)
	require.Nil(t, tree.nodeCache)
}

func TestSnapshotMaterializesAfterReopenAndClassifiesDuplicates(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))
	require.NoError(t, store.Close())

	store = openTestStore(t, dir)
	defer store.Close()
	tree = openPersistTree(t, store)

	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	duplicate, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	require.Equal(t, result.CandidateRoot, duplicate.OldRoot)
	require.Equal(t, result.CandidateRoot, duplicate.CandidateRoot)
	require.Empty(t, duplicate.AcceptedIndexes)
	require.Equal(t, []int{0}, duplicate.DuplicateIndexes)
	require.Empty(t, duplicate.Rejected)
	require.Equal(t, 1, duplicate.Stats.MaterializedNodes)
	snapshot.Discard()

	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	modification, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, []byte("value-three"))})
	require.NoError(t, err)
	require.Empty(t, modification.AcceptedIndexes)
	require.Empty(t, modification.DuplicateIndexes)
	require.Len(t, modification.Rejected, 1)
	require.Equal(t, disk.RejectLeafModification, modification.Rejected[0].Reason)
	require.Equal(t, result.CandidateRoot, modification.CandidateRoot)
	snapshot.Discard()

	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	next, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k2, v2)})
	require.NoError(t, err)
	require.Equal(t, 1, next.Stats.MaterializedNodes)
	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
	), next.CandidateRoot)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(2)))
}

func TestSnapshotFrontierMaterializationMatchesMemory(t *testing.T) {
	for _, readMode := range []string{FrontierReadPoint, FrontierReadIterator} {
		t.Run(readMode, func(t *testing.T) {
			dir := t.TempDir()
			store := openTestStore(t, dir)
			tree := openPersistTree(t, store)

			k0 := keyWithFirstByte(0x00)
			k1 := keyWithFirstByte(0x03)
			k2 := keyWithFirstByte(0x07)
			k3 := keyWithFirstByte(0x0f)
			k4 := keyWithFirstByte(0x01)
			v0 := []byte("value-zero")
			v1 := []byte("value-one")
			v2 := []byte("value-two")
			v3 := []byte("value-three")
			v4 := []byte("value-four")

			snapshot, err := tree.CreateSnapshot()
			require.NoError(t, err)
			initial, err := snapshot.AddLeaves([]disk.LeafInput{
				leafInput(k0, v0),
				leafInput(k1, v1),
				leafInput(k2, v2),
				leafInput(k3, v3),
			})
			require.NoError(t, err)
			require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))
			require.Equal(t, memoryRootAfterLeaves(t,
				memoryLeaf{key: k0, value: v0},
				memoryLeaf{key: k1, value: v1},
				memoryLeaf{key: k2, value: v2},
				memoryLeaf{key: k3, value: v3},
			), initial.CandidateRoot)
			require.NoError(t, store.Close())

			store = openTestStore(t, dir)
			defer store.Close()
			tree = openPersistTreeWithOptions(t, store, Options{
				MaterializeMode:  MaterializeFrontier,
				FrontierReadMode: readMode,
			})

			snapshot, err = tree.CreateSnapshot()
			require.NoError(t, err)
			next, err := snapshot.AddLeaves([]disk.LeafInput{
				leafInput(k4, v4),
				leafInput(k2, v2),
			})
			require.NoError(t, err)
			require.Equal(t, []int{0}, next.AcceptedIndexes)
			require.Equal(t, []int{1}, next.DuplicateIndexes)
			require.Empty(t, next.Rejected)
			require.Equal(t, memoryRootAfterLeaves(t,
				memoryLeaf{key: k0, value: v0},
				memoryLeaf{key: k1, value: v1},
				memoryLeaf{key: k2, value: v2},
				memoryLeaf{key: k3, value: v3},
				memoryLeaf{key: k4, value: v4},
			), next.CandidateRoot)
			require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(2)))
		})
	}
}

func TestNodeCacheServesRepeatedMaterializationWithoutPebbleRead(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTreeWithOptions(t, store, Options{EnableNodeCache: true, NodeCacheDepth: 0})

	k1 := keyWithFirstByte(0x01)
	v1 := []byte("value-one")

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))

	cacheStats := tree.NodeCacheStats()
	require.True(t, cacheStats.Enabled)
	require.Equal(t, 0, cacheStats.Depth)
	require.Equal(t, 1, cacheStats.Entries)

	before := store.Counters()
	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	duplicate, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	snapshot.Discard()
	after := store.Counters()

	require.Equal(t, result.CandidateRoot, duplicate.CandidateRoot)
	require.Equal(t, []int{0}, duplicate.DuplicateIndexes)
	require.Equal(t, 1, duplicate.Stats.MaterializedNodes)
	require.Zero(t, duplicate.Stats.NodeReads)
	require.Equal(t, 1, duplicate.Stats.MaterializeCacheHits)
	require.Positive(t, duplicate.Stats.MaterializeCacheBytes)
	require.Equal(t, before.NodePointReads, after.NodePointReads)
}

func TestNodeCacheUpdatesRootAfterCommit(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTreeWithOptions(t, store, Options{EnableNodeCache: true, NodeCacheDepth: 0})

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))

	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k2, v2)})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(2)))

	before := store.Counters()
	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	duplicate, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k2, v2)})
	require.NoError(t, err)
	snapshot.Discard()
	after := store.Counters()

	require.Equal(t, result.CandidateRoot, duplicate.CandidateRoot)
	require.Equal(t, []int{0}, duplicate.DuplicateIndexes)
	require.Equal(t, 1, duplicate.Stats.NodeReads)
	require.Equal(t, 1, duplicate.Stats.MaterializeCacheHits)
	require.Equal(t, before.NodePointReads+1, after.NodePointReads)
}

func TestSnapshotDiscardDoesNotWritePebble(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	before := store.Counters()
	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = snapshot.AddLeaves([]disk.LeafInput{leafInput(keyWithFirstByte(0x01), []byte("value-one"))})
	require.NoError(t, err)
	snapshot.Discard()
	after := store.Counters()

	require.Equal(t, before.BatchSets, after.BatchSets)
	require.Equal(t, before.BatchesCommitted, after.BatchesCommitted)
	require.Equal(t, disk.EmptyRootHash(), tree.RootHash())
}

func TestSnapshotCommitTombstonesLoadedKeysWithoutFinalWrites(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	staleKey, err := disk.NewNodeKey(1, disk.PrefixBits{})
	require.NoError(t, err)
	batch := store.NewBatch()
	require.NoError(t, batch.SetNode(staleKey, serializedLeaf(t, keyWithFirstByte(0x00), []byte("stale"))))
	require.NoError(t, batch.Commit())
	requireNodeExists(t, store, staleKey)

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	snapshot.loadedKeys[staleKey] = struct{}{}
	require.NoError(t, snapshot.rebuildOverlay())
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(3)))
	requireNodeMissing(t, store, staleKey)
}

func TestSnapshotCommitWriteWinsOverLoadedKeyTombstone(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	v1 := []byte("value-one")
	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = snapshot.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)
	snapshot.loadedKeys[disk.RootNodeKey()] = struct{}{}
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(4)))
	requireNodeExists(t, store, disk.RootNodeKey())
}

func TestSnapshotCommitPersistsNonRootLoadedNodeMovement(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	tree := openPersistTree(t, store)

	k0 := keyWithFirstByte(0x00) // root-left leaf
	k1 := keyWithFirstByte(0x07) // root-right subtree, bits 1110...
	k2 := keyWithFirstByte(0x0f) // root-right subtree, bits 1111...
	k3 := keyWithFirstByte(0x01) // root-right subtree, splits non-root path at bit 1
	v0 := []byte("value-zero")
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	v3 := []byte("value-three")

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	initial, err := snapshot.AddLeaves([]disk.LeafInput{
		leafInput(k0, v0),
		leafInput(k1, v1),
		leafInput(k2, v2),
	})
	require.NoError(t, err)
	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k0, value: v0},
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
	), initial.CandidateRoot)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))

	root := treeRootNode(t, store)
	rightKey, rightPrefix, rightStart, err := childNodeKey(disk.PrefixBits{}, 0, root.Path, 1)
	require.NoError(t, err)
	rightBefore := requireNodeExists(t, store, rightKey)
	rightNodeBefore, err := disk.UnmarshalInternal(rightBefore)
	require.NoError(t, err)
	require.Equal(t, uint8(3), rightNodeBefore.Depth)

	require.NoError(t, store.Close())
	store = openTestStore(t, dir)
	defer store.Close()
	tree = openPersistTree(t, store)

	snapshot, err = tree.CreateSnapshot()
	require.NoError(t, err)
	next, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(k3, v3)})
	require.NoError(t, err)
	require.Equal(t, 2, next.Stats.MaterializedNodes) // root + non-root internal
	require.Equal(t, 2, next.Stats.NodeReads)
	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k0, value: v0},
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
		memoryLeaf{key: k3, value: v3},
	), next.CandidateRoot)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(2)))

	rightAfter := requireNodeExists(t, store, rightKey)
	rightNodeAfter, err := disk.UnmarshalInternal(rightAfter)
	require.NoError(t, err)
	require.Equal(t, uint8(1), rightNodeAfter.Depth)

	movedOldKey, _, _, err := childNodeKey(rightPrefix, rightStart, rightNodeAfter.Path, 1)
	require.NoError(t, err)
	movedOld := requireNodeExists(t, store, movedOldKey)
	movedOldNode, err := disk.UnmarshalInternal(movedOld)
	require.NoError(t, err)
	require.Equal(t, uint8(3), movedOldNode.Depth)
	require.Equal(t, 1, movedOldNode.Path.Len())
	require.NotEqual(t, rightBefore, rightAfter)
}

func TestSnapshotForkUsesParentOverlayWithoutPersistingParent(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	parent, err := tree.CreateSnapshot()
	require.NoError(t, err)
	parentResult, err := parent.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)

	child, err := parent.Fork()
	require.NoError(t, err)
	before := store.Counters()
	childResult, err := child.AddLeaves([]disk.LeafInput{leafInput(k2, v2)})
	require.NoError(t, err)
	after := store.Counters()

	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
	), childResult.CandidateRoot)
	require.Equal(t, before.NodePointReads, after.NodePointReads, "child should read parent nodes from overlay, not DB")
	require.Equal(t, disk.EmptyRootHash(), tree.RootHash(), "parent fork must not persist before commit")

	require.NoError(t, parent.Commit(api.NewBigIntFromUint64(1)))
	require.Equal(t, parentResult.CandidateRoot, tree.RootHash())
	require.NoError(t, child.SetCommitTarget(tree))
	require.NoError(t, child.Commit(api.NewBigIntFromUint64(2)))
	require.Equal(t, childResult.CandidateRoot, tree.RootHash())
}

func TestSnapshotForkRejectsUncommittedGrandparentOverlay(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	parent, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = parent.AddLeaves([]disk.LeafInput{leafInput(keyWithFirstByte(0x01), []byte("value-one"))})
	require.NoError(t, err)

	child, err := parent.Fork()
	require.NoError(t, err)
	_, err = child.AddLeaves([]disk.LeafInput{leafInput(keyWithFirstByte(0x03), []byte("value-two"))})
	require.NoError(t, err)

	_, err = child.Fork()
	require.ErrorContains(t, err, "uncommitted grandparent overlay")
}

func TestSnapshotForkAfterOlderParentCommitDropsGrandparentOverlay(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	k3 := keyWithFirstByte(0x07)
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	v3 := []byte("value-three")

	parent, err := tree.CreateSnapshot()
	require.NoError(t, err)
	parentResult, err := parent.AddLeaves([]disk.LeafInput{leafInput(k1, v1)})
	require.NoError(t, err)

	child, err := parent.Fork()
	require.NoError(t, err)
	require.NoError(t, parent.Commit(api.NewBigIntFromUint64(1)))
	require.Equal(t, parentResult.CandidateRoot, tree.RootHash())

	childResult, err := child.AddLeaves([]disk.LeafInput{leafInput(k2, v2)})
	require.NoError(t, err)
	grandchild, err := child.Fork()
	require.NoError(t, err)
	grandchildResult, err := grandchild.AddLeaves([]disk.LeafInput{leafInput(k3, v3)})
	require.NoError(t, err)

	require.NoError(t, child.SetCommitTarget(tree))
	require.NoError(t, child.Commit(api.NewBigIntFromUint64(2)))
	require.Equal(t, childResult.CandidateRoot, tree.RootHash())

	require.NoError(t, grandchild.SetCommitTarget(tree))
	require.NoError(t, grandchild.Commit(api.NewBigIntFromUint64(3)))
	require.Equal(t, memoryRootAfterLeaves(t,
		memoryLeaf{key: k1, value: v1},
		memoryLeaf{key: k2, value: v2},
		memoryLeaf{key: k3, value: v3},
	), grandchildResult.CandidateRoot)
	require.Equal(t, grandchildResult.CandidateRoot, tree.RootHash())
}

func TestSnapshotRetargetAndLifecycleGuards(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	parent, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = parent.AddLeaves([]disk.LeafInput{leafInput(keyWithFirstByte(0x01), []byte("value-one"))})
	require.NoError(t, err)

	child, err := parent.Fork()
	require.NoError(t, err)
	require.ErrorContains(t, child.SetCommitTarget(tree), "does not match snapshot base root")

	parent.Discard()
	require.ErrorContains(t, child.SetCommitTarget(tree), "does not match snapshot base root")
	require.ErrorContains(t, parent.Commit(api.NewBigIntFromUint64(1)), "snapshot is closed")
	_, err = parent.Fork()
	require.ErrorContains(t, err, "snapshot is closed")
}

func TestSnapshotEmptyForkCommitWritesOnlyMetadata(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	parent, err := tree.CreateSnapshot()
	require.NoError(t, err)
	parentResult, err := parent.AddLeaves([]disk.LeafInput{leafInput(keyWithFirstByte(0x01), []byte("value-one"))})
	require.NoError(t, err)

	child, err := parent.Fork()
	require.NoError(t, err)
	require.NoError(t, parent.Commit(api.NewBigIntFromUint64(1)))
	require.NoError(t, child.SetCommitTarget(tree))

	before := store.Counters()
	require.NoError(t, child.Commit(api.NewBigIntFromUint64(2)))
	after := store.Counters()

	require.Equal(t, parentResult.CandidateRoot, tree.RootHash())
	require.Equal(t, before.NodeSets, after.NodeSets)
	require.Equal(t, before.NodeDeletes, after.NodeDeletes)
	require.Equal(t, before.MetaSets+2, after.MetaSets)
	require.ErrorContains(t, child.Commit(api.NewBigIntFromUint64(3)), "snapshot is closed")
}

func TestGetInclusionCertSingleLeafAfterReopen(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	tree := openPersistTree(t, store)

	key := keyWithFirstByte(0x01)
	value := []byte("solo-leaf-value")
	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{leafInput(key, value)})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))
	require.NoError(t, store.Close())

	store = openTestStore(t, dir)
	defer store.Close()
	tree = openPersistTree(t, store)

	cert, err := tree.GetInclusionCert(key[:])
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, [api.BitmapSize]byte{}, cert.Bitmap)
	require.Empty(t, cert.Siblings)
	root := result.CandidateRoot
	require.NoError(t, cert.Verify(key[:], value, root[:], api.SHA256))
}

func TestGetInclusionCertMatchesMemoryAfterReopen(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	tree := openPersistTree(t, store)

	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	k3 := keyWithFirstByte(0x08)
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	v3 := []byte("value-three")
	leaves := []memoryLeaf{
		{key: k1, value: v1},
		{key: k2, value: v2},
		{key: k3, value: v3},
	}

	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	result, err := snapshot.AddLeaves([]disk.LeafInput{
		leafInput(k1, v1),
		leafInput(k2, v2),
		leafInput(k3, v3),
	})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))
	require.NoError(t, store.Close())

	store = openTestStore(t, dir)
	defer store.Close()
	tree = openPersistTree(t, store)
	memoryTree := memoryTreeAfterLeaves(t, leaves...)
	require.Equal(t, memoryTree.GetRootHashRaw(), result.CandidateRoot[:])

	for _, leaf := range leaves {
		diskCert, err := tree.GetInclusionCert(leaf.key[:])
		require.NoError(t, err)
		memoryCert, err := memoryTree.GetInclusionCert(leaf.key[:])
		require.NoError(t, err)
		require.Equal(t, memoryCert.Bitmap, diskCert.Bitmap)
		require.Equal(t, memoryCert.Siblings, diskCert.Siblings)
		require.NoError(t, diskCert.Verify(leaf.key[:], leaf.value, result.CandidateRoot[:], api.SHA256))
	}
}

func TestGetInclusionCertMissingKey(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	present := keyWithFirstByte(0x01)
	missing := keyWithFirstByte(0x02)
	snapshot, err := tree.CreateSnapshot()
	require.NoError(t, err)
	_, err = snapshot.AddLeaves([]disk.LeafInput{leafInput(present, []byte("value-one"))})
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(api.NewBigIntFromUint64(1)))

	cert, err := tree.GetInclusionCert(missing[:])
	require.Error(t, err)
	require.Nil(t, cert)
}

func TestGetInclusionCertRejectsWrongKeyLength(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	cert, err := tree.GetInclusionCert([]byte{0x01})
	require.Error(t, err)
	require.Nil(t, cert)
}

func openTestStore(t *testing.T, dir string) *diskstore.Store {
	t.Helper()
	store, err := diskstore.Open(dir, diskstore.Options{})
	require.NoError(t, err)
	return store
}

func openPersistTree(t *testing.T, store *diskstore.Store) *Tree {
	t.Helper()
	tree, err := Open(store)
	require.NoError(t, err)
	return tree
}

func openPersistTreeWithOptions(t *testing.T, store *diskstore.Store, opts Options) *Tree {
	t.Helper()
	tree, err := OpenWithOptions(store, opts)
	require.NoError(t, err)
	return tree
}

func leafInput(key disk.Key, value []byte) disk.LeafInput {
	return disk.LeafInput{
		Key:   append([]byte(nil), key[:]...),
		Value: append([]byte(nil), value...),
	}
}

type memoryLeaf struct {
	key   disk.Key
	value []byte
}

func memoryRootAfterLeaves(t *testing.T, leaves ...memoryLeaf) disk.Hash {
	t.Helper()

	tree := memoryTreeAfterLeaves(t, leaves...)
	hash, err := disk.HashFromBytes(tree.GetRootHashRaw())
	require.NoError(t, err)
	return hash
}

func memoryTreeAfterLeaves(t *testing.T, leaves ...memoryLeaf) *smt.SparseMerkleTree {
	t.Helper()

	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	for _, leaf := range leaves {
		path, err := api.FixedBytesToPath(leaf.key[:], api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		require.NoError(t, tree.AddLeaf(path, leaf.value))
	}
	return tree
}

func keyWithFirstByte(firstByte byte) disk.Key {
	var key disk.Key
	key[0] = firstByte
	return key
}

func requireNodeExists(t *testing.T, store *diskstore.Store, key disk.NodeKey) []byte {
	t.Helper()
	value, ok, err := store.GetNode(key)
	require.NoError(t, err)
	require.True(t, ok)
	return value
}

func requireNodeMissing(t *testing.T, store *diskstore.Store, key disk.NodeKey) {
	t.Helper()
	_, ok, err := store.GetNode(key)
	require.NoError(t, err)
	require.False(t, ok)
}

func treeRootNode(t *testing.T, store *diskstore.Store) *disk.InternalNode {
	t.Helper()
	encoded := requireNodeExists(t, store, disk.RootNodeKey())
	tag, err := disk.SerializedTag(encoded)
	require.NoError(t, err)
	require.Equal(t, disk.TagInternal, tag)
	node, err := disk.UnmarshalInternal(encoded)
	require.NoError(t, err)
	return node
}

func serializedLeaf(t *testing.T, key disk.Key, value []byte) []byte {
	t.Helper()
	encoded, err := disk.MarshalLeaf(disk.NewLeaf(key, value).Leaf)
	require.NoError(t, err)
	return encoded
}
