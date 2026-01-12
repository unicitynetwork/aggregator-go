package smt

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestThreadSafeSMTSnapshot(t *testing.T) {
	t.Run("BasicSnapshotOperations", func(t *testing.T) {
		// Create ThreadSafeSMT instance
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Add initial data to the original SMT using a snapshot
		// (since the original SMT can't be modified directly)
		initialSnapshot := threadSafeSMT.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b100), []byte{1, 2, 3})
		require.NoError(t, err, "Should be able to add leaf to initial snapshot")

		err = initialSnapshot.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err, "Should be able to add second leaf to initial snapshot")

		// Commit initial data
		initialSnapshot.Commit(threadSafeSMT)

		originalHash := threadSafeSMT.GetRootHash()

		// Create a new snapshot and verify it has the same hash initially
		snapshot := threadSafeSMT.CreateSnapshot()
		snapshotHash := snapshot.GetRootHash()
		assert.Equal(t, originalHash, snapshotHash, "Snapshot should have same hash as original initially")

		// Add a leaf to the snapshot
		err = snapshot.AddLeaf(big.NewInt(0b111), []byte{7, 8, 9})
		require.NoError(t, err, "Should be able to add leaf to snapshot")

		// Verify original SMT is unchanged
		assert.Equal(t, originalHash, threadSafeSMT.GetRootHash(), "Original SMT should be unchanged")

		// Verify snapshot has different hash
		newSnapshotHash := snapshot.GetRootHash()
		assert.NotEqual(t, originalHash, newSnapshotHash, "Snapshot should have different hash after modification")

		// Commit snapshot and verify original SMT is updated
		snapshot.Commit(threadSafeSMT)
		assert.Equal(t, newSnapshotHash, threadSafeSMT.GetRootHash(), "Original SMT should match snapshot after commit")
	})

	t.Run("BatchOperationsInSnapshot", func(t *testing.T) {
		// Create ThreadSafeSMT instance
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Create snapshot
		snapshot := threadSafeSMT.CreateSnapshot()

		// Prepare batch of leaves
		leaves := []*Leaf{
			NewLeaf(big.NewInt(0b100), []byte{1}),
			NewLeaf(big.NewInt(0b101), []byte{2}),
			NewLeaf(big.NewInt(0b111), []byte{3}),
		}

		// Add leaves in batch
		rootHash, err := snapshot.AddLeaves(leaves)
		require.NoError(t, err, "Should be able to add leaves in batch to snapshot")
		assert.NotEmpty(t, rootHash, "Root hash should not be empty")

		// Commit and verify
		snapshot.Commit(threadSafeSMT)
		assert.Equal(t, rootHash, threadSafeSMT.GetRootHash(), "SMT hash should match snapshot hash after commit")

		// Verify we can retrieve the leaves from the original SMT after commit
		for _, leaf := range leaves {
			retrievedLeaf, err := threadSafeSMT.GetLeaf(leaf.Path)
			require.NoError(t, err, "Should be able to retrieve leaf from original SMT after commit")
			assert.Equal(t, leaf.Value, retrievedLeaf.Value, "Retrieved leaf value should match")
		}
	})

	t.Run("ConcurrentSnapshots", func(t *testing.T) {
		// Create ThreadSafeSMT instance with initial data
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Add initial data
		initialSnapshot := threadSafeSMT.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b100), []byte{1})
		require.NoError(t, err)
		initialSnapshot.Commit(threadSafeSMT)

		originalHash := threadSafeSMT.GetRootHash()

		// Create two snapshots
		snapshot1 := threadSafeSMT.CreateSnapshot()
		snapshot2 := threadSafeSMT.CreateSnapshot()

		// Both snapshots should initially have the same hash as original
		assert.Equal(t, originalHash, snapshot1.GetRootHash())
		assert.Equal(t, originalHash, snapshot2.GetRootHash())

		// Modify each snapshot differently
		err = snapshot1.AddLeaf(big.NewInt(0b101), []byte{2})
		require.NoError(t, err)

		err = snapshot2.AddLeaf(big.NewInt(0b111), []byte{3})
		require.NoError(t, err)

		// Snapshots should have different hashes now
		hash1 := snapshot1.GetRootHash()
		hash2 := snapshot2.GetRootHash()
		assert.NotEqual(t, hash1, hash2, "Snapshots should have different hashes")
		assert.NotEqual(t, originalHash, hash1, "Snapshot1 should differ from original")
		assert.NotEqual(t, originalHash, hash2, "Snapshot2 should differ from original")

		// Original should still be unchanged
		assert.Equal(t, originalHash, threadSafeSMT.GetRootHash(), "Original should be unchanged")
	})

	t.Run("SnapshotStats", func(t *testing.T) {
		smtInstance := NewSparseMerkleTree(api.SHA256, 1)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		snapshot := threadSafeSMT.CreateSnapshot()
		stats := snapshot.GetStats()

		assert.Contains(t, stats, "rootHash", "Stats should contain rootHash")
		assert.Contains(t, stats, "leafCount", "Stats should contain leafCount")
		assert.Contains(t, stats, "isSnapshot", "Stats should contain isSnapshot")
		assert.Contains(t, stats, "isLocked", "Stats should contain isLocked")
		assert.True(t, stats["isSnapshot"].(bool), "isSnapshot should be true for snapshot")
	})

	t.Run("SnapshotWriteOperations", func(t *testing.T) {
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Add data via snapshot
		snapshot := threadSafeSMT.CreateSnapshot()
		path := big.NewInt(0b101)
		value := []byte{1, 2, 3}

		err := snapshot.AddLeaf(path, value)
		require.NoError(t, err)

		// Test with write locks for adding more data
		err = snapshot.WithWriteLock(func(snap *SmtSnapshot) error {
			return snap.AddLeaf(big.NewInt(0b111), []byte{4, 5, 6})
		})
		require.NoError(t, err, "Should be able to use WithWriteLock")

		// Verify snapshot has data by checking root hash changed
		rootHash := snapshot.GetRootHash()
		assert.NotEmpty(t, rootHash, "Root hash should not be empty after adding leaves")

		// Commit and verify the original SMT has the data
		snapshot.Commit(threadSafeSMT)

		// Verify we can read from the original SMT after commit
		leaf, err := threadSafeSMT.GetLeaf(path)
		require.NoError(t, err, "Should be able to retrieve leaf from original SMT after commit")
		assert.Equal(t, value, leaf.Value, "Retrieved leaf value should match")

		// Test path generation on original SMT
		merkleTreePath, err := threadSafeSMT.GetPath(path)
		require.NoError(t, err)
		assert.NotNil(t, merkleTreePath, "Should be able to get Merkle tree path from original SMT")
		assert.NotEmpty(t, merkleTreePath.Root, "Root should not be empty")
		assert.NotEmpty(t, merkleTreePath.Steps, "Steps should not be empty")
	})

	t.Run("SnapshotChaining", func(t *testing.T) {
		// Test the snapshot chaining feature used in pipelined pre-collection
		// Scenario: Main SMT -> Snapshot A (Round N) -> Snapshot B (pre-collection)

		// Create main ThreadSafeSMT
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Add initial data (simulating previous rounds)
		initialSnapshot := threadSafeSMT.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b100), []byte{1, 2, 3})
		require.NoError(t, err)
		initialSnapshot.Commit(threadSafeSMT)
		mainRootHash := threadSafeSMT.GetRootHash()

		// Create Snapshot A for Round N
		snapshotA := threadSafeSMT.CreateSnapshot()
		err = snapshotA.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err)
		rootHashA := snapshotA.GetRootHash()

		// Main SMT should still be unchanged
		assert.Equal(t, mainRootHash, threadSafeSMT.GetRootHash())
		assert.NotEqual(t, mainRootHash, rootHashA)

		// Create Snapshot B from Snapshot A (for pre-collection)
		snapshotB := snapshotA.CreateSnapshot()

		// Initially, Snapshot B should have same hash as Snapshot A
		assert.Equal(t, rootHashA, snapshotB.GetRootHash())

		// Add pre-collected commitments to Snapshot B
		err = snapshotB.AddLeaf(big.NewInt(0b110), []byte{7, 8, 9})
		require.NoError(t, err)
		rootHashB := snapshotB.GetRootHash()

		// All three should now have different hashes
		assert.NotEqual(t, mainRootHash, rootHashA)
		assert.NotEqual(t, mainRootHash, rootHashB)
		assert.NotEqual(t, rootHashA, rootHashB)

		// Main SMT is still unchanged
		assert.Equal(t, mainRootHash, threadSafeSMT.GetRootHash())

		// Now simulate Round N finalization: Commit Snapshot A to Main SMT
		snapshotA.Commit(threadSafeSMT)
		assert.Equal(t, rootHashA, threadSafeSMT.GetRootHash())

		// Reparent Snapshot B to commit directly to Main SMT (key feature!)
		snapshotB.SetCommitTarget(threadSafeSMT)

		// Now commit Snapshot B - it should update Main SMT directly
		snapshotB.Commit(threadSafeSMT)
		assert.Equal(t, rootHashB, threadSafeSMT.GetRootHash())

		// Verify all leaves are retrievable from main SMT
		leaf, err := threadSafeSMT.GetLeaf(big.NewInt(0b100))
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, leaf.Value)

		leaf, err = threadSafeSMT.GetLeaf(big.NewInt(0b101))
		require.NoError(t, err)
		assert.Equal(t, []byte{4, 5, 6}, leaf.Value)

		leaf, err = threadSafeSMT.GetLeaf(big.NewInt(0b110))
		require.NoError(t, err)
		assert.Equal(t, []byte{7, 8, 9}, leaf.Value)
	})

	t.Run("SnapshotChainingDeep", func(t *testing.T) {
		// Test deeper snapshot chaining: Main -> A -> B -> C
		// Use key length 3 - paths need BitLen() == 4, so valid paths are 0b1000-0b1111
		smtInstance := NewSparseMerkleTree(api.SHA256, 3)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Create chain: Main -> A -> B -> C
		// For keyLength=3, path.BitLen()-1 must equal 3, so path.BitLen()=4
		// Valid paths: 0b1000 to 0b1111 (8 to 15)
		snapshotA := threadSafeSMT.CreateSnapshot()
		err := snapshotA.AddLeaf(big.NewInt(0b1000), []byte{1})
		require.NoError(t, err)

		snapshotB := snapshotA.CreateSnapshot()
		err = snapshotB.AddLeaf(big.NewInt(0b1010), []byte{2})
		require.NoError(t, err)

		snapshotC := snapshotB.CreateSnapshot()
		err = snapshotC.AddLeaf(big.NewInt(0b1111), []byte{3})
		require.NoError(t, err)

		// All have different hashes
		rootA := snapshotA.GetRootHash()
		rootB := snapshotB.GetRootHash()
		rootC := snapshotC.GetRootHash()
		assert.NotEqual(t, rootA, rootB)
		assert.NotEqual(t, rootB, rootC)

		// Commit A to main
		snapshotA.Commit(threadSafeSMT)
		assert.Equal(t, rootA, threadSafeSMT.GetRootHash())

		// Reparent B to main and commit
		snapshotB.SetCommitTarget(threadSafeSMT)
		snapshotB.Commit(threadSafeSMT)
		assert.Equal(t, rootB, threadSafeSMT.GetRootHash())

		// Reparent C to main and commit
		snapshotC.SetCommitTarget(threadSafeSMT)
		snapshotC.Commit(threadSafeSMT)
		assert.Equal(t, rootC, threadSafeSMT.GetRootHash())

		// Verify all leaves are present
		_, err = threadSafeSMT.GetLeaf(big.NewInt(0b1000))
		require.NoError(t, err, "Should find leaf 0b1000")
		_, err = threadSafeSMT.GetLeaf(big.NewInt(0b1010))
		require.NoError(t, err, "Should find leaf 0b1010")
		_, err = threadSafeSMT.GetLeaf(big.NewInt(0b1111))
		require.NoError(t, err, "Should find leaf 0b1111")
	})

	t.Run("SetCommitTargetBeforeModification", func(t *testing.T) {
		// Test that SetCommitTarget works when called before adding data
		smtInstance := NewSparseMerkleTree(api.SHA256, 2)
		mainSMT := NewThreadSafeSMT(smtInstance)

		// Create separate "original" that snapshot initially points to
		otherSMTInstance := NewSparseMerkleTree(api.SHA256, 2)
		otherSMT := NewThreadSafeSMT(otherSMTInstance)

		// Add data to other SMT
		otherSnapshot := otherSMT.CreateSnapshot()
		err := otherSnapshot.AddLeaf(big.NewInt(0b100), []byte{1})
		require.NoError(t, err)
		otherSnapshot.Commit(otherSMT)

		// Create snapshot from other SMT
		snapshot := otherSMT.CreateSnapshot()

		// Reparent to main SMT before adding data
		snapshot.SetCommitTarget(mainSMT)

		// Add data
		err = snapshot.AddLeaf(big.NewInt(0b101), []byte{2})
		require.NoError(t, err)

		// Commit to main SMT
		snapshot.Commit(mainSMT)

		// Main SMT should have the new leaf
		leaf, err := mainSMT.GetLeaf(big.NewInt(0b101))
		require.NoError(t, err)
		assert.Equal(t, []byte{2}, leaf.Value)

		// Note: Main SMT won't have the data from other SMT (0b100)
		// because snapshot was created after that data was added
		// and we're only committing the snapshot's changes
	})
}
