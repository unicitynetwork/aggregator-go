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
		merkleTreePath := threadSafeSMT.GetPath(path)
		assert.NotNil(t, merkleTreePath, "Should be able to get Merkle tree path from original SMT")
		assert.NotEmpty(t, merkleTreePath.Root, "Root should not be empty")
		assert.NotEmpty(t, merkleTreePath.Steps, "Steps should not be empty")
	})
}
