package round

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestSnapshotWorkflowIntegration(t *testing.T) {
	t.Run("SnapshotIsolationFromMainSMT", func(t *testing.T) {
		// Create main SMT and ThreadSafeSMT
		smtInstance := smt.NewSparseMerkleTree(api.SHA256)
		threadSafeSMT := NewThreadSafeSMT(smtInstance)

		// Get initial state
		initialHash := threadSafeSMT.GetRootHash()

		// Create snapshot (simulating Round creation)
		snapshot := threadSafeSMT.CreateSnapshot()
		assert.NotNil(t, snapshot, "Should create snapshot")

		// Verify snapshot initially matches main SMT
		snapshotHash := snapshot.GetRootHash()
		assert.Equal(t, initialHash, snapshotHash, "Snapshot should initially match main SMT")

		// Add data to snapshot (simulating processBatch)
		leaves := []*smt.Leaf{
			smt.NewLeaf(big.NewInt(0b10), []byte("value1")),
			smt.NewLeaf(big.NewInt(0b101), []byte("value2")),
			smt.NewLeaf(big.NewInt(0b111), []byte("value3")),
		}

		rootHash, err := snapshot.AddLeaves(leaves)
		require.NoError(t, err, "Should add leaves to snapshot")
		assert.NotEmpty(t, rootHash, "Should return root hash")

		// Verify main SMT is unchanged
		currentMainHash := threadSafeSMT.GetRootHash()
		assert.Equal(t, initialHash, currentMainHash, "Main SMT should be unchanged after snapshot modification")

		// Verify snapshot has changed
		currentSnapshotHash := snapshot.GetRootHash()
		assert.NotEqual(t, initialHash, currentSnapshotHash, "Snapshot should have changed")
		assert.Equal(t, rootHash, currentSnapshotHash, "Snapshot hash should match returned hash")

		// Commit snapshot (simulating FinalizeBlock)
		snapshot.Commit(threadSafeSMT)

		// Verify main SMT now has the changes
		finalMainHash := threadSafeSMT.GetRootHash()
		assert.Equal(t, rootHash, finalMainHash, "Main SMT should now have snapshot changes")

		// Verify we can read the data from main SMT
		for _, leaf := range leaves {
			retrievedLeaf, err := threadSafeSMT.GetLeaf(leaf.Path)
			require.NoError(t, err, "Should retrieve leaf from main SMT")
			assert.Equal(t, leaf.Value, retrievedLeaf.Value, "Retrieved value should match")
		}

		t.Logf("Test completed:")
		t.Logf("  Initial hash: %s", initialHash)
		t.Logf("  Final hash: %s", finalMainHash)
		t.Logf("  Processed %d leaves", len(leaves))
	})
}
