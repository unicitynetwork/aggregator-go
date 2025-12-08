package round

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

var conf = config.Config{
	Database: config.DatabaseConfig{
		Database:               "test_smt_persistence",
		ConnectTimeout:         30 * time.Second,
		ServerSelectionTimeout: 5 * time.Second,
		SocketTimeout:          30 * time.Second,
		MaxPoolSize:            100,
		MinPoolSize:            5,
		MaxConnIdleTime:        5 * time.Minute,
	},
}

// TestSmtPersistenceAndRestoration tests SMT persistence and restoration with consistent root hashes
func TestSmtPersistenceAndRestoration(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	ctx := context.Background()

	// Create test data
	testLeaves := []*smt.Leaf{
		{Path: big.NewInt(5), Value: []byte("test_value_5")},
		{Path: big.NewInt(12), Value: []byte("test_value_12")},
		{Path: big.NewInt(15), Value: []byte("test_value_15")},
		{Path: big.NewInt(16), Value: []byte("test_value_16")},
	}
	keyLen := 16 + 256
	for _, t := range testLeaves {
		t.Path = new(big.Int).SetBit(t.Path, keyLen, 1)
	}

	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
		},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create RoundManager")

	// Test persistence
	smtNodes := rm.convertLeavesToNodes(testLeaves)
	err = storage.SmtStorage().StoreBatch(ctx, smtNodes)
	require.NoError(t, err, "Should persist SMT nodes")

	// Verify nodes were stored
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testLeaves)), count, "Should have stored all SMT nodes")

	// Test restoration produces same root hash as fresh SMT
	freshSmt := smt.NewSparseMerkleTree(api.SHA256, keyLen)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT should accept leaves")
	freshHash := freshSmt.GetRootHashHex()

	// Create RoundManager and call Start() to trigger restoration
	restoredRm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create RoundManager")

	err = restoredRm.Start(ctx)
	require.NoError(t, err, "SMT restoration should succeed")
	defer restoredRm.Stop(ctx)
	restoredHash := restoredRm.smt.GetRootHash()

	assert.Equal(t, freshHash, restoredHash, "Restored SMT should have same root hash as fresh SMT")

	// Verify inclusion proofs work
	for _, leaf := range testLeaves {
		merkleTreePath, err := restoredRm.smt.GetPath(leaf.Path)
		require.NoError(t, err)
		require.NotNil(t, merkleTreePath, "Should be able to get Merkle path")
		assert.NotEmpty(t, merkleTreePath.Root, "Merkle path should have root hash")
	}
}

// TestLargeSmtRestoration tests multi-chunk restoration with large dataset
func TestLargeSmtRestoration(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	ctx := context.Background()
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
		},
	}
	rm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create RoundManager")

	const testNodeCount = 2500 // Ensure multiple chunks (chunkSize = 1000 in round_manager.go)

	// Create large dataset with non-sequential paths to test ordering
	testLeaves := make([]*smt.Leaf, testNodeCount)
	keyLen := 16 + 256
	for i := 0; i < testNodeCount; i++ {
		path := new(big.Int).SetBit(big.NewInt(int64((i+1)*700000)), keyLen, 1)
		value := []byte(fmt.Sprintf("large_test_value_%d", i))
		testLeaves[i] = smt.NewLeaf(path, value)
	}

	// Create fresh SMT for comparison
	freshSmt := smt.NewSparseMerkleTree(api.SHA256, keyLen)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT AddLeaves should succeed")
	freshHash := freshSmt.GetRootHashHex()

	// Persist leaves to storage
	smtNodes := rm.convertLeavesToNodes(testLeaves)
	err = storage.SmtStorage().StoreBatch(ctx, smtNodes)
	require.NoError(t, err, "Should persist large number of SMT nodes")

	// Verify count
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(testNodeCount), count, "Should have stored all nodes")

	// Create new RoundManager and call Start() to restore from storage (uses multiple chunks)
	newRm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create new RoundManager")

	err = newRm.Start(ctx)
	require.NoError(t, err, "Large SMT restoration should succeed")
	defer newRm.Stop(ctx)

	restoredHash := newRm.smt.GetRootHash()

	// Critical test: multi-chunk restoration should match single-batch creation
	assert.Equal(t, freshHash, restoredHash, "Multi-chunk restoration should produce same hash as fresh SMT")
}

// TestCompleteWorkflowWithRestart tests end-to-end workflow including service restart simulation
func TestCompleteWorkflowWithRestart(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	ctx := context.Background()

	// Create test commitments
	testCommitments := testutil.CreateTestCommitments(t, 3, "request")

	// Process commitments in first round manager instance
	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			BatchLimit: 1000,
		},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create RoundManager")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(1)),
		State:       RoundStateProcessing,
		Commitments: testCommitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	blockNumber := api.NewBigInt(big.NewInt(1))

	// Set the commitments in the round and process them
	rm.currentRound.Commitments = testCommitments

	// Process the commitments (processMiniBatch assumes caller holds mutex)
	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, testCommitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err, "processMiniBatch should succeed")

	// Get the root hash from the snapshot
	rootHash := rm.currentRound.Snapshot.GetRootHash()
	require.NotEmpty(t, rootHash, "Root hash should not be empty")

	// After processBatch, SMT nodes are not yet persisted - they're stored in round state
	// We need to finalize a block to trigger persistence
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err, "Should parse root hash")

	block := models.NewBlock(
		blockNumber,
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		nil,
		nil,
	)

	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed")

	// Now verify SMT nodes were persisted
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testCommitments)), count, "Should have persisted SMT nodes for all commitments")

	// Simulate service restart with new round manager
	cfg = &config.Config{Processing: config.ProcessingConfig{RoundDuration: time.Second}}
	newRm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "NewRoundManager should succeed after restart")

	// Call Start() to trigger SMT restoration
	err = newRm.Start(ctx)
	require.NoError(t, err, "Start should succeed and restore SMT")
	defer newRm.Stop(ctx)

	// Verify restored SMT has correct data
	restoredRootHash := newRm.smt.GetRootHash()
	assert.NotEmpty(t, restoredRootHash, "Restored SMT should have non-empty root hash")

	// Verify inclusion proofs work after restart
	for _, commitment := range testCommitments {
		path, err := commitment.RequestID.GetPath()
		require.NoError(t, err, "Should be able to get path from request ID")

		merkleTreePath, err := newRm.smt.GetPath(path)
		require.NoError(t, err)
		require.NotNil(t, merkleTreePath, "Should be able to get Merkle path")
		assert.NotEmpty(t, merkleTreePath.Root, "Merkle path should have root hash")
		assert.NotEmpty(t, merkleTreePath.Steps, "Merkle path should have steps")
	}
}

// TestSmtRestorationWithBlockVerification tests that SMT restoration verifies against existing blocks
func TestSmtRestorationWithBlockVerification(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	ctx := context.Background()
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// Create test data
	testLeaves := []*smt.Leaf{
		{Path: big.NewInt(0x110), Value: []byte("block_test_value_10")},
		{Path: big.NewInt(0x120), Value: []byte("block_test_value_20")},
		{Path: big.NewInt(0x130), Value: []byte("block_test_value_30")},
	}
	keyLen := 16 + 256
	for _, t := range testLeaves {
		t.Path = new(big.Int).SetBit(t.Path, keyLen, 1)
	}

	// Create fresh SMT to get expected root hash
	freshSmt := smt.NewSparseMerkleTree(api.SHA256, keyLen)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT should accept leaves")
	expectedRootHash := freshSmt.GetRootHashHex()
	expectedRootHashBytes := freshSmt.GetRootHash()

	// Create a block with the expected root hash
	block := &models.Block{
		Index:                api.NewBigInt(big.NewInt(1)),
		ChainID:              "test-chain",
		ShardID:              0,
		Version:              "1.0.0",
		ForkID:               "test-fork",
		RootHash:             api.HexBytes(expectedRootHashBytes), // Use bytes, not hex string
		PreviousBlockHash:    api.HexBytes("0000000000000000000000000000000000000000000000000000000000000000"),
		NoDeletionProofHash:  api.HexBytes(""),
		CreatedAt:            api.NewTimestamp(time.Now()),
		UnicityCertificate:   api.HexBytes("certificate_data"),
		ParentMerkleTreePath: nil,
		Finalized:            true, // Mark as finalized for test
	}

	// Store the block
	err = storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err, "Should store test block")

	// Create RoundManager and persist SMT nodes
	cfg := &config.Config{
		Processing: config.ProcessingConfig{RoundDuration: time.Second},
	}
	rm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
	require.NoError(t, err, "Should create RoundManager")

	// Persist SMT nodes to storage
	smtNodes := rm.convertLeavesToNodes(testLeaves)
	err = storage.SmtStorage().StoreBatch(ctx, smtNodes)
	require.NoError(t, err, "Should persist SMT nodes")

	// Test 1: Successful verification (matching root hash)
	t.Run("SuccessfulVerification", func(t *testing.T) {
		successRm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
		require.NoError(t, err, "Should create RoundManager")

		err = successRm.Start(ctx)
		require.NoError(t, err, "SMT restoration should succeed when root hashes match")
		defer successRm.Stop(ctx)

		// Verify the restored SMT has the correct hash
		restoredHash := successRm.smt.GetRootHash()
		assert.Equal(t, expectedRootHash, restoredHash, "Restored SMT should have expected root hash")
	})

	// Test 2: Failed verification (mismatched root hash)
	t.Run("FailedVerification", func(t *testing.T) {
		// Create a block with a different root hash to simulate mismatch
		wrongBlock := &models.Block{
			Index:                api.NewBigInt(big.NewInt(2)),
			ChainID:              "test-chain",
			ShardID:              0,
			Version:              "1.0.0",
			ForkID:               "test-fork",
			RootHash:             api.HexBytes("wrong_root_hash_value"), // Intentionally wrong hash
			PreviousBlockHash:    api.HexBytes("0000000000000000000000000000000000000000000000000000000000000001"),
			NoDeletionProofHash:  api.HexBytes(""),
			CreatedAt:            api.NewTimestamp(time.Now()),
			UnicityCertificate:   api.HexBytes("certificate_data"),
			ParentMerkleTreePath: nil,
			Finalized:            true, // Mark as finalized for test
		}

		// Store the wrong block (this will become the "latest" block)
		err = storage.BlockStorage().Store(ctx, wrongBlock)
		require.NoError(t, err, "Should store wrong test block")

		failRm, err := NewRoundManager(ctx, cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker())
		require.NoError(t, err, "Should create RoundManager")

		// This should fail because the restored SMT root hash doesn't match the latest block
		err = failRm.Start(ctx)
		require.Error(t, err, "SMT restoration should fail when root hashes don't match")
	})
}
