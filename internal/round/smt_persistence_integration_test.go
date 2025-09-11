package round

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	testDBName  = "test_smt_persistence"
	testTimeout = 30 * time.Second
)

// setupTestStorage creates a complete storage instance with MongoDB using testcontainers
func setupTestStorage(t *testing.T) (interfaces.Storage, func()) {
	ctx := context.Background()

	container, err := mongoContainer.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container (Docker not available?): %v", err)
		return nil, func() {}
	}

	mongoURI, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}

	dbConfig := &config.DatabaseConfig{
		URI:                    mongoURI,
		Database:               testDBName,
		ConnectTimeout:         testTimeout,
		ServerSelectionTimeout: 5 * time.Second,
		SocketTimeout:          30 * time.Second,
		MaxPoolSize:            100,
		MinPoolSize:            5,
		MaxConnIdleTime:        5 * time.Minute,
	}

	storage, err := mongodb.NewStorage(dbConfig)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		if err := storage.Close(ctx); err != nil {
			t.Logf("Failed to close storage: %v", err)
		}
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect MongoDB client: %v", err)
		}
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}

	return storage, cleanup
}

// TestSmtPersistenceAndRestoration tests SMT persistence and restoration with consistent root hashes
func TestSmtPersistenceAndRestoration(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Create test data
	testLeaves := []*smt.Leaf{
		{Path: big.NewInt(5), Value: []byte("test_value_5")},
		{Path: big.NewInt(12), Value: []byte("test_value_12")},
		{Path: big.NewInt(15), Value: []byte("test_value_15")},
		{Path: big.NewInt(16), Value: []byte("test_value_16")},
	}

	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
		},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create RoundManager")

	// Test persistence
	err = rm.persistSmtNodes(ctx, testLeaves)
	require.NoError(t, err, "Should persist SMT nodes")

	// Verify nodes were stored
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testLeaves)), count, "Should have stored all SMT nodes")

	// Test restoration produces same root hash as fresh SMT
	freshSmt := smt.NewSparseMerkleTree(api.SHA256)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT should accept leaves")
	freshHash := freshSmt.GetRootHashHex()

	// Create RoundManager and call Start() to trigger restoration
	restoredRm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create RoundManager")

	err = restoredRm.Start(ctx)
	require.NoError(t, err, "SMT restoration should succeed")
	defer func() {
		if err := restoredRm.Stop(ctx); err != nil {
			t.Logf("Failed to stop restored RoundManager: %v", err)
		}
	}()
	restoredHash := restoredRm.smt.GetRootHash()

	assert.Equal(t, freshHash, restoredHash, "Restored SMT should have same root hash as fresh SMT")

	// Verify inclusion proofs work
	for _, leaf := range testLeaves {
		merkleTreePath := restoredRm.smt.GetPath(leaf.Path)
		require.NotNil(t, merkleTreePath, "Should be able to get Merkle path")
		assert.NotEmpty(t, merkleTreePath.Root, "Merkle path should have root hash")
	}
}

// TestLargeSmtRestoration tests multi-chunk restoration with large dataset
func TestLargeSmtRestoration(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
		},
	}
	rm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create RoundManager")

	const testNodeCount = 2500 // Ensure multiple chunks (chunkSize = 1000 in round_manager.go)

	// Create large dataset with non-sequential paths to test ordering
	testLeaves := make([]*smt.Leaf, testNodeCount)
	for i := 0; i < testNodeCount; i++ {
		path := big.NewInt(int64(i*7 + 1))
		value := []byte(fmt.Sprintf("large_test_value_%d", i))
		testLeaves[i] = smt.NewLeaf(path, value)
	}

	// Create fresh SMT for comparison
	freshSmt := smt.NewSparseMerkleTree(api.SHA256)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT AddLeaves should succeed")
	freshHash := freshSmt.GetRootHashHex()

	// Persist leaves to storage
	err = rm.persistSmtNodes(ctx, testLeaves)
	require.NoError(t, err, "Should persist large number of SMT nodes")

	// Verify count
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(testNodeCount), count, "Should have stored all nodes")

	// Create new RoundManager and call Start() to restore from storage (uses multiple chunks)
	newRm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create new RoundManager")

	err = newRm.Start(ctx)
	require.NoError(t, err, "Large SMT restoration should succeed")
	defer func() {
		if err := newRm.Stop(ctx); err != nil {
			t.Logf("Failed to stop new RoundManager: %v", err)
		}
	}()

	restoredHash := newRm.smt.GetRootHash()

	// Critical test: multi-chunk restoration should match single-batch creation
	assert.Equal(t, freshHash, restoredHash, "Multi-chunk restoration should produce same hash as fresh SMT")
}

// TestCompleteWorkflowWithRestart tests end-to-end workflow including service restart simulation
func TestCompleteWorkflowWithRestart(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()

	// Create test commitments
	testCommitments := []*models.Commitment{
		createTestCommitment(t, "request_1"),
		createTestCommitment(t, "request_2"),
		createTestCommitment(t, "request_3"),
	}

	// Process commitments in first round manager instance
	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			BatchLimit: 1000,
		},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create RoundManager")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(1)),
		State:       RoundStateProcessing,
		Commitments: testCommitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	blockNumber := api.NewBigInt(big.NewInt(1))
	rootHash, records, err := rm.processBatch(ctx, testCommitments, blockNumber)
	require.NoError(t, err, "processBatch should succeed")
	require.NotEmpty(t, rootHash, "Root hash should not be empty")
	require.Equal(t, len(testCommitments), len(records), "Should create record for each commitment")

	// Verify SMT nodes were persisted
	count, err := storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testCommitments)), count, "Should have persisted SMT nodes for all commitments")

	// Simulate service restart with new round manager
	newRm, err := NewRoundManager(&config.Config{Processing: config.ProcessingConfig{RoundDuration: time.Second}}, testLogger, storage)
	require.NoError(t, err, "NewRoundManager should succeed after restart")

	// Call Start() to trigger SMT restoration
	err = newRm.Start(ctx)
	require.NoError(t, err, "Start should succeed and restore SMT")
	defer func() {
		if err := newRm.Stop(ctx); err != nil {
			t.Logf("Failed to stop restarted RoundManager: %v", err)
		}
	}()

	// Verify restored SMT has correct data
	restoredRootHash := newRm.smt.GetRootHash()
	assert.NotEmpty(t, restoredRootHash, "Restored SMT should have non-empty root hash")

	// Verify inclusion proofs work after restart
	for _, commitment := range testCommitments {
		path, err := commitment.RequestID.GetPath()
		require.NoError(t, err, "Should be able to get path from request ID")

		merkleTreePath := newRm.smt.GetPath(path)
		require.NotNil(t, merkleTreePath, "Should be able to get Merkle path")
		assert.NotEmpty(t, merkleTreePath.Root, "Merkle path should have root hash")
		assert.NotEmpty(t, merkleTreePath.Steps, "Merkle path should have steps")
	}
}

// TestSmtRestorationWithBlockVerification tests that SMT restoration verifies against existing blocks
func TestSmtRestorationWithBlockVerification(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// Create test data
	testLeaves := []*smt.Leaf{
		{Path: big.NewInt(10), Value: []byte("block_test_value_10")},
		{Path: big.NewInt(20), Value: []byte("block_test_value_20")},
		{Path: big.NewInt(30), Value: []byte("block_test_value_30")},
	}

	// Create fresh SMT to get expected root hash
	freshSmt := smt.NewSparseMerkleTree(api.SHA256)
	err = freshSmt.AddLeaves(testLeaves)
	require.NoError(t, err, "Fresh SMT should accept leaves")
	expectedRootHash := freshSmt.GetRootHashHex()
	expectedRootHashBytes := freshSmt.GetRootHash()

	// Create a block with the expected root hash
	block := &models.Block{
		Index:               api.NewBigInt(big.NewInt(1)),
		ChainID:             "test-chain",
		Version:             "1.0.0",
		ForkID:              "test-fork",
		RootHash:            api.HexBytes(expectedRootHashBytes), // Use bytes, not hex string
		PreviousBlockHash:   api.HexBytes("0000000000000000000000000000000000000000000000000000000000000000"),
		NoDeletionProofHash: api.HexBytes(""),
		CreatedAt:           api.NewTimestamp(time.Now()),
		UnicityCertificate:  api.HexBytes("certificate_data"),
	}

	// Store the block
	err = storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err, "Should store test block")

	// Create RoundManager and persist SMT nodes
	cfg := &config.Config{
		Processing: config.ProcessingConfig{RoundDuration: time.Second},
	}
	rm, err := NewRoundManager(cfg, testLogger, storage)
	require.NoError(t, err, "Should create RoundManager")

	// Persist SMT nodes to storage
	err = rm.persistSmtNodes(ctx, testLeaves)
	require.NoError(t, err, "Should persist SMT nodes")

	// Test 1: Successful verification (matching root hash)
	t.Run("SuccessfulVerification", func(t *testing.T) {
		successRm, err := NewRoundManager(cfg, testLogger, storage)
		require.NoError(t, err, "Should create RoundManager")

		err = successRm.Start(ctx)
		require.NoError(t, err, "SMT restoration should succeed when root hashes match")
		defer func() {
			if err := successRm.Stop(ctx); err != nil {
				t.Logf("Failed to stop RoundManager: %v", err)
			}
		}()

		// Verify the restored SMT has the correct hash
		restoredHash := successRm.smt.GetRootHash()
		assert.Equal(t, expectedRootHash, restoredHash, "Restored SMT should have expected root hash")
	})

	// Test 2: Failed verification (mismatched root hash)
	t.Run("FailedVerification", func(t *testing.T) {
		// Create a block with a different root hash to simulate mismatch
		wrongBlock := &models.Block{
			Index:               api.NewBigInt(big.NewInt(2)),
			ChainID:             "test-chain",
			Version:             "1.0.0",
			ForkID:              "test-fork",
			RootHash:            api.HexBytes("wrong_root_hash_value"), // Intentionally wrong hash
			PreviousBlockHash:   api.HexBytes("0000000000000000000000000000000000000000000000000000000000000001"),
			NoDeletionProofHash: api.HexBytes(""),
			CreatedAt:           api.NewTimestamp(time.Now()),
			UnicityCertificate:  api.HexBytes("certificate_data"),
		}

		// Store the wrong block (this will become the "latest" block)
		err = storage.BlockStorage().Store(ctx, wrongBlock)
		require.NoError(t, err, "Should store wrong test block")

		failRm, err := NewRoundManager(cfg, testLogger, storage)
		require.NoError(t, err, "Should create RoundManager")

		// This should fail because the restored SMT root hash doesn't match the latest block
		err = failRm.Start(ctx)
		require.Error(t, err, "SMT restoration should fail when root hashes don't match")
	})
}

// createTestCommitment creates a valid, signed commitment for testing
func createTestCommitment(t *testing.T, baseData string) *models.Commitment {
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	stateData := make([]byte, 32)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		require.NoError(t, err, "Failed to generate random state data")
	}
	stateHashImprint := signing.CreateDataHashImprint(stateData)

	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	require.NoError(t, err, "Failed to create request ID")

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_%s", baseData)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		require.NoError(t, err, "Failed to generate random transaction data")
	}
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	transactionHashBytes, err := transactionHashImprint.DataBytes()
	require.NoError(t, err, "Failed to extract transaction hash")

	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction")

	authenticator := models.Authenticator{
		Algorithm: "secp256k1",
		PublicKey: publicKeyBytes,
		Signature: signatureBytes,
		StateHash: stateHashImprint,
	}

	return models.NewCommitment(requestID, transactionHashImprint, authenticator)
}
