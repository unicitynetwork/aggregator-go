package round

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	redisStorage "github.com/unicitynetwork/aggregator-go/internal/storage/redis"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RecoveryTestSuite tests the recovery functionality with real MongoDB and Redis
type RecoveryTestSuite struct {
	suite.Suite
	ctx             context.Context
	storage         *mongodb.Storage
	commitmentQueue interfaces.CommitmentQueue
	redisClient     *redis.Client
	testLogger      *logger.Logger
	mongoCleanup    func()
	redisCleanup    func()
}

func TestRecoverySuite(t *testing.T) {
	suite.Run(t, new(RecoveryTestSuite))
}

func (s *RecoveryTestSuite) SetupSuite() {
	s.ctx = context.Background()

	// Start Redis container
	redisC, err := redisContainer.Run(s.ctx, "redis:7")
	s.Require().NoError(err)
	s.redisCleanup = func() {
		_ = redisC.Terminate(s.ctx)
	}
	redisURI, err := redisC.ConnectionString(s.ctx)
	s.Require().NoError(err)

	// Start MongoDB container with replica set (required for transactions)
	mongoC, err := mongoContainer.Run(s.ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	s.Require().NoError(err)
	s.mongoCleanup = func() {
		_ = mongoC.Terminate(s.ctx)
	}
	mongoURI, err := mongoC.ConnectionString(s.ctx)
	s.Require().NoError(err)
	mongoURI += "&directConnection=true"

	// Create logger
	s.testLogger, err = logger.New("info", "text", "stdout", false)
	s.Require().NoError(err)

	// Initialize MongoDB storage
	cfg := config.Config{
		Database: config.DatabaseConfig{
			URI:                    mongoURI,
			Database:               "test_recovery",
			ConnectTimeout:         30 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            100,
			MinPoolSize:            5,
			MaxConnIdleTime:        5 * time.Minute,
		},
	}
	s.storage, err = mongodb.NewStorage(s.ctx, cfg)
	s.Require().NoError(err)
	err = s.storage.Initialize(s.ctx)
	s.Require().NoError(err)

	// Initialize Redis commitment queue
	opt, err := redis.ParseURL(redisURI)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opt)
	s.commitmentQueue = redisStorage.NewCommitmentStorage(s.redisClient, "test_commitments", "test_server", redisStorage.DefaultBatchConfig(), s.testLogger)
	err = s.commitmentQueue.Initialize(s.ctx)
	s.Require().NoError(err)
}

func (s *RecoveryTestSuite) TearDownSuite() {
	if s.commitmentQueue != nil {
		_ = s.commitmentQueue.Close(s.ctx)
	}
	if s.redisClient != nil {
		_ = s.redisClient.Close()
	}
	if s.storage != nil {
		_ = s.storage.Close(s.ctx)
	}
	if s.redisCleanup != nil {
		s.redisCleanup()
	}
	if s.mongoCleanup != nil {
		s.mongoCleanup()
	}
}

func (s *RecoveryTestSuite) SetupTest() {
	// Clean MongoDB collections
	_ = s.storage.CleanAllCollections(s.ctx)
	// Recreate indexes after dropping collections
	_ = s.storage.Initialize(s.ctx)
	// Flush Redis
	_ = s.redisClient.FlushAll(s.ctx).Err()
	_ = s.commitmentQueue.Initialize(s.ctx)
}

// Helper to create and store test data
func (s *RecoveryTestSuite) createTestData(blockNum int64, commitmentCount int, prefix string) ([]*models.Commitment, *models.Block, []api.RequestID) {
	t := s.T()
	blockNumber := api.NewBigInt(big.NewInt(blockNum))

	// Create commitments
	commitments := testutil.CreateTestCommitments(t, commitmentCount, prefix)

	// Create request IDs
	requestIDs := make([]api.RequestID, len(commitments))
	for i, c := range commitments {
		requestIDs[i] = c.RequestID
	}

	// Compute SMT root hash
	smtTree := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	leaves := make([]*smt.Leaf, len(commitments))
	for i, c := range commitments {
		path, err := c.RequestID.GetPath()
		require.NoError(t, err)
		leafValue, err := c.CreateLeafValue()
		require.NoError(t, err)
		leaves[i] = &smt.Leaf{Path: path, Value: leafValue}
	}
	err := smtTree.AddLeaves(leaves)
	require.NoError(t, err)
	rootHashBytes := smtTree.GetRootHash()

	// Create block (unfinalized)
	block := models.NewBlock(blockNumber, "unicity", 0, "1.0", "mainnet", api.HexBytes(rootHashBytes), nil, nil, nil)
	block.Finalized = false

	return commitments, block, requestIDs
}

// Helper to store commitments in Redis pending queue
func (s *RecoveryTestSuite) storeCommitmentsInRedis(commitments []*models.Commitment) {
	for _, c := range commitments {
		err := s.commitmentQueue.Store(s.ctx, c)
		s.Require().NoError(err)
	}
	// Small delay to ensure Redis has flushed
	time.Sleep(200 * time.Millisecond)
}

// Helper to store SMT nodes
func (s *RecoveryTestSuite) storeSmtNodes(commitments []*models.Commitment) {
	nodes := make([]*models.SmtNode, len(commitments))
	for i, c := range commitments {
		path, err := c.RequestID.GetPath()
		s.Require().NoError(err)
		leafValue, err := c.CreateLeafValue()
		s.Require().NoError(err)
		nodes[i] = models.NewSmtNode(api.HexBytes(path.Bytes()), leafValue)
	}
	err := s.storage.SmtStorage().StoreBatch(s.ctx, nodes)
	s.Require().NoError(err)
}

// Helper to store aggregator records
func (s *RecoveryTestSuite) storeAggregatorRecords(commitments []*models.Commitment, blockNumber *api.BigInt) {
	records := make([]*models.AggregatorRecord, len(commitments))
	for i, c := range commitments {
		records[i] = models.NewAggregatorRecord(c, blockNumber, api.NewBigInt(big.NewInt(int64(i))))
	}
	err := s.storage.AggregatorRecordStorage().StoreBatch(s.ctx, records)
	s.Require().NoError(err)
}

// ============================================================================
// Test 1: No Unfinalized Blocks (Normal Startup)
// ============================================================================
func (s *RecoveryTestSuite) Test01_NoUnfinalizedBlocks() {
	t := s.T()

	// Create a finalized block
	commitments, block, requestIDs := s.createTestData(1, 3, "t01")
	block.Finalized = true

	// Store the finalized block
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store SMT nodes and aggregator records
	s.storeSmtNodes(commitments)
	s.storeAggregatorRecords(commitments, block.Index)

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.Recovered, "Should not recover anything")
	require.Nil(t, result.BlockNumber)

	t.Log("✓ Test01_NoUnfinalizedBlocks passed")
}

// ============================================================================
// Test 2: All Data Present (Just Needs Finalization)
// ============================================================================
func (s *RecoveryTestSuite) Test02_AllDataPresent() {
	t := s.T()

	// Create unfinalized block with all data present
	commitments, block, requestIDs := s.createTestData(2, 5, "t02")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store all SMT nodes and aggregator records
	s.storeSmtNodes(commitments)
	s.storeAggregatorRecords(commitments, block.Index)

	// Store commitments in Redis (for acking)
	s.storeCommitmentsInRedis(commitments)

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Recovered, "Should recover the block")
	require.Equal(t, block.Index.String(), result.BlockNumber.String())

	// Verify block is now finalized
	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock, "Block should be findable (finalized)")

	t.Log("✓ Test02_AllDataPresent passed")
}

// ============================================================================
// Test 3: Missing Aggregator Records (Recover from Redis)
// ============================================================================
func (s *RecoveryTestSuite) Test03_MissingAggregatorRecords() {
	t := s.T()

	// Create unfinalized block
	commitments, block, requestIDs := s.createTestData(3, 4, "t03")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store ONLY SMT nodes (no aggregator records)
	s.storeSmtNodes(commitments)

	// Store commitments in Redis (for recovery)
	s.storeCommitmentsInRedis(commitments)

	// Verify no aggregator records exist
	count, err := s.storage.AggregatorRecordStorage().Count(s.ctx)
	require.NoError(t, err)
	countBefore := count

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.True(t, result.Recovered)

	// Verify aggregator records were recovered
	countAfter, err := s.storage.AggregatorRecordStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, countBefore+int64(len(commitments)), countAfter, "Aggregator records should be recovered")

	// Verify block is finalized
	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock)

	t.Log("✓ Test03_MissingAggregatorRecords passed")
}

// ============================================================================
// Test 4: Missing SMT Nodes (Recover from Redis)
// ============================================================================
func (s *RecoveryTestSuite) Test04_MissingSmtNodes() {
	t := s.T()

	// Create unfinalized block
	commitments, block, requestIDs := s.createTestData(4, 4, "t04")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store ONLY aggregator records (no SMT nodes for this block's commitments)
	s.storeAggregatorRecords(commitments, block.Index)

	// Store commitments in Redis (for recovery)
	s.storeCommitmentsInRedis(commitments)

	// Get count before
	countBefore, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.True(t, result.Recovered)

	// Verify SMT nodes were recovered
	countAfter, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, countBefore+int64(len(commitments)), countAfter, "SMT nodes should be recovered")

	// Verify block is finalized
	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock)

	t.Log("✓ Test04_MissingSmtNodes passed")
}

// ============================================================================
// Test 5: Missing Both (Recover from Redis)
// ============================================================================
func (s *RecoveryTestSuite) Test05_MissingBoth() {
	t := s.T()

	// Create unfinalized block
	commitments, block, requestIDs := s.createTestData(5, 5, "t05")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store NO SMT nodes and NO aggregator records
	// Store commitments in Redis (for recovery)
	s.storeCommitmentsInRedis(commitments)

	// Get counts before
	smtCountBefore, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	recordCountBefore, err := s.storage.AggregatorRecordStorage().Count(s.ctx)
	require.NoError(t, err)

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.True(t, result.Recovered)

	// Verify both were recovered
	smtCountAfter, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, smtCountBefore+int64(len(commitments)), smtCountAfter, "SMT nodes should be recovered")

	recordCountAfter, err := s.storage.AggregatorRecordStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, recordCountBefore+int64(len(commitments)), recordCountAfter, "Aggregator records should be recovered")

	// Verify block is finalized
	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock)

	t.Log("✓ Test05_MissingBoth passed")
}

// ============================================================================
// Test 6: Multiple Unfinalized Blocks (FATAL Error)
// ============================================================================
func (s *RecoveryTestSuite) Test06_MultipleUnfinalizedBlocks() {
	t := s.T()

	// Create two unfinalized blocks
	_, block1, requestIDs1 := s.createTestData(6, 2, "t06a")
	_, block2, requestIDs2 := s.createTestData(7, 2, "t06b")

	// Store both blocks as unfinalized
	err := s.storage.BlockStorage().Store(s.ctx, block1)
	require.NoError(t, err)
	err = s.storage.BlockStorage().Store(s.ctx, block2)
	require.NoError(t, err)

	// Store block records for both
	err = s.storage.BlockRecordsStorage().Store(s.ctx, models.NewBlockRecords(block1.Index, requestIDs1))
	require.NoError(t, err)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, models.NewBlockRecords(block2.Index, requestIDs2))
	require.NoError(t, err)

	// Run recovery - should fail with FATAL error
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.Error(t, err, "Should return error for multiple unfinalized blocks")
	require.Nil(t, result)
	require.Contains(t, err.Error(), "FATAL")
	require.Contains(t, err.Error(), "2 unfinalized blocks")

	// Clean up - finalize both blocks to not affect other tests
	_ = s.storage.BlockStorage().SetFinalized(s.ctx, block1.Index, true)
	_ = s.storage.BlockStorage().SetFinalized(s.ctx, block2.Index, true)

	t.Log("✓ Test06_MultipleUnfinalizedBlocks passed")
}

// ============================================================================
// Test 7: Missing Block Records (FATAL Error)
// ============================================================================
func (s *RecoveryTestSuite) Test07_MissingBlockRecords() {
	t := s.T()

	// Create unfinalized block
	_, block, _ := s.createTestData(8, 3, "t07")

	// Store block (unfinalized) but NO block records
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Run recovery - should fail with FATAL error
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.Error(t, err, "Should return error for missing block records")
	require.Nil(t, result)
	require.Contains(t, err.Error(), "FATAL")
	require.Contains(t, err.Error(), "block records not found")

	// Clean up
	_ = s.storage.BlockStorage().SetFinalized(s.ctx, block.Index, true)

	t.Log("✓ Test07_MissingBlockRecords passed")
}

// ============================================================================
// Test 8: Commitment Not Found (FATAL Error)
// ============================================================================
func (s *RecoveryTestSuite) Test08_CommitmentNotFound() {
	t := s.T()

	// Create unfinalized block
	commitments, block, requestIDs := s.createTestData(9, 3, "t08")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store ONLY some commitments in Redis (not all)
	// Only store first commitment, missing the rest
	err = s.commitmentQueue.Store(s.ctx, commitments[0])
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Run recovery - should fail because commitment data not found
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.Error(t, err, "Should return error when commitment not found")
	require.Nil(t, result)
	require.Contains(t, err.Error(), "FATAL")
	require.Contains(t, err.Error(), "commitment not found")

	// Clean up
	_ = s.storage.BlockStorage().SetFinalized(s.ctx, block.Index, true)

	t.Log("✓ Test08_CommitmentNotFound passed")
}

// ============================================================================
// Test 9: Partial Aggregator Records - Verify LeafIndex Preservation
// ============================================================================
func (s *RecoveryTestSuite) Test09_PartialAggregatorRecords_LeafIndexPreserved() {
	t := s.T()

	// Create unfinalized block with 5 commitments
	commitments, block, requestIDs := s.createTestData(10, 5, "t09partial")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store ONLY aggregator records at positions 0, 1, and 4 (missing 2 and 3)
	existingIndices := []int{0, 1, 4}
	existingRecords := make([]*models.AggregatorRecord, len(existingIndices))
	for i, idx := range existingIndices {
		existingRecords[i] = models.NewAggregatorRecord(commitments[idx], block.Index, api.NewBigInt(big.NewInt(int64(idx))))
	}
	err = s.storage.AggregatorRecordStorage().StoreBatch(s.ctx, existingRecords)
	require.NoError(t, err)

	// Store all SMT nodes (so only aggregator records need recovery)
	s.storeSmtNodes(commitments)

	// Store ALL commitments in Redis (for recovery)
	s.storeCommitmentsInRedis(commitments)

	// Run recovery
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.True(t, result.Recovered)

	// Verify the recovered records have correct leaf indices
	// Record at position 2 should have leafIndex=2, record at position 3 should have leafIndex=3
	missingIndices := []int{2, 3}
	for _, idx := range missingIndices {
		record, err := s.storage.AggregatorRecordStorage().GetByRequestID(s.ctx, requestIDs[idx])
		require.NoError(t, err)
		require.NotNil(t, record, "Record at position %d should exist after recovery", idx)
		require.Equal(t, int64(idx), record.LeafIndex.Int.Int64(),
			"Record at position %d should have leafIndex=%d, got %d", idx, idx, record.LeafIndex.Int.Int64())
	}

	// Also verify existing records still have correct indices
	for _, idx := range existingIndices {
		record, err := s.storage.AggregatorRecordStorage().GetByRequestID(s.ctx, requestIDs[idx])
		require.NoError(t, err)
		require.NotNil(t, record, "Existing record at position %d should still exist", idx)
		require.Equal(t, int64(idx), record.LeafIndex.Int.Int64(),
			"Existing record at position %d should still have leafIndex=%d", idx, idx)
	}

	t.Log("✓ Test09_PartialAggregatorRecords_LeafIndexPreserved passed")
}

// ============================================================================
// Test 10: Partial SMT Nodes - Verify Correct Detection of Missing Nodes
// ============================================================================
func (s *RecoveryTestSuite) Test10_PartialSmtNodes_CorrectDetection() {
	t := s.T()

	// Create unfinalized block with 5 commitments
	commitments, block, requestIDs := s.createTestData(10, 5, "t10partial")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store block records
	blockRecords := models.NewBlockRecords(block.Index, requestIDs)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, blockRecords)
	require.NoError(t, err)

	// Store ALL aggregator records (so only SMT nodes need recovery)
	s.storeAggregatorRecords(commitments, block.Index)

	// Store ONLY SMT nodes at positions 0, 1, and 4 (missing 2 and 3)
	existingIndices := []int{0, 1, 4}
	existingNodes := make([]*models.SmtNode, len(existingIndices))
	for i, idx := range existingIndices {
		path, err := commitments[idx].RequestID.GetPath()
		require.NoError(t, err)
		leafValue, err := commitments[idx].CreateLeafValue()
		require.NoError(t, err)
		existingNodes[i] = models.NewSmtNode(api.HexBytes(path.Bytes()), leafValue)
	}
	err = s.storage.SmtStorage().StoreBatch(s.ctx, existingNodes)
	require.NoError(t, err)

	// Store ONLY the commitments that need recovery (positions 2 and 3) in Redis
	missingIndices := []int{2, 3}
	for _, idx := range missingIndices {
		err = s.commitmentQueue.Store(s.ctx, commitments[idx])
		require.NoError(t, err)
	}
	time.Sleep(200 * time.Millisecond)

	// Verify we have exactly 3 SMT nodes before recovery
	smtCountBefore, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), smtCountBefore, "Should have 3 SMT nodes before recovery")

	// Run recovery - should only recover 2 missing nodes
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err, "Recovery should succeed")
	require.True(t, result.Recovered)

	// Verify we now have all 5 SMT nodes
	smtCountAfter, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), smtCountAfter, "Should have 5 SMT nodes after recovery")

	t.Log("✓ Test10_PartialSmtNodes_CorrectDetection passed")
}

// ============================================================================
// Test 11: LoadRecoveredNodesIntoSMT
// ============================================================================
func (s *RecoveryTestSuite) Test11_LoadRecoveredNodesIntoSMT() {
	t := s.T()

	// Create test commitments and store their SMT nodes
	commitments := testutil.CreateTestCommitments(t, 4, "t09")
	requestIDs := make([]api.RequestID, len(commitments))
	for i, c := range commitments {
		requestIDs[i] = c.RequestID
	}

	// Compute expected root hash
	expectedSMT := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	leaves := make([]*smt.Leaf, len(commitments))
	for i, c := range commitments {
		path, err := c.RequestID.GetPath()
		require.NoError(t, err)
		leafValue, err := c.CreateLeafValue()
		require.NoError(t, err)
		leaves[i] = &smt.Leaf{Path: path, Value: leafValue}
	}
	err := expectedSMT.AddLeaves(leaves)
	require.NoError(t, err)
	expectedRootHash := expectedSMT.GetRootHashHex() // Use hex string for comparison

	// Store SMT nodes in MongoDB
	s.storeSmtNodes(commitments)

	// Create empty SMT to load into
	targetSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
	require.NotEqual(t, expectedRootHash, targetSMT.GetRootHash(), "SMT should be empty initially")

	// Load recovered nodes into SMT
	err = LoadRecoveredNodesIntoSMT(s.ctx, s.testLogger, s.storage, targetSMT, requestIDs)
	require.NoError(t, err)

	// Verify SMT has correct root hash (ThreadSafeSMT.GetRootHash() returns hex string)
	actualRootHash := targetSMT.GetRootHash()
	require.Equal(t, expectedRootHash, actualRootHash, "SMT root hash should match after loading recovered nodes")

	t.Log("✓ Test11_LoadRecoveredNodesIntoSMT passed")
}

// ============================================================================
// Test 12: CleanupProcessedPendingCommitments - No Pending
// ============================================================================
func (s *RecoveryTestSuite) Test12_CleanupNoPending() {
	t := s.T()

	// No pending commitments in Redis - cleanup should succeed with no-op
	err := CleanupProcessedPendingCommitments(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)

	t.Log("✓ Test12_CleanupNoPending passed")
}

// ============================================================================
// Test 13: CleanupProcessedPendingCommitments - All New (not processed)
// ============================================================================
func (s *RecoveryTestSuite) Test13_CleanupAllNew() {
	t := s.T()

	// Create and store commitments in Redis (but NOT in AggregatorRecords)
	commitments := testutil.CreateTestCommitments(t, 3, "t13")
	s.storeCommitmentsInRedis(commitments)

	// Read them to make them "pending" (claimed but not ACKed)
	s.readPendingToMakeThemClaimed()

	// Verify we have 3 unprocessed (pending)
	count, err := s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "Should have 3 unprocessed commitments")

	// Run cleanup - none should be ACKed since they're not in AggregatorRecords
	err = CleanupProcessedPendingCommitments(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)

	// Verify still 3 unprocessed (none were ACKed)
	count, err = s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "Should still have 3 unprocessed commitments")

	t.Log("✓ Test13_CleanupAllNew passed")
}

// ============================================================================
// Test 14: CleanupProcessedPendingCommitments - All Processed
// ============================================================================
func (s *RecoveryTestSuite) Test14_CleanupAllProcessed() {
	t := s.T()

	// Create commitments
	commitments, block, _ := s.createTestData(14, 3, "t14")

	// Store in Redis and make them pending
	s.storeCommitmentsInRedis(commitments)
	s.readPendingToMakeThemClaimed()

	// Store AggregatorRecords (simulate block was finalized)
	s.storeAggregatorRecords(commitments, block.Index)

	// Verify we have 3 unprocessed
	count, err := s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "Should have 3 unprocessed commitments")

	// Run cleanup - all should be ACKed since they're in AggregatorRecords
	err = CleanupProcessedPendingCommitments(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)

	// Verify 0 unprocessed (all were ACKed)
	count, err = s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "Should have 0 unprocessed commitments after cleanup")

	t.Log("✓ Test14_CleanupAllProcessed passed")
}

// ============================================================================
// Test 15: CleanupProcessedPendingCommitments - Mixed
// ============================================================================
func (s *RecoveryTestSuite) Test15_CleanupMixed() {
	t := s.T()

	// Create 4 commitments
	commitments := testutil.CreateTestCommitments(t, 4, "t15")
	blockNumber := api.NewBigInt(big.NewInt(15))

	// Store all 4 in Redis and make them pending
	s.storeCommitmentsInRedis(commitments)
	s.readPendingToMakeThemClaimed()

	// Store only first 2 in AggregatorRecords (simulate partial processing)
	processedCommitments := commitments[:2]
	s.storeAggregatorRecords(processedCommitments, blockNumber)

	// Verify we have 4 unprocessed
	count, err := s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), count, "Should have 4 unprocessed commitments")

	// Run cleanup - only 2 should be ACKed
	err = CleanupProcessedPendingCommitments(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)

	// Verify 2 unprocessed remain (the new ones)
	count, err = s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "Should have 2 unprocessed commitments after cleanup")

	t.Log("✓ Test15_CleanupMixed passed")
}

// ============================================================================
// Test 16: RecoverUnfinalizedBlock calls cleanup when no unfinalized block
// ============================================================================
func (s *RecoveryTestSuite) Test16_RecoveryCallsCleanup() {
	t := s.T()

	// Create a FINALIZED block with records
	commitments, block, requestIDs := s.createTestData(16, 3, "t16")
	block.Finalized = true
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)
	err = s.storage.BlockRecordsStorage().Store(s.ctx, models.NewBlockRecords(block.Index, requestIDs))
	require.NoError(t, err)
	s.storeAggregatorRecords(commitments, block.Index)
	s.storeSmtNodes(commitments)

	// Store commitments in Redis as pending (simulating MarkProcessed failure)
	s.storeCommitmentsInRedis(commitments)
	s.readPendingToMakeThemClaimed()

	// Verify we have 3 unprocessed
	count, err := s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "Should have 3 unprocessed commitments")

	// Run recovery - should call cleanup since no unfinalized blocks
	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.False(t, result.Recovered, "Should not have recovered any block")

	// Verify cleanup ran - 0 unprocessed (all were ACKed)
	count, err = s.commitmentQueue.CountUnprocessed(s.ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "Should have 0 unprocessed commitments after recovery cleanup")

	t.Log("✓ Test16_RecoveryCallsCleanup passed")
}

// Helper to read pending commitments to make them "claimed" state
func (s *RecoveryTestSuite) readPendingToMakeThemClaimed() {
	// Reading from stream claims the messages (moves them to pending state)
	commitmentChan := make(chan *models.Commitment, 1000)
	ctx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
	defer cancel()

	go func() {
		_ = s.commitmentQueue.StreamCommitments(ctx, commitmentChan)
	}()

	// Wait for timeout to ensure all are claimed
	<-ctx.Done()
	close(commitmentChan)
}
