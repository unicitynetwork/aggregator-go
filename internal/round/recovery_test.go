package round

import (
	"bytes"
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
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
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
	redisC, err := redisContainer.Run(s.ctx, "redis:7-alpine")
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
func (s *RecoveryTestSuite) createTestData(blockNum int64, commitmentCount int, prefix string) ([]*models.CertificationRequest, *models.Block, []api.StateID) {
	t := s.T()
	blockNumber := api.NewBigInt(big.NewInt(blockNum))

	// Create commitments
	commitments := testutil.CreateTestCertificationRequests(t, commitmentCount, prefix)

	// Create state IDs.
	stateIDs := make([]api.StateID, len(commitments))
	for i, c := range commitments {
		stateIDs[i] = c.StateID
	}

	// Compute SMT root hash
	smtTree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	leaves := make([]*smt.Leaf, len(commitments))
	for i, c := range commitments {
		path, err := c.StateID.GetPath()
		require.NoError(t, err)
		leafValue, err := c.LeafValue()
		require.NoError(t, err)
		leaves[i] = smt.NewLeaf(path, leafValue)
	}
	err := smtTree.AddLeaves(leaves)
	require.NoError(t, err)
	rootHashBytes := smtTree.GetRootHashRaw()

	// Create block (unfinalized)
	block := models.NewBlock(blockNumber, "unicity", 0, "1.0", "mainnet", api.HexBytes(rootHashBytes), nil, nil)
	block.Finalized = false
	block.ProposalID = "proposal-" + blockNumber.String()

	return commitments, block, stateIDs
}

// Helper to store commitments in Redis pending queue
func (s *RecoveryTestSuite) storeCommitmentsInRedis(commitments []*models.CertificationRequest) {
	for _, c := range commitments {
		err := s.commitmentQueue.Store(s.ctx, c)
		s.Require().NoError(err)
	}
	// Small delay to ensure Redis has flushed
	time.Sleep(200 * time.Millisecond)
}

// Helper to store SMT nodes
func (s *RecoveryTestSuite) storeSmtNodes(commitments []*models.CertificationRequest) {
	nodes := make([]*models.SmtNode, len(commitments))
	for i, c := range commitments {
		keyBytes, err := c.StateID.GetTreeKey()
		s.Require().NoError(err)
		leafValue, err := c.LeafValue()
		s.Require().NoError(err)
		nodes[i] = models.NewSmtNode(api.HexBytes(keyBytes), leafValue)
	}
	err := s.storage.SmtStorage().StoreBatch(s.ctx, nodes)
	s.Require().NoError(err)
}

// Helper to store aggregator records
func (s *RecoveryTestSuite) storeAggregatorRecords(commitments []*models.CertificationRequest, block *models.Block) {
	records := make([]*models.AggregatorRecord, len(commitments))
	for i, c := range commitments {
		records[i] = models.NewAggregatorRecord(c, block.Index, api.NewBigInt(big.NewInt(int64(i))))
		records[i].ProposalID = block.ProposalID
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
	commitments, block, _ := s.createTestData(1, 3, "t01")
	block.Finalized = true

	// Store the finalized block
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store SMT nodes and aggregator records
	s.storeSmtNodes(commitments)
	s.storeAggregatorRecords(commitments, block)

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
	commitments, block, _ := s.createTestData(2, 5, "t02")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store all SMT nodes and aggregator records
	s.storeSmtNodes(commitments)
	s.storeAggregatorRecords(commitments, block)

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
// Test 3: Missing SMT Nodes (Recover from Durable Records)
// ============================================================================
func (s *RecoveryTestSuite) Test04_MissingSmtNodes() {
	t := s.T()

	// Create unfinalized block
	commitments, block, _ := s.createTestData(4, 4, "t04")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store ONLY aggregator records (no SMT nodes for this block's commitments)
	s.storeAggregatorRecords(commitments, block)

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

func (s *RecoveryTestSuite) Test04b_MissingSmtNodesRecoverFromDurableRecordsWithoutRedis() {
	t := s.T()

	commitments, block, _ := s.createTestData(40, 4, "t04b")

	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Durable proposal records must be sufficient to rebuild the certified SMT
	// root even if Redis loses the original pending commitments.
	s.storeAggregatorRecords(commitments, block)

	smtCountBefore, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)

	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.True(t, result.Recovered)

	smtCountAfter, err := s.storage.SmtStorage().Count(s.ctx)
	require.NoError(t, err)
	require.Equal(t, smtCountBefore+int64(len(commitments)), smtCountAfter)

	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock)
}

func (s *RecoveryTestSuite) Test04c_FinalizeCertifiedProposalFromDurableRecordsWithoutRedis() {
	t := s.T()

	commitments, block, stateIDs := s.createTestData(41, 4, "t04c")
	block.Status = models.FinalityStatusProposed
	block.Finalized = false
	proposalUC := api.NewHexBytes([]byte{0x04, 0x0c})

	records := make([]*models.AggregatorRecord, len(commitments))
	for i, c := range commitments {
		record := models.NewAggregatorRecord(c, block.Index, api.NewBigInt(big.NewInt(int64(i))))
		record.ProposalID = block.ProposalID
		records[i] = record
	}

	require.NoError(t, s.storage.WithTransaction(s.ctx, func(txCtx context.Context) error {
		if err := s.storage.BlockStorage().Store(txCtx, block); err != nil {
			return err
		}
		return s.storage.AggregatorRecordStorage().StoreBatch(txCtx, records)
	}))
	require.NoError(t, s.redisClient.FlushAll(s.ctx).Err())

	cfg := config.Config{
		Database: config.DatabaseConfig{Database: "test_durable_proposal_recovery"},
		Processing: config.ProcessingConfig{
			CommitmentStreamBufferSize: 16,
			MaxCommitmentsPerRound:     1000,
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	rm, err := NewRoundManager(
		s.ctx,
		&cfg,
		s.testLogger,
		s.commitmentQueue,
		s.storage,
		nil,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(s.testLogger),
		smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)),
		nil,
	)
	require.NoError(t, err)

	recovered, err := rm.FinalizeCertifiedProposal(s.ctx, block.Index, block.RootHash, proposalUC)
	require.NoError(t, err)
	require.True(t, recovered)

	finalizedBlock, err := s.storage.BlockStorage().GetByNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, finalizedBlock)
	require.True(t, finalizedBlock.Finalized)
	require.Equal(t, models.FinalityStatusFinalized, finalizedBlock.Status)
	require.Equal(t, proposalUC, finalizedBlock.UnicityCertificate)

	for _, stateID := range stateIDs {
		record, err := s.storage.AggregatorRecordStorage().GetByStateID(s.ctx, stateID)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Equal(t, block.ProposalID, record.ProposalID)
	}
}

func (s *RecoveryTestSuite) Test04d_AbandonDurableProposalFreesStateIDsForRestage() {
	t := s.T()

	commitments, block, stateIDs := s.createTestData(42, 2, "t04d")
	block.Status = models.FinalityStatusProposed
	block.Finalized = false
	block.ProposalID = "proposal-42a"

	records := make([]*models.AggregatorRecord, len(commitments))
	for i, c := range commitments {
		record := models.NewAggregatorRecord(c, block.Index, api.NewBigInt(big.NewInt(int64(i))))
		record.ProposalID = block.ProposalID
		records[i] = record
	}

	require.NoError(t, s.storage.WithTransaction(s.ctx, func(txCtx context.Context) error {
		if err := s.storage.BlockStorage().Store(txCtx, block); err != nil {
			return err
		}
		return s.storage.AggregatorRecordStorage().StoreBatch(txCtx, records)
	}))

	rm := &RoundManager{storage: s.storage}
	require.NoError(t, rm.AbandonDurableProposal(s.ctx, block.Index, block.RootHash))

	abandonedBlock, err := getBlockAnyFinalization(s.ctx, s.storage, block.Index)
	require.NoError(t, err)
	require.NotNil(t, abandonedBlock)
	require.Equal(t, models.FinalityStatusAbandoned, abandonedBlock.Status)

	visibleRecords, err := s.storage.AggregatorRecordStorage().GetByBlockNumber(s.ctx, block.Index)
	require.NoError(t, err)
	require.Empty(t, visibleRecords)

	oldRecords, err := getAggregatorRecordsByBlockAnyFinalization(s.ctx, s.storage, block.Index)
	require.NoError(t, err)
	require.Len(t, oldRecords, len(stateIDs))

	restaged := models.NewAggregatorRecord(commitments[0], api.NewBigInt(big.NewInt(43)), api.NewBigInt(big.NewInt(0)))
	restaged.ProposalID = "proposal-43"
	require.NoError(t, s.storage.AggregatorRecordStorage().Store(s.ctx, restaged))

	storedRecords, err := getAggregatorRecordsByBlockAnyFinalization(s.ctx, s.storage, restaged.BlockNumber)
	require.NoError(t, err)
	require.Len(t, storedRecords, 1)
	require.Equal(t, "43", storedRecords[0].BlockNumber.String())
	require.Equal(t, restaged.ProposalID, storedRecords[0].ProposalID)
}

func (s *RecoveryTestSuite) Test04e_OrphanStagedRecordsDoNotAttachToReusedBlockNumber() {
	t := s.T()

	orphanCommitments, orphanBlock, orphanStateIDs := s.createTestData(45, 3, "t04e_orphan")
	orphanBlock.ProposalID = "proposal-45-orphan"
	s.storeAggregatorRecords(orphanCommitments, orphanBlock)

	visibleOrphans, err := s.storage.AggregatorRecordStorage().GetByBlockNumber(s.ctx, orphanBlock.Index)
	require.NoError(t, err)
	require.Empty(t, visibleOrphans, "records without a finalized block must stay invisible")
	for _, stateID := range orphanStateIDs {
		record, err := s.storage.AggregatorRecordStorage().GetByStateID(s.ctx, stateID)
		require.NoError(t, err)
		require.Nil(t, record, "orphan staged record must not be visible by stateID")
	}

	result, err := RecoverUnfinalizedBlock(s.ctx, s.testLogger, s.storage, s.commitmentQueue)
	require.NoError(t, err)
	require.False(t, result.Recovered, "records without a proposed block are not recoverable proposals")

	winnerCommitments, winnerBlock, winnerStateIDs := s.createTestData(45, 4, "t04e_winner")
	winnerBlock.Status = models.FinalityStatusProposed
	winnerBlock.Finalized = false
	winnerBlock.ProposalID = "proposal-45-winner"
	require.False(t, bytes.Equal(orphanBlock.RootHash, winnerBlock.RootHash), "test needs distinct proposal roots")

	winnerRecords := make([]*models.AggregatorRecord, len(winnerCommitments))
	for i, commitment := range winnerCommitments {
		record := models.NewAggregatorRecord(commitment, winnerBlock.Index, api.NewBigInt(big.NewInt(int64(i))))
		record.ProposalID = winnerBlock.ProposalID
		winnerRecords[i] = record
	}
	require.NoError(t, s.storage.WithTransaction(s.ctx, func(txCtx context.Context) error {
		if err := s.storage.BlockStorage().Store(txCtx, winnerBlock); err != nil {
			return err
		}
		return s.storage.AggregatorRecordStorage().StoreBatch(txCtx, winnerRecords)
	}))

	cfg := config.Config{
		Database: config.DatabaseConfig{Database: "test_durable_proposal_orphan_reuse"},
		Processing: config.ProcessingConfig{
			CommitmentStreamBufferSize: 16,
			MaxCommitmentsPerRound:     1000,
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	rm, err := NewRoundManager(
		s.ctx,
		&cfg,
		s.testLogger,
		s.commitmentQueue,
		s.storage,
		nil,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(s.testLogger),
		smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)),
		nil,
	)
	require.NoError(t, err)

	recovered, err := rm.FinalizeCertifiedProposal(s.ctx, winnerBlock.Index, winnerBlock.RootHash, api.NewHexBytes([]byte{0x04, 0x0e}))
	require.NoError(t, err)
	require.True(t, recovered)

	visibleWinnerRecords, err := s.storage.AggregatorRecordStorage().GetByBlockNumber(s.ctx, winnerBlock.Index)
	require.NoError(t, err)
	require.Len(t, visibleWinnerRecords, len(winnerStateIDs))
	for i, record := range visibleWinnerRecords {
		require.Equal(t, winnerBlock.ProposalID, record.ProposalID)
		require.Equal(t, winnerStateIDs[i], record.StateID)
	}
	for _, stateID := range orphanStateIDs {
		record, err := s.storage.AggregatorRecordStorage().GetByStateID(s.ctx, stateID)
		require.NoError(t, err)
		require.Nil(t, record, "orphan stateID must remain invisible after same-number finalization")
	}
}

// ============================================================================
// Test 5: Multiple Unfinalized Blocks (FATAL Error)
// ============================================================================
func (s *RecoveryTestSuite) Test06_MultipleUnfinalizedBlocks() {
	t := s.T()

	// Create two unfinalized blocks
	_, block1, _ := s.createTestData(6, 2, "t06a")
	_, block2, _ := s.createTestData(7, 2, "t06b")

	// Store both blocks as unfinalized
	err := s.storage.BlockStorage().Store(s.ctx, block1)
	require.NoError(t, err)
	err = s.storage.BlockStorage().Store(s.ctx, block2)
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
// Test 6: Partial SMT Nodes - Verify Correct Detection of Missing Nodes
// ============================================================================
func (s *RecoveryTestSuite) Test10_PartialSmtNodes_CorrectDetection() {
	t := s.T()

	// Create unfinalized block with 5 commitments
	commitments, block, _ := s.createTestData(10, 5, "t10partial")

	// Store block (unfinalized)
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)

	// Store ALL aggregator records (so only SMT nodes need recovery)
	s.storeAggregatorRecords(commitments, block)

	// Store ONLY SMT nodes at positions 0, 1, and 4 (missing 2 and 3)
	existingIndices := []int{0, 1, 4}
	existingNodes := make([]*models.SmtNode, len(existingIndices))
	for i, idx := range existingIndices {
		keyBytes, err := commitments[idx].StateID.GetTreeKey()
		require.NoError(t, err)
		leafValue, err := commitments[idx].LeafValue()
		require.NoError(t, err)
		existingNodes[i] = models.NewSmtNode(api.HexBytes(keyBytes), leafValue)
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
// Test 11: LoadRecoveredNodesIntoBackend
// ============================================================================
func (s *RecoveryTestSuite) Test11_LoadRecoveredNodesIntoBackend() {
	t := s.T()

	// Create test commitments and store their SMT nodes
	commitments := testutil.CreateTestCertificationRequests(t, 4, "t09")
	stateIDs := make([]api.StateID, len(commitments))
	for i, c := range commitments {
		stateIDs[i] = c.StateID
	}

	// Compute expected root hash
	expectedSMT := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	leaves := make([]*smt.Leaf, len(commitments))
	for i, c := range commitments {
		path, err := c.StateID.GetPath()
		require.NoError(t, err)
		leafValue, err := c.LeafValue()
		require.NoError(t, err)
		leaves[i] = smt.NewLeaf(path, leafValue)
	}
	err := expectedSMT.AddLeaves(leaves)
	require.NoError(t, err)
	expectedRootHash := expectedSMT.GetRootHashHex() // Use hex string for comparison

	// Store SMT nodes in MongoDB
	s.storeSmtNodes(commitments)

	// Create empty SMT to load into
	targetSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	backend := smtbackend.NewMemoryBackend(targetSMT)
	blockNumber := api.NewBigIntFromUint64(9)
	require.NotEqual(t, expectedRootHash, targetSMT.GetRootHash(), "SMT should be empty initially")

	// Load recovered nodes into SMT
	err = LoadRecoveredNodesIntoBackend(s.ctx, s.testLogger, s.storage, backend, blockNumber, stateIDs)
	require.NoError(t, err)

	// Verify SMT has correct root hash (ThreadSafeSMT.GetRootHash() returns hex string)
	actualRootHash := targetSMT.GetRootHash()
	require.Equal(t, expectedRootHash, actualRootHash, "SMT root hash should match after loading recovered nodes")
	state, err := backend.CommittedState(s.ctx)
	require.NoError(t, err)
	require.Equal(t, blockNumber.String(), state.BlockNumber.String(), "SMT backend block metadata should advance during recovery")

	t.Log("✓ Test11_LoadRecoveredNodesIntoBackend passed")
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
	commitments := testutil.CreateTestCertificationRequests(t, 3, "t13")
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
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized

	// Store in Redis and make them pending
	s.storeCommitmentsInRedis(commitments)
	s.readPendingToMakeThemClaimed()

	// Store finalized block and records (simulate block was finalized).
	require.NoError(t, s.storage.BlockStorage().Store(s.ctx, block))
	s.storeAggregatorRecords(commitments, block)

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
	commitments, block, _ := s.createTestData(15, 4, "t15")
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized

	// Store all 4 in Redis and make them pending
	s.storeCommitmentsInRedis(commitments)
	s.readPendingToMakeThemClaimed()

	// Store only first 2 in finalized AggregatorRecords (simulate partial processing)
	processedCommitments := commitments[:2]
	require.NoError(t, s.storage.BlockStorage().Store(s.ctx, block))
	s.storeAggregatorRecords(processedCommitments, block)

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
	commitments, block, _ := s.createTestData(16, 3, "t16")
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized
	err := s.storage.BlockStorage().Store(s.ctx, block)
	require.NoError(t, err)
	s.storeAggregatorRecords(commitments, block)
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
	commitmentChan := make(chan *models.CertificationRequest, 1000)
	ctx, cancel := context.WithTimeout(s.ctx, 500*time.Millisecond)
	defer cancel()

	go func() {
		_ = s.commitmentQueue.StreamCertificationRequests(ctx, commitmentChan)
	}()

	// Wait for timeout to ensure all are claimed. Do not close commitmentChan:
	// the streamer goroutine above may still be sending to it (closing here races
	// that send). The streamer stops on ctx.Done(); the channel is GC'd.
	<-ctx.Done()
}
