package round

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type FinalizeDuplicateTestSuite struct {
	suite.Suite
	storage *mongodb.Storage
	cfg     *config.Config
}

func TestFinalizeDuplicateSuite(t *testing.T) {
	suite.Run(t, new(FinalizeDuplicateTestSuite))
}

func (s *FinalizeDuplicateTestSuite) SetupSuite() {
	// Use test name as database name to avoid conflicts
	conf := config.Config{
		Database: config.DatabaseConfig{
			Database:               "test_finalize_dup_suite",
			ConnectTimeout:         30 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            100,
			MinPoolSize:            5,
			MaxConnIdleTime:        5 * time.Minute,
		},
	}
	s.storage = testutil.SetupTestStorage(s.T(), conf)
	s.cfg = &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
			BatchLimit:    1000,
		},
	}
}

// TestDuplicateRecovery tests that FinalizeBlock succeeds even when
// some SMT nodes and aggregator records already exist (simulating crash recovery).
func (s *FinalizeDuplicateTestSuite) Test1_DuplicateRecovery() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	// Generate test commitments with unique IDs
	commitments := testutil.CreateTestCertificationRequests(t, 5, "t1_req")

	// Set up the round
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(1)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	// Process commitments to populate PendingLeaves
	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	// Get counts before pre-population
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// Pre-populate storage with 2 out of 5 records (simulating partial write before crash)
	partialLeaves := rm.currentRound.PendingLeaves[:2]
	preExistingNodes := rm.convertLeavesToNodes(partialLeaves)
	err = s.storage.SmtStorage().StoreBatch(ctx, preExistingNodes)
	require.NoError(t, err, "Pre-populating SMT nodes should succeed")

	preExistingRecords := rm.convertCommitmentsToRecords(commitments[:2], api.NewBigInt(big.NewInt(1)))
	err = s.storage.AggregatorRecordStorage().StoreBatch(ctx, preExistingRecords)
	require.NoError(t, err, "Pre-populating aggregator records should succeed")

	// Verify pre-existing data added
	smtCount, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+2, smtCount, "Should have added 2 pre-existing SMT nodes")
	recordCount, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore+2, recordCount, "Should have added 2 pre-existing aggregator records")

	// Get root hash from snapshot
	rootHash := rm.currentRound.Snapshot.GetRootHash()
	require.NotEmpty(t, rootHash)
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	// Create block
	block := models.NewBlock(
		api.NewBigInt(big.NewInt(1)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	// FinalizeBlock should succeed despite duplicates
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed with duplicate recovery")

	// Verify all data was stored (5 total new nodes, 3 additional after the 2 pre-existing)
	smtCountAfter, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+5, smtCountAfter, "Should have all 5 SMT nodes after finalization")

	recordCountAfter, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore+5, recordCountAfter, "Should have all 5 aggregator records after finalization")

	t.Log("✓ FinalizeBlock succeeded with duplicate recovery")
}

// Test2_NoDuplicates tests when there are no duplicates (normal flow)
func (s *FinalizeDuplicateTestSuite) Test2_NoDuplicates() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t2_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(2)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := rm.currentRound.Snapshot.GetRootHash()
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(2)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	// Should succeed on first try (no duplicates)
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed without duplicates")

	t.Log("✓ FinalizeBlock succeeded without duplicates")
}

// Test3_AllDuplicates tests when all records already exist
func (s *FinalizeDuplicateTestSuite) Test3_AllDuplicates() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t3_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(3)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	// Get counts before pre-population
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// Pre-populate ALL SMT nodes and aggregator records
	allNodes := rm.convertLeavesToNodes(rm.currentRound.PendingLeaves)
	err = s.storage.SmtStorage().StoreBatch(ctx, allNodes)
	require.NoError(t, err)

	allRecords := rm.convertCommitmentsToRecords(commitments, api.NewBigInt(big.NewInt(3)))
	err = s.storage.AggregatorRecordStorage().StoreBatch(ctx, allRecords)
	require.NoError(t, err)

	rootHash := rm.currentRound.Snapshot.GetRootHash()
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(3)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	// Should succeed even when all records are duplicates
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed when all records are duplicates")

	// Counts should only increase by 3 (the pre-populated ones, not doubled)
	smtCountAfter, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+3, smtCountAfter, "Should still have only 3 new SMT nodes (duplicates ignored)")

	recordCountAfter, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore+3, recordCountAfter, "Should still have only 3 new records (duplicates ignored)")

	t.Log("✓ FinalizeBlock succeeded when all records were duplicates")
}

// Test4_DuplicateBlock tests that FinalizeBlock succeeds when the block itself
// already exists (simulating a retry after MarkProcessed failed).
func (s *FinalizeDuplicateTestSuite) Test4_DuplicateBlock() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t4_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(4)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := rm.currentRound.Snapshot.GetRootHash()
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(4)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	// Pre-store the block (simulating previous attempt that stored block but failed on MarkProcessed)
	block.Finalized = false
	err = s.storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err, "Pre-storing block should succeed")

	// Also pre-store block records
	requestIds := make([]api.StateID, len(commitments))
	for i, c := range commitments {
		requestIds[i] = c.StateID
	}
	err = s.storage.BlockRecordsStorage().Store(ctx, models.NewBlockRecords(block.Index, requestIds))
	require.NoError(t, err, "Pre-storing block records should succeed")

	// Get counts before FinalizeBlock
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// FinalizeBlock should succeed despite duplicate block
	// It should skip block storage and continue with remaining steps
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed with duplicate block")

	// Verify SMT nodes and aggregator records were still stored
	smtCountAfter, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+3, smtCountAfter, "Should have stored 3 SMT nodes despite duplicate block")

	recordCountAfter, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore+3, recordCountAfter, "Should have stored 3 aggregator records despite duplicate block")

	// Verify block was finalized
	storedBlock, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, storedBlock)
	require.True(t, storedBlock.Finalized, "Block should be marked as finalized")

	t.Log("✓ FinalizeBlock succeeded with duplicate block")
}

// Test5_DuplicateBlockAlreadyFinalized tests that FinalizeBlock succeeds when
// the block already exists AND is already finalized (full retry scenario).
func (s *FinalizeDuplicateTestSuite) Test5_DuplicateBlockAlreadyFinalized() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t5_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(5)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := rm.currentRound.Snapshot.GetRootHash()
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(5)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	// Pre-store the block as FINALIZED (simulating previous successful attempt except MarkProcessed)
	block.Finalized = true
	err = s.storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err, "Pre-storing finalized block should succeed")

	// Pre-store block records
	requestIds := make([]api.StateID, len(commitments))
	for i, c := range commitments {
		requestIds[i] = c.StateID
	}
	err = s.storage.BlockRecordsStorage().Store(ctx, models.NewBlockRecords(block.Index, requestIds))
	require.NoError(t, err, "Pre-storing block records should succeed")

	// Pre-store all SMT nodes and records (simulating full previous attempt)
	allNodes := rm.convertLeavesToNodes(rm.currentRound.PendingLeaves)
	err = s.storage.SmtStorage().StoreBatch(ctx, allNodes)
	require.NoError(t, err)

	allRecords := rm.convertCommitmentsToRecords(commitments, block.Index)
	err = s.storage.AggregatorRecordStorage().StoreBatch(ctx, allRecords)
	require.NoError(t, err)

	// Reset block.Finalized to false for the FinalizeBlock call
	block.Finalized = false

	// FinalizeBlock should succeed - all steps are idempotent
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed when block is already finalized")

	t.Log("✓ FinalizeBlock succeeded with already-finalized block")
}

// Test6_BlockRecordsMatchPendingCommitmentsOnConflict verifies that FinalizeBlock
// stores BlockRecords/SMT nodes/aggregator records from the filtered pending set,
// not from all round commitments when one commitment conflicts.
func (s *FinalizeDuplicateTestSuite) Test6_BlockRecordsMatchPendingCommitmentsOnConflict() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	// Create c1, conflict(c1), and c2.
	commitment1 := testutil.CreateTestCertificationRequest(t, "t6_req_1")
	commitment2 := testutil.CreateTestCertificationRequest(t, "t6_req_2")
	conflictingCommitment := *commitment1
	conflictingCommitment.CertificationData.TransactionHash = commitment2.CertificationData.TransactionHash

	leafValue1, err := commitment1.LeafValue()
	require.NoError(t, err)
	leafValueConflict, err := conflictingCommitment.LeafValue()
	require.NoError(t, err)
	require.NotEqual(t, leafValue1, leafValueConflict, "conflicting commitment must produce a different leaf value")

	commitments := []*models.CertificationRequest{commitment1, &conflictingCommitment, commitment2}
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(6)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	rm.roundMutex.Lock()
	err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	// Pending sets should only contain c1 and c2.
	require.Len(t, rm.currentRound.PendingLeaves, 2)
	require.Len(t, rm.currentRound.PendingCommitments, 2)
	require.True(t, rm.currentRound.PendingCommitments[0] == commitment1)
	require.True(t, rm.currentRound.PendingCommitments[1] == commitment2)

	smtCountBefore, err := s.storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	recordCountBefore, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)

	rootHash := rm.currentRound.Snapshot.GetRootHash()
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(6)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
		nil,
	)

	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err)

	blockRecords, err := s.storage.BlockRecordsStorage().GetByBlockNumber(ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, blockRecords)
	require.Equal(t, []api.StateID{commitment1.StateID, commitment2.StateID}, blockRecords.StateIDs)

	recordsByBlock, err := s.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, block.Index)
	require.NoError(t, err)
	require.Len(t, recordsByBlock, 2)

	smtCountAfter, err := s.storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, smtCountBefore+2, smtCountAfter, "should persist only filtered SMT leaves")

	recordCountAfter, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, recordCountBefore+2, recordCountAfter, "should persist only filtered aggregator records")
}
