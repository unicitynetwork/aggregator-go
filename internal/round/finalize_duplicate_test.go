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
	ctx := context.Background()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	// Generate test commitments with unique IDs
	commitments := testutil.CreateTestCommitments(t, 5, "t1_req")

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
	ctx := context.Background()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCommitments(t, 3, "t2_req")

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
	ctx := context.Background()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT)
	require.NoError(t, err)

	commitments := testutil.CreateTestCommitments(t, 3, "t3_req")

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
