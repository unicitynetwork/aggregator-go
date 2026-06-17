package round

import (
	"context"
	"errors"
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
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type FinalizeDuplicateTestSuite struct {
	suite.Suite
	storage *mongodb.Storage
	cfg     *config.Config
}

type failingMarkProcessedQueue struct {
	interfaces.CommitmentQueue
	err error
}

func (q *failingMarkProcessedQueue) MarkProcessed(context.Context, []interfaces.CertificationRequestAck) error {
	return q.err
}

type discardCountingSnapshot struct {
	smtbackend.Snapshot
	discards int
}

func (s *discardCountingSnapshot) Discard(ctx context.Context) {
	s.discards++
	s.Snapshot.Discard(ctx)
}

func storeDurableProposalForCurrentRound(t *testing.T, ctx context.Context, rm *RoundManager, block *models.Block) []*models.AggregatorRecord {
	t.Helper()
	require.NotNil(t, rm.currentRound)
	if rm.currentRound.ProposalID == "" {
		rm.currentRound.ProposalID = "proposal-" + block.Index.String()
	}
	block.ProposalID = rm.currentRound.ProposalID
	records, err := rm.proposalRecordsForRound(rm.currentRound, block.Index)
	require.NoError(t, err)
	require.NoError(t, rm.storeProposedBlockAndRecords(ctx, block, records))
	return records
}

func recordsForBlock(commitments []*models.CertificationRequest, block *models.Block) []*models.AggregatorRecord {
	records := make([]*models.AggregatorRecord, len(commitments))
	for i, commitment := range commitments {
		record := models.NewAggregatorRecord(commitment, block.Index, api.NewBigInt(big.NewInt(int64(i))))
		record.ProposalID = block.ProposalID
		records[i] = record
	}
	return records
}

func TestSameStateIDsRequiresSameMembershipNotOrder(t *testing.T) {
	a := []api.StateID{
		api.RequireNewImprintV2("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		api.RequireNewImprintV2("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		api.RequireNewImprintV2("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
	}
	b := []api.StateID{a[2], a[0], a[1]}
	missing := []api.StateID{a[0], a[1]}
	duplicate := []api.StateID{a[0], a[1], a[1]}

	require.True(t, sameStateIDs(a, b))
	require.False(t, sameStateIDs(a, missing))
	require.False(t, sameStateIDs(a, duplicate))
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

// TestDuplicateRecovery tests that FinalizeBlock succeeds even when some SMT
// nodes already exist (simulating a retry after a partial local node write).
func (s *FinalizeDuplicateTestSuite) Test1_DuplicateRecovery() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	// Generate test commitments with unique IDs
	commitments := testutil.CreateTestCertificationRequests(t, 5, "t1_req")

	// Set up the round
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(1)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	// Process commitments to populate PendingLeaves
	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	// Get counts before pre-population
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// Pre-populate storage with 2 out of 5 SMT nodes (simulating partial write before crash)
	partialLeaves := rm.currentRound.PendingLeaves[:2]
	preExistingNodes, err := rm.convertLeavesToNodes(partialLeaves)
	require.NoError(t, err)
	err = s.storage.SmtStorage().StoreBatch(ctx, preExistingNodes)
	require.NoError(t, err, "Pre-populating SMT nodes should succeed")

	// Verify pre-existing data added
	smtCount, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+2, smtCount, "Should have added 2 pre-existing SMT nodes")
	recordCount, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore, recordCount, "Should not have pre-existing aggregator records")

	// Get root hash from snapshot
	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

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

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t2_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(2)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

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

	smtInstance := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t3_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(3)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	// Get counts before pre-population
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// Pre-populate ALL SMT nodes. Aggregator records are stored once as the
	// durable proposal before certification.
	allNodes, err := rm.convertLeavesToNodes(rm.currentRound.PendingLeaves)
	require.NoError(t, err)
	err = s.storage.SmtStorage().StoreBatch(ctx, allNodes)
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

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

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t4_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(4)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)

	// Pre-store the durable proposal (simulating the real pre-certification flow).
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

	// Get counts before FinalizeBlock
	smtCountBefore, _ := s.storage.SmtStorage().Count(ctx)
	recordCountBefore, _ := s.storage.AggregatorRecordStorage().Count(ctx)

	// FinalizeBlock should use the already-stored proposal and not rewrite records.
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed with duplicate block")

	// Verify SMT nodes and aggregator records were still stored
	smtCountAfter, _ := s.storage.SmtStorage().Count(ctx)
	require.Equal(t, smtCountBefore+3, smtCountAfter, "Should have stored 3 SMT nodes despite duplicate block")

	recordCountAfter, _ := s.storage.AggregatorRecordStorage().Count(ctx)
	require.Equal(t, recordCountBefore, recordCountAfter, "FinalizeBlock should not store aggregator records")

	// Verify block was finalized
	storedBlock, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.NotNil(t, storedBlock)
	require.True(t, storedBlock.Finalized, "Block should be marked as finalized")

	t.Log("✓ FinalizeBlock succeeded with duplicate block")
}

// Test4b_MarkProcessedFailureAfterFinalization tests that Redis ACK failure does
// not fail a block that has already been committed and marked finalized.
func (s *FinalizeDuplicateTestSuite) Test4b_MarkProcessedFailureAfterFinalization() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	queue := &failingMarkProcessedQueue{
		CommitmentQueue: s.storage.CommitmentQueue(),
		err:             errors.New("redis ack timeout"),
	}
	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, queue, s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t4b_req")
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(7)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(
		api.NewBigInt(big.NewInt(7)),
		"unicity",
		0,
		"1.0",
		"mainnet",
		rootHashBytes,
		api.HexBytes{},
		api.HexBytes{},
	)
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "post-finalization ACK failure should not fail FinalizeBlock")

	storedBlock, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.True(t, storedBlock.Finalized, "block should remain finalized")

	recordCount, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, recordCount, int64(len(commitments)))

	t.Log("✓ FinalizeBlock succeeded despite post-finalization MarkProcessed failure")
}

// Test5_DuplicateBlockAlreadyFinalized tests that FinalizeBlock succeeds when
// the block already exists AND is already finalized (full retry scenario).
func (s *FinalizeDuplicateTestSuite) Test5_DuplicateBlockAlreadyFinalized() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 3, "t5_req")

	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(5)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)

	// Pre-store the block as FINALIZED (simulating previous successful attempt except MarkProcessed)
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized
	block.ProposalID = "proposal-" + block.Index.String()
	err = s.storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err, "Pre-storing finalized block should succeed")

	// Pre-store all SMT nodes and records (simulating full previous attempt)
	allNodes, err := rm.convertLeavesToNodes(rm.currentRound.PendingLeaves)
	require.NoError(t, err)
	err = s.storage.SmtStorage().StoreBatch(ctx, allNodes)
	require.NoError(t, err)

	allRecords := recordsForBlock(commitments, block)
	err = s.storage.AggregatorRecordStorage().StoreBatch(ctx, allRecords)
	require.NoError(t, err)

	require.NoError(t, rm.currentRound.Snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: block.RootHash}))
	discardSpy := &discardCountingSnapshot{Snapshot: testRMSnapshot(t, ctx, rm)}
	rm.currentRound.Snapshot = discardSpy

	smtCountBefore, err := s.storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	recordCountBefore, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)

	// Reset block.Finalized to false for the FinalizeBlock call
	block.Finalized = false

	// FinalizeBlock should succeed without re-storing already-finalized data.
	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err, "FinalizeBlock should succeed when block is already finalized")
	require.True(t, block.Finalized)
	require.Equal(t, 1, discardSpy.discards, "already-finalized duplicate no-op should discard the unused round snapshot")

	smtCountAfter, err := s.storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, smtCountBefore, smtCountAfter, "already-finalized duplicate should not re-store SMT nodes")

	recordCountAfter, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, recordCountBefore, recordCountAfter, "already-finalized duplicate should not re-store aggregator records")

	t.Log("✓ FinalizeBlock succeeded with already-finalized block")
}

// Test6_ProposalRecordsMatchPendingCommitmentsOnConflict verifies that the
// durable proposal is built from the filtered pending set, not from all round
// commitments when one commitment conflicts.
func (s *FinalizeDuplicateTestSuite) Test6_ProposalRecordsMatchPendingCommitmentsOnConflict() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
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
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
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

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
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
	)
	storeDurableProposalForCurrentRound(t, ctx, rm, block)

	err = rm.FinalizeBlock(ctx, block)
	require.NoError(t, err)

	recordsByBlock, err := s.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, block.Index)
	require.NoError(t, err)
	require.Len(t, recordsByBlock, 2)
	require.Equal(t, []api.StateID{commitment1.StateID, commitment2.StateID}, []api.StateID{recordsByBlock[0].StateID, recordsByBlock[1].StateID})

	smtCountAfter, err := s.storage.SmtStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, smtCountBefore+2, smtCountAfter, "should persist only filtered SMT leaves")

	recordCountAfter, err := s.storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)
	require.Equal(t, recordCountBefore+2, recordCountAfter, "should stage only filtered aggregator records")
}

func (s *FinalizeDuplicateTestSuite) Test7_FinalizeBlockRejectsRoundNumberMismatch() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 2, "t7_req")
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(8)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}
	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	block := models.NewBlock(api.NewBigInt(big.NewInt(7)), "unicity", 0, "1.0", "mainnet", rootHashBytes, api.HexBytes{}, api.HexBytes{})
	err = rm.FinalizeBlock(ctx, block)
	require.ErrorContains(t, err, "does not match active round")
}

func (s *FinalizeDuplicateTestSuite) Test8_DuplicateBlockMustMatchRootAndStateIDs() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	commitments := testutil.CreateTestCertificationRequests(t, 2, "t8_req")
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(8)),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    testRMSnapshot(t, ctx, rm),
	}
	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rootHash := testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot)
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	require.NoError(t, err)

	existing := models.NewBlock(api.NewBigInt(big.NewInt(8)), "unicity", 0, "1.0", "mainnet", api.HexBytes(repeatByte(32, 7)), api.HexBytes{}, api.HexBytes{})
	existing.Finalized = true
	existing.Status = models.FinalityStatusFinalized
	existing.ProposalID = "proposal-8-existing"
	require.NoError(t, s.storage.BlockStorage().Store(ctx, existing))
	require.NoError(t, s.storage.AggregatorRecordStorage().StoreBatch(ctx, recordsForBlock(commitments[:1], existing)))

	block := models.NewBlock(api.NewBigInt(big.NewInt(8)), "unicity", 0, "1.0", "mainnet", rootHashBytes, api.HexBytes{}, api.HexBytes{})
	err = rm.FinalizeBlock(ctx, block)
	require.ErrorContains(t, err, "root mismatch")
}

func (s *FinalizeDuplicateTestSuite) Test9_EmptyRoundCannotFinalizeChangedRoot() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, s.cfg, testLogger, s.storage.CommitmentQueue(), s.storage, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), threadSafeSMT, nil)
	require.NoError(t, err)

	rm.currentRound = &Round{
		Number:             api.NewBigInt(big.NewInt(9)),
		State:              RoundStateProcessing,
		Commitments:        nil,
		PendingCommitments: nil,
		PendingLeaves:      nil,
		Snapshot:           testRMSnapshot(t, ctx, rm),
	}

	block := models.NewBlock(api.NewBigInt(big.NewInt(9)), "unicity", 0, "1.0", "mainnet", api.HexBytes(repeatByte(32, 9)), api.HexBytes{}, api.HexBytes{})
	err = rm.FinalizeBlock(ctx, block)
	require.ErrorContains(t, err, "snapshot root")
}

func (s *FinalizeDuplicateTestSuite) Test10_FinalizeBlockWithRetryPropagatesCancellation() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	block := models.NewBlock(api.NewBigInt(big.NewInt(10)), "unicity", 0, "1.0", "mainnet", api.HexBytes{}, api.HexBytes{}, api.HexBytes{})
	rm := &RoundManager{}

	err := rm.FinalizeBlockWithRetry(ctx, block)
	require.ErrorIs(s.T(), err, context.Canceled)
}

func repeatByte(n int, value byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = value
	}
	return out
}
