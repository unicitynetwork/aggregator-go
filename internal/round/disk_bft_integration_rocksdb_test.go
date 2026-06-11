//go:build rocksdb

package round

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	diskstorage "github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskSMTBFTShardEmptyRoundCommitsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_empty_round")
	rm, _ := newTestDiskBFTShardRoundManager(t, ctx, cfg)

	before := testDiskStoreCounters(t, ctx, rm)
	block, dropped := finalizeManualDiskRound(t, ctx, rm, 1, nil)
	require.Empty(t, dropped)
	require.True(t, block.Finalized)

	after := testDiskStoreCounters(t, ctx, rm)
	require.Equal(t, before.NodeSets, after.NodeSets, "empty round must not write SMT nodes")
	require.Equal(t, before.NodeDeletes, after.NodeDeletes, "empty round must not delete SMT nodes")
	require.Equal(t, before.BatchesCommitted+1, after.BatchesCommitted)
	require.Equal(t, before.MetaSets+2, after.MetaSets)

	state, err := rm.smtBackend.CommittedState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.BlockNumber)
	require.EqualValues(t, 1, state.BlockNumber.Int64())
	require.Equal(t, block.RootHash, api.HexBytes(state.RootHash))
}

func TestDiskSMTBFTShardPendingAlignmentWithDuplicateFiltering(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_pending_alignment")
	rm, storage := newTestDiskBFTShardRoundManager(t, ctx, cfg)

	c1 := testutil.CreateTestCertificationRequest(t, "disk_align_1")
	c2 := testutil.CreateTestCertificationRequest(t, "disk_align_2")
	sameBatchConflict := *c1
	sameBatchConflict.CertificationData.TransactionHash = c2.CertificationData.TransactionHash

	block1, dropped := finalizeManualDiskRound(t, ctx, rm, 1, []*models.CertificationRequest{c1, &sameBatchConflict, c2})
	require.Len(t, dropped, 1)
	require.True(t, block1.Finalized)

	block1Records, err := storage.BlockRecordsStorage().GetByBlockNumber(ctx, block1.Index)
	require.NoError(t, err)
	require.NotNil(t, block1Records)
	require.Equal(t, []api.StateID{c1.StateID, c2.StateID}, block1Records.StateIDs)

	c3 := testutil.CreateTestCertificationRequest(t, "disk_align_3")
	committedConflict := *c1
	committedConflict.CertificationData.TransactionHash = c3.CertificationData.TransactionHash

	block2, dropped := finalizeManualDiskRound(t, ctx, rm, 2, []*models.CertificationRequest{&committedConflict, c3})
	require.Len(t, dropped, 1)
	require.True(t, block2.Finalized)

	block2Records, err := storage.BlockRecordsStorage().GetByBlockNumber(ctx, block2.Index)
	require.NoError(t, err)
	require.NotNil(t, block2Records)
	require.Equal(t, []api.StateID{c3.StateID}, block2Records.StateIDs)
}

func TestDiskSMTBFTShardSequentialRoundsMatchMemoryBackend(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_memory_parity")
	rm, storage := newTestDiskBFTShardRoundManager(t, ctx, cfg)
	log := newTestLogger(t)

	memoryBackend := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	defer func() { require.NoError(t, memoryBackend.Close()) }()

	c1 := testutil.CreateTestCertificationRequest(t, "parity_r1_1")
	c2 := testutil.CreateTestCertificationRequest(t, "parity_r1_2")
	c3 := testutil.CreateTestCertificationRequest(t, "parity_r2_1")
	c4 := testutil.CreateTestCertificationRequest(t, "parity_r2_2")
	c5 := testutil.CreateTestCertificationRequest(t, "parity_r3_1")
	c6 := testutil.CreateTestCertificationRequest(t, "parity_r4_1")
	c7 := testutil.CreateTestCertificationRequest(t, "parity_r5_1")

	inBatchDuplicate := *c3
	inBatchDuplicate.StreamID = "in-batch-duplicate"
	inBatchDuplicate.CertificationData.TransactionHash = c4.CertificationData.TransactionHash

	committedDuplicate := *c1
	committedDuplicate.StreamID = "committed-duplicate"

	committedModification := *c2
	committedModification.StreamID = "committed-modification"
	committedModification.CertificationData.TransactionHash = c6.CertificationData.TransactionHash

	invalidStateID := *c7
	invalidStateID.StreamID = "invalid-state-id"
	invalidStateID.StateID = api.StateID([]byte{0x01, 0x02})

	rounds := [][]*models.CertificationRequest{
		{c1, c2},
		{c3, &inBatchDuplicate, c4},
		{&committedDuplicate, c5},
		{&committedModification, c6},
		{&invalidStateID, c7},
	}

	for i, commitments := range rounds {
		blockNumber := uint64(i + 1)
		block, diskDropped := finalizeManualDiskRound(t, ctx, rm, blockNumber, commitments)
		expectedRoot, expectedStateIDs, expectedDropped := applyMemoryRound(t, ctx, log, memoryBackend, blockNumber, commitments)

		require.Equal(t, api.HexBytes(expectedRoot), block.RootHash, "round %d root mismatch", blockNumber)
		require.Equal(t, stateIDsFromAcks(expectedDropped), stateIDsFromAcks(diskDropped), "round %d dropped set mismatch", blockNumber)

		blockRecords, err := storage.BlockRecordsStorage().GetByBlockNumber(ctx, block.Index)
		require.NoError(t, err)
		require.NotNil(t, blockRecords)
		require.Equal(t, expectedStateIDs, blockRecords.StateIDs, "round %d block records mismatch", blockNumber)

		diskState, err := rm.smtBackend.CommittedState(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedRoot, diskState.RootHash, "round %d committed disk root mismatch", blockNumber)
	}
}

func TestDiskSMTBFTShardActivePrecollectorLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testDiskBFTShardConfig(t, "disk_active_precollector")
	cfg.Storage.UseRedisForCommitments = true
	cfg.Processing.PrecollectorGracePeriod = 50 * time.Millisecond
	cfg.Processing.CollectPhaseDuration = 200 * time.Millisecond
	cfg.BFT.StubDelay = time.Second

	rm, storage := newTestDiskBFTShardRoundManager(t, ctx, cfg)
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	require.Eventually(t, func() bool {
		rm.roundMutex.RLock()
		defer rm.roundMutex.RUnlock()
		return rm.precollector != nil
	}, 2*time.Second, 25*time.Millisecond, "round 1 should start the active precollector during BFT wait")

	lateCommitment := testutil.CreateTestCertificationRequest(t, "disk_precollector_late")
	select {
	case rm.commitmentStream <- lateCommitment:
	case <-time.After(time.Second):
		t.Fatal("timed out sending commitment to active precollector")
	}

	require.Eventually(t, func() bool {
		rm.roundMutex.RLock()
		defer rm.roundMutex.RUnlock()
		return rm.currentRound != nil &&
			rm.currentRound.Number.Int64() == 2 &&
			rm.currentRound.PreCollected &&
			!rm.currentRound.ProposalTime.IsZero()
	}, 4*time.Second, 25*time.Millisecond, "round 2 should be proposed from the disk precollector snapshot")

	rm.roundMutex.RLock()
	currentRound := rm.currentRound
	commitments := append([]*models.CertificationRequest(nil), currentRound.Commitments...)
	pendingLeaves := append([]smtbackend.LeafInput(nil), currentRound.PendingLeaves...)
	rm.roundMutex.RUnlock()

	require.Len(t, commitments, 1)
	require.Equal(t, lateCommitment.StateID, commitments[0].StateID)
	require.Len(t, pendingLeaves, 1)

	block1, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
	require.NoError(t, err)
	require.NotNil(t, block1)

	state, err := rm.smtBackend.CommittedState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.BlockNumber)
	require.GreaterOrEqual(t, state.BlockNumber.Int64(), int64(1))
}

func testDiskBFTShardConfig(t *testing.T, name string) config.Config {
	t.Helper()
	return config.Config{
		Database: config.DatabaseConfig{
			Database:               "test_" + name,
			ConnectTimeout:         30 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            20,
			MinPoolSize:            1,
			MaxConnIdleTime:        time.Minute,
		},
		Processing: config.ProcessingConfig{
			RoundDuration:              100 * time.Millisecond,
			BatchLimit:                 1000,
			MaxCommitmentsPerRound:     1000,
			CollectPhaseDuration:       100 * time.Millisecond,
			CommitmentStreamBufferSize: 1000,
			SkipDuplicateCheck:         true,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeBFTShard,
		},
		Chain: config.ChainConfig{
			ID:      "unicity",
			Version: "1.0",
			ForkID:  "test",
		},
		BFT: config.BFTConfig{
			Enabled:   false,
			StubDelay: 0,
		},
		SMT: config.SMTConfig{
			Backend:                  config.SMTBackendRocksDB,
			DiskPath:                 t.TempDir(),
			RocksDBCacheMB:           8,
			RocksDBBGJobs:            2,
			RocksDBSubcompactions:    1,
			RocksDBBloomBits:         10,
			RocksDBMemTableMB:        8,
			MaterializeWorkers:       2,
			StartupReplayLimitBlocks: 100,
		},
	}
}

func newTestDiskBFTShardRoundManager(t *testing.T, ctx context.Context, cfg config.Config) (*RoundManager, interfaces.Storage) {
	t.Helper()
	storage := testutil.SetupTestStorage(t, cfg)
	log := newTestLogger(t)
	rm, err := NewRoundManager(
		ctx,
		&cfg,
		log,
		storage.CommitmentQueue(),
		storage,
		nil,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(log),
		smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)),
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rm.Stop(shutdownCtx)
	})
	return rm, storage
}

func finalizeManualDiskRound(
	t *testing.T,
	ctx context.Context,
	rm *RoundManager,
	blockNumber uint64,
	commitments []*models.CertificationRequest,
) (*models.Block, []interfaces.CertificationRequestAck) {
	t.Helper()

	snapshot := testRMSnapshot(t, ctx, rm)
	rm.roundMutex.Lock()
	rm.currentRound = &Round{
		Number:      api.NewBigIntFromUint64(blockNumber),
		State:       RoundStateProcessing,
		Commitments: commitments,
		Snapshot:    snapshot,
	}
	rm.roundMutex.Unlock()

	var dropped []interfaces.CertificationRequestAck
	if len(commitments) > 0 {
		var err error
		rm.roundMutex.Lock()
		dropped, err = rm.processMiniBatch(ctx, commitments)
		rm.roundMutex.Unlock()
		require.NoError(t, err)
	}

	rm.roundMutex.RLock()
	require.Len(t, rm.currentRound.PendingLeaves, len(rm.currentRound.PendingCommitments))
	root, err := rm.currentRound.Snapshot.RootHashRaw(ctx)
	rm.roundMutex.RUnlock()
	require.NoError(t, err)

	rootHash := api.NewHexBytes(root)
	uc, err := testProofUC(blockNumber, blockNumber, rootHash)
	require.NoError(t, err)
	block := models.NewBlock(
		api.NewBigIntFromUint64(blockNumber),
		rm.config.Chain.ID,
		0,
		rm.config.Chain.Version,
		rm.config.Chain.ForkID,
		rootHash,
		api.HexBytes{},
		uc,
	)
	require.NoError(t, rm.FinalizeBlock(ctx, block))
	return block, dropped
}

func testDiskStoreCounters(t *testing.T, ctx context.Context, rm *RoundManager) diskstorage.Counters {
	t.Helper()
	stats := rm.smtBackend.Stats(ctx)
	counters, ok := stats.Raw["store_counters"].(diskstorage.Counters)
	require.True(t, ok, "disk backend stats must expose store_counters")
	return counters
}

func applyMemoryRound(
	t *testing.T,
	ctx context.Context,
	log *logger.Logger,
	backend smtbackend.Backend,
	blockNumber uint64,
	commitments []*models.CertificationRequest,
) ([]byte, []api.StateID, []interfaces.CertificationRequestAck) {
	t.Helper()

	snapshot := testBackendSnapshot(t, ctx, backend)
	leaves := make([]smtbackend.LeafInput, 0, len(commitments))
	validCommitments := make([]*models.CertificationRequest, 0, len(commitments))
	for _, commitment := range commitments {
		leaf, err := commitmentLeafInput(commitment)
		if err != nil {
			continue
		}
		leaves = append(leaves, leaf)
		validCommitments = append(validCommitments, commitment)
	}

	addedCommitments, _, dropped, err := addCommitmentLeaves(ctx, log, snapshot, leaves, validCommitments)
	require.NoError(t, err)
	root, err := snapshot.RootHashRaw(ctx)
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(ctx, smtbackend.CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(blockNumber),
		RootHash:    root,
	}))

	stateIDs := make([]api.StateID, len(addedCommitments))
	for i, commitment := range addedCommitments {
		stateIDs[i] = commitment.StateID
	}
	return root, stateIDs, dropped
}

func stateIDsFromAcks(acks []interfaces.CertificationRequestAck) []api.StateID {
	out := make([]api.StateID, len(acks))
	for i, ack := range acks {
		out[i] = ack.StateID
	}
	return out
}
