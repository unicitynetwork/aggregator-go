//go:build rocksdb

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskSMTBFTShardProofBeforeAndAfterFinalization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testDiskBFTShardServiceConfig(t)
	storage := testutil.SetupTestStorage(t, cfg)
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := round.NewRoundManager(
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
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	service := NewAggregatorService(&cfg, log, rm, storage.CommitmentQueue(), storage, nil)
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	req1 := testutil.CreateTestCertificationRequest(t, "disk_proof_ready_1").ToAPI()
	resp, err := service.CertificationRequest(ctx, req1)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Status)

	require.Eventually(t, func() bool {
		record, err := storage.AggregatorRecordStorage().GetByStateID(ctx, req1.StateID)
		return err == nil && record != nil
	}, 5*time.Second, 25*time.Millisecond, "first request should finalize before testing pending proof behavior")

	req2 := testutil.CreateTestCertificationRequest(t, "disk_proof_pending_2").ToAPI()
	resp, err = service.CertificationRequest(ctx, req2)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Status)

	require.Eventually(t, func() bool {
		_, ok := rm.GetKnownNotReadyBlock(req2.StateID)
		return ok
	}, 2*time.Second, 25*time.Millisecond, "second request should be pending against an unfinalized disk snapshot")

	earlyProof, err := service.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req2.StateID})
	require.NoError(t, err)
	require.NotNil(t, earlyProof.InclusionProof)
	require.Nil(t, earlyProof.InclusionProof.CertificationData)
	require.Empty(t, earlyProof.InclusionProof.CertificateBytes)

	require.Eventually(t, func() bool {
		record, err := storage.AggregatorRecordStorage().GetByStateID(ctx, req2.StateID)
		return err == nil && record != nil
	}, 5*time.Second, 25*time.Millisecond, "second request should eventually finalize")

	require.Eventually(t, func() bool {
		readyProof, err := service.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req2.StateID})
		return err == nil &&
			readyProof != nil &&
			readyProof.InclusionProof != nil &&
			readyProof.InclusionProof.CertificationData != nil &&
			len(readyProof.InclusionProof.CertificateBytes) > 0
	}, 5*time.Second, 25*time.Millisecond, "second request should eventually have a ready proof")
}

func testDiskBFTShardServiceConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		Database: config.DatabaseConfig{
			Database:               "test_disk_bft_proof",
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
			StubDelay: 750 * time.Millisecond,
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
