//go:build rocksdb

package round

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	ha "github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskSMTHAFollowerCatchesUpAndPromotes(t *testing.T) {
	// This drives promotion synchronously instead of running leader election; cmd/aggregator
	// tests cover leadership-loss cancellation around the same promotion sync call.
	ctx := context.Background()
	cfg := testDiskBFTShardHAConfig(t, "disk_ha_failover")
	storage := testutil.SetupTestStorage(t, cfg)

	leaderCfg := cfg
	leaderCfg.HA.ServerID = "leader"
	leaderCfg.SMT.DiskPath = t.TempDir()
	leader := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, leaderCfg, storage)
	require.NoError(t, leader.Start(ctx))

	firstCommitment := testutil.CreateTestCertificationRequest(t, "disk-ha-before-failover")
	block1, dropped := finalizeManualDiskRound(t, ctx, leader, 1, []*models.CertificationRequest{firstCommitment})
	require.Empty(t, dropped)
	require.True(t, block1.Finalized)

	followerCfg := cfg
	followerCfg.HA.ServerID = "follower"
	followerCfg.SMT.DiskPath = t.TempDir()
	follower := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, followerCfg, storage)
	require.NoError(t, follower.Start(ctx))

	followerState, err := follower.smtBackend.CommittedState(ctx)
	require.NoError(t, err)
	require.Nil(t, followerState.BlockNumber, "fresh HA follower starts stale and waits for BlockSyncer catch-up")

	syncer := ha.NewBlockSyncer(
		newTestLogger(t),
		&testHAFollowerSelector{},
		storage,
		follower.smtBackend,
		0,
		time.Second,
		follower.stateTracker,
	)
	require.NoError(t, syncer.SyncToLatestBlock(ctx))
	requireDiskCommittedState(t, ctx, follower.smtBackend, block1)
	requirePublishedProof(t, ctx, follower.smtBackend, firstCommitment, block1.RootHash)

	require.NoError(t, leader.Deactivate(ctx))

	target, err := syncer.SyncToLatestFinalizedBlock(ctx)
	require.NoError(t, err)
	require.NotNil(t, target)
	require.Equal(t, block1.Index.String(), target.Index.String())
	requireDiskCommittedState(t, ctx, follower.smtBackend, block1)
	require.NoError(t, follower.Activate(ctx))

	secondCommitment := testutil.CreateTestCertificationRequest(t, "disk-ha-after-failover")
	block2, dropped := finalizeManualDiskRound(t, ctx, follower, 2, []*models.CertificationRequest{secondCommitment})
	require.Empty(t, dropped)
	require.True(t, block2.Finalized)

	requireDiskCommittedState(t, ctx, follower.smtBackend, block2)
	requirePublishedProof(t, ctx, follower.smtBackend, secondCommitment, block2.RootHash)

	latest, err := storage.BlockStorage().GetLatest(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, block2.Index.String(), latest.Index.String())
	require.Equal(t, block2.RootHash.String(), latest.RootHash.String())
}

func TestDiskSMTHAFollowerRejectsDivergentFinalizedRoot(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardHAConfig(t, "disk_ha_divergent_root")
	storage := testutil.SetupTestStorage(t, cfg)

	commitment := testutil.CreateTestCertificationRequest(t, "disk-ha-divergent-root")
	leaf, err := commitmentLeafInput(commitment)
	require.NoError(t, err)

	wrongRoot := api.NewHexBytes(make([]byte, api.SiblingSize))
	wrongRoot[len(wrongRoot)-1] = 1
	uc, err := testProofUC(1, 1, wrongRoot)
	require.NoError(t, err)
	block := models.NewBlock(
		api.NewBigIntFromUint64(1),
		cfg.Chain.ID,
		0,
		cfg.Chain.Version,
		cfg.Chain.ForkID,
		wrongRoot,
		api.HexBytes{},
		uc,
	)
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized
	block.ProposalID = "proposal-" + block.Index.String()
	require.NoError(t, storage.BlockStorage().Store(ctx, block))
	record := models.NewAggregatorRecord(commitment, block.Index, api.NewBigIntFromUint64(0))
	record.ProposalID = block.ProposalID
	require.NoError(t, storage.AggregatorRecordStorage().Store(ctx, record))
	require.NoError(t, storage.SmtStorage().Store(ctx, models.NewSmtNode(leaf.Key, leaf.Value)))

	followerCfg := cfg
	followerCfg.HA.ServerID = "follower"
	followerCfg.SMT.DiskPath = t.TempDir()
	follower := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, followerCfg, storage)
	require.NoError(t, follower.Start(ctx))

	syncer := ha.NewBlockSyncer(
		newTestLogger(t),
		&testHAFollowerSelector{},
		storage,
		follower.smtBackend,
		0,
		time.Second,
		follower.stateTracker,
	)
	target, err := syncer.SyncToLatestFinalizedBlock(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, ha.ErrDiskSMTDiverged), "expected ErrDiskSMTDiverged, got %v", err)
	require.NotNil(t, target)
	require.Equal(t, block.Index.String(), target.Index.String())

	state, err := follower.smtBackend.CommittedState(ctx)
	require.NoError(t, err)
	require.Nil(t, state.BlockNumber, "divergent replay must fail before committing local RocksDB metadata")
}

func testDiskBFTShardHAConfig(t *testing.T, name string) config.Config {
	t.Helper()
	cfg := testDiskBFTShardConfig(t, name)
	cfg.HA.Enabled = true
	cfg.HA.LockID = "test-" + name
	cfg.HA.LockTTLSeconds = 1
	cfg.HA.LeaderHeartbeatInterval = 100 * time.Millisecond
	cfg.HA.LeaderElectionPollingInterval = 50 * time.Millisecond
	return cfg
}

func newTestDiskBFTShardRoundManagerWithStorage(
	t *testing.T,
	ctx context.Context,
	cfg config.Config,
	storage *mongodb.Storage,
) *RoundManager {
	t.Helper()
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
	return rm
}

func requireDiskCommittedState(t *testing.T, ctx context.Context, backend smtbackend.Backend, block *models.Block) {
	t.Helper()
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.NotNil(t, state.BlockNumber)
	require.Equal(t, block.Index.String(), state.BlockNumber.String())
	require.Equal(t, block.RootHash.String(), api.HexBytes(state.RootHash).String())
}

func requirePublishedProof(
	t *testing.T,
	ctx context.Context,
	backend smtbackend.Backend,
	commitment *models.CertificationRequest,
	root api.HexBytes,
) {
	t.Helper()
	reader, ok := backend.(smtbackend.PublishedProofReader)
	require.True(t, ok)
	leaf, err := commitmentLeafInput(commitment)
	require.NoError(t, err)
	publishedRoot, err := reader.PublishedRoot(ctx)
	require.NoError(t, err)
	cert, err := reader.GetPublishedInclusionCertAtRoot(ctx, publishedRoot, leaf.Key)
	require.NoError(t, err)
	require.Equal(t, root.String(), api.HexBytes(publishedRoot).String())
	require.NoError(t, cert.Verify(leaf.Key, leaf.Value, root, api.InclusionProofV2HashAlgorithm))
}

type testHAFollowerSelector struct{}

func (testHAFollowerSelector) IsLeader(context.Context) (bool, error) {
	return false, nil
}
