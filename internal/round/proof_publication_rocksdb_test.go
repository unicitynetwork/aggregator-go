//go:build rocksdb

package round

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func attachDiskProofPub(t *testing.T, rm *RoundManager) *proofview.Catalog {
	t.Helper()
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	rm.proofPub = proofpub.New(cat, rm.smtBackend, 0, nil, []byte("lineage"), newTestLogger(t))
	require.NotNil(t, rm.proofPub, "the RocksDB backend must support retained read views")
	return cat
}

// Retained disk views keep serving their finalized root, with correct proofs, after
// later rounds have committed (and tombstoned/overwritten nodes) on the live store.
func TestDiskSMTPublicationRetainsFinalizedViewsAcrossLaterWrites(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_pub_retain")
	rm, _ := newTestDiskBFTShardRoundManager(t, ctx, cfg)
	require.NoError(t, rm.Start(ctx))
	cat := attachDiskProofPub(t, rm)

	c1 := testutil.CreateTestCertificationRequest(t, "disk-pub-1")
	b1, dropped := finalizeManualDiskRound(t, ctx, rm, 1, []*models.CertificationRequest{c1})
	require.Empty(t, dropped)

	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte(b1.RootHash), l.Root())
	require.Equal(t, []byte(b1.UnicityCertificate), l.UC())
	l.Release()

	c2 := testutil.CreateTestCertificationRequest(t, "disk-pub-2")
	c3 := testutil.CreateTestCertificationRequest(t, "disk-pub-3")
	b2, dropped := finalizeManualDiskRound(t, ctx, rm, 2, []*models.CertificationRequest{c2, c3})
	require.Empty(t, dropped)
	require.NotEqual(t, b1.RootHash, b2.RootHash)

	latest, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte(b2.RootHash), latest.Root())
	require.EqualValues(t, 2, latest.Block().Number.Uint64())
	latest.Release()

	// the older exact root is still served, from its pinned snapshot, not live RocksDB
	old, err := cat.AcquireRoot(ctx, b1.RootHash)
	require.NoError(t, err)
	defer old.Release()
	leaf, err := materializeCommitmentLeaf(c1, 1755000000)
	require.NoError(t, err)
	cert, err := old.GetInclusionCert(leaf.Key)
	require.NoError(t, err)
	require.NoError(t, cert.Verify(leaf.Key, leaf.Value, b1.RootHash, api.InclusionProofV2HashAlgorithm))
	leaf2, err := materializeCommitmentLeaf(c2, 1755000000)
	require.NoError(t, err)
	_, err = old.GetInclusionCert(leaf2.Key)
	require.Error(t, err, "a later leaf is not in the older view")
}

// A failed finalization never publishes, and ordinary finalization is unaffected by a
// dead catalog.
func TestDiskSMTPublicationFailureNeverFailsFinalization(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_pub_fail")
	rm, storage := newTestDiskBFTShardRoundManager(t, ctx, cfg)
	require.NoError(t, rm.Start(ctx))
	cat := attachDiskProofPub(t, rm)
	require.NoError(t, cat.Close())

	c := testutil.CreateTestCertificationRequest(t, "disk-pub-dead")
	b, _ := finalizeManualDiskRound(t, ctx, rm, 1, []*models.CertificationRequest{c})
	stored, err := storage.BlockStorage().GetByNumber(ctx, b.Index)
	require.NoError(t, err)
	require.True(t, stored.Finalized)
}

// A follower retains the disk state it replayed and keeps publishing after promotion.
func TestDiskSMTPublicationFollowerReplayAndPromotion(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardHAConfig(t, "disk_pub_ha")
	storage := testutil.SetupTestStorage(t, cfg)

	leaderCfg := cfg
	leaderCfg.HA.ServerID = "leader"
	leaderCfg.SMT.DiskPath = t.TempDir()
	leader := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, leaderCfg, storage)
	require.NoError(t, leader.Start(ctx))
	c1 := testutil.CreateTestCertificationRequest(t, "disk-pub-ha-1")
	b1, _ := finalizeManualDiskRound(t, ctx, leader, 1, []*models.CertificationRequest{c1})

	followerCfg := cfg
	followerCfg.HA.ServerID = "follower"
	followerCfg.SMT.DiskPath = t.TempDir()
	follower := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, followerCfg, storage)
	require.NoError(t, follower.Start(ctx))
	cat := attachDiskProofPub(t, follower)

	syncer := ha.NewBlockSyncer(newTestLogger(t), &testHAFollowerSelector{}, storage, follower.smtBackend, 0, time.Second, follower.stateTracker)
	syncer.SetViewPublisher(follower.proofPub)
	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte(b1.RootHash), l.Root())
	require.Equal(t, []byte(b1.UnicityCertificate), l.UC())
	l.Release()

	require.NoError(t, leader.Deactivate(ctx))
	_, err = syncer.SyncToLatestFinalizedBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, follower.Activate(ctx))
	c2 := testutil.CreateTestCertificationRequest(t, "disk-pub-ha-2")
	b2, _ := finalizeManualDiskRound(t, ctx, follower, 2, []*models.CertificationRequest{c2})

	l, err = cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte(b2.RootHash), l.Root())
	l.Release()
	old, err := cat.AcquireRoot(ctx, b1.RootHash)
	require.NoError(t, err, "the state replayed before promotion is still retained")
	old.Release()
}

// Startup publishes the verified current disk state, and nothing else.
func TestDiskSMTPublicationStartup(t *testing.T) {
	ctx := context.Background()
	cfg := testDiskBFTShardConfig(t, "disk_pub_start")
	rm, storage := newTestDiskBFTShardRoundManager(t, ctx, cfg)
	require.NoError(t, rm.Start(ctx))
	c := testutil.CreateTestCertificationRequest(t, "disk-pub-start")
	b, _ := finalizeManualDiskRound(t, ctx, rm, 1, []*models.CertificationRequest{c})
	require.NoError(t, rm.Stop(ctx))

	rm2 := newTestDiskBFTShardRoundManagerWithStorage(t, ctx, cfg, storage.(*mongodb.Storage))
	cat := attachDiskProofPub(t, rm2)
	require.NoError(t, rm2.Start(ctx))
	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, []byte(b.RootHash), l.Root())
}
