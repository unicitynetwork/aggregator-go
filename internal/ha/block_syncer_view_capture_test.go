package ha

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// replayBlocks answers the replay's reload of the finalized blocks through mutate, the way the leader's
// cleanup (or a newer certification) changes the stored block between the follower's sampling of
// its sync target and the replay.
type replayBlocks struct {
	interfaces.BlockStorage
	mutate func(*models.Block) *models.Block
}

func (r *replayBlocks) GetNextFinalizedAfter(ctx context.Context, after, to *api.BigInt) (*models.Block, error) {
	b, err := r.BlockStorage.GetNextFinalizedAfter(ctx, after, to)
	if err != nil || b == nil {
		return b, err
	}
	c := *b
	return r.mutate(&c), nil
}

type replayStorage struct {
	interfaces.Storage
	blocks *replayBlocks
}

func (s *replayStorage) BlockStorage() interfaces.BlockStorage { return s.blocks }

type captureEnv struct {
	syncer  *BlockSyncer
	catalog *proofview.Catalog
	f       *prooftest.Fixture
	uc      []byte
	root    api.HexBytes
}

func newCaptureEnv(t *testing.T, name string, mutate func(*models.Block) *models.Block) *captureEnv {
	ctx := t.Context()
	storage := testutil.SetupTestStorage(t, config.Config{Database: config.DatabaseConfig{
		Database: name, ConnectTimeout: 30 * time.Second, ServerSelectionTimeout: 5 * time.Second,
		SocketTimeout: 30 * time.Second, MaxPoolSize: 100, MinPoolSize: 5, MaxConnIdleTime: 5 * time.Minute,
	}})
	log := testBlockSyncerLogger(t)
	backend := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	catalog, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { catalog.Close() })
	pub := proofpub.New(catalog, backend, nil, log)
	pub.SetSealVerifier(prooftest.New(t, 2, 1, prooftest.Hash("x"), 80).Verifier())

	root := createBlock(t, storage, 1)
	f := prooftest.New(t, 2, 1, root, 81)
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigIntFromUint64(1))
	require.NoError(t, err)
	block.UnicityCertificate = api.NewHexBytes(uc)
	block.PartitionView = api.NewHexBytes(f.Raw)
	require.NoError(t, storage.BlockStorage().(interface {
		SetFinalizedWithCertificate(context.Context, *models.Block) error
	}).SetFinalizedWithCertificate(ctx, block))

	syncer := NewBlockSyncer(log, &mockLeaderSelector{}, &replayStorage{Storage: storage,
		blocks: &replayBlocks{BlockStorage: storage.BlockStorage(), mutate: mutate}},
		backend, 0, time.Second, state.NewSyncStateTracker())
	syncer.SetViewPublisher(pub)
	return &captureEnv{syncer: syncer, catalog: catalog, f: f, uc: uc, root: root}
}

// The leader removes the superseded block's view between the follower sampling its target and
// the replay reloading the block: the attachment captured with the target is still published,
// because the reloaded block is the very same certification.
func TestBlockSyncerPublishesCapturedViewWhenCleanedMeanwhile(t *testing.T) {
	e := newCaptureEnv(t, "test_view_capture_same", func(b *models.Block) *models.Block {
		b.PartitionView = nil
		return b
	})
	require.NoError(t, e.syncer.SyncToLatestBlock(t.Context()))

	l, err := e.catalog.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, e.uc, l.UC())
	require.Equal(t, e.f.Raw, l.PartitionView(), "captured with the sync target")
}

// A target which was recertified meanwhile (same root, different UC) must not get the captured view
// of the older certification: the reloaded block is published as it is stored, without a view.
func TestBlockSyncerNeverSubstitutesAnOlderView(t *testing.T) {
	var uc2 []byte
	e := newCaptureEnv(t, "test_view_capture_other_uc", func(b *models.Block) *models.Block {
		b.UnicityCertificate = api.NewHexBytes(uc2)
		b.PartitionView = nil
		return b
	})
	f2 := prooftest.New(t, 2, 1, e.root, 82)
	var err error
	uc2, err = types.Cbor.Marshal(f2.UC)
	require.NoError(t, err)
	require.NotEqual(t, e.uc, uc2)

	require.NoError(t, e.syncer.SyncToLatestBlock(t.Context()))

	l, err := e.catalog.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, uc2, l.UC(), "the current certification is published")
	require.Nil(t, l.PartitionView(), "retryably unavailable for the sharded latest, never the older view")
	require.Equal(t, []byte(e.root), l.Root())
}

// A leader promotion samples a fixed target the same way.
func TestBlockSyncerPromotionTargetKeepsCapturedView(t *testing.T) {
	e := newCaptureEnv(t, "test_view_capture_promotion", func(b *models.Block) *models.Block {
		b.PartitionView = nil
		return b
	})
	target, err := e.syncer.SyncToLatestFinalizedBlock(t.Context())
	require.NoError(t, err)
	require.Equal(t, big.NewInt(1), target.Index.Int)
	l, err := e.catalog.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, e.f.Raw, l.PartitionView())
}
