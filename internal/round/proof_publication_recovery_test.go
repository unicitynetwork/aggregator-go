package round

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Database-free: recovery-driven finalization (runtime retry after a transient write
// failure, HA activation) must publish the recovered head, or invalidate latest, never
// leave the catalog serving the previous block (codex review of A4, major 1).

type fixedBlockStorage struct {
	interfaces.BlockStorage
	blocks map[uint64]*models.Block
}

func (s *fixedBlockStorage) GetByNumber(_ context.Context, n *api.BigInt) (*models.Block, error) {
	return s.blocks[n.Uint64()], nil
}

type fixedStorage struct {
	interfaces.Storage
	bs *fixedBlockStorage
}

func (s *fixedStorage) BlockStorage() interfaces.BlockStorage { return s.bs }

type recoveryEnv struct {
	rm  *RoundManager
	cat *proofview.Catalog
	bs  *fixedBlockStorage
	b   *smtbackend.MemoryBackend
}

func newRecoveryEnv(t *testing.T) *recoveryEnv {
	log := newTestLogger(t)
	b := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	topo := prooftest.New(t, 2, 1, prooftest.Hash("x"), 80)
	bs := &fixedBlockStorage{blocks: map[uint64]*models.Block{}}
	rm := &RoundManager{
		logger:     log,
		smtBackend: b,
		storage:    &fixedStorage{bs: bs},
		proofPub:   proofpub.New(cat, b, prooftest.Partition, nil, topo.Verifier(), log),
	}
	return &recoveryEnv{rm: rm, cat: cat, bs: bs, b: b}
}

func (e *recoveryEnv) commitBlock(t *testing.T, number uint64, leaf byte) (*models.Block, smtbackend.Snapshot) {
	ctx := context.Background()
	snap, err := e.b.CreateSnapshot(ctx)
	require.NoError(t, err)
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0] = leaf
	res, err := snap.AddLeavesClassified(ctx, []smtbackend.LeafInput{{Key: key, Value: make([]byte, 32)}})
	require.NoError(t, err)
	f := prooftest.New(t, 2, number, res.CandidateRoot, 80+number)
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	block := models.NewBlock(api.NewBigIntFromUint64(number), "unicity", 0, "1.0", "mainnet",
		api.NewHexBytes(res.CandidateRoot), nil, api.NewHexBytes(uc), 1)
	block.Finalized = true
	block.PartitionView = api.NewHexBytes(f.Raw)
	return block, snap
}

func Test_ReconcileRecoveredFinalizationPublishesRecoveredHead(t *testing.T) {
	ctx := context.Background()
	e := newRecoveryEnv(t)

	b1, snap1 := e.commitBlock(t, 1, 1)
	require.NoError(t, snap1.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: b1.Index, RootHash: b1.RootHash}))
	e.rm.proofPub.PublishFinalized(ctx, b1)

	// block 2 was finalized by recovery; the in-memory round snapshot is committed by reconcile
	b2, snap2 := e.commitBlock(t, 2, 2)
	e.bs.blocks[2] = b2
	e.rm.currentRound = &Round{Number: b2.Index, Snapshot: snap2}
	require.NoError(t, e.rm.reconcileRecoveredFinalization(ctx, &RecoveryResult{Recovered: true, BlockNumber: b2.Index, Block: b2}))

	l, err := e.cat.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.EqualValues(t, 2, l.Block().Number.Uint64(), "latest must be the recovered block, not block 1")
	require.Equal(t, []byte(b2.RootHash), l.Root())
	require.Equal(t, []byte(b2.PartitionView), l.PartitionView())
}

func Test_RecoveredBlockWhichCannotBePublishedInvalidatesLatest(t *testing.T) {
	ctx := context.Background()
	e := newRecoveryEnv(t)
	b1, snap1 := e.commitBlock(t, 1, 1)
	require.NoError(t, snap1.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: b1.Index, RootHash: b1.RootHash}))
	e.rm.proofPub.PublishFinalized(ctx, b1)

	t.Run("block missing from storage", func(t *testing.T) {
		e.rm.publishRecoveredBlock(ctx, api.NewBigIntFromUint64(2))
		_, err := e.cat.AcquireLatest(ctx)
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	})
	t.Run("a later publication heals latest", func(t *testing.T) {
		b2, snap2 := e.commitBlock(t, 2, 2)
		require.NoError(t, snap2.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: b2.Index, RootHash: b2.RootHash}))
		e.bs.blocks[2] = b2
		e.rm.publishRecoveredBlock(ctx, b2.Index)
		l, err := e.cat.AcquireLatest(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 2, l.Block().Number.Uint64())
		l.Release()
	})
	t.Run("recovered block without a certification", func(t *testing.T) {
		b3, snap3 := e.commitBlock(t, 3, 3)
		require.NoError(t, snap3.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: b3.Index, RootHash: b3.RootHash}))
		b3.UnicityCertificate = nil
		e.bs.blocks[3] = b3
		e.rm.publishRecoveredBlock(ctx, b3.Index)
		_, err := e.cat.AcquireLatest(ctx)
		require.ErrorIs(t, err, proofview.ErrViewNotRetained, "not served as block 2")
	})
}
