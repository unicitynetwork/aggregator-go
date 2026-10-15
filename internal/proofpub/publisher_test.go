package proofpub_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-go-base/types"
)

var lineage = []byte("config-hash")

func commit(t testing.TB, b *smtbackend.MemoryBackend, block uint64, from, to uint32) []byte {
	ctx := context.Background()
	var inputs []smtbackend.LeafInput
	for n := from; n < to; n++ {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		binary.BigEndian.PutUint32(key[:4], n*2654435761)
		binary.BigEndian.PutUint32(key[len(key)-4:], n)
		value := make([]byte, 32)
		binary.BigEndian.PutUint32(value[28:], n+1)
		inputs = append(inputs, smtbackend.LeafInput{Key: key, Value: value})
	}
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	res, err := snap.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

type env struct {
	b   *smtbackend.MemoryBackend
	cat *proofview.Catalog
	pub *proofpub.Publisher
}

func newEnv(t *testing.T) *env {
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	b := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	pub := proofpub.New(cat, b, prooftest.Partition, lineage, log)
	require.NotNil(t, pub)
	return &env{b: b, cat: cat, pub: pub}
}

// finalizedBlock builds the finalized block for the root with a certification
// (and optionally the matching partition view).
func finalizedBlock(t *testing.T, number uint64, root []byte, rootRound uint64, withView bool) (*models.Block, *prooftest.Fixture) {
	f := prooftest.New(t, 2, number, root, rootRound)
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	b := models.NewBlock(api.NewBigIntFromUint64(number), "unicity", 0, "1.0", "mainnet",
		api.NewHexBytes(root), api.NewHexBytes(prooftest.Hash("prev")), api.NewHexBytes(uc), 1)
	b.Finalized = true
	if withView {
		b.PartitionView = api.NewHexBytes(f.Raw)
	}
	return b, f
}

func latest(t *testing.T, c *proofview.Catalog) *proofview.Lease {
	l, err := c.AcquireLatest(t.Context())
	require.NoError(t, err)
	t.Cleanup(l.Release)
	return l
}

func Test_PublishFinalized(t *testing.T) {
	e := newEnv(t)
	root := commit(t, e.b, 1, 0, 20)
	block, f := finalizedBlock(t, 1, root, 80, true)

	e.pub.PublishFinalized(t.Context(), block)

	l := latest(t, e.cat)
	require.Equal(t, root, l.Root())
	require.Equal(t, []byte(block.UnicityCertificate), l.UC())
	require.Equal(t, f.Raw, l.PartitionView())
	require.EqualValues(t, 1, l.Block().Number.Uint64())
	require.Equal(t, lineage, l.Block().ConfigLineage)
}

func Test_PublishFinalized_WithoutSidecar(t *testing.T) {
	e := newEnv(t)
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), block)
	require.Nil(t, latest(t, e.cat).PartitionView(), "latest is served even without a sidecar")
}

// No state which is not finalized is ever published, and a failure makes latest
// unavailable instead of falling back to an older view.
func Test_NoUnfinalizedRootAndNoLatestFallback(t *testing.T) {
	e := newEnv(t)
	root1 := commit(t, e.b, 1, 0, 20)
	b1, _ := finalizedBlock(t, 1, root1, 80, true)
	e.pub.PublishFinalized(t.Context(), b1)

	root2 := commit(t, e.b, 2, 20, 40)
	b2, _ := finalizedBlock(t, 2, root2, 81, true)
	b2.Finalized = false
	e.pub.PublishFinalized(t.Context(), b2)

	_, err := e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "latest must not fall back to block 1")
	_, err = e.cat.AcquireRoot(t.Context(), root2)
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "unfinalized root is not published")
	l, err := e.cat.AcquireRoot(t.Context(), root1)
	require.NoError(t, err, "older exact root still serves")
	l.Release()

	// a later successful publication heals latest
	b2.Finalized = true
	e.pub.PublishFinalized(t.Context(), b2)
	require.Equal(t, root2, latest(t, e.cat).Root())
}

func Test_RootMismatchAndMissingCertificate(t *testing.T) {
	e := newEnv(t)
	root1 := commit(t, e.b, 1, 0, 20)
	b1, _ := finalizedBlock(t, 1, root1, 80, true)
	e.pub.PublishFinalized(t.Context(), b1)

	root2 := commit(t, e.b, 2, 20, 40)

	t.Run("opportunistic publication of a stale block leaves the catalog alone", func(t *testing.T) {
		e.pub.EnsurePublished(t.Context(), b1) // backend has moved on to root2
		require.Equal(t, root1, latest(t, e.cat).Root())
	})
	t.Run("strict mismatch makes latest unavailable", func(t *testing.T) {
		stale, _ := finalizedBlock(t, 2, root1, 81, true) // claims root1, backend is at root2
		e.pub.PublishFinalized(t.Context(), stale)
		_, err := e.cat.AcquireLatest(t.Context())
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	})
	t.Run("no certificate", func(t *testing.T) {
		b2, _ := finalizedBlock(t, 2, root2, 81, false)
		b2.UnicityCertificate = nil
		e.pub.PublishFinalized(t.Context(), b2)
		_, err := e.cat.AcquireLatest(t.Context())
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
		_, err = e.cat.AcquireRoot(t.Context(), root2)
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	})
}

func Test_StoredViewIsRevalidated(t *testing.T) {
	e := newEnv(t)
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, false)
	other := prooftest.New(t, 2, 9, prooftest.Hash("elsewhere"), 99)
	block.PartitionView = api.NewHexBytes(other.Raw) // belongs to another UC
	e.pub.PublishFinalized(t.Context(), block)
	l := latest(t, e.cat)
	require.Equal(t, root, l.Root(), "certification is still published")
	require.Nil(t, l.PartitionView(), "unvalidated view is dropped")
}

func material(t *testing.T, kind proofmaterial.Kind, f *prooftest.Fixture, view []byte, block uint64, hasBlock bool) *proofmaterial.Material {
	digest, ucBytes, err := proofmaterial.DigestOfUC(f.UC)
	require.NoError(t, err)
	return &proofmaterial.Material{Kind: kind, UC: f.UC, UCBytes: ucBytes, UCDigest: digest, PartitionView: view, BlockNumber: block, HasBlock: hasBlock}
}

func Test_Sink_LateViewAndRepeat(t *testing.T) {
	e := newEnv(t)
	root := commit(t, e.b, 1, 0, 20)
	block, f := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), block)
	require.Nil(t, latest(t, e.cat).PartitionView())

	// recovery path: finalized material arrives with the view afterwards
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindFinalized, f, f.Raw, 1, true)))
	l := latest(t, e.cat)
	require.Equal(t, f.Raw, l.PartitionView())
	l.Release()

	// repeat/timeout UC on the unchanged state: newer seal, same root
	rep := prooftest.New(t, 2, 1, root, 90)
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, rep, rep.Raw, 0, false)))
	l2 := latest(t, e.cat)
	repBytes, _ := types.Cbor.Marshal(rep.UC)
	require.Equal(t, repBytes, l2.UC(), "latest is the newest certification")
	require.Equal(t, rep.Raw, l2.PartitionView())

	// a state which is not retained is ignored without error
	unknown := prooftest.New(t, 2, 5, prooftest.Hash("unknown"), 95)
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, unknown, nil, 0, false)))
	require.Equal(t, repBytes, latest(t, e.cat).UC())
}

func Test_NilPublisherIsInert(t *testing.T) {
	var p *proofpub.Publisher
	p.PublishFinalized(t.Context(), nil)
	p.EnsurePublished(t.Context(), nil)
	require.NoError(t, p.PublishProofMaterial(t.Context(), nil))
	require.Nil(t, p.Catalog())
}

func Test_UnsupportedBackend(t *testing.T) {
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	defer cat.Close()
	require.Nil(t, proofpub.New(cat, nil, 1, nil, nil))
}
