package proofpub_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
	"strings"
	"sync"
	"testing"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
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

func newEnv(t *testing.T, f *prooftest.Fixture) *env {
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	b := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	pub := proofpub.New(cat, b, nil, log)
	require.NotNil(t, pub)
	pub.SetSealVerifier(f.Verifier())
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
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	block, f := finalizedBlock(t, 1, root, 80, true)

	e.pub.PublishFinalized(t.Context(), block)

	l := latest(t, e.cat)
	require.Equal(t, root, l.Root())
	require.Equal(t, []byte(block.UnicityCertificate), l.UC())
	require.Equal(t, f.Raw, l.PartitionView())
	require.EqualValues(t, 1, l.Block().Number.Uint64())
	require.Equal(t, []byte(f.UC.ShardConfHash), l.Block().ConfigLineage, "identity comes from the accepted UC, not process startup")
}

func Test_RetryLatest(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("retry"), 80))
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, true)
	require.True(t, e.pub.RetryLatest(t.Context(), block))
	require.True(t, e.cat.HasLatest(block.Index, block.RootHash, block.UnicityCertificate))
	require.False(t, e.pub.RetryLatest(t.Context(), block))
	require.False(t, e.cat.HasLatest(api.NewBigIntFromUint64(2), block.RootHash, block.UnicityCertificate))
	require.False(t, e.cat.HasLatest(block.Index, prooftest.Hash("different"), block.UnicityCertificate))
	other, _ := finalizedBlock(t, 1, root, 81, true)
	require.False(t, e.cat.HasLatest(block.Index, block.RootHash, other.UnicityCertificate))

	e.pub.InvalidateLatest()
	require.False(t, e.cat.HasLatest(block.Index, block.RootHash, block.UnicityCertificate))
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.False(t, e.pub.RetryLatest(cancelled, block))
	_, err := e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	require.True(t, e.pub.RetryLatest(t.Context(), block))
	require.Equal(t, root, latest(t, e.cat).Root())

	var disabled *proofpub.Publisher
	require.False(t, disabled.RetryLatest(t.Context(), block))
}

func Test_RetryLatestSuppressesUnchangedWarnings(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("retry-warning"), 80))
	var logs bytes.Buffer
	log := &logger.Logger{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	pub := proofpub.New(e.cat, e.b, nil, log)
	warnings := func() int { return strings.Count(logs.String(), "Not publishing retained proof view") }
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, false)
	missing := *block
	missing.UnicityCertificate = nil
	counter := metrics.ProofViewPublicationTotal.WithLabelValues(proofpub.ResultNoCert)
	before := promtest.ToFloat64(counter)
	pub.PublishFinalized(t.Context(), &missing)
	require.Equal(t, 1, warnings())
	for i := 0; i < 5; i++ {
		require.False(t, pub.RetryLatest(t.Context(), &missing))
	}
	require.Equal(t, 1, warnings(), "same-head failures keep their first warning")
	require.Equal(t, before+6, promtest.ToFloat64(counter), "every failed attempt is counted")
	var concurrent sync.WaitGroup
	for i := 0; i < 16; i++ {
		concurrent.Go(func() { pub.RetryLatest(t.Context(), &missing) })
	}
	concurrent.Wait()
	require.Equal(t, 1, warnings(), "concurrent identical failures share one warning")
	require.Equal(t, before+22, promtest.ToFloat64(counter))

	invalid := *block
	invalid.UnicityCertificate = api.HexBytes{0}
	for i := 0; i < 5; i++ {
		require.False(t, pub.RetryLatest(t.Context(), &invalid))
	}
	require.Equal(t, 2, warnings(), "a changed failure is reported")
	invalid.UnicityCertificate = api.HexBytes{1}
	require.False(t, pub.RetryLatest(t.Context(), &invalid))
	require.Equal(t, 3, warnings(), "a changed certification is a new candidate")

	require.True(t, pub.RetryLatest(t.Context(), block))
	require.False(t, pub.RetryLatest(t.Context(), block))
	pub.PublishFinalized(t.Context(), &missing)
	require.Equal(t, 4, warnings(), "recovery resets warning suppression")
	missing.Index = api.NewBigIntFromUint64(2)
	require.False(t, pub.RetryLatest(t.Context(), &missing))
	require.Equal(t, 5, warnings(), "a changed block is reported")
	missing.RootHash = api.HexBytes(prooftest.Hash("different"))
	require.False(t, pub.RetryLatest(t.Context(), &missing))
	require.Equal(t, 6, warnings(), "a changed root is reported")
}

func Test_PublishFinalized_WithoutSidecar(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), block)
	require.Nil(t, latest(t, e.cat).PartitionView(), "latest is served even without a sidecar")
}

// No state which is not finalized is ever published, and a failure makes latest
// unavailable instead of falling back to an older view.
func Test_NoUnfinalizedRootAndNoLatestFallback(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
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
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
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
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
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
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
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

	// a state which is not retained is reported (the caller keeps the fill retryable)
	unknown := prooftest.New(t, 2, 5, prooftest.Hash("unknown"), 95)
	require.ErrorIs(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, unknown, nil, 0, false)), proofview.ErrViewNotRetained)
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
	require.Nil(t, proofpub.New(cat, nil, nil, nil))
}

func commitEmpty(t testing.TB, b *smtbackend.MemoryBackend, block uint64) {
	ctx := context.Background()
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	root, err := snap.RootHashRaw(ctx)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: root}))
}

// Different finalized empty rounds share one root. A historical block must never be
// recreated from the current state, nor replace latest (codex review, major 2).
func Test_HistoricalRetryWithSameRootNeverRegressesLatest(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	first, _ := finalizedBlock(t, 1, root, 80, true)
	e.pub.PublishFinalized(t.Context(), first)

	var head *models.Block
	for n := uint64(2); n <= 18; n++ { // 17 views > the 16 retained: block 1 is evicted
		commitEmpty(t, e.b, n)
		head, _ = finalizedBlock(t, n, root, 80+n, false)
		e.pub.PublishFinalized(t.Context(), head)
	}
	require.EqualValues(t, 18, latest(t, e.cat).Block().Number.Uint64())

	e.pub.EnsurePublished(t.Context(), first) // historical retry, same root as the head
	l := latest(t, e.cat)
	require.EqualValues(t, 18, l.Block().Number.Uint64(), "latest must not regress to block 1")
	require.Equal(t, []byte(head.UnicityCertificate), l.UC())
	var ucHead types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(head.UnicityCertificate, &ucHead))
	id, found := e.cat.ViewForRootCompatible(root, proofview.CompatDigest(&ucHead))
	require.True(t, found)
	require.EqualValues(t, 18, id.Number.Uint64())

	// the same applies to a strict publication of a block the backend is not at
	e.pub.PublishFinalized(t.Context(), first)
	_, err := e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "a stale strict publication invalidates latest, it does not promote block 1")
}

func Test_StartupPublicationAllowsUnknownCommittedBlock(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	// a restored memory backend knows its root but not its block number
	ctx := context.Background()
	snap, err := e.b.CreateSnapshot(ctx)
	require.NoError(t, err)
	var in []smtbackend.LeafInput
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0] = 7
	in = append(in, smtbackend.LeafInput{Key: key, Value: make([]byte, 32)})
	res, err := snap.AddLeavesClassified(ctx, in)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{RootHash: res.CandidateRoot}))
	block, _ := finalizedBlock(t, 5, res.CandidateRoot, 85, true)

	e.pub.EnsurePublished(t.Context(), block)
	_, err = e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrNoFinalizedView, "ordinary retries need a known committed block")

	e.pub.EnsurePublishedAtStartup(t.Context(), block)
	require.Equal(t, res.CandidateRoot, latest(t, e.cat).Root())
}

func Test_Sink_ReturnsAttachmentFailures(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	f := prooftest.New(t, 2, 1, prooftest.Hash("nothing retained"), 80)
	err := e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, f, f.Raw, 0, false))
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	err = e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindFinalized, f, f.Raw, 1, true))
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)

	root := commit(t, e.b, 1, 0, 20)
	block, bf := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), block)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, e.pub.PublishProofMaterial(cancelled, material(t, proofmaterial.KindFinalized, bf, bf.Raw, 1, true)))
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindFinalized, bf, bf.Raw, 1, true)))
}

// Metadata is bound to the accepted identity: equal state hashes across config epochs or
// differing input records must not attach (architect audit, aggregator-go#196).
func Test_Sink_RejectsIncompatibleAttachments(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	block, f := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), block)
	before := latest(t, e.cat).UC()

	t.Run("different shard configuration", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 90)
		g.UC.ShardConfHash = prooftest.Hash("another epoch's config")
		err := e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g, nil, 0, false))
		require.ErrorIs(t, err, proofpub.ErrIncompatibleCertification)
	})
	t.Run("different input record on the same state hash", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 91)
		g.UC.InputRecord.Epoch++
		err := e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g, nil, 0, false))
		require.ErrorIs(t, err, proofpub.ErrIncompatibleCertification)
		g2 := prooftest.New(t, 2, 2, root, 92) // other shard round, same root
		err = e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g2, nil, 0, false))
		require.ErrorIs(t, err, proofpub.ErrIncompatibleCertification)
	})
	t.Run("different previous hash on the same state hash", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 96)
		g.UC.InputRecord.PreviousHash = prooftest.Hash("another predecessor")
		err := e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g, nil, 0, false))
		require.ErrorIs(t, err, proofpub.ErrIncompatibleCertification)
	})
	t.Run("finalized material for a block identity with another configuration", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 93)
		g.UC.ShardConfHash = prooftest.Hash("elsewhere")
		err := e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindFinalized, g, g.Raw, 1, true))
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	})
	t.Run("no configuration identity", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 94)
		g.UC.ShardConfHash = nil
		require.Error(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g, nil, 0, false)))
	})
	require.Equal(t, before, latest(t, e.cat).UC(), "nothing was attached")

	t.Run("a compatible repeat attaches", func(t *testing.T) {
		g := prooftest.New(t, 2, 1, root, 95)
		require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, g, g.Raw, 0, false)))
		ucBytes, _ := types.Cbor.Marshal(g.UC)
		require.Equal(t, ucBytes, latest(t, e.cat).UC())
	})
	_ = f
}

func Test_PublishRequiresConfigIdentityInUC(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	f := prooftest.New(t, 2, 1, root, 80)
	f.UC.ShardConfHash = nil
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	block, _ := finalizedBlock(t, 1, root, 80, false)
	block.UnicityCertificate = api.NewHexBytes(uc)
	e.pub.PublishFinalized(t.Context(), block)
	_, err = e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "an unsupported boundary is rejected, not labelled with a startup value")
}

// A late view for an older block whose root equals a newer empty block's is attached to
// the older block by the exact certification digest (architect/codex re-check, minor 1).
func Test_SidecarFillReachesOlderBlockWithSameRoot(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	b1, f1 := finalizedBlock(t, 1, root, 80, false) // block 1 finalized without its view
	e.pub.PublishFinalized(t.Context(), b1)
	commitEmpty(t, e.b, 2)
	b2, _ := finalizedBlock(t, 2, root, 81, false) // empty block 2: same root
	e.pub.PublishFinalized(t.Context(), b2)

	// duplicate of UC1 now brings the view
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindSidecarFill, f1, f1.Raw, 0, false)))

	// the view landed on the certification of block 1; latest (block 2) is untouched
	id, _, ok := e.cat.ViewForDigest(sha256.Sum256(b1.UnicityCertificate))
	require.True(t, ok)
	require.EqualValues(t, 1, id.Number.Uint64())
	got, ok := e.cat.PartitionViewForDigest(sha256.Sum256(b1.UnicityCertificate))
	require.True(t, ok)
	require.Equal(t, f1.Raw, got)
	require.Nil(t, latest(t, e.cat).PartitionView(), "latest (block 2) was not modified")
}

// Repeats select the same-root view whose accepted IR they repeat, not the newest one.
func Test_RepeatPicksCompatibleViewAmongSameRootViews(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	root := commit(t, e.b, 1, 0, 20)
	b1, _ := finalizedBlock(t, 1, root, 80, false)
	e.pub.PublishFinalized(t.Context(), b1)
	commitEmpty(t, e.b, 2)
	b2, _ := finalizedBlock(t, 2, root, 81, false)
	e.pub.PublishFinalized(t.Context(), b2)

	// a repeat of block 1's input record (round 1) is compatible with block 1's view only
	rep1 := prooftest.New(t, 2, 1, root, 95)
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, rep1, rep1.Raw, 0, false)))
	// a repeat of block 2 attaches to block 2, which is latest
	rep2 := prooftest.New(t, 2, 2, root, 96)
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, rep2, rep2.Raw, 0, false)))
	uc2, _ := types.Cbor.Marshal(rep2.UC)
	require.Equal(t, uc2, latest(t, e.cat).UC())
	// a repeat of neither is incompatible
	rep3 := prooftest.New(t, 2, 9, root, 97)
	require.ErrorIs(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, rep3, nil, 0, false)), proofpub.ErrIncompatibleCertification)
}

// The live path reuses the view admitted at intake (no second authentication in the commit
// lock); restored material is always re-authenticated.
func Test_AdmittedViewIsNotReauthenticatedButStoredViewIs(t *testing.T) {
	f := prooftest.New(t, 2, 1, prooftest.Hash("x"), 80)
	e := newEnv(t, f)
	var calls int
	inner := f.Verifier()
	e.pub.SetSealVerifier(func(ctx context.Context) types.SealVerifier { calls++; return inner(ctx) })

	root := commit(t, e.b, 1, 0, 20)
	b1, f1 := finalizedBlock(t, 1, root, 80, true)
	e.pub.PublishFinalizedAdmitted(t.Context(), b1)
	require.Zero(t, calls, "admitted view: no second authentication")
	require.Equal(t, f1.Raw, latest(t, e.cat).PartitionView())

	commitEmpty(t, e.b, 2)
	b2, f2 := finalizedBlock(t, 2, root, 81, true)
	e.pub.PublishFinalized(t.Context(), b2) // stored/recovered material
	require.Positive(t, calls, "stored view is re-authenticated")
	require.Equal(t, f2.Raw, latest(t, e.cat).PartitionView())
}

// Long-poll wake-ups: finalization publication does not signal itself (its caller signals once
// the proof-ready update is done, otherwise a waiter would be woken before the serving boundary);
// invalidation and late certification material do.
func Test_Notify_OnlyForInvalidationAndLateMaterial(t *testing.T) {
	e := newEnv(t, prooftest.New(t, 2, 1, prooftest.Hash("x"), 80))
	var n int
	e.pub.SetNotify(func() { n++ })
	root := commit(t, e.b, 1, 0, 20)
	block, f := finalizedBlock(t, 1, root, 80, false)

	e.pub.PublishFinalized(t.Context(), block)
	e.pub.EnsurePublished(t.Context(), block)
	require.Zero(t, n, "publication outcomes are reported by the finalizing caller")

	// a rejected attachment does not wake; an accepted one does
	unknown := prooftest.New(t, 2, 5, prooftest.Hash("unknown"), 95)
	require.Error(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindRepeat, unknown, nil, 0, false)))
	require.Zero(t, n)
	require.NoError(t, e.pub.PublishProofMaterial(t.Context(), material(t, proofmaterial.KindFinalized, f, f.Raw, 1, true)))
	require.Equal(t, 1, n, "a late view turns partition_view_unavailable into a proof")

	e.pub.InvalidateLatest()
	require.Equal(t, 1, n, "no wake from inside the caller's lock: the finalizing caller signals after unlocking")
	_, err := e.cat.AcquireLatest(t.Context())
	require.Error(t, err)

	var nilPub *proofpub.Publisher
	nilPub.SetNotify(func() { t.Fatal("nil publisher") })
	nilPub.InvalidateLatest()
}
