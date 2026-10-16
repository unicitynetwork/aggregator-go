package round

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

// countingView counts how often the catalog asks a retained view for its charge (the hardware independent
// cost of the accounting) and forwards everything else, including leaving the accounting.
type countingView struct {
	smtbackend.ReadView
	reads *atomic.Int64
}

func (c *countingView) RetainedBytes() uint64 {
	c.reads.Add(1)
	return c.ReadView.RetainedBytes()
}

func (c *countingView) RetireAccounting() {
	if r, ok := c.ReadView.(smtbackend.AccountingRetirer); ok {
		r.RetireAccounting()
	}
}

func newMemoryBackend() *smtbackend.MemoryBackend {
	return smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
}

func ucOf(t testing.TB, root []byte, round uint64) []byte {
	uc := types.UnicityCertificate{
		Version:     1,
		InputRecord: &types.InputRecord{Version: 1, RoundNumber: round, Hash: root, SummaryValue: []byte{}},
		UnicitySeal: &types.UnicitySeal{Version: 1, RootChainRoundNumber: round},
	}
	b, err := types.Cbor.Marshal(&uc)
	require.NoError(t, err)
	return b
}

// commitLeaf commits one more leaf to the backend and returns the new root.
func commitLeaf(t testing.TB, be smtbackend.Backend, n int) []byte {
	ctx := context.Background()
	snap, err := be.CreateSnapshot(ctx)
	require.NoError(t, err)
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0], key[1], key[2] = byte(n*37), byte(n), byte(n>>8)
	res, err := snap.AddLeavesClassified(ctx, []smtbackend.LeafInput{{Key: key, Value: make([]byte, 32)}})
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(uint64(n)), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

// accountingBackend counts the accounting questions asked about the retained views of the backend: the
// charge of each reader (RetainedBytes) and the backend total (RetainedBytesTotal). It embeds the real backend,
// so it is a retainer and an accountant exactly like it.
type accountingBackend struct {
	smtbackend.Backend
	reads, totals *atomic.Int64
}

func (b *accountingBackend) RetainReadView(ctx context.Context) (smtbackend.ReadView, error) {
	rv, err := b.Backend.(smtbackend.ReadViewRetainer).RetainReadView(ctx)
	if err != nil {
		return nil, err
	}
	return &countingView{ReadView: rv, reads: b.reads}, nil
}

func (b *accountingBackend) RetainedBytesTotal() uint64 {
	b.totals.Add(1)
	return b.Backend.(smtbackend.RetentionAccountant).RetainedBytesTotal()
}

func (b *accountingBackend) queries() int64 { return b.reads.Load() + b.totals.Load() }

// plainBackend is a backend which can retain views but cannot total them.
type plainBackend struct {
	smtbackend.Backend
	reads *atomic.Int64
}

func (b *plainBackend) RetainReadView(ctx context.Context) (smtbackend.ReadView, error) {
	rv, err := b.Backend.(smtbackend.ReadViewRetainer).RetainReadView(ctx)
	if err != nil {
		return nil, err
	}
	return &countingView{ReadView: rv, reads: b.reads}, nil
}

// proofRoundManager builds a RoundManager the way production does for the proof views: the backend, the BFT
// client of the shard configuration, and the unchanged RoundManager.initProofViews.
func proofRoundManager(t testing.TB, be smtbackend.Backend, views int) *RoundManager {
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	pdr := &types.PartitionDescriptionRecord{Version: 1, NetworkID: prooftest.NetworkID, PartitionID: prooftest.Partition, Epoch: 1}
	cfg := &config.Config{BFT: config.BFTConfig{ShardConf: pdr}, SMT: config.SMTConfig{RetainedViews: views}}
	client, err := bft.NewBFTClient(context.Background(), &cfg.BFT, nil, nil, nil, log, nil)
	require.NoError(t, err)
	rm := &RoundManager{smtBackend: be, logger: log}
	require.NoError(t, rm.initProofViews(cfg, client))
	require.NotNil(t, rm.ProofViewPublisher(), "the backend retains read views")
	return rm
}

// publishBlock commits one more leaf and publishes the finalized block through the publisher of the RoundManager.
func publishBlock(t testing.TB, rm *RoundManager, be smtbackend.Backend, n int) {
	root := commitLeaf(t, be, n)
	f := prooftest.New(t, 2, uint64(n), root, uint64(80+n))
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	block := models.NewBlock(api.NewBigIntFromUint64(uint64(n)), "unicity", 0, "1.0", "mainnet", api.NewHexBytes(root), api.HexBytes{}, api.NewHexBytes(uc), 1755000000)
	block.Finalized = true
	rm.ProofViewPublisher().PublishFinalized(context.Background(), block)
}

// runCatalogScenario publishes v views and then measures batches of {publish with eviction, acquire, release,
// stats} through the RoundManager's publisher and catalog; it returns the accounting questions per batch and
// checks the maintained total against the slow summation after every operation.
func runCatalogScenario(t testing.TB, rm *RoundManager, be smtbackend.Backend, queries func() int64, v int) float64 {
	ctx := context.Background()
	catalog := rm.ProofViewCatalog()
	for n := 1; n <= v; n++ {
		publishBlock(t, rm, be, n)
	}
	require.Equal(t, v, catalog.Stats().Views)
	const ops = 10
	var counted int64
	for k := 0; k < ops; k++ {
		before := queries()
		publishBlock(t, rm, be, v+1+k)
		l, err := catalog.AcquireLatest(ctx)
		require.NoError(t, err)
		l.Release()
		got := catalog.Stats().RetainedBytes
		counted += queries() - before
		// the slow reference is not part of the measured work
		require.Equal(t, catalog.RetainedBytesBySummation(), got, "the maintained total equals the slow sum")
	}
	return float64(counted) / ops
}

// The production path (RoundManager.initProofViews) builds the catalog with the backend's O(1) retained-byte
// total: through the real publisher and catalog the accounting questions per batch are flat in the number of
// retained views, counting both the per-reader charges and the backend total. A backend which cannot total
// its views gets the linear fallback, which grows with V: the counter would show a regression of the wiring.
func TestInitProofViewsWiresTheAccountantMemoryBackend(t *testing.T) {
	var wired, linear []float64
	for _, v := range []int{8, 16, 32, 64} {
		var reads, totals atomic.Int64
		be := &accountingBackend{Backend: newMemoryBackend(), reads: &reads, totals: &totals}
		rm := proofRoundManager(t, be, v)
		require.True(t, rm.ProofViewCatalog().UsesAccountant(), "initProofViews must install the accountant")
		wired = append(wired, runCatalogScenario(t, rm, be, be.queries, v))

		var preads atomic.Int64
		pb := &plainBackend{Backend: newMemoryBackend(), reads: &preads}
		prm := proofRoundManager(t, pb, v)
		require.False(t, prm.ProofViewCatalog().UsesAccountant(), "a backend which cannot total its views has no accountant")
		linear = append(linear, runCatalogScenario(t, prm, pb, preads.Load, v))
	}
	for _, w := range wired {
		require.LessOrEqual(t, w, 40.0, "accounting questions per batch must not depend on V: %v", wired)
	}
	require.InDelta(t, wired[0], wired[len(wired)-1], 3.0, "flat in V: %v", wired)
	require.Greater(t, linear[len(linear)-1], 4*linear[0], "the linear fallback grows with V: %v", linear)
	require.Greater(t, linear[len(linear)-1], 2*wired[len(wired)-1], "wired %v linear %v", wired, linear)
}

// A backend which cannot total its views gets the (correct) linear fallback instead of failing.
func TestProductionCatalogConstructorWithoutAccountantBackend(t *testing.T) {
	c, err := newProofViewCatalog(proofview.DefaultLimits(), nil)
	require.NoError(t, err)
	require.False(t, c.UsesAccountant())
}

// runCertifiedEmptyThenBlock drives the production publication path (RoundManager.initProofViews, its publisher
// and catalog) through the empty genesis state and the first real block: the certified empty tree serves a
// verifiable absence, a later block supersedes it as latest, and an included key is reported as included.
func runCertifiedEmptyThenBlock(t *testing.T, be smtbackend.Backend) {
	ctx := context.Background()
	rm := proofRoundManager(t, be, 8)
	auth, err := nifixture.NewAuthority()
	require.NoError(t, err)
	tb := auth.TrustBase()
	pub := rm.ProofViewPublisher()
	pub.SetSealVerifier(func(context.Context) types.SealVerifier { return types.TrustBaseSealVerifier(tb) })
	verifier := func() *noninclusion.Verifier {
		v, err := noninclusion.NewVerifier(noninclusion.Context{Partition: nifixture.Partition,
			Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil }})
		require.NoError(t, err)
		return v
	}
	proofOf := func(key []byte) (*api.NonInclusionProofV1, error) {
		l, err := rm.ProofViewCatalog().AcquireLatest(ctx)
		require.NoError(t, err)
		defer l.Release()
		cert, err := l.GetNonInclusionCert(key)
		if err != nil {
			return nil, err
		}
		return &api.NonInclusionProofV1{Certificate: cert, UnicityCertificate: types.RawCBOR(l.UC())}, nil
	}

	// before anything is certified there is nothing to serve
	_, err = rm.ProofViewCatalog().AcquireLatest(ctx)
	require.Error(t, err)

	// the genesis UC (empty input record hash) of a node without committed blocks: certified empty tree
	uc, ucb, err := auth.CertifyRepeat()
	require.NoError(t, err)
	d, _, err := proofmaterial.DigestOfUC(uc)
	require.NoError(t, err)
	require.NoError(t, pub.PublishProofMaterial(ctx, &proofmaterial.Material{Kind: proofmaterial.KindInitial, UC: uc, UCBytes: ucb, UCDigest: d}))
	key := make([]byte, 32)
	key[0] = 0x42
	p, err := proofOf(key)
	require.NoError(t, err)
	require.True(t, p.Certificate.IsEmptyTree())
	res, err := verifier().VerifyFor(key, p)
	require.NoError(t, err)
	require.True(t, res.EmptyTree)

	// the first block: committed, certified, published; latest is the block, the proof is not an empty-tree one
	root := commitLeaf(t, be, 1)
	_, ucb1, err := auth.CertifyRoot(root)
	require.NoError(t, err)
	blk := models.NewBlock(api.NewBigIntFromUint64(1), "unicity", 0, "1.0", "mainnet", api.NewHexBytes(root), api.HexBytes{}, api.NewHexBytes(ucb1), 1)
	blk.Finalized = true
	pub.PublishFinalized(ctx, blk)
	p, err = proofOf(key)
	require.NoError(t, err)
	require.False(t, p.Certificate.IsEmptyTree())
	res, err = verifier().VerifyFor(key, p)
	require.NoError(t, err)
	require.False(t, res.EmptyTree)
	// the committed key (n=1 of commitLeaf) is included at that root
	inc := make([]byte, 32)
	inc[0], inc[1], inc[2] = byte(1*37), byte(1), byte(1>>8)
	_, err = proofOf(inc)
	require.ErrorIs(t, err, api.ErrStateIncluded)

	// a repeated delivery of the initial UC is idempotent (it attaches to the retained snapshot) and never
	// makes the old empty snapshot latest again
	require.NoError(t, pub.PublishProofMaterial(ctx, &proofmaterial.Material{Kind: proofmaterial.KindInitial, UC: uc, UCBytes: ucb, UCDigest: d}))
	require.Equal(t, 2, rm.ProofViewCatalog().Stats().Views)
	p, err = proofOf(key)
	require.NoError(t, err)
	require.False(t, p.Certificate.IsEmptyTree())
}

func TestProductionPathServesTheCertifiedEmptyTreeMemoryBackend(t *testing.T) {
	runCertifiedEmptyThenBlock(t, newMemoryBackend())
}
