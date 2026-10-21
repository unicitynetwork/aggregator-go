package proofpub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type emptyEnv struct {
	auth *nifixture.Authority
	b    *smtbackend.MemoryBackend
	cat  *proofview.Catalog
	pub  *proofpub.Publisher
}

func newEmptyEnv(t *testing.T) *emptyEnv {
	auth, err := nifixture.NewAuthority()
	require.NoError(t, err)
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	b := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	pub := proofpub.New(cat, b, nil, log)
	require.NotNil(t, pub)
	pub.SetSealVerifier(func(context.Context) types.SealVerifier { return types.TrustBaseSealVerifier(auth.TrustBase()) })
	return &emptyEnv{auth: auth, b: b, cat: cat, pub: pub}
}

func (e *emptyEnv) material(t *testing.T, kind proofmaterial.Kind, uc *types.UnicityCertificate) *proofmaterial.Material {
	d, b, err := proofmaterial.DigestOfUC(uc)
	require.NoError(t, err)
	return &proofmaterial.Material{Kind: kind, UC: uc, UCBytes: b, UCDigest: d}
}

func TestCertifiedEmptyTreeIsPublishedAndRecertified(t *testing.T) {
	ctx := context.Background()
	e := newEmptyEnv(t)
	uc1, _, err := e.auth.CertifyRepeat() // a certified UC of the initial empty state
	require.NoError(t, err)
	require.Empty(t, uc1.InputRecord.Hash)

	_, err = e.cat.AcquireLatest(ctx)
	require.Error(t, err, "nothing is served before a certified empty snapshot exists")

	require.NoError(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindInitial, uc1)))
	l, err := e.cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.True(t, l.EmptyTree())
	require.Empty(t, l.Root())
	require.EqualValues(t, proofpub.EmptyTreeBlockNumber, l.Block().Number.Uint64())
	cert, err := l.GetNonInclusionCert(make([]byte, 32))
	require.NoError(t, err)
	require.True(t, cert.IsEmptyTree())
	l.Release()

	// a repeat/timeout UC of the same empty state attaches (newer certification of the snapshot)
	uc2, ucb2, err := e.auth.CertifyRepeat()
	require.NoError(t, err)
	require.NoError(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindRepeat, uc2)))
	l, err = e.cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, ucb2, l.UC())
	l.Release()
	// a second initial delivery of the same UC is harmless
	require.NoError(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindInitial, uc2)))
	require.Equal(t, 1, e.cat.Stats().Views)

	// no exact-root selector reaches the empty snapshot: neither empty nor zero roots
	_, err = e.cat.AcquireRoot(ctx, nil)
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	_, err = e.cat.AcquireRoot(ctx, make([]byte, 32))
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
}

func TestEmptyRootNeverAuthorizesAnythingElse(t *testing.T) {
	ctx := context.Background()
	e := newEmptyEnv(t)
	uc, _, err := e.auth.CertifyRepeat()
	require.NoError(t, err)

	// a finalized block never carries an empty root
	m := e.material(t, proofmaterial.KindFinalized, uc)
	m.HasBlock, m.BlockNumber = true, 1
	require.ErrorIs(t, e.pub.PublishProofMaterial(ctx, m), proofpub.ErrEmptyTreeNotPublishable)
	require.ErrorIs(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindSidecarFill, uc)), proofpub.ErrEmptyTreeNotPublishable)

	// a seal which does not authenticate is refused
	bad := newEmptyEnv(t)
	bad.pub.SetSealVerifier(func(context.Context) types.SealVerifier { return rejectingVerifier{} })
	require.ErrorIs(t, bad.pub.PublishProofMaterial(ctx, bad.material(t, proofmaterial.KindInitial, uc)), proofpub.ErrEmptyTreeNotPublishable)
	noVerifier := newEmptyEnv(t)
	noVerifier.pub.SetSealVerifier(nil)
	require.ErrorIs(t, noVerifier.pub.PublishProofMaterial(ctx, noVerifier.material(t, proofmaterial.KindInitial, uc)), proofpub.ErrEmptyTreeNotPublishable)

	// an empty byte string is not the empty tree (only native null is)
	bs := *uc
	ir := *uc.InputRecord
	ir.Hash = []byte{}
	bs.InputRecord = &ir
	require.ErrorIs(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindInitial, &bs)), proofpub.ErrEmptyTreeNotPublishable)

	// a node with committed history is not at the empty state
	root := commit(t, e.b, 1, 0, 5)
	require.NotEmpty(t, root)
	require.ErrorIs(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindInitial, uc)), proofpub.ErrEmptyTreeNotPublishable)
	require.ErrorIs(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindRepeat, uc)), proofpub.ErrEmptyTreeNotPublishable)
	require.Equal(t, 0, e.cat.Stats().Views)

	// the initial UC of a nonempty state is not the business of this path
	ucN, _, err := e.auth.CertifyRoot(root)
	require.NoError(t, err)
	require.NoError(t, e.pub.PublishProofMaterial(ctx, e.material(t, proofmaterial.KindInitial, ucN)))
	require.Equal(t, 0, e.cat.Stats().Views)
}

// A UC with an empty hash and an explicitly empty backend that has a committed block number (state
// reverted to empty is impossible), and an empty backend certified with the 32 byte zero root, are
// distinct from the certified empty tree.
func TestZeroRootIsAnOrdinaryRootNotTheEmptyTree(t *testing.T) {
	ctx := context.Background()
	e := newEmptyEnv(t)
	zero := make([]byte, 32)
	uc, ucb, err := e.auth.CertifyRoot(zero)
	require.NoError(t, err)
	_ = uc
	blk := models.NewBlock(api.NewBigIntFromUint64(1), "unicity", 0, "1.0", "mainnet", api.NewHexBytes(zero), api.NewHexBytes(zero), api.NewHexBytes(ucb), 1)
	blk.Finalized = true
	// the backend is empty and its root hash bytes are the zeros: the block publishes as an ordinary root...
	snap, err := e.b.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(1), RootHash: zero}))
	e.pub.PublishFinalized(ctx, blk)
	l, err := e.cat.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	// ...as a view of root 32 zero bytes, which is NOT the certified empty tree
	require.False(t, l.EmptyTree())
	require.Equal(t, zero, l.Root())
	_, err = e.cat.AcquireRoot(ctx, zero)
	require.NoError(t, err)
}

type rejectingVerifier struct{}

func (rejectingVerifier) VerifySeal(*types.UnicitySeal) error { return errors.New("not authenticated") }
