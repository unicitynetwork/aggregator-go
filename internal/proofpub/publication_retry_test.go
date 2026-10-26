package proofpub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

type retryRetainer struct {
	*smtbackend.MemoryBackend
	fail  bool
	calls int
}

func (b *retryRetainer) RetainReadView(ctx context.Context) (smtbackend.ReadView, error) {
	b.calls++
	if b.fail {
		return nil, errors.New("temporary retention failure")
	}
	return b.MemoryBackend.RetainReadView(ctx)
}

func Test_RetryLatestWithEvictedOriginalCertification(t *testing.T) {
	f := prooftest.New(t, 2, 1, prooftest.Hash("retry"), 80)
	e := newEnv(t, f)
	be := &retryRetainer{MemoryBackend: e.b}
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	pub := proofpub.New(e.cat, be, nil, log)
	verifications := 0
	pub.SetSealVerifier(func(ctx context.Context) types.SealVerifier {
		verifications++
		return f.Verifier()(ctx)
	})
	root := commit(t, e.b, 1, 0, 20)
	block, _ := finalizedBlock(t, 1, root, 80, true)
	pub.PublishFinalized(t.Context(), block)
	var newest []byte
	for round := uint64(81); round <= 86; round++ {
		repeat := prooftest.New(t, 2, 1, root, round)
		require.NoError(t, pub.PublishProofMaterial(t.Context(),
			material(t, proofmaterial.KindRepeat, repeat, repeat.Raw, 0, false)))
		newest, err = types.Cbor.Marshal(repeat.UC)
		require.NoError(t, err)
	}
	before := verifications
	be.fail = true
	for i := 0; i < 5; i++ {
		require.False(t, pub.RetryLatest(t.Context(), block))
	}
	require.Equal(t, 1, be.calls, "a superseded original must not trigger retention")
	require.Equal(t, before, verifications, "healthy retries do not re-authenticate a partition view")
	require.Equal(t, newest, latest(t, e.cat).UC())

	pub.InvalidateLatest(block.Index)
	require.False(t, pub.RetryLatest(t.Context(), block))
	be.fail = false
	require.True(t, pub.RetryLatest(t.Context(), block))
	require.Equal(t, 3, be.calls)
	require.Equal(t, newest, latest(t, e.cat).UC())
}

func Test_RetryLatestPreservesNewerFailureBeforeCatalog(t *testing.T) {
	f := prooftest.New(t, 2, 1, prooftest.Hash("retry"), 80)
	e := newEnv(t, f)
	be := &retryRetainer{MemoryBackend: e.b}
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	pub := proofpub.New(e.cat, be, nil, log)
	root := commit(t, e.b, 1, 0, 20)
	original, _ := finalizedBlock(t, 1, root, 80, false)
	pub.PublishFinalized(t.Context(), original)
	newer, _ := finalizedBlock(t, 1, root, 90, false)
	be.fail = true
	pub.PublishFinalized(t.Context(), newer)
	be.fail = false
	require.False(t, pub.RetryLatest(t.Context(), original))
	_, err = e.cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	require.True(t, pub.RetryLatest(t.Context(), newer))
	require.Equal(t, []byte(newer.UnicityCertificate), latest(t, e.cat).UC())
}
