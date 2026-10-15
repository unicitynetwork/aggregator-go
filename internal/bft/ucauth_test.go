package bft

import (
	"context"
	"crypto"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
)

func resign(t *testing.T, f *prooftest.Fixture, mod func(s *types.UnicitySeal)) *types.UnicityCertificate {
	uc := *f.UC
	seal := *f.UC.UnicitySeal
	seal.Signatures = nil
	mod(&seal)
	require.NoError(t, seal.Sign("node", f.Signer))
	uc.UnicitySeal = &seal
	return &uc
}

func TestUCAuthenticator_VerifyUC(t *testing.T) {
	root := prooftest.Hash("root")
	f := prooftest.New(t, 2, 7, root, 80)
	a := newUCAuthenticator(prooftest.NetworkID, stubTrustBases{f.TrustBase})
	verify := func(uc *types.UnicityCertificate) error {
		return a.verifyUC(context.Background(), uc, prooftest.Partition, f.Shard)
	}
	require.NoError(t, verify(f.UC))
	require.NoError(t, verify(f.UC), "memoized")

	t.Run("signed by the authority but of another network", func(t *testing.T) {
		// the same keys signing for another network must not count as this network's certificate
		err := verify(resign(t, f, func(s *types.UnicitySeal) { s.NetworkID++ }))
		require.ErrorContains(t, err, "network")
		// and the native verification alone would have accepted it
		uc := resign(t, f, func(s *types.UnicitySeal) { s.NetworkID++ })
		require.NoError(t, uc.UnicitySeal.Verify(f.TrustBase), "signatures are valid: only the context check refuses it")
	})
	t.Run("wrong epoch named by the seal", func(t *testing.T) {
		err := verify(resign(t, f, func(s *types.UnicitySeal) { s.Epoch = 2 }))
		require.Error(t, err)
	})
	t.Run("round before the epoch start", func(t *testing.T) {
		tb := *f.TrustBase
		tb.EpochStart = 100 // the epoch only starts after the round of the seal
		a2 := newUCAuthenticator(prooftest.NetworkID, stubTrustBases{&tb})
		err := a2.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard)
		require.ErrorContains(t, err, "before the start")
	})
	t.Run("another authority", func(t *testing.T) {
		signer, err := abcrypto.NewInMemorySecp256K1SignerFromKey(prooftest.Hash("another authority"))
		require.NoError(t, err)
		v, err := signer.Verifier()
		require.NoError(t, err)
		pub, err := v.MarshalPublicKey()
		require.NoError(t, err)
		tb, err := types.NewTrustBase(prooftest.NetworkID, []*types.NodeInfo{{NodeID: "node", SigKey: pub, Stake: 1}}, types.WithEpoch(1), types.WithEpochStart(1))
		require.NoError(t, err)
		a2 := newUCAuthenticator(prooftest.NetworkID, stubTrustBases{tb})
		require.Error(t, a2.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard))
	})
	t.Run("tampered certificate and seal", func(t *testing.T) {
		uc := *f.UC
		seal := *f.UC.UnicitySeal
		seal.Hash = prooftest.Hash("elsewhere")
		uc.UnicitySeal = &seal
		require.Error(t, verify(&uc))
		uc = *f.UC
		seal = *f.UC.UnicitySeal
		seal.RootChainRoundNumber++ // signature no longer matches
		uc.UnicitySeal = &seal
		require.Error(t, verify(&uc))
	})
	t.Run("null path elements and missing parts never panic", func(t *testing.T) {
		uc := *f.UC
		utc := *f.UC.UnicityTreeCertificate
		utc.HashSteps = []*types.PathItem{nil}
		uc.UnicityTreeCertificate = &utc
		require.NotPanics(t, func() { require.Error(t, verify(&uc)) })
		require.ErrorContains(t, validateUCShape(&uc), "null")
		uc = *f.UC
		uc.UnicityTreeCertificate = nil
		require.NotPanics(t, func() { require.Error(t, verify(&uc)) })
		uc = *f.UC
		uc.UnicitySeal = nil
		require.NotPanics(t, func() { require.Error(t, verify(&uc)) })
		uc = *f.UC
		uc.InputRecord = nil
		require.NotPanics(t, func() { require.Error(t, verify(&uc)) })
		require.Error(t, validateUCShape(nil))
	})
}

// A store (or REST fallback) answering for another epoch must not authenticate anything.
func TestUCAuthenticator_ResolvedTrustBaseMustBeOfTheRequestedEpoch(t *testing.T) {
	f := prooftest.New(t, 2, 7, prooftest.Hash("root"), 80)
	// the seal names epoch 2, the (hostile) store answers with its own epoch 1 base
	uc := resign(t, f, func(s *types.UnicitySeal) { s.Epoch = 2 })
	a := newUCAuthenticator(prooftest.NetworkID, wrongEpochStore{f.TrustBase})
	_, err := a.trustBase(context.Background(), 2)
	require.ErrorContains(t, err, "requested epoch 2")
	require.Error(t, a.verifyUC(context.Background(), uc, prooftest.Partition, f.Shard))
	// even though the signatures are valid under that base
	require.NoError(t, types.TrustBaseSealVerifier(f.TrustBase).VerifySeal(f.UC.UnicitySeal))
	require.Error(t, types.TrustBaseSealVerifier(f.TrustBase).VerifySeal(uc.UnicitySeal))

	// another network's base is refused as well
	other := *f.TrustBase
	other.NetworkID = prooftest.NetworkID + 1
	a = newUCAuthenticator(prooftest.NetworkID, stubTrustBases{&other})
	_, err = a.trustBase(context.Background(), 1)
	require.ErrorContains(t, err, "network")

	// absent / failing store
	a = newUCAuthenticator(prooftest.NetworkID, stubTrustBases{})
	_, err = a.trustBase(context.Background(), 1)
	require.Error(t, err)
	a = newUCAuthenticator(prooftest.NetworkID, wrongEpochStore{nil})
	_, err = a.trustBase(context.Background(), 1)
	require.Error(t, err)
	_ = errors.New
	_ = crypto.SHA256
}

func TestUCAuthenticator_ClientIntake(t *testing.T) {
	root := prooftest.Hash("root")
	f := prooftest.New(t, 2, 7, root, 80)
	c, _ := newProofClient(t, &stubRoundManager{}, f)
	c.shardID = f.Shard
	cr := response(f, nil)
	cr.Technical.Round = 8
	cr.Technical.Epoch = 1
	// nil partition path elements are an error, not a crash, at the network boundary
	bad := *cr
	utc := *f.UC.UnicityTreeCertificate
	utc.HashSteps = []*types.PathItem{nil}
	bad.UC.UnicityTreeCertificate = &utc
	require.NotPanics(t, func() {
		err := c.handleCertificationResponse(t.Context(), &bad)
		require.ErrorContains(t, err, "malformed")
	})
}
