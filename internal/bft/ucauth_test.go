package bft

import (
	"context"
	"crypto"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
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

// historyStore is a trust base store with history which can grow after authentications.
type historyStore struct {
	mu        sync.Mutex
	bases     map[uint64]*types.RootTrustBaseV1
	getCalls  []uint64 // epochs asked through GetByEpoch
	admitted  bool     // implements the local-only lookup when true
	storeErr  error
	admitCall []uint64
}

func (s *historyStore) put(tb *types.RootTrustBaseV1) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bases[tb.GetEpoch()] = tb
}

func (s *historyStore) GetByEpoch(_ context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls = append(s.getCalls, epoch)
	if s.storeErr != nil {
		return nil, s.storeErr
	}
	if tb, ok := s.bases[epoch]; ok {
		return tb, nil
	}
	return nil, interfaces.ErrTrustBaseNotFound
}

// admittedStore additionally offers the local-only lookup.
type admittedStore struct{ *historyStore }

func (s admittedStore) GetAdmitted(_ context.Context, epoch uint64) (*types.RootTrustBaseV1, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admitCall = append(s.admitCall, epoch)
	tb, ok := s.bases[epoch]
	return tb, ok, nil
}

func nextEpochBase(t *testing.T, f *prooftest.Fixture, epoch, start uint64) *types.RootTrustBaseV1 {
	// the rotated authority: the old keys are retired but still able to sign
	signer, err := abcrypto.NewInMemorySecp256K1SignerFromKey(prooftest.Hash("rotated authority"))
	require.NoError(t, err)
	v, err := signer.Verifier()
	require.NoError(t, err)
	pub, err := v.MarshalPublicKey()
	require.NoError(t, err)
	tb, err := types.NewTrustBase(prooftest.NetworkID, []*types.NodeInfo{{NodeID: "node2", SigKey: pub, Stake: 1}}, types.WithEpoch(epoch), types.WithEpochStart(start))
	require.NoError(t, err)
	return tb
}

// Retired authority cannot certify at or after the start of the next epoch, when that
// history is admitted locally; also for seals which were authenticated before it was learned.
func TestUCAuthenticator_KnownEpochEnd(t *testing.T) {
	root := prooftest.Hash("root")
	for _, withAdmitted := range []bool{false, true} {
		name := map[bool]string{false: "plain store", true: "local-only lookup"}[withAdmitted]
		t.Run(name, func(t *testing.T) {
			at := func(round uint64) *prooftest.Fixture { return prooftest.New(t, 2, 7, root, round) }
			f79, f80, f81, f500 := at(79), at(80), at(81), at(500)
			hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f80.TrustBase}}
			var store TrustBaseStore = hs
			if withAdmitted {
				store = admittedStore{hs}
			}
			a := newUCAuthenticator(prooftest.NetworkID, store)
			verify := func(f *prooftest.Fixture) error {
				return a.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard)
			}

			// no later epoch known: nothing bounds the interval (design: unknown future is allowed)
			for _, f := range []*prooftest.Fixture{f79, f80, f81, f500} {
				require.NoError(t, verify(f))
			}
			// history learned AFTER the seals were authenticated and memoized
			hs.put(nextEpochBase(t, f80, 2, 80))
			require.NoError(t, verify(f79), "start-1 is still the old epoch")
			for _, f := range []*prooftest.Fixture{f80, f81, f500} {
				err := verify(f)
				require.ErrorContains(t, err, "retired", "cached seal must not survive the boundary")
			}
			// a cold authenticator agrees
			cold := newUCAuthenticator(prooftest.NetworkID, store)
			require.NoError(t, cold.verifyUC(context.Background(), f79.UC, prooftest.Partition, f79.Shard))
			for _, f := range []*prooftest.Fixture{f80, f81, f500} {
				require.Error(t, cold.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard))
			}
			if withAdmitted {
				require.NotContains(t, hs.getCalls, uint64(2), "the next epoch must be looked up locally, never through the fetching resolver")
				require.Contains(t, hs.admitCall, uint64(2))
			}
			// the seal's view is bounded the same way
			require.Error(t, a.sealVerifier(context.Background()).VerifySeal(f81.UC.UnicitySeal))
			require.NoError(t, a.sealVerifier(context.Background()).VerifySeal(f79.UC.UnicitySeal))
		})
	}
}

func TestUCAuthenticator_InconsistentOrUnavailableHistoryFailsClosed(t *testing.T) {
	f := prooftest.New(t, 2, 7, prooftest.Hash("root"), 80)
	// a history entry which is not the next epoch of the same network
	hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f.TrustBase}}
	wrongEpoch := nextEpochBase(t, f, 5, 80)
	hs.bases[2] = wrongEpoch
	a := newUCAuthenticator(prooftest.NetworkID, hs)
	require.Error(t, a.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard))
	otherNet := nextEpochBase(t, f, 2, 80)
	otherNet.NetworkID++
	hs.bases[2] = otherNet
	require.Error(t, a.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard))
	// a storage error is not "unknown"
	hs = &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f.TrustBase}, storeErr: errors.New("database down")}
	a = newUCAuthenticator(prooftest.NetworkID, hs)
	require.Error(t, a.verifyUC(context.Background(), f.UC, prooftest.Partition, f.Shard))
}

// Adversarial deliveries through the real response entry point never change state.
func TestUCAuthenticator_ResponseEntryPointRejectsBeforeAnyStateChange(t *testing.T) {
	root := prooftest.Hash("root")
	f := prooftest.New(t, 2, 7, root, 80)
	hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f.TrustBase}}
	mk := func() (*BFTClientImpl, *stubRoundManager) {
		rm := &stubRoundManager{}
		c, _ := newProofClient(t, rm, f)
		c.authn = newUCAuthenticator(prooftest.NetworkID, hs)
		c.shardID = f.Shard
		proposeBlock(c, 7, root)
		return c, rm
	}
	// control: the genuine response is processed past the intake boundary
	c, rm := mk()
	require.NoError(t, c.handleCertificationResponse(t.Context(), response(f, f.Raw)))
	require.Len(t, rm.finalizedBlocks, 1)

	retired := prooftest.New(t, 2, 7, root, 90)
	hs.put(nextEpochBase(t, f, 2, 85))
	nilPath := response(f, f.Raw)
	utc := *f.UC.UnicityTreeCertificate
	utc.HashSteps = []*types.PathItem{nil}
	nilPath.UC.UnicityTreeCertificate = &utc
	stripped := response(f, f.Raw)
	seal := *f.UC.UnicitySeal
	seal.Signatures = nil
	stripped.UC.UnicitySeal = &seal
	early := prooftest.New(t, 2, 7, root, 80)
	tb := *early.TrustBase
	tb.EpochStart = 1000
	earlyStore := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: &tb}}

	cases := map[string]struct {
		cr    func() *certification.CertificationResponse
		store *historyStore
	}{
		"cross-network signatures of reused keys": {func() *certification.CertificationResponse {
			r := response(f, f.Raw)
			r.UC = *resign(t, f, func(s *types.UnicitySeal) { s.NetworkID++ })
			return r
		}, hs},
		"wrong epoch": {func() *certification.CertificationResponse {
			r := response(f, f.Raw)
			r.UC = *resign(t, f, func(s *types.UnicitySeal) { s.Epoch = 2 })
			return r
		}, hs},
		"round before the epoch start": {func() *certification.CertificationResponse { return response(f, f.Raw) }, earlyStore},
		"round after the retirement of the epoch": {func() *certification.CertificationResponse {
			r := response(retired, retired.Raw)
			r.Technical = retired.TR
			return r
		}, hs},
		"null partition path element": {func() *certification.CertificationResponse { return nilPath }, hs},
		"stripped signatures":         {func() *certification.CertificationResponse { return stripped }, hs},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, rm := mk()
			c.authn = newUCAuthenticator(prooftest.NetworkID, tc.store)
			before := c.luc.Load()
			require.NotPanics(t, func() {
				require.Error(t, c.handleCertificationResponse(t.Context(), tc.cr()))
			})
			require.Equal(t, before, c.luc.Load(), "latest UC replaced")
			require.Empty(t, rm.finalizedBlocks, "block finalized")
			require.Zero(t, c.proofStaging.Len(), "sidecar staged")
		})
	}
}
