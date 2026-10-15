package consolidation

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func ts() fixtureOpts { return fixtureOpts{refTime: 1000, timestamp: 2000} }

func requireOutcome(t *testing.T, err error, want Outcome) {
	t.Helper()
	require.Error(t, err)
	got, ok := OutcomeOf(err)
	require.True(t, ok, "untyped error: %v", err)
	require.Equal(t, want, got, err.Error())
}

func TestConsolidate_HappyPath(t *testing.T) {
	f := newFixture(t, ts())
	s := f.session()
	out, err := s.Consolidate(f.in)
	require.NoError(t, err)
	require.Equal(t, Counters{QuorumChecks: 1, BodyChecks: 2, LeafChecks: 2}, s.Counters())
	require.Equal(t, f.round, out.RootRound)
	require.Len(t, out.Shards, 2)

	masterSeal, err := out.Shards[0].UC.UnicitySeal.MarshalCBOR()
	require.NoError(t, err)
	for i, so := range out.Shards {
		// identical complete master seal in every shard UC
		sb, err := so.UC.UnicitySeal.MarshalCBOR()
		require.NoError(t, err)
		require.Equal(t, masterSeal, sb)
		require.Equal(t, uint64(7), so.Proofs[0].BlockNumber)
		// the legacy full verifier accepts every replacement proof
		vctx := &api.VerifierContext{TrustBase: f.tb, PartitionID: testPartition, ExpectedShardID: so.ShardID, ShardConfHash: f.policy.Topology.ShardConfHashes[so.ShardID.Key()]}
		require.NoError(t, so.Proofs[0].InclusionProof.Verify(f.reqs[i], vctx))
		// original data is preserved
		require.Equal(t, f.refTimes[i], *so.Proofs[0].InclusionProof.ReferenceTime)
		require.Equal(t, f.certs[i], []byte(so.Proofs[0].InclusionProof.CertificateBytes))
	}
	// inputs are untouched
	require.NotEqual(t, f.in.Peers[0].Proofs[0].InclusionProof.UnicityCertificate, out.Shards[1].Proofs[0].InclusionProof.UnicityCertificate)
	require.Equal(t, f.in.Peers[0].Proofs[0].InclusionProof.UnicityCertificate, types.RawCBOR(out.Shards[1].SourceUC))
}

func TestConsolidate_SessionReuseKeepsOneQuorumCheck(t *testing.T) {
	f := newFixture(t, ts())
	s := f.session()
	for range 3 {
		_, err := s.Consolidate(f.in)
		require.NoError(t, err)
	}
	c := s.Counters()
	require.Equal(t, 1, c.QuorumChecks, "complete seal verified once per session")
	require.Equal(t, 2, c.BodyChecks, "distinct shard bodies verified once")
	require.Equal(t, 6, c.LeafChecks, "leaf checks never memoized")
}

func TestConsolidate_ReferenceTimeAfterTarget(t *testing.T) {
	for name, tc := range map[string]struct {
		o    fixtureOpts
		want Outcome
	}{
		"equal is accepted":  {fixtureOpts{refTime: 2000, timestamp: 2000}, ""},
		"after is rejected":  {fixtureOpts{refTime: 2001, timestamp: 2000}, OutcomeReferenceTime},
		"expired is typed":   {fixtureOpts{refTime: 1000, timestamp: 2000, expiresAt: u64p(1000)}, OutcomeExpired},
		"unexpired accepted": {fixtureOpts{refTime: 1000, timestamp: 2000, expiresAt: u64p(1001)}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, tc.o)
			_, err := f.session().Consolidate(f.in)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			requireOutcome(t, err, tc.want)
		})
	}
}

func TestConsolidate_RMin(t *testing.T) {
	f := newFixture(t, ts())
	f.in.MinRootRound = nil
	_, err := f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnavailable)

	f = newFixture(t, ts())
	above := f.round + 1
	f.in.MinRootRound = &above
	_, err = f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeStale)

	f = newFixture(t, ts())
	f.in.PartitionView = nil
	_, err = f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnavailable)
}

func TestConsolidate_UnauthenticatedMaster(t *testing.T) {
	f := newFixture(t, ts())
	// a different authority never accepts the master seal
	other := newFixture(t, ts())
	s, err := NewSession(f.policy, other.resolver())
	require.NoError(t, err)
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnauthenticated)
	require.Equal(t, 1, s.Counters().QuorumChecks)

	// a resolver failure is not authentication either
	s, err = NewSession(f.policy, func(*types.UnicitySeal) (types.RootTrustBase, error) { return nil, errors.New("unknown epoch") })
	require.NoError(t, err)
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnauthenticated)
}

func TestConsolidate_AlteredSealWitnessMisses(t *testing.T) {
	f := newFixture(t, ts())
	s := f.session()
	_, err := s.Consolidate(f.in)
	require.NoError(t, err)

	// same authority, same policy, same seal fields but a different witness
	uc := mustUC(t, f.in.Master.Proofs[0].InclusionProof.UnicityCertificate)
	uc.UnicitySeal.Signatures["test"][0] ^= 0xff
	seal := uc.UnicitySeal
	err = s.VerifySeal(seal)
	require.Error(t, err)
	require.Equal(t, 2, s.Counters().QuorumChecks, "altered witness is a cache miss and runs the native check")
	// and failures are not cached
	require.Error(t, s.VerifySeal(seal))
	require.Equal(t, 3, s.Counters().QuorumChecks)
}

func TestSession_AuthorityAndPolicyKeyTheMemo(t *testing.T) {
	f := newFixture(t, ts())
	sealUC := mustUC(t, f.in.Master.Proofs[0].InclusionProof.UnicityCertificate)

	var tb types.RootTrustBase = f.tb
	s, err := NewSession(f.policy, func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil })
	require.NoError(t, err)
	require.NoError(t, s.VerifySeal(sealUC.UnicitySeal))
	require.NoError(t, s.VerifySeal(sealUC.UnicitySeal))
	require.Equal(t, 1, s.Counters().QuorumChecks)

	// altered authority (another node set) must not reuse the success
	_, v2 := testsig.CreateSignerAndVerifier(t)
	k2, err := v2.MarshalPublicKey()
	require.NoError(t, err)
	tb, err = types.NewTrustBase(types.NetworkMainNet, []*types.NodeInfo{{NodeID: "test", SigKey: k2, Stake: 1}})
	require.NoError(t, err)
	require.Error(t, s.VerifySeal(sealUC.UnicitySeal))
	require.Equal(t, 2, s.Counters().QuorumChecks)

	a1, err := authorityIdentity(f.tb)
	require.NoError(t, err)
	a2, err := authorityIdentity(tb)
	require.NoError(t, err)
	require.NotEqual(t, a1, a2)

	// altered policy has another identity; keys are length delimited
	p2 := f.policy
	p2.ID = []byte("other")
	require.NotEqual(t, f.policy.id(), p2.id())
	require.NotEqual(t, sealKey([]byte("ab"), []byte("c"), nil), sealKey([]byte("a"), []byte("bc"), nil))
}

func TestConsolidate_BindingFailures(t *testing.T) {
	cases := map[string]struct {
		mutate func(f *fixture)
		want   Outcome
	}{
		"peer at another root": {func(f *fixture) {
			f.in.Peers[0].Proofs[0].InclusionProof.UnicityCertificate = f.altRoot(t)
		}, OutcomeBindingMismatch},
		"tampered certification data": {func(f *fixture) {
			f.in.Peers[0].Proofs[0].InclusionProof.CertificationData.Witness = []byte{1}
		}, OutcomeBindingMismatch},
		"tampered inclusion path": {func(f *fixture) {
			f.in.Peers[0].Proofs[0].InclusionProof.CertificateBytes = append([]byte(nil), f.in.Peers[0].Proofs[0].InclusionProof.CertificateBytes...)
			f.in.Peers[0].Proofs[0].InclusionProof.CertificateBytes[0] = 0x80
		}, OutcomeBindingMismatch},
		"changed reference time breaks the leaf": {func(f *fixture) {
			rt := uint64(999)
			f.in.Peers[0].Proofs[0].InclusionProof.ReferenceTime = &rt
		}, OutcomeBindingMismatch},
		"peer shard not in view": {func(f *fixture) {
			f.in.Peers[0].ShardID = types.ShardID{}
		}, OutcomeBindingMismatch},
		"master proof of other shard": {func(f *fixture) {
			a, b := f.in.Master.ShardID, f.in.Peers[0].ShardID
			f.in.Master.ShardID, f.in.Peers[0].ShardID = b, a
		}, OutcomeBindingMismatch},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, ts())
			tc.mutate(f)
			_, err := f.session().Consolidate(f.in)
			requireOutcome(t, err, tc.want)
		})
	}
}

func TestConsolidate_InvalidInputs(t *testing.T) {
	cases := map[string]func(f *fixture){
		"count mismatch":  func(f *fixture) { f.in.Peers[0].Proofs = nil },
		"duplicate state": func(f *fixture) { f.in.Peers[0].Requests[0] = f.in.Master.Requests[0] },
		"duplicate shard": func(f *fixture) { f.in.Peers[0].ShardID = f.in.Master.ShardID },
		"nil proof":       func(f *fixture) { f.in.Peers[0].Proofs[0].InclusionProof = nil },
		"garbage view":    func(f *fixture) { f.in.PartitionView = []byte{0x83, 1, 2} },
		"mixed master UCs": func(f *fixture) {
			f.in.Master.Proofs = append(f.in.Master.Proofs, f.in.Peers[0].Proofs[0])
			f.in.Master.Requests = append(f.in.Master.Requests, f.reqs[1])
		},
		"view of other partition": func(f *fixture) {
			v := f.view.Clone()
			v.PartitionID++
			f.in.PartitionView, _ = v.MarshalCBOR()
		},
		"view with other leaf": func(f *fixture) {
			v := f.view.Clone()
			v.Leaves[1].InputRecord.RoundNumber++
			f.in.PartitionView, _ = v.MarshalCBOR()
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, ts())
			mutate(f)
			_, err := f.session().Consolidate(f.in)
			requireOutcome(t, err, OutcomeInvalidInput)
		})
	}
}

func TestConsolidate_NoncanonicalMasterUCRejected(t *testing.T) {
	f := newFixture(t, ts())
	raw := f.in.Master.Proofs[0].InclusionProof.UnicityCertificate
	// a trailing garbage byte can never be normalised into a cache hit
	for _, p := range f.in.Master.Proofs {
		p.InclusionProof.UnicityCertificate = append(bytes.Clone(raw), 0x00)
	}
	_, err := f.session().Consolidate(f.in)
	require.Error(t, err)
	o, _ := OutcomeOf(err)
	require.Contains(t, []Outcome{OutcomeInvalidInput, OutcomeUnsupported}, o)
}

func TestNewSession_Validation(t *testing.T) {
	f := newFixture(t, ts())
	_, err := NewSession(f.policy, nil)
	requireOutcome(t, err, OutcomeInvalidInput)
	p := f.policy
	p.PartitionID = 0
	_, err = NewSession(p, f.resolver())
	requireOutcome(t, err, OutcomeInvalidInput)
	p = f.policy
	p.Topology.ShardConfHashes = map[string][]byte{}
	_, err = NewSession(p, f.resolver())
	requireOutcome(t, err, OutcomeInvalidInput)
}

func mustUC(t *testing.T, raw []byte) *types.UnicityCertificate {
	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(raw, &uc))
	return &uc
}

// altRoot is a canonical UC of the peer shard with a different state root.
func (f *fixture) altRoot(t *testing.T) []byte {
	uc := mustUC(t, f.in.Peers[0].Proofs[0].InclusionProof.UnicityCertificate)
	ir := *uc.InputRecord
	ir.Hash = h32("another root")
	uc.InputRecord = &ir
	b, err := uc.MarshalCBOR()
	require.NoError(t, err)
	return b
}

func u64p(v uint64) *uint64 { return &v }

func TestCheckLeaf_StateOfAnotherShard(t *testing.T) {
	f := newFixture(t, ts())
	out, err := f.session().Consolidate(f.in)
	require.NoError(t, err)
	// shard 1's certificate does not cover the state of shard 0
	err = f.session().checkLeaf(0, f.reqs[0], out.Shards[0].Proofs[0].InclusionProof, out.Shards[1].UC)
	requireOutcome(t, err, OutcomeBindingMismatch)
	require.Contains(t, err.Error(), "does not belong to shard")
}
