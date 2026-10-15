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
	out, err := f.consolidate(s)
	require.NoError(t, err)
	require.Equal(t, Counters{QuorumChecks: 1, BodyChecks: 2, LeafChecks: 2, EvidenceQuorumChecks: 2}, s.Counters())
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
		_, err := f.consolidate(s)
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
			_, err := f.consolidate(f.session())
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			requireOutcome(t, err, tc.want)
		})
	}
}

func TestConsolidate_RMin(t *testing.T) {
	// unknown bound is unavailable
	f := newFixture(t, ts())
	_, err := f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnavailable)

	// a master seal older than the authenticated evidence is stale
	f = newFixture(t, ts())
	s := f.session()
	f.in.Bound, err = s.AuthenticateEvidence([]Evidence{f.evidenceAt(0, f.round+1), f.evidenceAt(1, f.round-5)})
	require.NoError(t, err)
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeStale)

	// the bound is the maximum evidence round, and equal rounds pass
	f = newFixture(t, ts())
	s = f.session()
	f.in.Bound, err = s.AuthenticateEvidence([]Evidence{f.evidenceAt(0, f.round-5), f.evidenceAt(1, f.round)})
	require.NoError(t, err)
	require.Equal(t, f.round, f.in.Bound.Round())
	_, err = s.Consolidate(f.in)
	require.NoError(t, err)

	f = newFixture(t, ts())
	f.in.PartitionView = nil
	_, err = f.consolidate(f.session())
	requireOutcome(t, err, OutcomeUnavailable)
}

func TestConsolidate_BoundCannotBeForgedOrLowered(t *testing.T) {
	f := newFixture(t, ts())
	// a zero-value (guessed) bound carries no lineage and is refused
	f.in.Bound = &RootRoundBound{}
	_, err := f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeIncompatibleLineage)

	// a genuine bound of another policy/history is refused
	other := newFixture(t, ts())
	other.policy.ID = []byte("another history")
	os, err := NewSession(other.policy, other.resolver())
	require.NoError(t, err)
	f.in.Bound, err = os.AuthenticateEvidence(other.defaultEvidence())
	require.NoError(t, err)
	_, err = f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeIncompatibleLineage)

	// the bound cannot be taken from unauthenticated evidence
	f = newFixture(t, ts())
	s, err := NewSession(f.policy, newFixture(t, ts()).resolver()) // other authority
	require.NoError(t, err)
	_, err = s.AuthenticateEvidence(f.defaultEvidence())
	requireOutcome(t, err, OutcomeUnauthenticated)

	// forged evidence: tampered proof, no evidence
	f = newFixture(t, ts())
	ev := f.defaultEvidence()
	ev[1].Proof.CertificationData.Witness = []byte{1}
	_, err = f.session().AuthenticateEvidence(ev)
	requireOutcome(t, err, OutcomeBindingMismatch)
	_, err = f.session().AuthenticateEvidence(nil)
	requireOutcome(t, err, OutcomeUnavailable)

	// evidence with a rewritten seal round no longer reaches its seal signature
	ev = f.defaultEvidence()
	uc := mustUC(t, ev[0].Proof.UnicityCertificate)
	uc.UnicitySeal.RootChainRoundNumber++
	ev[0].Proof.UnicityCertificate, err = uc.MarshalCBOR()
	require.NoError(t, err)
	_, err = f.session().AuthenticateEvidence(ev)
	requireOutcome(t, err, OutcomeUnauthenticated)
}

func TestAuthenticateEvidence_CrossNetwork(t *testing.T) {
	f := newFixture(t, ts())
	e0 := f.evidenceAt(0, f.round-1)
	e1 := f.evidenceAt(1, f.round-1)
	uc := mustUC(t, e1.Proof.UnicityCertificate)
	uc.UnicitySeal.NetworkID++
	require.NoError(t, uc.UnicitySeal.Sign("test", f.signer))
	var err error
	e1.Proof.UnicityCertificate, err = uc.MarshalCBOR()
	require.NoError(t, err)
	_, err = f.session().AuthenticateEvidence([]Evidence{e0, e1})
	requireOutcome(t, err, OutcomeIncompatibleLineage)
}

func TestConsolidate_InputsAndOutputsAreIndependent(t *testing.T) {
	exp := uint64(5000)
	f := newFixture(t, fixtureOpts{refTime: 1000, timestamp: 2000, expiresAt: &exp})
	out, err := f.consolidate(f.session())
	require.NoError(t, err)

	before, err := types.Cbor.Marshal(f.in.Peers[0].Proofs[0].InclusionProof.CertificationData)
	require.NoError(t, err)
	for _, so := range out.Shards {
		cd := so.Proofs[0].InclusionProof.CertificationData
		cd.Witness[0] ^= 0xff
		cd.TransactionHash[0] ^= 0xff
		cd.SourceStateHash[0] ^= 0xff
		cd.OwnerPredicate.Params[0] ^= 0xff
		cd.OwnerPredicate.Code[0] ^= 0xff
		*cd.ExpiresAt = 1
		*so.Proofs[0].InclusionProof.ReferenceTime = 1
		so.Proofs[0].InclusionProof.CertificateBytes = append(so.Proofs[0].InclusionProof.CertificateBytes, 1)
	}
	after, err := types.Cbor.Marshal(f.in.Peers[0].Proofs[0].InclusionProof.CertificationData)
	require.NoError(t, err)
	require.Equal(t, before, after, "mutating the output must not alter the original data")
	require.Equal(t, uint64(5000), *f.reqs[1].CertificationData.ExpiresAt)

	// and vice versa: reusing input buffers does not change an output bundle
	f2 := newFixture(t, fixtureOpts{refTime: 1000, timestamp: 2000, expiresAt: &exp})
	out2, err := f2.consolidate(f2.session())
	require.NoError(t, err)
	want, err := types.Cbor.Marshal(out2.Shards[1].Proofs[0].InclusionProof.CertificationData)
	require.NoError(t, err)
	cd := f2.in.Peers[0].Proofs[0].InclusionProof.CertificationData
	cd.Witness[0] ^= 0xff
	cd.OwnerPredicate.Params[0] ^= 0xff
	*cd.ExpiresAt = 1
	got, err := types.Cbor.Marshal(out2.Shards[1].Proofs[0].InclusionProof.CertificationData)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestConsolidate_UnauthenticatedMaster(t *testing.T) {
	f := newFixture(t, ts())
	// the authority accepts the evidence seals but not the master seal
	other := newFixture(t, ts())
	var tb types.RootTrustBase = f.tb
	s, err := NewSession(f.policy, func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil })
	require.NoError(t, err)
	f.in.Bound, err = s.AuthenticateEvidence(f.defaultEvidence())
	require.NoError(t, err)
	tb = other.tb
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnauthenticated)
	require.Equal(t, 1, s.Counters().QuorumChecks)

	// a resolver failure is not authentication either
	f = newFixture(t, ts())
	fail := false
	s, err = NewSession(f.policy, func(*types.UnicitySeal) (types.RootTrustBase, error) {
		if fail {
			return nil, errors.New("unknown epoch")
		}
		return f.tb, nil
	})
	require.NoError(t, err)
	f.in.Bound, err = s.AuthenticateEvidence(f.defaultEvidence())
	require.NoError(t, err)
	fail = true
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeUnauthenticated)
}

func TestConsolidate_BoundIsBoundToSessionAndRequiredInputs(t *testing.T) {
	// (a) a bound issued under another signing authority (same policy and network) is refused
	f := newFixture(t, ts())
	foreign := newFixture(t, ts())
	foreign.policy = f.policy
	fs, err := NewSession(foreign.policy, foreign.resolver())
	require.NoError(t, err)
	// the foreign authority authenticates *its own* evidence, which is not ours; even with
	// our evidence re-signed by it, the session identity differs
	f.in.Bound, err = fs.AuthenticateEvidence(foreign.defaultEvidence())
	require.NoError(t, err)
	_, err = f.session().Consolidate(f.in)
	requireOutcome(t, err, OutcomeIncompatibleLineage)

	// (b) authenticating only the older of two required inputs does not allow a lower master round
	f = newFixture(t, ts())
	s := f.session()
	f.in.Bound, err = s.AuthenticateEvidence([]Evidence{f.evidenceAt(0, f.round-10)})
	require.NoError(t, err)
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeIncompatibleLineage)
	require.Contains(t, err.Error(), "not covered")
}

func TestAuthenticateEvidence_RejectsDuplicateAndAlteredData(t *testing.T) {
	f := newFixture(t, ts())
	e := f.evidenceAt(0, f.round-1)
	_, err := f.session().AuthenticateEvidence([]Evidence{e, e})
	requireOutcome(t, err, OutcomeInvalidInput)

	// a request whose data differs from the authenticated evidence is not covered
	f = newFixture(t, ts())
	s := f.session()
	f.in.Bound, err = s.AuthenticateEvidence(f.defaultEvidence())
	require.NoError(t, err)
	r := *f.in.Peers[0].Requests[0]
	r.CertificationData.Witness = append([]byte(nil), r.CertificationData.Witness...)
	r.CertificationData.Witness[0] ^= 1
	f.in.Peers[0].Requests = []*api.CertificationRequest{&r}
	_, err = s.Consolidate(f.in)
	requireOutcome(t, err, OutcomeIncompatibleLineage)
}

func TestAuthenticateEvidence_NullPathStepIsTypedNotPanic(t *testing.T) {
	f := newFixture(t, ts())
	ev := f.defaultEvidence()
	uc := mustUC(t, ev[0].Proof.UnicityCertificate)
	require.NotEmpty(t, uc.UnicityTreeCertificate.HashSteps)
	uc.UnicityTreeCertificate.HashSteps[0] = nil
	var err error
	ev[0].Proof.UnicityCertificate, err = types.Cbor.Marshal(uc)
	require.NoError(t, err)
	require.NotPanics(t, func() { _, err = f.session().AuthenticateEvidence(ev) })
	requireOutcome(t, err, OutcomeInvalidInput)
	require.Contains(t, err.Error(), "null")
}

func TestAuthenticateEvidence_FullLeafRules(t *testing.T) {
	// state outside the certified shard: valid seal and matching path, wrong shard
	f := newFixture(t, ts())
	e0, e1 := f.evidenceAt(0, f.round-1), f.evidenceAt(1, f.round-1)
	swapped := Evidence{Request: e1.Request, Proof: e1.Proof}
	cp := *e1.Proof
	cp.UnicityCertificate = e0.Proof.UnicityCertificate // shard 0 certificate, shard 1 state
	swapped.Proof = &cp
	_, err := f.session().AuthenticateEvidence([]Evidence{swapped})
	requireOutcome(t, err, OutcomeBindingMismatch)
	require.Contains(t, err.Error(), "does not belong to shard")

	// reference time after the evidence UC timestamp
	f = newFixture(t, fixtureOpts{refTime: 2001, timestamp: 2000})
	_, err = f.session().AuthenticateEvidence(f.defaultEvidence())
	requireOutcome(t, err, OutcomeReferenceTime)

	// expired at its reference time
	exp := uint64(1000)
	f = newFixture(t, fixtureOpts{refTime: 1000, timestamp: 2000, expiresAt: &exp})
	_, err = f.session().AuthenticateEvidence(f.defaultEvidence())
	requireOutcome(t, err, OutcomeExpired)
}

func TestConsolidate_AlteredSealWitnessMisses(t *testing.T) {
	f := newFixture(t, ts())
	s := f.session()
	_, err := f.consolidate(s)
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
			_, err := f.consolidate(f.session())
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
			_, err := f.consolidate(f.session())
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
	_, err := f.consolidate(f.session())
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
	out, err := f.consolidate(f.session())
	require.NoError(t, err)
	// shard 1's certificate does not cover the state of shard 0
	err = f.session().checkLeaf(0, f.reqs[0], out.Shards[0].Proofs[0].InclusionProof, out.Shards[1].UC)
	requireOutcome(t, err, OutcomeBindingMismatch)
	require.Contains(t, err.Error(), "does not belong to shard")
}

func TestRootRoundBound_ShardOfMaximumRound(t *testing.T) {
	f := newFixture(t, ts())
	sid0, sid1 := f.view.Leaves[0].ShardID, f.view.Leaves[1].ShardID
	for name, tc := range map[string]struct {
		r0, r1 uint64
		want   types.ShardID
		round  uint64
	}{
		"later maximum on shard 1":    {f.round - 10, f.round - 2, sid1, f.round - 2},
		"later maximum on shard 0":    {f.round - 2, f.round - 10, sid0, f.round - 2},
		"equal rounds: first wins":    {f.round - 3, f.round - 3, sid0, f.round - 3},
		"equal rounds, shard 1 first": {f.round - 3, f.round - 3, sid1, f.round - 3},
	} {
		t.Run(name, func(t *testing.T) {
			ev := []Evidence{f.evidenceAt(0, tc.r0), f.evidenceAt(1, tc.r1)}
			if name == "equal rounds, shard 1 first" {
				ev = []Evidence{ev[1], ev[0]}
			}
			b, err := f.session().AuthenticateEvidence(ev)
			require.NoError(t, err)
			require.Equal(t, tc.round, b.Round())
			require.True(t, b.Shard().Equal(tc.want), "got %q want %q", b.Shard(), tc.want)
			// the shard is an admitted participant of the topology
			order, _ := types.CanonicalShardOrder(f.policy.Topology.Scheme)
			require.Contains(t, order, b.Shard())
		})
	}
}
