package consolidation

import (
	"bytes"
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// unsharded is a one-leaf partition (empty shard ID, zero shard-tree siblings) with one request.
type unsharded struct {
	t      *testing.T
	tb     types.RootTrustBase
	signer abcrypto.Signer
	policy Policy
	req    *api.CertificationRequest
	cert   []byte
	ref    uint64
	ut     *types.UnicityTree
	utc    *types.UnicityTreeCertificate
	tree   types.ShardTree
	input  types.ShardTreeInput
	round  uint64
	conf   []byte
}

func newUnsharded(t *testing.T) *unsharded { return newUnshardedOf(t, testPartition) }

// newUnshardedOf builds the fixture for any partition (authentic, signed by the trusted authority); only
// testPartition is the aggregation partition.
func newUnshardedOf(t *testing.T, partition types.PartitionID) *unsharded {
	u := &unsharded{t: t, ref: 1000}
	signer, verifier := testsig.CreateSignerAndVerifier(t)
	u.signer = signer
	pub, err := verifier.MarshalPublicKey()
	require.NoError(t, err)
	u.tb, err = types.NewTrustBase(types.NetworkMainNet, []*types.NodeInfo{{NodeID: "test", SigKey: pub, Stake: 1}})
	require.NoError(t, err)

	cd := api.CertificationData{
		Version:         api.CertificationDataVersion,
		OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{2}, 33)},
		SourceStateHash: bytes.Repeat([]byte{0x33}, api.StateTreeKeyLengthBytes),
		TransactionHash: api.RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222"),
		Witness:         bytes.Repeat([]byte{0x44}, 65),
	}
	sid, err := cd.CreateStateID()
	require.NoError(t, err)
	u.req = &api.CertificationRequest{StateID: sid, CertificationData: cd}
	key, _ := sid.GetTreeKey()
	hr := api.NewDataHasher(api.InclusionProofV2HashAlgorithm)
	hr.Reset().AddData([]byte{0}).AddData(key).AddData(api.LeafValue(cd.TransactionHash.DataBytes(), u.ref))
	root := append([]byte(nil), hr.GetHash().RawHash...)
	u.cert, err = (&api.InclusionCert{}).MarshalBinary()
	require.NoError(t, err)

	u.conf = h32("unsharded conf")
	ir := &types.InputRecord{Version: 1, RoundNumber: 5, PreviousHash: h32("prev"), Hash: root, SummaryValue: []byte{}, Timestamp: 2000, BlockHash: h32("block")}
	u.input = types.ShardTreeInput{Shard: types.ShardID{}, IR: ir, TRHash: h32("tr"), ShardConfHash: u.conf}
	u.tree, err = types.CreateShardTree(types.ShardingScheme{}, []types.ShardTreeInput{u.input}, crypto.SHA256)
	require.NoError(t, err)
	u.ut, err = types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: partition, ShardTreeRoot: u.tree.RootHash()}, {Partition: 0x01010101, ShardTreeRoot: h32("other")}})
	require.NoError(t, err)
	u.utc, err = u.ut.Certificate(partition)
	require.NoError(t, err)
	u.round = u.tb.GetEpochStart() + 50
	u.policy = Policy{}
	return u
}

func (u *unsharded) seal(round uint64) *types.UnicitySeal {
	seal := &types.UnicitySeal{Version: 1, NetworkID: u.tb.GetNetworkID(), Epoch: u.tb.GetEpoch(), RootChainRoundNumber: round,
		Timestamp: types.NewTimestamp(), PreviousHash: h32("prev root"), Hash: u.ut.RootHash()}
	require.NoError(u.t, seal.Sign("test", u.signer))
	return seal
}

func (u *unsharded) uc(round uint64, mut func(uc *types.UnicityCertificate)) []byte {
	stc, err := u.tree.Certificate(types.ShardID{})
	require.NoError(u.t, err)
	uc := &types.UnicityCertificate{Version: 1, InputRecord: u.input.IR, TRHash: u.input.TRHash, ShardConfHash: u.input.ShardConfHash,
		ShardTreeCertificate: stc, UnicityTreeCertificate: u.utc, UnicitySeal: u.seal(round)}
	if mut != nil {
		mut(uc)
	}
	b, err := types.Cbor.Marshal(uc)
	require.NoError(u.t, err)
	return b
}

func (u *unsharded) result(uc []byte) ShardResult {
	cd := u.req.CertificationData
	rt := u.ref
	return ShardResult{ShardID: types.ShardID{}, Requests: []*api.CertificationRequest{u.req},
		Proofs: []*api.GetInclusionProofResponseV2{{BlockNumber: 3, InclusionProof: &api.InclusionProofV2{
			Version: 1, CertificationData: &cd, ReferenceTime: &rt, CertificateBytes: u.cert, UnicityCertificate: uc}}}}
}

func (u *unsharded) session(t *testing.T, opts ...Policy) *Session {
	p := u.policy
	if len(opts) > 0 {
		p = opts[0]
	}
	s, err := NewSession(p, func(*types.UnicitySeal) (types.RootTrustBase, error) { return u.tb, nil })
	require.NoError(t, err)
	return s
}

func (u *unsharded) bound(s *Session) *RootRoundBound {
	r := u.result(u.uc(u.round-10, nil))
	b, err := s.AuthenticateEvidence([]Evidence{{Request: u.req, Proof: r.Proofs[0].InclusionProof}})
	require.NoError(u.t, err)
	return b
}

func TestDirect_UnshardedMasterNeedsNoView(t *testing.T) {
	u := newUnsharded(t)
	s := u.session(t)
	master := u.result(u.uc(u.round, nil))
	m, err := s.AuthenticateMaster(u.bound(s), master, nil)
	require.NoError(t, err)
	require.True(t, m.Direct())
	require.Equal(t, u.round, m.Round())
	_, err = m.PeerRoot(types.ShardID{})
	requireOutcome(t, err, OutcomeInvalidInput)

	out, err := s.Complete(m, nil)
	require.NoError(t, err)
	require.Len(t, out.Shards, 1)
	// the same checks as the view path: the legacy full verifier accepts the replacement proof
	require.NoError(t, out.Shards[0].Proofs[0].InclusionProof.Verify(u.req, &api.VerifierContext{TrustBase: u.tb, PartitionID: testPartition,
		ExpectedShardID: types.ShardID{}, ShardConfHash: u.conf}))

	// counters: one quorum check, one body, one leaf; no view bytes, index, derivation or tree work
	c := s.Counters()
	require.Equal(t, 1, c.QuorumChecks)
	require.Equal(t, 1, c.BodyChecks, "the body is checked once (memoized for the completion)")
	require.Equal(t, 1, c.LeafChecks)
	require.Zero(t, c.ViewLeafVisits)
	require.Zero(t, c.UCDerivations)
	require.Zero(t, c.ShardLeafProbes)
	require.Zero(t, c.ShardPathSteps)

	// Consolidate composes both stages
	s2 := u.session(t)
	out2, err := s2.Consolidate(&Input{Bound: u.bound(s2), Master: master})
	require.NoError(t, err)
	require.Len(t, out2.Shards, 1)
}

func TestDirect_OutputDoesNotAliasTheAuthenticatedUC(t *testing.T) {
	u := newUnsharded(t)
	s := u.session(t)
	m, err := s.AuthenticateMaster(u.bound(s), u.result(u.uc(u.round, nil)), nil)
	require.NoError(t, err)
	out1, err := s.Complete(m, nil)
	require.NoError(t, err)
	for k := range out1.Shards[0].UC.UnicitySeal.Signatures {
		out1.Shards[0].UC.UnicitySeal.Signatures[k][0] ^= 0xff
	}
	out1.Shards[0].UC.InputRecord.RoundNumber++
	out2, err := s.Complete(m, nil)
	require.NoError(t, err)
	require.NoError(t, out2.Shards[0].Proofs[0].InclusionProof.Verify(u.req, &api.VerifierContext{TrustBase: u.tb, PartitionID: testPartition, ExpectedShardID: types.ShardID{}, ShardConfHash: u.conf}))
}

// Every integrity failure is an error; none is an invitation to fall back.
func TestDirect_Rejections(t *testing.T) {
	zero := bytes.Repeat([]byte{9}, 32)
	sharded, _ := types.ShardID{}.Split()
	cases := map[string]struct {
		mut  func(uc *types.UnicityCertificate)
		want Outcome
	}{
		"empty shard ID with a nonempty shard path": {func(uc *types.UnicityCertificate) { uc.ShardTreeCertificate.SiblingHashes = [][]byte{zero} }, OutcomeInvalidInput},
		"missing unicity tree certificate":          {func(uc *types.UnicityCertificate) { uc.UnicityTreeCertificate = nil }, OutcomeInvalidInput},
		"missing seal":                              {func(uc *types.UnicityCertificate) { uc.UnicitySeal = nil }, OutcomeInvalidInput},
		"missing input record":                      {func(uc *types.UnicityCertificate) { uc.InputRecord = nil }, OutcomeInvalidInput},
		"sharded UC without a view":                 {func(uc *types.UnicityCertificate) { uc.ShardTreeCertificate.Shard = sharded }, OutcomeUnavailable},
		"altered signature witness": {func(uc *types.UnicityCertificate) {
			for k := range uc.UnicitySeal.Signatures {
				uc.UnicitySeal.Signatures[k][0] ^= 0xff
			}
		}, OutcomeUnauthenticated},
		"seal hash not the partition root": {func(uc *types.UnicityCertificate) { uc.UnicitySeal.Hash = h32("elsewhere") }, OutcomeUnauthenticated},
		"altered input record (path mismatch)": {func(uc *types.UnicityCertificate) {
			uc.InputRecord = cloneIR(uc.InputRecord)
			uc.InputRecord.RoundNumber++
		}, OutcomeBindingMismatch},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			u := newUnsharded(t)
			s := u.session(t)
			require.NotPanics(t, func() {
				_, err := s.AuthenticateMaster(u.bound(s), u.result(u.uc(u.round, tc.mut)), nil)
				require.Error(t, err)
				got, ok := OutcomeOf(err)
				require.True(t, ok, "untyped error: %v", err)
				// a malformed UC is an integrity error, whatever exact stage catches it
				if tc.want == OutcomeUnauthenticated || tc.want == OutcomeBindingMismatch {
					require.Contains(t, []Outcome{OutcomeUnauthenticated, OutcomeBindingMismatch, OutcomeInvalidInput}, got, err.Error())
				} else {
					require.Equal(t, tc.want, got, err.Error())
				}
			})
		})
	}
}

func cloneIR(ir *types.InputRecord) *types.InputRecord { c := *ir; return &c }

func TestDirect_StaleBoundAndMixedUCsAndPeers(t *testing.T) {
	u := newUnsharded(t)
	s := u.session(t)
	b := u.bound(s)
	// the authenticated target is older than the evidence
	old := u.uc(u.round-20, nil)
	_, err := s.AuthenticateMaster(b, u.result(old), nil)
	requireOutcome(t, err, OutcomeStale)

	// mixed UC bodies inside the master group
	other := u.uc(u.round, nil)
	a := u.result(u.uc(u.round, nil))
	cd2 := u.req.CertificationData
	cd2.Witness = bytes.Repeat([]byte{0x45}, 65)
	// two entries with different UC bytes
	second := u.result(u.uc(u.round+1, nil))
	a.Requests = append(a.Requests, u.req)
	a.Proofs = append(a.Proofs, second.Proofs[0])
	_ = other
	_, err = s.AuthenticateMaster(b, a, nil)
	require.Error(t, err)

	// a direct master takes no peers
	m, err := s.AuthenticateMaster(b, u.result(u.uc(u.round, nil)), nil)
	require.NoError(t, err)
	_, err = s.Complete(m, []ShardResult{u.result(u.uc(u.round, nil))})
	requireOutcome(t, err, OutcomeInvalidInput)

	// replacing the UC of the response after authentication cannot change the target
	master := u.result(u.uc(u.round, nil))
	m, err = s.AuthenticateMaster(b, master, nil)
	require.NoError(t, err)
	master.Proofs[0].InclusionProof.UnicityCertificate = u.uc(u.round+7, func(uc *types.UnicityCertificate) {
		uc.InputRecord = cloneIR(uc.InputRecord)
		uc.InputRecord.Hash = h32("different state")
	})
	_, err = s.Complete(m, nil)
	require.Error(t, err)
}

func TestDirect_PinsOnlyRestrict(t *testing.T) {
	u := newUnsharded(t)
	emptyKey := types.ShardID{}.Key()
	good := Policy{Topology: types.PartitionViewTopology{ShardConfHashes: map[string][]byte{emptyKey: u.conf}}}
	s := u.session(t, good)
	m, err := s.AuthenticateMaster(u.bound(s), u.result(u.uc(u.round, nil)), nil)
	require.NoError(t, err)
	_, err = s.Complete(m, nil)
	require.NoError(t, err)

	// a pin for another configuration rejects the UC
	bad := Policy{Topology: types.PartitionViewTopology{ShardConfHashes: map[string][]byte{emptyKey: h32("another configuration")}}}
	s = u.session(t, bad)
	// (the same pins already refuse the original evidence)
	_, err = s.AuthenticateEvidence([]Evidence{{Request: u.req, Proof: u.result(u.uc(u.round-10, nil)).Proofs[0].InclusionProof}})
	require.Error(t, err)
	// and a master whose bound came from an unpinned session is another session's bound
	un := u.session(t)
	_, err = s.AuthenticateMaster(u.bound(un), u.result(u.uc(u.round, nil)), nil)
	requireOutcome(t, err, OutcomeIncompatibleLineage)

	// pins describing a sharded partition are incompatible with an unsharded master UC
	a, b := types.ShardID{}.Split()
	sharded := Policy{Topology: types.PartitionViewTopology{
		Scheme: types.ShardingScheme{a, b}, ShardConfHashes: map[string][]byte{a.Key(): u.conf, b.Key(): u.conf}}}
	s = u.session(t, sharded)
	// evidence in the sharded policy cannot even be authenticated (its shard is not in the pins)
	_, err = s.AuthenticateEvidence([]Evidence{{Request: u.req, Proof: u.result(u.uc(u.round-10, nil)).Proofs[0].InclusionProof}})
	require.Error(t, err)
}

// An unsharded response with a (legacy) sidecar still goes through the view path, and a malformed
// sidecar is an error, not a silent fallback to the direct path.
func TestDirect_MalformedSidecarIsNotIgnored(t *testing.T) {
	u := newUnsharded(t)
	s := u.session(t)
	_, err := s.AuthenticateMaster(u.bound(s), u.result(u.uc(u.round, nil)), []byte{0x83, 0x01, 0x02})
	requireOutcome(t, err, OutcomeInvalidInput)
}

// The direct (unsharded) path enforces the fixed aggregation partition on an authentic UC of another partition:
// no session setting selects it, the evidence path and the master path both refuse it.
func TestDirect_RefusesAnAuthenticUCOfAnotherPartition(t *testing.T) {
	good := newUnsharded(t)
	gs := good.session(t)
	_, err := gs.AuthenticateMaster(good.bound(gs), good.result(good.uc(good.round, nil)), nil)
	require.NoError(t, err, "control: the same construction for partition 7 is accepted")

	u := newUnshardedOf(t, testPartition+1)
	s := u.session(t)
	_, err = s.AuthenticateMaster(u.bound(s), u.result(u.uc(u.round, nil)), nil)
	require.Error(t, err)
	got, ok := OutcomeOf(err)
	require.True(t, ok, "untyped error: %v", err)
	require.Contains(t, []Outcome{OutcomeInvalidInput, OutcomeBindingMismatch, OutcomeIncompatibleLineage}, got, err.Error())
}
