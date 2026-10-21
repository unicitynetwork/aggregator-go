package consolidation

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const testPartition types.PartitionID = api.AggregationPartitionID

func h32(label string) []byte {
	h := sha256.Sum256([]byte(label))
	return h[:]
}

// fixture is a two shard partition, one request per shard, single leaf SMTs.
type fixture struct {
	t        *testing.T
	tb       types.RootTrustBase
	policy   Policy
	scheme   types.ShardingScheme
	view     *types.PartitionViewV1
	viewRaw  []byte
	reqs     [2]*api.CertificationRequest
	refTimes [2]uint64
	certs    [2][]byte
	round    uint64
	in       *Input

	signer abcrypto.Signer
	ut     *types.UnicityTree
	utc    *types.UnicityTreeCertificate
	tree   types.ShardTree
	inputs []types.ShardTreeInput
}

type fixtureOpts struct {
	refTime   uint64 // reference time of every request
	timestamp uint64 // IR timestamp of every shard
	expiresAt *uint64
}

func newFixture(t *testing.T, o fixtureOpts) *fixture {
	f := &fixture{t: t}
	sid0, sid1 := types.ShardID{}.Split()
	f.scheme = types.ShardingScheme{sid0, sid1}
	sids := [2]types.ShardID{sid0, sid1}

	signer, verifier := testsig.CreateSignerAndVerifier(t)
	f.signer = signer
	sigKey, err := verifier.MarshalPublicKey()
	require.NoError(t, err)
	f.tb, err = types.NewTrustBase(types.NetworkMainNet, []*types.NodeInfo{{NodeID: "test", SigKey: sigKey, Stake: 1}})
	require.NoError(t, err)

	roots := [2][]byte{}
	for i := range 2 {
		cd := api.CertificationData{
			Version:         api.CertificationDataVersion,
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{0x02}, 33)},
			SourceStateHash: bytes.Repeat([]byte{0x33}, api.StateTreeKeyLengthBytes),
			TransactionHash: api.RequireNewImprintV2(strings.Repeat("22", 32)),
			Witness:         bytes.Repeat([]byte{0x44}, 65),
			ExpiresAt:       o.expiresAt,
		}
		var sid api.StateID
		for c := 0; c < 512; c++ { // route to shard i by the first key bit
			cd.SourceStateHash[len(cd.SourceStateHash)-1] = byte(c)
			cd.SourceStateHash[len(cd.SourceStateHash)-2] = byte(c >> 8)
			sid, err = cd.CreateStateID()
			require.NoError(t, err)
			if (sid.DataBytes()[0]&0x80 != 0) == (i == 1) {
				break
			}
		}
		f.reqs[i] = &api.CertificationRequest{StateID: sid, CertificationData: cd}
		f.refTimes[i] = o.refTime
		key, err := sid.GetTreeKey()
		require.NoError(t, err)
		hr := api.NewDataHasher(api.InclusionProofV2HashAlgorithm)
		hr.Reset().AddData([]byte{0}).AddData(key).AddData(api.LeafValue(cd.TransactionHash.DataBytes(), o.refTime))
		roots[i] = append([]byte(nil), hr.GetHash().RawHash...)
		f.certs[i], err = (&api.InclusionCert{}).MarshalBinary()
		require.NoError(t, err)
	}

	f.view = &types.PartitionViewV1{Version: 1, PartitionID: testPartition}
	f.policy = Policy{Topology: types.PartitionViewTopology{Scheme: f.scheme, ShardConfHashes: map[string][]byte{}}}
	var inputs []types.ShardTreeInput
	for i, sid := range sids {
		ir := &types.InputRecord{
			Version: 1, RoundNumber: 5, PreviousHash: h32("prev"), Hash: roots[i], SummaryValue: []byte{},
			Timestamp: o.timestamp, BlockHash: h32("block"),
		}
		leaf := types.PartitionViewLeafV1{ShardID: sid, InputRecord: ir, TRHash: h32("tr"), ShardConfHash: h32("conf" + sid.String())}
		f.view.Leaves = append(f.view.Leaves, leaf)
		f.policy.Topology.ShardConfHashes[sid.Key()] = leaf.ShardConfHash
		inputs = append(inputs, types.ShardTreeInput{Shard: sid, IR: ir, TRHash: leaf.TRHash, ShardConfHash: leaf.ShardConfHash})
	}
	f.viewRaw, err = f.view.MarshalCBOR()
	require.NoError(t, err)

	tree, err := types.CreateShardTree(f.scheme, inputs, crypto.SHA256)
	require.NoError(t, err)
	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: testPartition, ShardTreeRoot: tree.RootHash()},
		{Partition: 0x01010101, ShardTreeRoot: h32("other")},
	})
	require.NoError(t, err)
	utc, err := ut.Certificate(testPartition)
	require.NoError(t, err)
	f.tree, f.ut, f.utc, f.inputs = tree, ut, utc, inputs
	f.round = f.tb.GetEpochStart() + 50
	seal := &types.UnicitySeal{Version: 1, NetworkID: f.tb.GetNetworkID(), Epoch: f.tb.GetEpoch(), RootChainRoundNumber: f.round,
		Timestamp: types.NewTimestamp(), PreviousHash: h32("prev root"), Hash: ut.RootHash()}
	require.NoError(t, seal.Sign("test", signer))

	// master = shard 0 with the genuine UC; the peer (shard 1) carries a
	// local UC under its own, untrusted, seal.
	masterUC := f.localUC(tree, inputs, 0, utc, seal)
	peerSeal := &types.UnicitySeal{Version: 1, NetworkID: f.tb.GetNetworkID(), Epoch: f.tb.GetEpoch(), RootChainRoundNumber: f.round - 3,
		Timestamp: types.NewTimestamp(), PreviousHash: h32("x"), Hash: h32("peer local root"),
		Signatures: map[string]hex.Bytes{"test": bytes.Repeat([]byte{9}, 65)}}
	peerUC := f.localUC(tree, inputs, 1, utc, peerSeal)

	f.in = &Input{
		Master:        f.shardResult(sid0, 0, masterUC),
		PartitionView: f.viewRaw,
		Peers:         []ShardResult{f.shardResult(sid1, 1, peerUC)},
	}
	return f
}

func (f *fixture) localUC(tree types.ShardTree, in []types.ShardTreeInput, i int, utc *types.UnicityTreeCertificate, seal *types.UnicitySeal) []byte {
	stc, err := tree.Certificate(in[i].Shard)
	require.NoError(f.t, err)
	uc := &types.UnicityCertificate{Version: 1, InputRecord: in[i].IR, TRHash: in[i].TRHash, ShardConfHash: in[i].ShardConfHash,
		ShardTreeCertificate: stc, UnicityTreeCertificate: utc, UnicitySeal: seal}
	b, err := uc.MarshalCBOR()
	require.NoError(f.t, err)
	return b
}

func (f *fixture) shardResult(sid types.ShardID, i int, uc []byte) ShardResult {
	cd := f.reqs[i].CertificationData
	rt := f.refTimes[i]
	return ShardResult{
		ShardID:  sid,
		Requests: []*api.CertificationRequest{f.reqs[i]},
		Proofs: []*api.GetInclusionProofResponseV2{{BlockNumber: 7, InclusionProof: &api.InclusionProofV2{
			Version: 1, CertificationData: &cd, ReferenceTime: &rt, CertificateBytes: f.certs[i], UnicityCertificate: uc,
		}}},
	}
}

func (f *fixture) resolver() TrustBaseResolver {
	return func(*types.UnicitySeal) (types.RootTrustBase, error) { return f.tb, nil }
}

func (f *fixture) session() *Session {
	s, err := NewSession(f.policy, f.resolver())
	require.NoError(f.t, err)
	return s
}

// evidenceAt is the original proof of shard i, certified under its own seal of
// the given root round (signed by the trusted authority).
func (f *fixture) evidenceAt(i int, round uint64) Evidence {
	seal := &types.UnicitySeal{Version: 1, NetworkID: f.tb.GetNetworkID(), Epoch: f.tb.GetEpoch(), RootChainRoundNumber: round,
		Timestamp: types.NewTimestamp(), PreviousHash: h32("prev root"), Hash: f.ut.RootHash()}
	require.NoError(f.t, seal.Sign("test", f.signer))
	uc := f.localUC(f.tree, f.inputs, i, f.utc, seal)
	sid := f.view.Leaves[i].ShardID
	r := f.shardResult(sid, i, uc)
	return Evidence{Request: r.Requests[0], Proof: r.Proofs[0].InclusionProof}
}

// defaultEvidence yields R_min = round-2 over two original proofs.
func (f *fixture) defaultEvidence() []Evidence {
	return []Evidence{f.evidenceAt(0, f.round-10), f.evidenceAt(1, f.round-2)}
}

// consolidate authenticates the default evidence (unless a bound is set) and consolidates.
func (f *fixture) consolidate(s *Session) (*Output, error) {
	if f.in.Bound == nil {
		b, err := s.AuthenticateEvidence(f.defaultEvidence())
		if err != nil {
			return nil, err
		}
		f.in.Bound = b
	}
	return s.Consolidate(f.in)
}
