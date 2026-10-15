// Package prooftest builds test fixtures of partition views and their enclosing UCs.
// The seal is signed by a deterministic public test key of the fixture trust base
// (network 3, epoch 1, epoch start 1); never use the key outside tests.
package prooftest

import (
	"crypto"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-go-base/types"
)

const Partition types.PartitionID = 0x0a0b0c0d

func Hash(label string) []byte {
	h := sha256.Sum256([]byte(label))
	return h[:]
}

// Scheme returns 2^bits equal-prefix shards (empty scheme for 0 bits).
func Scheme(bits int) types.ShardingScheme {
	if bits == 0 {
		return types.ShardingScheme{}
	}
	ids := []types.ShardID{{}}
	for ; bits > 0; bits-- {
		var next []types.ShardID
		for _, id := range ids {
			a, b := id.Split()
			next = append(next, a, b)
		}
		ids = next
	}
	return types.ShardingScheme(ids)
}

type Fixture struct {
	View  *types.PartitionViewV1
	Raw   []byte // canonical view bytes
	UC    *types.UnicityCertificate
	Topo  types.PartitionViewTopology
	Shard types.ShardID // shard of UC (first leaf)
	// TrustBase is the authority which signed the seal; Signer its key.
	TrustBase *types.RootTrustBaseV1
	Signer    abcrypto.Signer
	// TR is the technical record of the shard of UC; UC.TRHash is its hash.
	TR certification.TechnicalRecord
}

// NetworkID of the fixture authority.
const NetworkID types.NetworkID = 3

// New builds a view over 2^bits shards. The enclosing UC belongs to the first shard,
// whose IR has the given round and state hash.
func New(t testing.TB, bits int, round uint64, stateHash []byte, rootRound uint64) *Fixture {
	scheme := Scheme(bits)
	order, err := types.CanonicalShardOrder(scheme)
	require.NoError(t, err)
	tr := certification.TechnicalRecord{Round: round + 1, Epoch: 1, Leader: "leader", FeeHash: Hash("fees"), StatHash: Hash("stat")}
	trHash, err := tr.Hash()
	require.NoError(t, err)
	v := &types.PartitionViewV1{Version: 1, PartitionID: Partition}
	var inputs []types.ShardTreeInput
	confHashes := map[string][]byte{}
	for i, id := range order {
		ir := &types.InputRecord{
			Version: 1, RoundNumber: round, Epoch: 1,
			PreviousHash: Hash("prev-" + id.String()), Hash: Hash("state-" + id.String()),
			SummaryValue: []byte{}, Timestamp: 1731504540 + uint64(i), BlockHash: Hash("block-" + id.String()),
		}
		if i == 0 {
			ir.Hash, ir.BlockHash = stateHash, stateHash
		}
		leaf := types.PartitionViewLeafV1{ShardID: id, InputRecord: ir, TRHash: Hash("tr-" + id.String()), ShardConfHash: Hash("conf-" + id.String())}
		if i == 0 {
			leaf.TRHash = trHash
		}
		confHashes[id.Key()] = leaf.ShardConfHash
		v.Leaves = append(v.Leaves, leaf)
		inputs = append(inputs, types.ShardTreeInput{Shard: id, IR: ir, TRHash: leaf.TRHash, ShardConfHash: leaf.ShardConfHash})
	}
	tree, err := types.CreateShardTree(scheme, inputs, crypto.SHA256)
	require.NoError(t, err)
	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: Partition, ShardTreeRoot: tree.RootHash()},
		{Partition: 0x01010101, ShardTreeRoot: Hash("other partition")},
	})
	require.NoError(t, err)
	utc, err := ut.Certificate(Partition)
	require.NoError(t, err)
	stc, err := tree.Certificate(order[0])
	require.NoError(t, err)
	signer, err := abcrypto.NewInMemorySecp256K1SignerFromKey(Hash("prooftest authority key"))
	require.NoError(t, err)
	verifier, err := signer.Verifier()
	require.NoError(t, err)
	pub, err := verifier.MarshalPublicKey()
	require.NoError(t, err)
	tb, err := types.NewTrustBase(NetworkID, []*types.NodeInfo{{NodeID: "node", SigKey: pub, Stake: 1}}, types.WithEpoch(1), types.WithEpochStart(1))
	require.NoError(t, err)
	seal := &types.UnicitySeal{Version: 1, NetworkID: NetworkID, Epoch: 1, RootChainRoundNumber: rootRound,
		Timestamp: 1731504600, PreviousHash: Hash("prev root"), Hash: ut.RootHash()}
	require.NoError(t, seal.Sign("node", signer))
	l := v.Leaves[0]
	raw, err := types.Cbor.Marshal(v)
	require.NoError(t, err)
	return &Fixture{
		TrustBase: tb, Signer: signer, TR: tr,
		View: v, Raw: raw, Topo: types.PartitionViewTopology{Scheme: scheme, ShardConfHashes: confHashes}, Shard: order[0],
		UC: &types.UnicityCertificate{Version: 1, InputRecord: l.InputRecord, TRHash: l.TRHash, ShardConfHash: l.ShardConfHash,
			ShardTreeCertificate: stc, UnicityTreeCertificate: utc, UnicitySeal: seal},
	}
}

// Provider returns the trusted topology provider matching the fixture.
func (f *Fixture) Provider(t testing.TB) *proofmaterial.StaticTopology {
	p, err := proofmaterial.NewStaticTopology(Partition, f.Topo.Scheme, f.Topo.ShardConfHashes)
	require.NoError(t, err)
	return p
}
