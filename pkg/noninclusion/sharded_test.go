package noninclusion_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

// A sharded partition of four shards: the prefix bits of the shard ID are not junctions of the shard's own tree,
// so only the shard membership rule keeps a certificate of shard S from "proving" absence of a key S does not own.
type shardedEnv struct {
	auth  *nifixture.Authority
	shard types.ShardID // shard 0: keys starting with bits 00
}

func newShardedEnv(t *testing.T) *shardedEnv {
	a, err := nifixture.NewShardedAuthority(2)
	require.NoError(t, err)
	return &shardedEnv{auth: a, shard: a.ShardID(0)}
}

func key(top byte, tail ...byte) []byte { // top is the first byte; the shard of keys with top < 0x40 is 00
	k := make([]byte, 32)
	k[0] = top
	copy(k[1:], tail)
	return k
}

func (e *shardedEnv) verifier(t *testing.T, shard types.ShardID) *noninclusion.Verifier {
	tb := e.auth.TrustBase()
	v, err := noninclusion.NewVerifier(noninclusion.Context{Shard: shard,
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil }})
	require.NoError(t, err)
	return v
}

func (e *shardedEnv) proof(t *testing.T, shardIdx int, leaves []nifixture.Leaf, query []byte, mutate func(*types.UnicityCertificate)) *api.NonInclusionProofV1 {
	tree, err := nifixture.Build(leaves)
	require.NoError(t, err)
	root, _ := tree.Root()
	uc, _, err := e.auth.CertifyShardRoot(shardIdx, root[:])
	require.NoError(t, err)
	if mutate != nil {
		mutate(uc)
	}
	ucb, err := uc.MarshalCBOR()
	require.NoError(t, err)
	var c api.NonInclusionCert
	require.NoError(t, c.UnmarshalBinary(tree.Certificate([32]byte(query))))
	return &api.NonInclusionProofV1{Certificate: &c, UnicityCertificate: types.RawCBOR(ucb)}
}

func leaf(k []byte, v byte) nifixture.Leaf {
	return nifixture.Leaf{Key: [32]byte(k), Value: [32]byte(bytes.Repeat([]byte{v}, 32))}
}

func TestShardedAbsenceAndMembership(t *testing.T) {
	e := newShardedEnv(t)
	// two leaves of shard 00 which differ well below the shard prefix
	inShard := []nifixture.Leaf{leaf(key(0x10, 0x00), 1), leaf(key(0x10, 0x80), 2)}

	// a valid sharded absence: the query is in the shard
	q := key(0x10, 0x01)
	_, err := e.verifier(t, e.shard).Verify(q, e.proof(t, 0, inShard, q, nil))
	require.NoError(t, err)

	// a query OUTSIDE the shard follows the same junction bits, so the certificate still folds: only
	// the membership rule rejects it
	out := key(0xd0, 0x01) // top bits 11
	p := e.proof(t, 0, inShard, out, nil)
	require.NoError(t, p.Certificate.VerifyAgainstTrustedRoot(out, mustRoot(t, p), api.SHA256), "the fold alone accepts it")
	_, err = e.verifier(t, e.shard).Verify(out, p)
	require.Equal(t, noninclusion.RejectionCertInvalid, noninclusion.RejectionOf(err))

	// a terminal OUTSIDE the shard (a shard tree which holds foreign leaves) is rejected even for an in-shard query
	foreign := []nifixture.Leaf{leaf(key(0xd0, 0x00), 3), leaf(key(0xd0, 0x80), 4)}
	p = e.proof(t, 0, foreign, q, nil)
	require.NoError(t, p.Certificate.VerifyAgainstTrustedRoot(q, mustRoot(t, p), api.SHA256))
	_, err = e.verifier(t, e.shard).Verify(q, p)
	require.Equal(t, noninclusion.RejectionCertInvalid, noninclusion.RejectionOf(err))
}

func TestShardedUCStructure(t *testing.T) {
	e := newShardedEnv(t)
	leaves := []nifixture.Leaf{leaf(key(0x10, 0x00), 1), leaf(key(0x10, 0x80), 2)}
	q := key(0x10, 0x01)
	good := e.proof(t, 0, leaves, q, nil)

	// the context shard is not the UC's shard
	_, err := e.verifier(t, e.auth.ShardID(1)).Verify(q, good)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
	_, err = e.verifier(t, types.ShardID{}).Verify(q, good)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err), "an unsharded context never accepts a shard UC")

	// the UC of shard 1 presented for a context of shard 0
	other := e.proof(t, 1, []nifixture.Leaf{leaf(key(0x50, 0x00), 1), leaf(key(0x50, 0x80), 2)}, key(0x50, 1), nil)
	_, err = e.verifier(t, e.shard).Verify(key(0x50, 1), other)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))

	// a missing or short shard path
	short := e.proof(t, 0, leaves, q, func(uc *types.UnicityCertificate) {
		require.NotEmpty(t, uc.ShardTreeCertificate.SiblingHashes)
		uc.ShardTreeCertificate.SiblingHashes = uc.ShardTreeCertificate.SiblingHashes[:len(uc.ShardTreeCertificate.SiblingHashes)-1]
	})
	_, err = e.verifier(t, e.shard).Verify(q, short)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
	none := e.proof(t, 0, leaves, q, func(uc *types.UnicityCertificate) { uc.ShardTreeCertificate.SiblingHashes = nil })
	_, err = e.verifier(t, e.shard).Verify(q, none)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
}

func mustRoot(t *testing.T, p *api.NonInclusionProofV1) []byte {
	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(p.UnicityCertificate, &uc))
	return uc.InputRecord.Hash
}
