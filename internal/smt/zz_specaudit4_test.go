package smt

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Probe: with only two of 16 shards populated, the canonical RSMT has exactly
// one junction (depth 0) on the path to either shard. Does the parent tree
// emit a compressed path, or does it commit phantom all-zero subtrees?
func TestProbe_ParentTreePhantomSubtrees(t *testing.T) {
	const shardBits = 4
	parent := NewParentSparseMerkleTree(api.SHA256, shardBits)

	rA := make([]byte, 32)
	rB := make([]byte, 32)
	rand.Read(rA)
	rand.Read(rB)

	// shard path bits 0..3 = 0,0,0,0  -> sentinel int 0b1_0000 = 16
	// shard path bits 0..3 = 1,1,1,1  -> sentinel int 0b1_1111 = 31
	if err := parent.AddLeaf(big.NewInt(16), rA); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddLeaf(big.NewInt(31), rB); err != nil {
		t.Fatal(err)
	}

	frag, err := parent.GetShardInclusionFragment(16)
	if err != nil {
		t.Fatal(err)
	}
	var cert api.InclusionCert
	if err := cert.UnmarshalBinary(frag.CertificateBytes); err != nil {
		t.Fatal(err)
	}
	depths := []int{}
	for d := 0; d < 256; d++ {
		if api.KeyBitBE(cert.Bitmap[:], d) == 1 {
			depths = append(depths, d)
		}
	}
	t.Logf("junction depths on path to shard 0000 with only 2/16 shards live: %v (siblings=%d)", depths, len(cert.Siblings))
	for i, s := range cert.Siblings {
		t.Logf("  sibling[%d] = %x", i, s)
	}
	t.Logf("parent root = %x", parent.GetRootHashRaw())
}
