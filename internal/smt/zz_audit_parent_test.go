package smt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// canonical RSMT internal node hash for a two-leaf tree bifurcating at depth d
func rsmtNode(d int, key []byte, l, r []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x01})
	h.Write([]byte{byte(d)})
	h.Write(api.RegionFromKeyBytes(key, d))
	h.Write(l)
	h.Write(r)
	return h.Sum(nil)
}

func TestAudit_EmptyParentRoot(t *testing.T) {
	for _, kl := range []int{1, 2, 4, 8} {
		tree := NewParentSparseMerkleTree(api.SHA256, kl)
		root := tree.GetRootHashRaw()
		fmt.Printf("keyLength=%d empty PARENT root = %s\n", kl, hex.EncodeToString(root))

		std := NewSparseMerkleTree(api.SHA256, kl)
		fmt.Printf("keyLength=%d empty STANDALONE root = %s\n", kl, hex.EncodeToString(std.GetRootHashRaw()))
	}
}

func TestAudit_ParentVsCanonical(t *testing.T) {
	const keyLength = 4
	tree := NewParentSparseMerkleTree(api.SHA256, keyLength)

	// two child roots
	vA := make([]byte, 32)
	vB := make([]byte, 32)
	for i := range vA {
		vA[i] = 0xAA
		vB[i] = 0xBB
	}

	// shard ids: sentinel-prefixed 4-bit paths.
	// 0b10000 = 16 -> key bits 0000 ; 0b10001 = 17 -> ?
	sidA := big.NewInt(0b10000)
	sidB := big.NewInt(0b11111)

	keyA, _ := api.PathToFixedBytes(sidA, keyLength)
	keyB, _ := api.PathToFixedBytes(sidB, keyLength)
	fmt.Printf("keyA=%x keyB=%x\n", keyA, keyB)

	if err := tree.AddLeaf(sidA, vA); err != nil {
		t.Fatal(err)
	}
	if err := tree.AddLeaf(sidB, vB); err != nil {
		t.Fatal(err)
	}
	parentRoot := tree.GetRootHashRaw()
	fmt.Printf("PARENT-mode root (2 shards live, SHARD_ID_LENGTH=4) = %x\n", parentRoot)

	// canonical RSMT over the same spliced child-leaf hashes:
	// keys 0000 and 1111 bifurcate at depth 0.
	d := 0
	for ; d < keyLength; d++ {
		if api.KeyBitBE(keyA, d) != api.KeyBitBE(keyB, d) {
			break
		}
	}
	fmt.Printf("bifurcation depth = %d\n", d)
	var canon []byte
	if api.KeyBitBE(keyA, d) == 0 {
		canon = rsmtNode(d, keyA, vA, vB)
	} else {
		canon = rsmtNode(d, keyA, vB, vA)
	}
	fmt.Printf("CANONICAL RSMT root (splice semantics)  = %x\n", canon)

	// also: standalone tree with the same two (path,value) pairs, ordinary leaves
	std := NewSparseMerkleTree(api.SHA256, keyLength)
	if err := std.AddLeaf(sidA, vA); err != nil {
		t.Fatal(err)
	}
	if err := std.AddLeaf(sidB, vB); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("STANDALONE (rsmt_leaf_hash leaves) root = %x\n", std.GetRootHashRaw())

	// same two leaves but parent mode with SHARD_ID_LENGTH=1 is not possible
	// (only 2 slots); use keyLength=4 vs 5 to show dependence on the parameter.
}

func TestAudit_FragmentAllZeroSibling(t *testing.T) {
	const keyLength = 4
	tree := NewParentSparseMerkleTree(api.SHA256, keyLength)
	vA := make([]byte, 32)
	for i := range vA {
		vA[i] = 0xAA
	}
	sidA := big.NewInt(0b10000) // key bits 0000
	if err := tree.AddLeaf(sidA, vA); err != nil {
		t.Fatal(err)
	}
	vB := make([]byte, 32)
	for i := range vB {
		vB[i] = 0xBB
	}
	sidB := big.NewInt(0b11111)
	if err := tree.AddLeaf(sidB, vB); err != nil {
		t.Fatal(err)
	}

	frag, err := tree.GetShardInclusionFragment(api.ShardID(0b10000))
	if err != nil {
		t.Fatal(err)
	}
	if frag == nil {
		t.Fatal("nil fragment")
	}
	var cert api.InclusionCert
	if err := cert.UnmarshalBinary(frag.CertificateBytes); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("fragment shard leaf value = %x\n", frag.ShardLeafValue)
	fmt.Printf("bitmap (first 4 bytes) = %x\n", cert.Bitmap[:4])
	depths := []int{}
	for d := 0; d < 256; d++ {
		if api.KeyBitBE(cert.Bitmap[:], d) == 1 {
			depths = append(depths, d)
		}
	}
	fmt.Printf("bitmap set depths = %v\n", depths)
	zero := make([]byte, 32)
	for i, s := range cert.Siblings {
		isZero := string(s[:]) == string(zero)
		fmt.Printf("sibling[%d] = %x  allZero=%v\n", i, s[:], isZero)
	}

	// verify the fragment against the parent root, i.e. confirm the phantom
	// junctions are load-bearing in the certified path
	root := tree.GetRootHashRaw()
	keyA, _ := api.PathToFixedBytes(sidA, keyLength)
	fullKey := make([]byte, 32)
	copy(fullKey, keyA)
	fmt.Printf("root=%x\n", root)
	_ = fullKey
}
