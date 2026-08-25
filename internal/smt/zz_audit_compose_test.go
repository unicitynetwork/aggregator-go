package smt

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// End-to-end: child shard cert + parent fragment -> composed cert verified by
// an end client against the parent UC.IR.h. Shows whether the all-zero
// phantom sibling is load-bearing in the client-verified path.
func TestAudit_ComposedCertCarriesZeroSibling(t *testing.T) {
	const shardIDLen = 4

	// ---- child aggregator for shard 0b10000 (shard prefix bits 0000) ----
	childShardID := api.ShardID(0b10000)
	child := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, childShardID)

	key := make([]byte, 32)
	// top 4 bits must be 0000 to live in this shard; set some lower bits
	key[0] = 0x0A
	key[1] = 0x5C
	key[31] = 0x99
	path, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("leaf-value-bytes")
	if err := child.AddLeaf(path, value); err != nil {
		t.Fatal(err)
	}
	childRoot := child.GetRootHashRaw()
	childCert, err := child.GetInclusionCert(key)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("childRoot = %x\n", childRoot)

	// ---- parent aggregator ----
	parent := NewParentSparseMerkleTree(api.SHA256, shardIDLen)
	if err := parent.AddLeaf(big.NewInt(int64(childShardID)), childRoot); err != nil {
		t.Fatal(err)
	}
	// a second live shard so the root isn't a degenerate unary chain
	other := make([]byte, 32)
	for i := range other {
		other[i] = 0xBB
	}
	if err := parent.AddLeaf(big.NewInt(0b11111), other); err != nil {
		t.Fatal(err)
	}
	parentRoot := parent.GetRootHashRaw()
	fmt.Printf("parentRoot (= UC.IR.h) = %x\n", parentRoot)

	frag, err := parent.GetShardInclusionFragment(childShardID)
	if err != nil {
		t.Fatal(err)
	}

	composed, err := api.ComposeInclusionCert(frag, childCert, childRoot)
	if err != nil {
		t.Fatal(err)
	}

	zero := [32]byte{}
	nZero := 0
	for i, s := range composed.Siblings {
		if s == zero {
			nZero++
			fmt.Printf("composed sibling[%d] is ALL-ZERO\n", i)
		}
	}
	depths := []int{}
	for d := 0; d < 256; d++ {
		if api.KeyBitBE(composed.Bitmap[:], d) == 1 {
			depths = append(depths, d)
		}
	}
	fmt.Printf("composed bitmap depths = %v (siblings=%d, all-zero=%d)\n",
		depths, len(composed.Siblings), nZero)

	if err := composed.Verify(key, value, parentRoot, api.SHA256); err != nil {
		t.Fatalf("composed cert failed to verify: %v", err)
	}
	fmt.Println("composed cert VERIFIES against parent root -> zero sibling is load-bearing")

	// Sanity: dropping the all-zero sibling (as a canonical RSMT would) breaks it.
	stripped := &api.InclusionCert{Bitmap: composed.Bitmap}
	for i, s := range composed.Siblings {
		if s == zero {
			api.ClearSuffixBE(nil, 0) // no-op, keep import stable
			// clear the corresponding bitmap bit
			idx := depths[i]
			stripped.Bitmap[idx/8] &^= 0x80 >> (uint(idx) % 8)
			continue
		}
		stripped.Siblings = append(stripped.Siblings, s)
	}
	err = stripped.Verify(key, value, parentRoot, api.SHA256)
	fmt.Printf("canonical (zero-sibling-compressed) cert verify -> %v\n", err)
}
