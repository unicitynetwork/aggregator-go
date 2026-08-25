package smt

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func mkKey(firstByte byte, tail byte) []byte {
	k := make([]byte, 32)
	k[0] = firstByte
	for i := 1; i < 32; i++ {
		k[i] = tail
	}
	return k
}

func mkVal(b byte) []byte {
	v := make([]byte, 32)
	for i := range v {
		v[i] = b
	}
	return v
}

func pathOf(t *testing.T, key []byte) *big.Int {
	t.Helper()
	p, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func canonicalRoot(t *testing.T, kv map[string][]byte) []byte {
	t.Helper()
	std := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	for ks, v := range kv {
		key := []byte(ks)
		if err := std.AddLeaf(pathOf(t, key), v); err != nil {
			t.Fatal(err)
		}
	}
	return std.GetRootHashRaw()
}

// Case 1: fully-populated, every shard non-empty. Splice should be canonical.
func TestLens_ParentSplice_FullyOccupied(t *testing.T) {
	keyA := mkKey(0x00, 0x11) // bit 0 = 0 -> shard 0b10
	keyB := mkKey(0x80, 0x22) // bit 0 = 1 -> shard 0b11
	valA, valB := mkVal(0xAA), mkVal(0xBB)

	c0 := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b10)
	if err := c0.AddLeaf(pathOf(t, keyA), valA); err != nil {
		t.Fatal(err)
	}
	c1 := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b11)
	if err := c1.AddLeaf(pathOf(t, keyB), valB); err != nil {
		t.Fatal(err)
	}

	parent := NewParentSparseMerkleTree(api.SHA256, 1)
	if err := parent.AddLeaf(big.NewInt(0b10), c0.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddLeaf(big.NewInt(0b11), c1.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}

	canon := canonicalRoot(t, map[string][]byte{string(keyA): valA, string(keyB): valB})
	got := parent.GetRootHashRaw()
	t.Logf("canonical=%x parent=%x", canon, got)
	if !bytes.Equal(canon, got) {
		t.Errorf("expected match in fully-occupied case")
	}
}

// Case 2: SHARD_ID_LENGTH=1 (the reference sharding-compose.yml value), both
// shards live and configured, but shard 0b11 had no commitments this round so
// its child SMT root is the empty-tree root.
func TestLens_ParentSplice_LiveButEmptyShard(t *testing.T) {
	keyA := mkKey(0x00, 0x11)
	valA := mkVal(0xAA)

	c0 := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b10)
	if err := c0.AddLeaf(pathOf(t, keyA), valA); err != nil {
		t.Fatal(err)
	}
	c1 := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b11)
	emptyChildRoot := c1.GetRootHashRaw()
	t.Logf("empty CHILD tree root = %x", emptyChildRoot)

	parent := NewParentSparseMerkleTree(api.SHA256, 1)
	if err := parent.AddLeaf(big.NewInt(0b10), c0.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddLeaf(big.NewInt(0b11), emptyChildRoot); err != nil {
		t.Fatal(err)
	}
	parentRoot := parent.GetRootHashRaw()

	canon := canonicalRoot(t, map[string][]byte{string(keyA): valA})
	t.Logf("canonical RSMT root of {A}       = %x", canon)
	t.Logf("certified parent root (IR.h)     = %x", parentRoot)
	if bytes.Equal(canon, parentRoot) {
		t.Errorf("unexpectedly equal")
	}

	// What ships to the end client.
	frag, err := parent.GetShardInclusionFragment(0b10)
	if err != nil {
		t.Fatal(err)
	}
	var pcert api.InclusionCert
	if err := pcert.UnmarshalBinary(frag.CertificateBytes); err != nil {
		t.Fatal(err)
	}
	childCert, err := c0.GetInclusionCert(keyA)
	if err != nil {
		t.Fatal(err)
	}
	composed, err := api.ComposeInclusionCert(frag, childCert, c0.GetRootHashRaw())
	if err != nil {
		t.Fatal(err)
	}
	depths := []int{}
	for d := 0; d < 256; d++ {
		if api.KeyBitBE(composed.Bitmap[:], d) == 1 {
			depths = append(depths, d)
		}
	}
	t.Logf("composed cert junction depths = %v", depths)
	for i, s := range composed.Siblings {
		t.Logf("  sibling[%d] = %x", i, s)
	}
	// Canonical: a 1-leaf RSMT has zero junctions, so the canonical cert is empty.
	if err := composed.Verify(keyA, valA, parentRoot, api.SHA256); err != nil {
		t.Errorf("composed cert should verify against the certified (non-canonical) root: %v", err)
	}
	if err := composed.Verify(keyA, valA, canon, api.SHA256); err == nil {
		t.Errorf("composed cert unexpectedly verifies against the canonical root")
	}
}

// Case 3: SHARD_ID_LENGTH=4 default, only shards 0000 and 1111 live.
func TestLens_ParentSplice_PartiallyLive16(t *testing.T) {
	keyA := mkKey(0x00, 0x11) // top 4 bits 0000
	keyB := mkKey(0xF0, 0x22) // top 4 bits 1111
	valA, valB := mkVal(0xAA), mkVal(0xBB)

	c0 := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b1_0000)
	if err := c0.AddLeaf(pathOf(t, keyA), valA); err != nil {
		t.Fatal(err)
	}
	cF := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, 0b1_1111)
	if err := cF.AddLeaf(pathOf(t, keyB), valB); err != nil {
		t.Fatal(err)
	}

	parent := NewParentSparseMerkleTree(api.SHA256, 4)
	t.Logf("EMPTY parent tree root (keyLength=4) = %x", parent.GetRootHashRaw())
	if err := parent.AddLeaf(big.NewInt(0b1_0000), c0.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddLeaf(big.NewInt(0b1_1111), cF.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}
	parentRoot := parent.GetRootHashRaw()
	canon := canonicalRoot(t, map[string][]byte{string(keyA): valA, string(keyB): valB})
	t.Logf("canonical RSMT root  = %x", canon)
	t.Logf("parent (certified)   = %x", parentRoot)
	if bytes.Equal(canon, parentRoot) {
		t.Errorf("unexpectedly equal")
	}

	frag, err := parent.GetShardInclusionFragment(0b1_0000)
	if err != nil {
		t.Fatal(err)
	}
	childCert, err := c0.GetInclusionCert(keyA)
	if err != nil {
		t.Fatal(err)
	}
	composed, err := api.ComposeInclusionCert(frag, childCert, c0.GetRootHashRaw())
	if err != nil {
		t.Fatal(err)
	}
	depths := []int{}
	for d := 0; d < 256; d++ {
		if api.KeyBitBE(composed.Bitmap[:], d) == 1 {
			depths = append(depths, d)
		}
	}
	t.Logf("composed cert junction depths = %v (canonical would be [0])", depths)
	zeros := 0
	for i, s := range composed.Siblings {
		t.Logf("  sibling[%d] = %s", i, hex.EncodeToString(s[:]))
		if bytes.Equal(s[:], make([]byte, 32)) {
			zeros++
		}
	}
	t.Logf("all-zero siblings shipped to client: %d", zeros)
	if err := composed.Verify(keyA, valA, parentRoot, api.SHA256); err != nil {
		t.Errorf("composed cert must verify against certified root: %v", err)
	}
}

// Case 4: empty-tree root, standalone vs parent mode.
func TestLens_EmptyTreeRoots(t *testing.T) {
	t.Logf("empty standalone (256-bit) root   = %x", NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits).GetRootHashRaw())
	for _, n := range []int{1, 2, 4, 8} {
		t.Logf("empty parent root SHARD_ID_LENGTH=%2d = %x", n, NewParentSparseMerkleTree(api.SHA256, n).GetRootHashRaw())
	}
}
