package smt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// v6a interop vectors (issue #167). The constructions below are the shared
// cross-SDK acceptance cases: empty tree, one leaf, shallow split, deep
// split, and a multi-leaf tree. Roots are the v6a construction
// (yellowpaper Appendix C.3.2):
//
//	leaf: H(0x00 || key || value)
//	node: H(0x01 || depth_1B || region_32B || left_32B || right_32B)
//	region: key bits 0..depth-1 packed LSB-in-byte into 32 bytes, rest zero
//	empty tree root: all-zero 32 bytes
//
// TestV6AInterop_JSSDKParity asserts the JS SDK's published v6a root for the
// same tree — a cross-implementation anchor in the reverse direction
// (JS-generated, Go-reproduced).

func interopKey(b ...byte) []byte {
	key := make([]byte, 32)
	copy(key, b)
	return key
}

func interopAddLeaf(t *testing.T, tree *SparseMerkleTree, key []byte, value []byte, keyBits int) {
	t.Helper()
	path, err := api.FixedBytesToPath(key[:keyBits/8], keyBits)
	require.NoError(t, err)
	require.NoError(t, tree.AddLeaf(path, value))
}

func interopRoot(t *testing.T, tree *SparseMerkleTree) string {
	t.Helper()
	return tree.GetRootHashHex()
}

func interopLeafHash(key, value []byte) string {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(key)
	h.Write(value)
	return hex.EncodeToString(h.Sum(nil))
}

// TestV6AInterop_JSSDKParity builds the identical sparse tree used by the JS
// SDK v6a test suite (32-byte keys with only the first byte set) and asserts
// the JS SDK's expected root, proving cross-implementation agreement.
func TestV6AInterop_JSSDKParity(t *testing.T) {
	// Keys and values mirror the JS SDK's SparseMerkleTree v6a test
	// ("should verify the tree").
	firstBytes := []byte{0b10010000, 0b00000000, 0b00010000, 0b10000000, 0b01100000, 0b00010100}

	tree := NewSparseMerkleTree(api.SHA256, 256)
	for i, b := range firstBytes {
		interopAddLeaf(t, tree, interopKey(b), []byte(fmt.Sprintf("value%d", i)), 256)
	}

	// Expected root published by the JS SDK v6a test (imprint prefix stripped).
	require.Equal(t,
		"cd23fc1265484a7173323cd862b85a61796b8e0af31149944a828e6c1734b846",
		interopRoot(t, tree),
		"Go v6a root must match the JS SDK v6a root for the identical tree")

	for i, b := range firstBytes {
		requireCertRoundTrip(t, tree, interopKey(b), []byte(fmt.Sprintf("value%d", i)))
	}
}

func TestV6AInterop_EmptyTree(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, 256)
	require.Equal(t,
		"0000000000000000000000000000000000000000000000000000000000000000",
		interopRoot(t, tree),
		"v6a empty tree root is the all-zero hash")
}

func TestV6AInterop_OneLeaf(t *testing.T) {
	key := interopKey(0xB2) // 0b10110010, mirrors the JS single-leaf case shape
	value := []byte{9, 9, 9}

	tree := NewSparseMerkleTree(api.SHA256, 256)
	interopAddLeaf(t, tree, key, value, 256)

	// Single leaf: the root is the leaf hash itself (unary passthrough,
	// no interior node, no region involved).
	require.Equal(t, interopLeafHash(key, value), interopRoot(t, tree))
}

func TestV6AInterop_ShallowSplit(t *testing.T) {
	// Two keys differing at bit 0: one junction at depth 0 with a zero region.
	a := interopKey(0x00)
	b := interopKey(0x01)

	tree := NewSparseMerkleTree(api.SHA256, 256)
	interopAddLeaf(t, tree, a, []byte("left"), 256)
	interopAddLeaf(t, tree, b, []byte("right"), 256)

	require.Equal(t, shallowSplitExpectedRoot, interopRoot(t, tree))
	requireCertRoundTrip(t, tree, a, []byte("left"))
	requireCertRoundTrip(t, tree, b, []byte("right"))
}

func TestV6AInterop_DeepSplit(t *testing.T) {
	// Two keys identical except bit 255 (the high bit of the last byte):
	// a single junction at depth 255 whose region is 255 shared zero bits.
	a := interopKey() // all zeros
	b := interopKey()
	b[31] = 0x80

	valueA := interopKey()
	valueA[0] = 1
	valueB := interopKey()
	valueB[0] = 2

	tree := NewSparseMerkleTree(api.SHA256, 256)
	interopAddLeaf(t, tree, a, valueA, 256)
	interopAddLeaf(t, tree, b, valueB, 256)

	require.Equal(t, deepSplitExpectedRoot, interopRoot(t, tree))
	requireCertRoundTrip(t, tree, a, valueA)
	requireCertRoundTrip(t, tree, b, valueB)
}

func TestV6AInterop_MultiLeaf(t *testing.T) {
	// Five keys producing junctions at several depths, including byte
	// boundaries (bits 0, 2, 8, 16).
	keys := [][]byte{
		interopKey(0x00),             // bits: all zero
		interopKey(0x04),             // diverges at bit 2
		interopKey(0x01),             // diverges at bit 0
		interopKey(0x00, 0x01),       // diverges at bit 8
		interopKey(0x00, 0x00, 0x01), // diverges at bit 16
	}

	tree := NewSparseMerkleTree(api.SHA256, 256)
	for i, k := range keys {
		interopAddLeaf(t, tree, k, []byte(fmt.Sprintf("value%d", i)), 256)
	}

	require.Equal(t, multiLeafExpectedRoot, interopRoot(t, tree))
	for i, k := range keys {
		requireCertRoundTrip(t, tree, k, []byte(fmt.Sprintf("value%d", i)))
	}
}

func requireCertRoundTrip(t *testing.T, tree *SparseMerkleTree, key, value []byte) {
	t.Helper()
	cert, err := tree.GetInclusionCert(key)
	require.NoError(t, err)
	root := tree.GetRootHashRaw()
	require.NoError(t, cert.Verify(key, value, root, api.SHA256),
		"v6a cert must verify for key %x", key)
}

// Shared cross-implementation interop vector roots for the v6a construction
// (issue #167): the canonical values the Go, JS, Java, and Rust SMTs must all
// reproduce for these trees. The deep-split and multi-leaf cases are the ones
// that exercise multi-byte region packing, so they are the load-bearing
// cross-implementation checks. Per-SDK cross-verification status is tracked on
// the issue, not here.
const (
	shallowSplitExpectedRoot = "8cc069f48345d8117664a31590eea28dae79cac066a4c457cd467c4d4d2648e2"
	deepSplitExpectedRoot    = "789f3ba1c3b31402bef371ad3cb8a7a176589648d898cb792303a0e3fe128611"
	multiLeafExpectedRoot    = "7fe744edd3bfe7e973675d773c49e159fccb54e27aa59972deb703fe466bbf2e"
)
