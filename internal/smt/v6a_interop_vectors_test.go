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
//	region: key bits 0..depth-1 packed big-endian into 32 bytes, rest zero
//	empty tree root: all-zero 32 bytes
//
// The roots below are the big-endian (issue #169) construction. They are
// additionally cross-checked structurally against an independent big-endian
// reference implementation in TestBigEndianReferenceMatchesProduction.

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

// TestV6AInterop_JSSDKParity builds the same sparse tree used by the JS SDK
// big-endian test suite (32-byte keys with only the first byte set) and pins
// the shared cross-implementation root. This value is cross-verified against
// the JS SDK: its "should verify the tree" test (state-transition-sdk-js#135,
// issue #134) computes the identical root imprint
// 0000c854db11...166ede for byte-identical inputs, so Go and JS agree in both
// directions on the big-endian construction.
func TestV6AInterop_JSSDKParity(t *testing.T) {
	// Keys and values mirror the JS SDK's SparseMerkleTree test
	// ("should verify the tree"): leavesSparse with values "value0".."value5".
	firstBytes := []byte{0b10010000, 0b00000000, 0b00010000, 0b10000000, 0b01100000, 0b00010100}

	tree := NewSparseMerkleTree(api.SHA256, 256)
	for i, b := range firstBytes {
		interopAddLeaf(t, tree, interopKey(b), []byte(fmt.Sprintf("value%d", i)), 256)
	}

	// Shared cross-SDK big-endian root (JS SDK sdk-js#135 pins the same value).
	require.Equal(t,
		"c854db11e92d269e7a4dc558adb201da311604d0bcc9883a8f1f017862166ede",
		interopRoot(t, tree),
		"Go root must match the JS SDK big-endian root for the identical tree")

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
	// Two keys differing at big-endian bit 7 (the LSB of byte 0): one junction
	// at depth 7 with a zero region.
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
	// Two keys identical except big-endian bit 255 (the LSB of the last byte):
	// a single junction at depth 255 whose region is 255 shared zero bits. Same
	// inputs as the JS SDK "deep split at depth 255" test (sdk-js#135).
	a := interopKey() // all zeros
	b := interopKey()
	b[31] = 0x01

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
	// Five keys producing junctions at several big-endian depths, including
	// cross-byte boundaries.
	keys := [][]byte{
		interopKey(0x00),             // all zero
		interopKey(0x04),             // diverges at big-endian bit 5
		interopKey(0x01),             // diverges at big-endian bit 7
		interopKey(0x00, 0x01),       // diverges at big-endian bit 15
		interopKey(0x00, 0x00, 0x01), // diverges at big-endian bit 23
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
	shallowSplitExpectedRoot = "5a758eb5528a347e00b9c8346a3724afc81a5a061cb4b24217d904b8f31f2744"
	deepSplitExpectedRoot    = "5ae94a6edf95904ebf8d3acbdd0e77c8a991ce6adb8999105325049613ba5580"
	multiLeafExpectedRoot    = "aeee01fda31fe042467954e26a5788363db7201bbe49b667cdf06c187a6c586b"
)
