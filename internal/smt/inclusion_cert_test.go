package smt

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestGetInclusionCert_SingleLeaf exercises the single-leaf / unary-root
// edge case: bitmap is all zeros, no siblings, and verification reduces to
// H_leaf(key, value) == root.
func TestGetInclusionCert_SingleLeaf(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	key := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	value := []byte("solo-leaf-value")

	path, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
	require.NoError(t, err)
	require.NoError(t, tree.AddLeaf(path, value))

	cert, err := tree.GetInclusionCert(key)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, 0, bitmapPopcountForTest(&cert.Bitmap), "single-leaf cert bitmap must be empty")
	require.Len(t, cert.Siblings, 0, "single-leaf cert must have zero siblings")

	root := tree.GetRootHashRaw()
	require.Len(t, root, api.SiblingSize)

	require.NoError(t, cert.Verify(key, value, root, api.SHA256))
}

// TestGetInclusionCert_TwoLeaves covers a two-leaf tree: each proof should
// verify against the same root and the two bitmaps must be identical (shared
// split depth), with exactly one sibling each.
func TestGetInclusionCert_TwoLeaves(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	keyA := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	keyB := mustHex(t, "0200000000000000000000000000000000000000000000000000000000000000")
	valA := []byte("value-a")
	valB := []byte("value-b")

	pathA, err := api.FixedBytesToPath(keyA, api.StateTreeKeyLengthBits)
	require.NoError(t, err)
	pathB, err := api.FixedBytesToPath(keyB, api.StateTreeKeyLengthBits)
	require.NoError(t, err)

	require.NoError(t, tree.AddLeaf(pathA, valA))
	require.NoError(t, tree.AddLeaf(pathB, valB))

	root := tree.GetRootHashRaw()

	certA, err := tree.GetInclusionCert(keyA)
	require.NoError(t, err)
	certB, err := tree.GetInclusionCert(keyB)
	require.NoError(t, err)

	require.Equal(t, certA.Bitmap, certB.Bitmap, "shared split depth → identical bitmaps")
	require.Len(t, certA.Siblings, 1, "two-leaf cert has exactly one sibling")
	require.Len(t, certB.Siblings, 1, "two-leaf cert has exactly one sibling")
	require.Equal(t, 1, bitmapPopcountForTest(&certA.Bitmap))

	require.NoError(t, certA.Verify(keyA, valA, root, api.SHA256))
	require.NoError(t, certB.Verify(keyB, valB, root, api.SHA256))
}

// TestGetInclusionCert_GoldenVector cross-checks the Go inclusion cert
// generator against the frozen golden vector already used by
// golden_vectors_test.go. The same three keys, values and root must produce
// the same bitmap and sibling list under the new wire format.
func TestGetInclusionCert_GoldenVector(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	k1 := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	k2 := mustHex(t, "0300000000000000000000000000000000000000000000000000000000000000")
	k3 := mustHex(t, "0800000000000000000000000000000000000000000000000000000000000000")

	v1 := mustHex(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	v2 := mustHex(t, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	v3 := mustHex(t, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	addLeaf(t, tree, k1, v1)
	addLeaf(t, tree, k2, v2)
	addLeaf(t, tree, k3, v3)

	const expectedRoot = "0000b08cae8f98a168a4b39dced99fc3ea2833291c8c53a0eb447e0056044dee598a"
	require.Equal(t, expectedRoot, tree.GetRootHashHex())

	cert, err := tree.GetInclusionCert(k2)
	require.NoError(t, err)

	const expectedBitmap = "0300000000000000000000000000000000000000000000000000000000000000"
	require.Equal(t, expectedBitmap, hex.EncodeToString(cert.Bitmap[:]),
		"cert bitmap must match golden vector")

	expectedSiblings := []string{
		"4a67e1a8224ab28c6a641ea0a66def8366990856d3b6c6176319c66062821ea6",
		"40c73c53d4a8ba73ccc286520242668a11e66f253da145cd54c5a465ff640552",
	}
	require.Len(t, cert.Siblings, len(expectedSiblings))
	for i, sib := range cert.Siblings {
		require.Equal(t, expectedSiblings[i], hex.EncodeToString(sib[:]),
			"sibling %d must match golden vector", i)
	}

	root := tree.GetRootHashRaw()
	require.NoError(t, cert.Verify(k2, v2, root, api.SHA256))
}

// TestGetInclusionCert_AllKeysVerify builds a tree with several leaves and
// verifies that every leaf's cert round-trips through
// Marshal → Unmarshal → Verify.
func TestGetInclusionCert_AllKeysVerify(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	keys := [][]byte{
		mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000"),
		mustHex(t, "0300000000000000000000000000000000000000000000000000000000000000"),
		mustHex(t, "0800000000000000000000000000000000000000000000000000000000000000"),
		mustHex(t, "ff00000000000000000000000000000000000000000000000000000000000000"),
		mustHex(t, "aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55aa55"),
	}
	values := [][]byte{
		[]byte("value-1"),
		[]byte("value-2"),
		[]byte("value-3"),
		[]byte("value-4"),
		[]byte("value-5"),
	}
	for i := range keys {
		addLeaf(t, tree, keys[i], values[i])
	}

	root := tree.GetRootHashRaw()

	for i := range keys {
		cert, err := tree.GetInclusionCert(keys[i])
		require.NoError(t, err, "GetInclusionCert key=%d", i)

		// Round-trip through wire encoding.
		wire, err := cert.MarshalBinary()
		require.NoError(t, err)
		var decoded api.InclusionCert
		require.NoError(t, decoded.UnmarshalBinary(wire))
		require.Equal(t, cert.Bitmap, decoded.Bitmap)
		require.Equal(t, len(cert.Siblings), len(decoded.Siblings))

		require.NoError(t, decoded.Verify(keys[i], values[i], root, api.SHA256),
			"round-tripped cert must verify for key %d", i)
	}
}

// TestGetInclusionCert_RandomLeaves fuzzes a moderately sized tree with
// random keys and values, and verifies that every cert verifies against the
// SMT root. Uses a deterministic seed via crypto/rand (non-reproducible but
// exercises many tree shapes).
func TestGetInclusionCert_RandomLeaves(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	const n = 64
	keys := make([][]byte, 0, n)
	values := make([][]byte, 0, n)
	seen := make(map[string]struct{}, n)

	for len(keys) < n {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		_, err := rand.Read(key)
		require.NoError(t, err)
		if _, dup := seen[string(key)]; dup {
			continue
		}
		seen[string(key)] = struct{}{}
		val := make([]byte, 16)
		_, err = rand.Read(val)
		require.NoError(t, err)
		addLeaf(t, tree, key, val)
		keys = append(keys, key)
		values = append(values, val)
	}

	root := tree.GetRootHashRaw()

	for i := range keys {
		cert, err := tree.GetInclusionCert(keys[i])
		require.NoError(t, err)
		require.NoError(t, cert.Verify(keys[i], values[i], root, api.SHA256),
			"random cert must verify for key %d", i)
	}
}

// TestGetInclusionCert_WrongValueFails verifies that a cert generated for
// key K cannot be used to attest an incorrect value.
func TestGetInclusionCert_WrongValueFails(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	k1 := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	k2 := mustHex(t, "0300000000000000000000000000000000000000000000000000000000000000")
	v1 := []byte("truth")
	v2 := []byte("other")

	addLeaf(t, tree, k1, v1)
	addLeaf(t, tree, k2, v2)
	root := tree.GetRootHashRaw()

	cert, err := tree.GetInclusionCert(k1)
	require.NoError(t, err)

	err = cert.Verify(k1, []byte("lie"), root, api.SHA256)
	require.Error(t, err, "verifying with wrong value must fail")
}

// TestGetInclusionCert_MissingKey ensures that requesting a cert for a key
// that does not exist in the tree returns an error rather than producing an
// invalid proof.
func TestGetInclusionCert_MissingKey(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	present := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	addLeaf(t, tree, present, []byte("v"))

	missing := mustHex(t, "0200000000000000000000000000000000000000000000000000000000000000")
	cert, err := tree.GetInclusionCert(missing)
	require.Error(t, err)
	require.Nil(t, cert)
}

// TestGetInclusionCert_EmptyTree ensures that calling GetInclusionCert on
// an empty tree fails rather than returning a spurious cert.
func TestGetInclusionCert_EmptyTree(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	key := mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000")

	cert, err := tree.GetInclusionCert(key)
	require.Error(t, err)
	require.Nil(t, cert)
}

// TestGetInclusionCert_PlaceholderLeaf ensures parent-mode placeholder leaves
// are treated as missing entries rather than returning an empty partial cert.
func TestGetInclusionCert_PlaceholderLeaf(t *testing.T) {
	tree := NewParentSparseMerkleTree(api.SHA256, 2)
	key, err := api.PathToFixedBytes(big.NewInt(0b100), 2)
	require.NoError(t, err)

	cert, err := tree.GetInclusionCert(key)
	require.Error(t, err)
	require.Nil(t, cert)
}

// TestGetInclusionCert_WrongKeyLength ensures short/long keys are rejected
// by the Go SMT generator before any traversal starts.
func TestGetInclusionCert_WrongKeyLength(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	addLeaf(t, tree,
		mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000"),
		[]byte("v"))

	cases := []struct {
		name string
		key  []byte
	}{
		{"nil", nil},
		{"short", make([]byte, api.StateTreeKeyLengthBytes-1)},
		{"long", make([]byte, api.StateTreeKeyLengthBytes+1)},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cert, err := tree.GetInclusionCert(tc.key)
			require.Error(t, err)
			require.ErrorIs(t, err, api.ErrCertKeyLength)
			require.Nil(t, cert)
		})
	}
}

// TestGetShardInclusionFragment_SkipsNilSiblingAfterNonNilSibling exercises
// the parent-tree traversal branch where a shallower sibling exists but a
// deeper sibling subtree is empty (nil hash). That depth must be skipped as a
// unary passthrough while keeping previously recorded siblings valid.
func TestGetShardInclusionFragment_SkipsNilSiblingAfterNonNilSibling(t *testing.T) {
	parent := NewParentSparseMerkleTree(api.SHA256, 2)

	// Update two shards on opposite root sides so root-level sibling is present.
	leaf4 := bytes.Repeat([]byte{0xA4}, api.SiblingSize)
	leaf5 := bytes.Repeat([]byte{0xB5}, api.SiblingSize)
	require.NoError(t, parent.AddLeaf(big.NewInt(0b100), leaf4))
	require.NoError(t, parent.AddLeaf(big.NewInt(0b101), leaf5))

	fragment, err := parent.GetShardInclusionFragment(0b100)
	require.NoError(t, err)
	require.NotNil(t, fragment)

	var cert api.InclusionCert
	require.NoError(t, cert.UnmarshalBinary(fragment.CertificateBytes))
	require.Equal(t, 1, bitmapPopcountForTest(&cert.Bitmap), "one deeper depth should be skipped as unary passthrough")
	require.Len(t, cert.Siblings, 1, "only the non-empty shallower sibling should be present")

	root := parent.GetRootHashRaw()
	require.NoError(t, fragment.Verify(0b100, 2, leaf4, root, api.SHA256))
}

// TestGetRootHashRaw_MatchesHex confirms that GetRootHashRaw produces
// the 32-byte hash portion of GetRootHashHex.
func TestGetRootHashRaw_MatchesHex(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	addLeaf(t, tree,
		mustHex(t, "0100000000000000000000000000000000000000000000000000000000000000"),
		[]byte("v1"))
	addLeaf(t, tree,
		mustHex(t, "0300000000000000000000000000000000000000000000000000000000000000"),
		[]byte("v2"))

	rawHex := hex.EncodeToString(tree.GetRootHashRaw())
	require.Len(t, rawHex, 64, "raw root must be 32 bytes hex-encoded")

	fullHex := tree.GetRootHashHex()
	require.Equal(t, "0000"+rawHex, fullHex, "raw root must match the hash portion of the hex root")
}

// addLeaf is a small helper that converts a 32-byte key to its path form
// and inserts it into the tree.
func addLeaf(t *testing.T, tree *SparseMerkleTree, key, value []byte) {
	t.Helper()
	path, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
	require.NoError(t, err)
	require.NoError(t, tree.AddLeaf(path, value))
}

// bitmapPopcountForTest counts set bits in a bitmap. Lives in this test
// file so we don't export the unexported helper from pkg/api.
func bitmapPopcountForTest(b *[api.BitmapSize]byte) int {
	total := 0
	for _, byteVal := range b {
		total += bits8(byteVal)
	}
	return total
}

// bits8 counts set bits in a single byte without pulling math/bits into
// the test file.
func bits8(x byte) int {
	n := 0
	for x != 0 {
		n += int(x & 1)
		x >>= 1
	}
	return n
}
