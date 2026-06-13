package disk

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestNodeKeyEncodingMatchesRugregatorShape(t *testing.T) {
	require.Equal(t, []byte{0xff, 0xff}, RootNodeKey().Bytes())
	require.True(t, RootNodeKey().IsRoot())

	depthZero, err := NewNodeKey(0, PrefixBits{})
	require.NoError(t, err)
	require.False(t, depthZero.IsRoot())
	require.Equal(t, []byte{0x00, 0x00}, depthZero.Bytes())

	left, err := NewNodeKey(1, PrefixBits{})
	require.NoError(t, err)
	var rightPrefix PrefixBits
	rightPrefix[0] = 0x01
	right, err := NewNodeKey(1, rightPrefix)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01, 0x00, 0x00}, left.Bytes())
	require.Equal(t, []byte{0x01, 0x00, 0x01}, right.Bytes())

	var deepPrefix PrefixBits
	deepPrefix[0] = 0x01
	deepPrefix[1] = 0x01
	deep, err := NewNodeKey(9, deepPrefix)
	require.NoError(t, err)
	require.Equal(t, []byte{0x09, 0x00, 0x01, 0x01}, deep.Bytes())

	parsed, err := ParseNodeKey(deep.Bytes())
	require.NoError(t, err)
	require.Equal(t, deep, parsed)
	require.Equal(t, 9, parsed.DepthBits())
}

func TestParseNodeKeyRejectsNonCanonicalPrefixBits(t *testing.T) {
	_, err := ParseNodeKey([]byte{0x09, 0x00, 0x01, 0x03})
	require.Error(t, err)
}

func TestNodeKeyLessMatchesEncodedOrder(t *testing.T) {
	keys := []NodeKey{
		RootNodeKey(),
		mustNodeKey(t, 0, PrefixBits{}),
		mustNodeKey(t, 1, PrefixBits{}),
		mustNodeKey(t, 1, prefixBits(0x01)),
		mustNodeKey(t, 9, prefixBits(0x01, 0x01)),
		mustNodeKey(t, 16, prefixBits(0xff, 0x00)),
		mustNodeKey(t, 256, prefixBits(0xff, 0xff, 0xff, 0xff)),
	}

	for _, left := range keys {
		for _, right := range keys {
			want := bytes.Compare(left.Bytes(), right.Bytes()) < 0
			require.Equalf(t, want, left.Less(right), "%x < %x", left.Bytes(), right.Bytes())
		}
	}
}

func TestCompressedPathFromKeyRange(t *testing.T) {
	key := mustKey(t, "a501000000000000000000000000000000000000000000000000000000000000")

	path, err := NewCompressedPathFromKeyRange(key, 0, 13)
	require.NoError(t, err)
	require.Equal(t, 13, path.Len())
	require.Equal(t, byte(1), path.BitAt(0))
	require.Equal(t, byte(0), path.BitAt(1))
	require.Equal(t, byte(1), path.BitAt(2))
	require.Equal(t, []byte{0xa5, 0x01}, path.Bytes())

	roundTrip, err := NewCompressedPathFromRaw(path.Len(), path.Bytes())
	require.NoError(t, err)
	require.True(t, path.Equal(roundTrip))
}

func TestCompressedPathRejectsNonCanonicalRawBits(t *testing.T) {
	_, err := NewCompressedPathFromRaw(13, []byte{0xa5, 0xe1})
	require.Error(t, err)
}

func TestHashLeafMatchesMemorySingleLeafRoot(t *testing.T) {
	key := mustKey(t, "0100000000000000000000000000000000000000000000000000000000000000")
	value := []byte("value-one")

	got := HashLeaf(key, value)
	require.Equal(t, memoryRootAfterLeaves(t, leafInput{key: key, value: value}), got)
}

func TestEmptyRootMatchesMemory(t *testing.T) {
	require.Equal(t, emptyMemoryRoot(t), EmptyRootHash())
	require.Equal(t, emptyMemoryRoot(t), memoryRootAfterLeaves(t))
}

func TestUnaryPassthroughMatchesMemorySingleLeafRoot(t *testing.T) {
	key := mustKey(t, "0100000000000000000000000000000000000000000000000000000000000000")
	value := []byte("value-one")

	leafHash := HashLeaf(key, value)

	require.Equal(t, leafHash, memoryRootAfterLeaves(t, leafInput{key: key, value: value}))
}

func TestHashNodeMatchesMemoryAndGoldenRoot(t *testing.T) {
	k1 := mustKey(t, "0100000000000000000000000000000000000000000000000000000000000000")
	k2 := mustKey(t, "0200000000000000000000000000000000000000000000000000000000000000")
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	l1 := NewLeaf(k1, v1)
	l2 := NewLeaf(k2, v2)

	var left, right *Branch
	if KeyBit(k1, 0) == 0 {
		left, right = l1, l2
	} else {
		left, right = l2, l1
	}

	root, err := NewInternal(EmptyPath(), 0, left, right)
	require.NoError(t, err)
	got, err := root.HashValue()
	require.NoError(t, err)

	require.Equal(t, memoryRootAfterLeaves(
		t,
		leafInput{key: k1, value: v1},
		leafInput{key: k2, value: v2},
	), got)
	require.Equal(t, mustHash(t, "20563433422d651813394a07697b9c09f9c2ab2ddb95eaa8ed2dc3211de3e869"), got)
}

func TestLeafSerializationRoundTrip(t *testing.T) {
	key := mustKey(t, "0300000000000000000000000000000000000000000000000000000000000000")
	leaf := NewLeaf(key, []byte("leaf-value")).Leaf

	encoded, err := MarshalLeaf(leaf)
	require.NoError(t, err)
	require.Equal(t, TagLeaf, encoded[0])

	tag, err := SerializedTag(encoded)
	require.NoError(t, err)
	require.Equal(t, TagLeaf, tag)

	decoded, err := UnmarshalLeaf(encoded)
	require.NoError(t, err)
	require.Equal(t, leaf.Key, decoded.Key)
	require.Equal(t, leaf.Value, decoded.Value)
	require.Equal(t, leaf.Hash, decoded.Hash)
}

func TestInternalSerializationRoundTrip(t *testing.T) {
	key := mustKey(t, "a501000000000000000000000000000000000000000000000000000000000000")
	path, err := NewCompressedPathFromKeyRange(key, 0, 13)
	require.NoError(t, err)

	left := NewLeaf(mustKey(t, "0000000000000000000000000000000000000000000000000000000000000000"), []byte("left"))
	right := NewLeaf(mustKey(t, "0100000000000000000000000000000000000000000000000000000000000000"), []byte("right"))
	branch, err := NewInternal(path, 13, left, right)
	require.NoError(t, err)

	encoded, err := MarshalInternal(branch.Internal)
	require.NoError(t, err)

	leftHash, err := left.HashValue()
	require.NoError(t, err)
	rightHash, err := right.HashValue()
	require.NoError(t, err)
	expected := []byte{TagInternal, 13, 13, 0xa5, 0x01}
	expected = append(expected, leftHash[:]...)
	expected = append(expected, rightHash[:]...)
	require.Equal(t, expected, encoded)

	decoded, err := UnmarshalInternal(encoded)
	require.NoError(t, err)
	require.Equal(t, branch.Internal.Depth, decoded.Depth)
	require.True(t, branch.Internal.Path.Equal(decoded.Path))
	require.Equal(t, branch.Internal.Hash, decoded.Hash)
	decodedLeftHash, err := decoded.Left.HashValue()
	require.NoError(t, err)
	decodedRightHash, err := decoded.Right.HashValue()
	require.NoError(t, err)
	require.Equal(t, leftHash, decodedLeftHash)
	require.Equal(t, rightHash, decodedRightHash)

	withTrailing := append(append([]byte(nil), encoded...), branch.Internal.Hash[:]...)
	_, err = UnmarshalInternal(withTrailing)
	require.Error(t, err)
}

func TestUnmarshalInternalRejectsMalformedData(t *testing.T) {
	left := NewLeaf(mustKey(t, "0000000000000000000000000000000000000000000000000000000000000000"), []byte("left"))
	right := NewLeaf(mustKey(t, "0100000000000000000000000000000000000000000000000000000000000000"), []byte("right"))
	branch, err := NewInternal(EmptyPath(), 0, left, right)
	require.NoError(t, err)

	encoded, err := MarshalInternal(branch.Internal)
	require.NoError(t, err)

	_, err = UnmarshalInternal(append([]byte(nil), encoded[:len(encoded)-1]...))
	require.Error(t, err)

	withTrailing := append(append([]byte(nil), encoded...), 0x00)
	_, err = UnmarshalInternal(withTrailing)
	require.Error(t, err)

	withWrongTag := append([]byte(nil), encoded...)
	withWrongTag[0] = TagLeaf
	_, err = UnmarshalInternal(withWrongTag)
	require.Error(t, err)

	nonCanonicalPath := []byte{TagInternal, 13, 13, 0xa5, 0xe1}
	nonCanonicalPath = append(nonCanonicalPath, make([]byte, 2*HashSize)...)
	_, err = UnmarshalInternal(nonCanonicalPath)
	require.Error(t, err)

	_, err = UnmarshalInternal([]byte{TagInternal, 0, 1})
	require.Error(t, err)
}

func TestUnmarshalLeafRejectsMalformedData(t *testing.T) {
	key := mustKey(t, "0300000000000000000000000000000000000000000000000000000000000000")
	leaf := NewLeaf(key, []byte("leaf-value")).Leaf

	encoded, err := MarshalLeaf(leaf)
	require.NoError(t, err)

	_, err = UnmarshalLeaf(append([]byte(nil), encoded[:len(encoded)-1]...))
	require.Error(t, err)

	withTrailing := append(append([]byte(nil), encoded...), 0x00)
	_, err = UnmarshalLeaf(withTrailing)
	require.Error(t, err)

	badVarint := append([]byte{TagLeaf}, key[:]...)
	badVarint = append(badVarint, 0x80)
	_, err = UnmarshalLeaf(badVarint)
	require.Error(t, err)
}

type leafInput struct {
	key   Key
	value []byte
}

func memoryRootAfterLeaves(t *testing.T, leaves ...leafInput) Hash {
	t.Helper()

	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	for _, leaf := range leaves {
		path, err := api.FixedBytesToPath(leaf.key[:], api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		require.NoError(t, tree.AddLeaf(path, leaf.value))
	}

	return mustHashFromBytes(t, tree.GetRootHashRaw())
}

func emptyMemoryRoot(t *testing.T) Hash {
	t.Helper()
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	return mustHashFromBytes(t, tree.GetRootHashRaw())
}

func mustKey(t *testing.T, s string) Key {
	t.Helper()
	data, err := hex.DecodeString(s)
	require.NoError(t, err)
	key, err := KeyFromBytes(data)
	require.NoError(t, err)
	return key
}

func mustNodeKey(t *testing.T, depth int, prefix PrefixBits) NodeKey {
	t.Helper()
	key, err := NewNodeKey(depth, prefix)
	require.NoError(t, err)
	return key
}

func prefixBits(values ...byte) PrefixBits {
	var prefix PrefixBits
	copy(prefix[:], values)
	return prefix
}

func mustHash(t *testing.T, s string) Hash {
	t.Helper()
	data, err := hex.DecodeString(s)
	require.NoError(t, err)
	return mustHashFromBytes(t, data)
}

func mustHashFromBytes(t *testing.T, data []byte) Hash {
	t.Helper()
	hash, err := HashFromBytes(data)
	require.NoError(t, err)
	return hash
}
