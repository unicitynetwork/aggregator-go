package smt

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestGoldenVector_RootMatches(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	k1 := mustPathFromKeyHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	k2 := mustPathFromKeyHex(t, "0200000000000000000000000000000000000000000000000000000000000000")

	require.NoError(t, tree.AddLeaf(k1, []byte("value-one")))
	require.NoError(t, tree.AddLeaf(k2, []byte("value-two")))

	const expectedRoot = "000020563433422d651813394a07697b9c09f9c2ab2ddb95eaa8ed2dc3211de3e869"
	require.Equal(t, expectedRoot, tree.GetRootHashHex())
}

func TestGoldenVector_ProofBitmapAndSiblingsMatch(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)

	k1 := mustPathFromKeyHex(t, "0100000000000000000000000000000000000000000000000000000000000000")
	k2 := mustPathFromKeyHex(t, "0300000000000000000000000000000000000000000000000000000000000000")
	k3 := mustPathFromKeyHex(t, "0800000000000000000000000000000000000000000000000000000000000000")

	v1 := mustHex(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	v2 := mustHex(t, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	v3 := mustHex(t, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	require.NoError(t, tree.AddLeaf(k1, v1))
	require.NoError(t, tree.AddLeaf(k2, v2))
	require.NoError(t, tree.AddLeaf(k3, v3))

	const expectedRoot = "0000b08cae8f98a168a4b39dced99fc3ea2833291c8c53a0eb447e0056044dee598a"
	require.Equal(t, expectedRoot, tree.GetRootHashHex())

	path, err := tree.GetPath(k2)
	require.NoError(t, err)
	require.NotNil(t, path)

	vr, err := path.Verify(k2)
	require.NoError(t, err)
	require.True(t, vr.PathValid)
	require.True(t, vr.PathIncluded)
	require.True(t, vr.Result)

	bitmap, siblings, err := pathToBitmapAndSiblings(path, k2.BitLen()-1)
	require.NoError(t, err)

	const expectedBitmap = "0300000000000000000000000000000000000000000000000000000000000000"
	require.Equal(t, expectedBitmap, hex.EncodeToString(bitmap[:]))

	expectedSiblings := []string{
		"4a67e1a8224ab28c6a641ea0a66def8366990856d3b6c6176319c66062821ea6",
		"40c73c53d4a8ba73ccc286520242668a11e66f253da145cd54c5a465ff640552",
	}
	require.Len(t, siblings, len(expectedSiblings))
	for i, s := range siblings {
		require.Equal(t, expectedSiblings[i], hex.EncodeToString(s))
	}
}

func pathToBitmapAndSiblings(path *api.MerkleTreePath, fullKeyBits int) ([32]byte, [][]byte, error) {
	var bitmap [32]byte
	if path == nil {
		return bitmap, nil, fmt.Errorf("nil path")
	}
	if len(path.Steps) == 0 {
		return bitmap, nil, fmt.Errorf("empty path")
	}

	currentPath, ok := new(big.Int).SetString(path.Steps[0].Path, 10)
	if !ok || currentPath.Sign() < 0 {
		return bitmap, nil, fmt.Errorf("invalid first path segment")
	}

	siblings := make([][]byte, 0, len(path.Steps))
	for i := 1; i < len(path.Steps); i++ {
		step := path.Steps[i]
		stepPath, ok := new(big.Int).SetString(step.Path, 10)
		if !ok || stepPath.Sign() < 0 {
			return bitmap, nil, fmt.Errorf("invalid step path at index %d", i)
		}

		var depth int
		if currentPath.BitLen() >= 2 {
			depth = fullKeyBits - (currentPath.BitLen() - 1)
		} else {
			depth = stepPath.BitLen() - 1
		}
		if depth < 0 || depth > 255 {
			return bitmap, nil, fmt.Errorf("invalid depth %d at index %d", depth, i)
		}

		if step.Data != nil {
			sibling, err := hex.DecodeString(*step.Data)
			if err != nil {
				return bitmap, nil, fmt.Errorf("invalid sibling hex at index %d: %w", i, err)
			}
			if len(sibling) != 32 {
				return bitmap, nil, fmt.Errorf("invalid sibling length at index %d: %d", i, len(sibling))
			}
			bitmap[depth/8] |= 1 << (depth % 8)
			siblings = append(siblings, sibling)
		}

		if currentPath.BitLen() < 2 {
			currentPath = big.NewInt(1)
		}
		pathLen := stepPath.BitLen() - 1
		if pathLen < 0 {
			return bitmap, nil, fmt.Errorf("invalid path length at index %d", i)
		}
		mask := new(big.Int).SetBit(new(big.Int).Set(stepPath), pathLen, 0)
		currentPath.Lsh(currentPath, uint(pathLen))
		currentPath.Or(currentPath, mask)
	}

	// Current Go proof API (`MerkleTreePath.Steps`) is emitted leaf-to-root.
	// Rugregator bitmap proofs serialize siblings root-to-leaf.
	// This reverse is only for test-vector normalization between the two wire
	// formats; it does not change tree/hash semantics.
	for i, j := 0, len(siblings)-1; i < j; i, j = i+1, j-1 {
		siblings[i], siblings[j] = siblings[j], siblings[i]
	}

	return bitmap, siblings, nil
}

func mustPathFromKeyHex(t *testing.T, hexKey string) *big.Int {
	t.Helper()
	key := mustHex(t, hexKey)
	p, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
	require.NoError(t, err)
	return p
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
