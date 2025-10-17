package api_test

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Test helper to create a leaf
func createLeaf(path int64, value []byte) *smt.Leaf {
	return &smt.Leaf{
		Path:  big.NewInt(path),
		Value: value,
	}
}

// TestMerkleTreePathVerify tests comprehensive verification scenarios
func TestMerkleTreePathVerify(t *testing.T) {
	t.Run("SingleLeaf", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 5)

		leaf := createLeaf(42, []byte("test"))
		err := tree.AddLeaves([]*smt.Leaf{leaf})
		require.NoError(t, err)

		path, err := tree.GetPath(big.NewInt(42))
		require.NoError(t, err)
		require.NotNil(t, path)

		result, err := path.Verify(big.NewInt(42))
		require.NoError(t, err)
		require.True(t, result.PathIncluded, "Path should be included")
		require.True(t, result.PathValid, "Path should be valid")
	})

	t.Run("TwoLeaves", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 3)

		leaves := []*smt.Leaf{
			createLeaf(10, []byte("leaf10")),
			createLeaf(12, []byte("leaf12")),
		}
		err := tree.AddLeaves(leaves)
		require.NoError(t, err)

		// Verify both paths
		for _, leafPath := range []int64{10, 12} {
			path, err := tree.GetPath(big.NewInt(leafPath))
			require.NoError(t, err)
			require.NotNil(t, path)

			result, err := path.Verify(big.NewInt(leafPath))
			require.NoError(t, err)
			require.True(t, result.PathIncluded, "Path %d should be included", leafPath)
			require.True(t, result.PathValid, "Path %d should be valid", leafPath)
		}
	})

	t.Run("MultipleLeaves", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 48)

		// Add multiple leaves with various paths
		paths := []int64{10, 100, 1000, 10000, 100000}
		var leaves []*smt.Leaf
		for i, p := range paths {
			leaves = append(leaves, createLeaf(0x1000000000000+p, []byte("leaf"+string(rune(p)))))
			t.Logf("%d %s", i, leaves[i].Path.Text(2))
		}

		err := tree.AddLeaves(leaves)
		require.NoError(t, err)

		// Verify each path
		for _, p := range paths {
			path, err := tree.GetPath(big.NewInt(0x1000000000000 + p))
			require.NoError(t, err)
			require.NotNil(t, path)

			result, err := path.Verify(big.NewInt(0x1000000000000 + p))
			require.NoError(t, err)
			require.True(t, result.PathIncluded, "Path %d should be included", p)
			require.True(t, result.PathValid, "Path %d should be valid", p)
		}
	})

	t.Run("LargePaths", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 272)

		// Test with the actual large paths from the failing test
		mintPath, _ := new(big.Int).SetString("7588607046638288532898314259371162887598150843702815116345200719347816808430746270", 10)
		transferPath, _ := new(big.Int).SetString("7588595804959218369815512972651793411311840553453637142956782535261123804631684864", 10)

		leaves := []*smt.Leaf{
			{Path: mintPath, Value: []byte("mint")},
			{Path: transferPath, Value: []byte("transfer")},
		}

		err := tree.AddLeaves(leaves)
		require.NoError(t, err)

		// Verify transfer path
		path, err := tree.GetPath(transferPath)
		require.NoError(t, err)
		require.NotNil(t, path)

		result, err := path.Verify(transferPath)
		require.NoError(t, err)
		require.True(t, result.PathIncluded, "Transfer path should be included")
		require.True(t, result.PathValid, "Transfer path should be valid")

		// Verify mint path
		pathMint, err := tree.GetPath(mintPath)
		require.NoError(t, err)
		require.NotNil(t, pathMint)

		resultMint, err := pathMint.Verify(mintPath)
		require.NoError(t, err)
		require.True(t, resultMint.PathIncluded, "Mint path should be included")
		require.True(t, resultMint.PathValid, "Mint path should be valid")
	})

	t.Run("NonExistentPath", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 9)

		leaves := []*smt.Leaf{
			createLeaf(1000, []byte("exists")),
		}
		err := tree.AddLeaves(leaves)
		require.NoError(t, err)

		// For a sparse merkle tree, getting a path for a non-existent leaf returns
		// a valid path showing where that leaf would be inserted. Since leaf 1000 goes
		// left (bit 0 = 0) and 999 would go right (bit 0 = 1), we get a path to the
		// empty right branch with the left subtree as sibling.
		path, err := tree.GetPath(big.NewInt(999))
		require.NoError(t, err)
		require.NotNil(t, path)

		// When we verify this path with requestId 999:
		// - PathIncluded should be false (999 is not in the tree)
		// - PathValid should be true (the path is cryptographically valid)
		result, err := path.Verify(big.NewInt(999))
		require.NoError(t, err)
		require.False(t, result.PathIncluded, "Path for 999 should not be included")
		require.True(t, result.PathValid, "Path should be cryptographically valid")
	})

	t.Run("CrossVerification", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 12)

		leaves := []*smt.Leaf{
			createLeaf(0x1000+5, []byte("five")),
			createLeaf(0x1000+15, []byte("fifteen")),
		}
		err := tree.AddLeaves(leaves)
		require.NoError(t, err)

		// Get path for 5
		path5, err := tree.GetPath(big.NewInt(0x1000 + 5))
		require.NoError(t, err)

		// Try to verify with wrong requestId
		result, err := path5.Verify(big.NewInt(0x1000 + 15))
		require.NoError(t, err)
		// The verification checks if the path can be reconstructed to match the requestId
		// Since we're using path for 5 with requestId 15, PathIncluded should be false
		require.False(t, result.PathIncluded, "Path for 5 should not include requestId 15")
		// PathValid checks cryptographic validity which should still be true
		require.True(t, result.PathValid, "Path should still be cryptographically valid")
	})

	t.Run("BinaryPatterns", func(t *testing.T) {
		// Test various binary patterns that might cause issues
		testCases := []struct {
			name  string
			paths []int64
		}{
			{"complete", []int64{0b100, 0b101, 0b110, 0b111}},
			{"chain", []int64{0b1000000, 0b1000001, 0b1000010, 0b1000100, 0b1001000, 0b1010000, 0b1100000}},
			{"sparse", []int64{0x100000 + 10, 0x100000 + 1000, 0x100000 + 1000000}},
			{"mixed", []int64{0b10000011, 0b10000111, 0b10001111, 0b10011111, 0b10111111}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tree := smt.NewSparseMerkleTree(api.SHA256, big.NewInt(tc.paths[0]).BitLen()-1)

				var leaves []*smt.Leaf
				for _, p := range tc.paths {
					leaves = append(leaves, createLeaf(p, []byte("data")))
				}

				err := tree.AddLeaves(leaves)
				require.NoError(t, err)

				for _, p := range tc.paths {
					path, err := tree.GetPath(big.NewInt(p))
					require.NoError(t, err)
					result, err := path.Verify(big.NewInt(p))
					require.NoError(t, err)
					require.True(t, result.PathIncluded && result.PathValid,
						"Path %d should be valid for pattern %s", p, tc.name)
				}
			})
		}
	})

	t.Run("RealRequestIDs", func(t *testing.T) {
		// Test with actual requestID format (34-byte with algorithm prefix)
		tree := smt.NewSparseMerkleTree(api.SHA256, 16+256)

		// Create requestIDs with proper format
		requestID1 := "00007d535ade796772c5088b095e79a18e282437ee8d8238f5aa9d9c61694948ba9e"
		requestID2 := "00006478ca42f6949cfbd4b9e4a41b9a384ea78261c1776808da70cf21e98c345700"

		req1, err := api.NewImprintHexString(requestID1)
		require.NoError(t, err)
		path1, err := req1.GetPath()
		require.NoError(t, err)

		req2, err := api.NewImprintHexString(requestID2)
		require.NoError(t, err)
		path2, err := req2.GetPath()
		require.NoError(t, err)

		// Add to tree
		leaves := []*smt.Leaf{
			{Path: path1, Value: []byte("value1")},
			{Path: path2, Value: []byte("value2")},
		}
		err = tree.AddLeaves(leaves)
		require.NoError(t, err)

		// Verify paths
		treePath1, err := tree.GetPath(path1)
		require.NoError(t, err)
		result1, err := treePath1.Verify(path1)
		require.NoError(t, err)
		require.True(t, result1.PathIncluded && result1.PathValid,
			"RequestID1 path should be valid")

		treePath2, err := tree.GetPath(path2)
		require.NoError(t, err)
		result2, err := treePath2.Verify(path2)
		require.NoError(t, err)
		require.True(t, result2.PathIncluded && result2.PathValid,
			"RequestID2 path should be valid")
	})

	t.Run("ConsistencyAfterMultipleAdds", func(t *testing.T) {
		tree := smt.NewSparseMerkleTree(api.SHA256, 20)

		// Add leaves incrementally
		for i := int64(1); i <= 5; i++ {
			leaf := createLeaf(0x100000+i*100, []byte("leaf"))
			err := tree.AddLeaves([]*smt.Leaf{leaf})
			require.NoError(t, err)

			// Verify all previously added leaves still work
			for j := int64(1); j <= i; j++ {
				path, err := tree.GetPath(big.NewInt(0x100000 + j*100))
				require.NoError(t, err)
				result, err := path.Verify(big.NewInt(0x100000 + j*100))
				require.NoError(t, err)
				require.True(t, result.PathIncluded && result.PathValid,
					"Path %d should remain valid after adding leaf %d", j*100, i*100)
			}
		}
	})
}

// TestMerkleTreePathVerifyEdgeCases tests edge cases and error conditions
func TestMerkleTreePathVerifyEdgeCases(t *testing.T) {
	t.Run("InvalidPathString", func(t *testing.T) {
		path := &api.MerkleTreePath{
			Root: "0000abcd",
			Steps: []api.MerkleTreeStep{
				{Path: "invalid", Data: nil},
			},
		}

		_, err := path.Verify(big.NewInt(1))
		require.Error(t, err, "Should error on invalid path string")
	})

	t.Run("InvalidDataHex", func(t *testing.T) {
		data := "invalid"
		path := &api.MerkleTreePath{
			Root: "0000abcd",
			Steps: []api.MerkleTreeStep{
				{Path: "1", Data: &data},
			},
		}

		_, err := path.Verify(big.NewInt(1))
		require.Error(t, err, "Should error on invalid branch hex")
	})

	t.Run("EmptySteps", func(t *testing.T) {
		path := &api.MerkleTreePath{
			Root:  "0000abcd",
			Steps: []api.MerkleTreeStep{},
		}

		result, err := path.Verify(big.NewInt(1))
		require.NoError(t, err)
		require.False(t, result.PathValid, "Empty steps should not be valid")
	})
}

// TestMerkleTreePathVerifyDuplicates tests handling of duplicate leaves
func TestMerkleTreePathVerifyDuplicates(t *testing.T) {
	tree := smt.NewSparseMerkleTree(api.SHA256, 6)

	// Add a leaf
	leaf1 := createLeaf(100, []byte("original"))
	err := tree.AddLeaves([]*smt.Leaf{leaf1})
	require.NoError(t, err)

	// Try to add new value (should error)
	leaf2 := createLeaf(100, []byte("modified"))
	err = tree.AddLeaves([]*smt.Leaf{leaf2})
	require.Error(t, err)

	// Verify the original value is still there
	path, err := tree.GetPath(big.NewInt(100))
	require.NoError(t, err)
	result, err := path.Verify(big.NewInt(100))
	require.NoError(t, err)
	require.True(t, result.PathIncluded && result.PathValid,
		"Original leaf should still be valid")

	// The leaf value should be the original
	require.NotEmpty(t, path.Steps)
	value, _ := hex.DecodeString(*path.Steps[0].Data)
	require.Equal(t, []byte("original"), value, "Should have original value")
}

func TestMerkleTreePathVerifyAlternateAlgorithm(t *testing.T) {
	leaves := []*smt.Leaf{
		createLeaf(0b10101, []byte{1, 0, 1, 0}),
		createLeaf(0b11010, []byte{0, 1, 0, 1}),
	}

	for _, algo := range []api.HashAlgorithm{api.SHA256, api.SHA3_256} {
		t.Run(fmt.Sprintf("Algorithm %d", algo), func(t *testing.T) {
			tree := smt.NewSparseMerkleTree(algo, 4)
			tree.AddLeaves(leaves)
			root := tree.GetRootHashHex()
			require.Equal(t, root[:4], fmt.Sprintf("%04x", algo))

			for _, leaf := range leaves {
				path, err := tree.GetPath(leaf.Path)
				require.NoError(t, err)
				require.Equal(t, root, path.Root)
				res, err := path.Verify(leaf.Path)
				require.NoError(t, err)
				require.True(t, res.Result)
			}
		})
	}
}
