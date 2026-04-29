package smt

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestThreadSafeSMT_AddPreHashedLeaf_StoresChildRoot(t *testing.T) {
	tree := NewThreadSafeSMT(NewParentSparseMerkleTree(api.SHA256, 2))

	path := big.NewInt(4) // shard ID with sentinel bit for a 2-bit parent tree
	hash := bytes.Repeat([]byte{0xab}, 32)

	require.NoError(t, tree.AddPreHashedLeaf(path, hash))

	leaf, err := tree.GetLeaf(path)
	require.NoError(t, err)
	require.Equal(t, hash, leaf.Value)
	require.True(t, leaf.isChild)
}

func TestThreadSafeSMT_GetShardInclusionFragment_ReturnsNativeParentFragment(t *testing.T) {
	tree := NewThreadSafeSMT(NewParentSparseMerkleTree(api.SHA256, 2))

	path := big.NewInt(4) // shard ID with sentinel bit for a 2-bit parent tree
	hash := bytes.Repeat([]byte{0xcd}, 32)

	require.NoError(t, tree.AddPreHashedLeaf(path, hash))

	fragment, err := tree.GetShardInclusionFragment(4)
	require.NoError(t, err)
	require.NotNil(t, fragment)
	require.Equal(t, hash, []byte(fragment.ShardLeafValue))
	require.GreaterOrEqual(t, len(fragment.CertificateBytes), api.BitmapSize)
	require.Equal(t, 0, len(fragment.CertificateBytes)%api.SiblingSize)
	require.NoError(t, fragment.Verify(4, 2, hash, tree.GetRootHashRaw(), api.SHA256))
}

func TestThreadSafeSMT_PrimesHashesOnConstruction(t *testing.T) {
	tree := NewThreadSafeSMT(NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	requireBranchHashesPrimed(t, tree.smt.root)
}

func TestThreadSafeSMTSnapshot_PrimesHashesOnConstruction(t *testing.T) {
	tree := NewThreadSafeSMT(NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	snapshot := tree.CreateSnapshot()
	requireBranchHashesPrimed(t, snapshot.snapshot.root)
}

func requireBranchHashesPrimed(t *testing.T, b branch) {
	t.Helper()

	switch node := b.(type) {
	case *LeafBranch:
		require.True(t, node.hashSet)
	case *NodeBranch:
		require.True(t, node.hashSet)
		if node.Left != nil {
			requireBranchHashesPrimed(t, node.Left)
		}
		if node.Right != nil {
			requireBranchHashesPrimed(t, node.Right)
		}
	default:
		require.Failf(t, "unexpected branch type", "%T", b)
	}
}
