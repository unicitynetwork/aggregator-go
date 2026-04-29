package smt

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestLeafHash_DomainSeparated(t *testing.T) {
	leaf := newLeafBranch(big.NewInt(0b10101), []byte("value"))
	hasher := api.NewDataHasher(api.SHA256)

	got := leaf.calculateHash(hasher)

	expectedHasher := api.NewDataHasher(api.SHA256)
	expectedHasher.Reset().
		AddData([]byte{0x00}).
		AddData(leaf.Key).
		AddData(leaf.Value)
	expected := expectedHasher.GetHash().RawHash

	require.Equal(t, expected, got)
}

func TestNodeHash_UnaryPassthrough(t *testing.T) {
	leftLeaf := newLeafBranch(big.NewInt(0b101), []byte("left"))
	node := newNodeBranch(big.NewInt(0b10), leftLeaf, nil)

	hasher := api.NewDataHasher(api.SHA256)
	got := node.calculateHash(hasher)

	require.Equal(t, leftLeaf.calculateHash(api.NewDataHasher(api.SHA256)), got)
}

func TestNodeHash_BinaryDomainSeparated(t *testing.T) {
	leftLeaf := newLeafBranch(big.NewInt(0b10), []byte("left"))
	rightLeaf := newLeafBranch(big.NewInt(0b11), []byte("right"))
	node := newNodeBranch(big.NewInt(0b10), leftLeaf, rightLeaf)

	hasher := api.NewDataHasher(api.SHA256)
	got := node.calculateHash(hasher)

	expectedHasher := api.NewDataHasher(api.SHA256)
	expectedHasher.Reset().
		AddData([]byte{0x01, node.Depth}).
		AddData(leftLeaf.calculateHash(api.NewDataHasher(api.SHA256))).
		AddData(rightLeaf.calculateHash(api.NewDataHasher(api.SHA256)))
	expected := expectedHasher.GetHash().RawHash

	require.Equal(t, expected, got)
}
