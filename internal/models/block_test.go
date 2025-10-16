package models

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBlock_ToBSONFromBSON(t *testing.T) {
	block := createTestBlock()

	blockBSON, err := block.ToBSON()
	require.NoError(t, err)

	blockFromBSON, err := blockBSON.FromBSON()
	require.NoError(t, err)

	require.Equal(t, block, blockFromBSON)
}

func createTestBlock() *Block {
	randomHash, _ := api.NewHexBytesFromString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

	// create timestamp with millisecond precision so that no information is lost
	// after storing the block
	createdAt := &api.Timestamp{Time: time.UnixMilli(time.Now().UnixMilli())}

	return &Block{
		Index:               api.NewBigInt(big.NewInt(1)),
		ChainID:             "test-chain-id",
		ShardID:             0b11,
		Version:             "1.0.0",
		ForkID:              "test-fork",
		RootHash:            randomHash,
		PreviousBlockHash:   randomHash,
		NoDeletionProofHash: randomHash,
		CreatedAt:           createdAt,
		UnicityCertificate:  randomHash,
		ParentMerkleTreePath: &api.MerkleTreePath{
			Root: randomHash.String(),
		},
	}
}
