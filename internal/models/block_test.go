package models

import (
	"encoding/json"
	"math/big"
	"testing"

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

	return &Block{
		Index:               api.NewBigInt(big.NewInt(1)),
		ChainID:             "test-chain-id",
		ShardID:             0b11,
		Version:             "1.0.0",
		ForkID:              "test-fork",
		RootHash:            randomHash,
		PreviousBlockHash:   randomHash,
		NoDeletionProofHash: randomHash,
		CreatedAt:           api.Now(),
		UnicityCertificate:  randomHash,
		ParentFragment: &api.ParentInclusionFragment{
			CertificateBytes: randomHash,
			ShardLeafValue:   randomHash,
		},
		ParentBlockNumber: 7,
	}
}

func TestBlockBSONPartitionViewRoundTrip(t *testing.T) {
	block := NewBlock(api.NewBigIntFromUint64(5), "unicity", 0, "1.0", "mainnet",
		api.NewHexBytes([]byte{1}), api.NewHexBytes([]byte{2}), api.NewHexBytes([]byte{3}), 7)

	t.Run("without view stays null", func(t *testing.T) {
		bb, err := block.ToBSON()
		require.NoError(t, err)
		require.Empty(t, bb.PartitionView)
		back, err := bb.FromBSON()
		require.NoError(t, err)
		require.Nil(t, back.PartitionView)
	})

	t.Run("with view", func(t *testing.T) {
		b := *block
		b.PartitionView = api.NewHexBytes([]byte{0x83, 0x01, 0x02, 0x80})
		bb, err := b.ToBSON()
		require.NoError(t, err)
		require.Equal(t, "83010280", bb.PartitionView)
		back, err := bb.FromBSON()
		require.NoError(t, err)
		require.Equal(t, b.PartitionView, back.PartitionView)
	})

	t.Run("not part of the JSON API", func(t *testing.T) {
		b := *block
		b.PartitionView = api.NewHexBytes([]byte{1})
		data, err := json.Marshal(&b)
		require.NoError(t, err)
		require.NotContains(t, string(data), "artitionView")
	})
}
