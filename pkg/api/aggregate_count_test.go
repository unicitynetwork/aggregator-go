package api_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestAggregateRequestCountSerialization(t *testing.T) {
	t.Run("SubmitCommitmentRequest JSON serialization", func(t *testing.T) {
		req := &api.SubmitCommitmentRequest{
			RequestID:       "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			Authenticator: api.Authenticator{
				Algorithm: "secp256k1",
				PublicKey: api.HexBytes{0x01, 0x02, 0x03},
				Signature: api.HexBytes{0x04, 0x05, 0x06},
				StateHash: "0000abcd",
			},
			AggregateRequestCount: 100,
		}

		// Marshal to JSON
		data, err := json.Marshal(req)
		require.NoError(t, err)

		// Check that aggregateRequestCount is serialized as string
		var jsonMap map[string]interface{}
		err = json.Unmarshal(data, &jsonMap)
		require.NoError(t, err)
		
		aggregateCount, exists := jsonMap["aggregateRequestCount"]
		require.True(t, exists, "aggregateRequestCount should exist in JSON")
		require.Equal(t, "100", aggregateCount, "aggregateRequestCount should be serialized as string")

		// Unmarshal back
		var decoded api.SubmitCommitmentRequest
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, uint64(100), decoded.AggregateRequestCount)
	})

	t.Run("GetBlockResponse JSON serialization", func(t *testing.T) {
		blockIndex := api.NewBigInt(nil)
		blockIndex.SetInt64(1)
		resp := &api.GetBlockResponse{
			Block: &api.Block{
				Index:   blockIndex,
				ChainID: "test",
			},
			TotalCommitments: 186,
		}

		// Marshal to JSON
		data, err := json.Marshal(resp)
		require.NoError(t, err)

		// Check that totalCommitments is serialized as string
		var jsonMap map[string]interface{}
		err = json.Unmarshal(data, &jsonMap)
		require.NoError(t, err)
		
		totalCommitments, exists := jsonMap["totalCommitments"]
		require.True(t, exists, "totalCommitments should exist in JSON")
		require.Equal(t, "186", totalCommitments, "totalCommitments should be serialized as string")

		// Unmarshal back
		var decoded api.GetBlockResponse
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, uint64(186), decoded.TotalCommitments)
	})

	t.Run("AggregatorRecord JSON serialization", func(t *testing.T) {
		blockNumber := api.NewBigInt(nil)
		blockNumber.SetInt64(1)
		leafIndex := api.NewBigInt(nil)
		leafIndex.SetInt64(0)
		
		record := &api.AggregatorRecord{
			RequestID:             "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			TransactionHash:       "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			Authenticator: api.Authenticator{
				Algorithm: "secp256k1",
				PublicKey: api.HexBytes{0x01, 0x02, 0x03},
				Signature: api.HexBytes{0x04, 0x05, 0x06},
				StateHash: "0000abcd",
			},
			AggregateRequestCount: 500,
			BlockNumber:           blockNumber,
			LeafIndex:             leafIndex,
			CreatedAt:             api.Now(),
			FinalizedAt:           api.Now(),
		}

		// Marshal to JSON
		data, err := json.Marshal(record)
		require.NoError(t, err)

		// Check that aggregateRequestCount is serialized as string
		var jsonMap map[string]interface{}
		err = json.Unmarshal(data, &jsonMap)
		require.NoError(t, err)
		
		aggregateCount, exists := jsonMap["aggregateRequestCount"]
		require.True(t, exists, "aggregateRequestCount should exist in JSON")
		require.Equal(t, "500", aggregateCount, "aggregateRequestCount should be serialized as string")

		// Unmarshal back
		var decoded api.AggregatorRecord
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, uint64(500), decoded.AggregateRequestCount)
	})
}