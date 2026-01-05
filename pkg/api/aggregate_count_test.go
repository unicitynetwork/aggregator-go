package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

func TestAggregateRequestCountSerialization(t *testing.T) {
	t.Run("CertificationRequest CBOR serialization", func(t *testing.T) {
		req := &CertificationRequest{
			StateID: "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			CertificationData: CertificationData{
				OwnerPredicate:  NewPayToPublicKeyPredicate([]byte{0x01, 0x02, 0x03}),
				SourceStateHash: "0000abcd",
				TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
				Witness:         HexBytes{0x04, 0x05, 0x06},
			},
			AggregateRequestCount: 100,
		}

		// Marshal to CBOR
		data, err := types.Cbor.Marshal(req)
		require.NoError(t, err)

		// Unmarshal back
		var decoded CertificationRequest
		require.NoError(t, types.Cbor.Unmarshal(data, &decoded))
		require.Equal(t, req.StateID, decoded.StateID)
		require.Equal(t, uint64(100), decoded.AggregateRequestCount)
	})

	t.Run("GetBlockResponse JSON serialization", func(t *testing.T) {
		blockIndex := NewBigInt(nil)
		blockIndex.SetInt64(1)
		resp := &GetBlockResponse{
			Block: &Block{
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
		var decoded GetBlockResponse
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, uint64(186), decoded.TotalCommitments)
	})

	t.Run("AggregatorRecord JSON serialization", func(t *testing.T) {
		blockNumber := NewBigInt(nil)
		blockNumber.SetInt64(1)
		leafIndex := NewBigInt(nil)
		leafIndex.SetInt64(0)

		record := &AggregatorRecord{
			StateID: "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			CertificationData: CertificationData{
				OwnerPredicate:  NewPayToPublicKeyPredicate([]byte{0x01, 0x02, 0x03}),
				Witness:         HexBytes{0x04, 0x05, 0x06},
				SourceStateHash: "0000abcd",
				TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			},
			AggregateRequestCount: 500,
			BlockNumber:           blockNumber,
			LeafIndex:             leafIndex,
			CreatedAt:             Now(),
			FinalizedAt:           Now(),
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
		var decoded AggregatorRecord
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, uint64(500), decoded.AggregateRequestCount)
		require.Equal(t, record.StateID, decoded.StateID)
	})
}
