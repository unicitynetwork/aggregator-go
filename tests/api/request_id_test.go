package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/api"
	"github.com/unicitynetwork/aggregator-go/internal/models"
)

func TestRequestID_CreateAndSerialize(t *testing.T) {
	// Test data that should produce the exact same result as TypeScript
	// From RequestIdTest.ts:
	// RequestId.create(new Uint8Array(20), DataHash.fromImprint(new Uint8Array(34)))
	// Expected JSON: '0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40'
	// Expected CBOR: '58220000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40'

	t.Run("should create RequestID with exact TypeScript compatibility", func(t *testing.T) {
		// Create 20-byte public key (all zeros)
		publicKey := make([]byte, 20)
		
		// Create 34-byte state hash (DataHash.fromImprint with all zeros)
		stateHashBytes := make([]byte, 34)
		stateHash, err := models.NewStateHashFromBytes(stateHashBytes)
		require.NoError(t, err)

		// Create RequestID
		requestID, err := api.CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Test JSON serialization matches TypeScript
		expectedJSON := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		jsonBytes, err := json.Marshal(requestID)
		require.NoError(t, err)
		
		var jsonStr string
		err = json.Unmarshal(jsonBytes, &jsonStr)
		require.NoError(t, err)
		assert.Equal(t, expectedJSON, jsonStr)

		// Test that we can deserialize back
		var deserializedRequestID models.RequestID
		err = json.Unmarshal(jsonBytes, &deserializedRequestID)
		require.NoError(t, err)
		assert.Equal(t, requestID, deserializedRequestID)

		// Test string representation
		assert.Equal(t, expectedJSON, requestID.String())
		assert.Len(t, requestID.String(), 68) // Must be 68 characters (4 algorithm + 64 hash)
	})

	t.Run("should validate hex format", func(t *testing.T) {
		// Valid 68-character hex string
		validHex := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		requestID, err := models.NewRequestID(validHex)
		require.NoError(t, err)
		assert.Equal(t, validHex, requestID.String())

		// Invalid length
		_, err = models.NewRequestID("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be 68 characters")

		// Invalid hex characters (correct length but invalid hex)
		invalidHex := "xxxx0000000000000000000000000000000000000000000000000000000000000000"
		_, err = models.NewRequestID(invalidHex)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be valid hex")
	})

	t.Run("should convert to bytes correctly", func(t *testing.T) {
		requestIDStr := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		requestID, err := models.NewRequestID(requestIDStr)
		require.NoError(t, err)

		bytes, err := requestID.Bytes()
		require.NoError(t, err)
		assert.Len(t, bytes, 34) // 68 hex chars = 34 bytes

		// Convert back to hex and verify
		hexStr := hex.EncodeToString(bytes)
		assert.Equal(t, requestIDStr, hexStr)
	})
}