package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestID_CreateAndSerialize(t *testing.T) {

	t.Run("should create RequestID with exact TypeScript compatibility", func(t *testing.T) {
		// Create 20-byte public key (all zeros)
		publicKey := make([]byte, 20)

		// Create 34-byte state hash (DataHash.fromImprint with all zeros)
		stateHashBytes := make([]byte, 34)
		stateHash, err := NewImprintHexString(fmt.Sprintf("%x", stateHashBytes))
		require.NoError(t, err)

		// Create RequestID
		requestID, err := CreateRequestID(publicKey, stateHash)
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
		var deserializedRequestID RequestID
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
		requestID, err := NewImprintHexString(validHex)
		require.NoError(t, err)
		assert.Equal(t, validHex, requestID.String())

		// Invalid length
		_, err = NewImprintHexString("inv")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "imprint must be at least 3 bytes")

		// Invalid hex characters (correct length but invalid hex)
		invalidHex := "xxxx0000000000000000000000000000000000000000000000000000000000000000"
		_, err = NewImprintHexString(invalidHex)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid hex")
	})

	t.Run("should convert to bytes correctly", func(t *testing.T) {
		requestIDStr := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		requestID, err := NewImprintHexString(requestIDStr)
		require.NoError(t, err)

		bytes, err := requestID.Imprint()
		require.NoError(t, err)
		assert.Len(t, bytes, 34) // 68 hex chars = 34 bytes

		// Convert back to hex and verify
		hexStr := hex.EncodeToString(bytes)
		assert.Equal(t, requestIDStr, hexStr)
	})
}
