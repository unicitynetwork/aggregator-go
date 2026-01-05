package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateID_CreateAndSerialize(t *testing.T) {
	t.Run("should create StateID with exact TypeScript compatibility", func(t *testing.T) {
		// Create 20-byte public key (all zeros)
		publicKey := make([]byte, 20)
		ownerPredicate := NewPayToPublicKeyPredicate(publicKey)

		// Create 34-byte state hash (DataHash.fromImprint with all zeros)
		sourceStateHashBytes := make([]byte, 34)
		sourceStateHash, err := NewImprintHexString(fmt.Sprintf("%x", sourceStateHashBytes))
		require.NoError(t, err)

		// Create StateID
		stateID, err := CreateStateID(ownerPredicate, sourceStateHash)
		require.NoError(t, err)

		jsonBytes, err := json.Marshal(stateID)
		require.NoError(t, err)

		// Test that we can deserialize back
		var deserializedStateID StateID
		err = json.Unmarshal(jsonBytes, &deserializedStateID)
		require.NoError(t, err)
		assert.Equal(t, stateID, deserializedStateID)

		// Test string representation
		assert.Len(t, stateID.String(), 68) // Must be 68 characters (4 algorithm + 64 hash)
	})

	t.Run("should validate hex format", func(t *testing.T) {
		// Valid 68-character hex string
		validHex := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		stateID, err := NewImprintHexString(validHex)
		require.NoError(t, err)
		assert.Equal(t, validHex, stateID.String())

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
		stateIDStr := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		stateID, err := NewImprintHexString(stateIDStr)
		require.NoError(t, err)

		bytes, err := stateID.Imprint()
		require.NoError(t, err)
		assert.Len(t, bytes, 34) // 68 hex chars = 34 bytes

		// Convert back to hex and verify
		hexStr := hex.EncodeToString(bytes)
		assert.Equal(t, stateIDStr, hexStr)
	})
}
