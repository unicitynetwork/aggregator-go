package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateID_CreateAndSerialize(t *testing.T) {
	t.Run("should create StateID with exact TypeScript compatibility", func(t *testing.T) {
		// Create 20-byte public key (all zeros)
		publicKey := make([]byte, 20)
		ownerPredicate := NewPayToPublicKeyPredicate(publicKey)

		// Create 32-byte raw source state hash.
		sourceStateHashBytes := make([]byte, StateTreeKeyLengthBytes)
		sourceStateHash := ImprintV2(sourceStateHashBytes)

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
		assert.Len(t, stateID.String(), 64) // Must be 64 characters
	})

	t.Run("should validate hex format", func(t *testing.T) {
		validHex := "ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		stateID, err := NewImprintV2(validHex)
		require.NoError(t, err)
		assert.Equal(t, validHex, stateID.String())

		// Invalid hex characters (correct length but invalid hex)
		invalidHex := "xxxx9cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		_, err = NewImprintV2(invalidHex)
		assert.ErrorContains(t, err, "invalid hex string")
	})

	t.Run("should convert to bytes correctly", func(t *testing.T) {
		stateIDStr := "ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		stateID, err := NewImprintV2(stateIDStr)
		require.NoError(t, err)

		bytes := stateID.Imprint()
		assert.Len(t, bytes, StateTreeKeyLengthBytes)

		// Convert back to hex and verify
		hexStr := hex.EncodeToString(bytes)
		assert.Equal(t, stateIDStr, hexStr)
	})
}
