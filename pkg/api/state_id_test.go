package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
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

	t.Run("state ID hash preserves canonical CBOR input bytes", func(t *testing.T) {
		publicKey := []byte{0x01, 0x02, 0x03}
		ownerPredicate := NewPayToPublicKeyPredicate(publicKey)
		sourceStateHash := make([]byte, 32)
		sourceStateHash[0] = 0xcd

		predicateBytes, err := types.Cbor.Marshal(ownerPredicate)
		require.NoError(t, err)

		expectedBytes := append([]byte{0x82}, predicateBytes...)
		expectedBytes = append(expectedBytes, []byte{0x58, 0x20}...)
		expectedBytes = append(expectedBytes, sourceStateHash...)
		expectedHash := NewDataHasher(SHA256).AddData(expectedBytes).GetHash()

		type stateIDInput struct {
			_               struct{} `cbor:",toarray"`
			OwnerPredicate  Predicate
			SourceStateHash []byte
		}
		canonicalBytes, err := types.Cbor.Marshal(stateIDInput{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
		})
		require.NoError(t, err)
		assert.Equal(t, expectedBytes, canonicalBytes)

		gotHash, err := StateIDDataHash(ownerPredicate, sourceStateHash)
		require.NoError(t, err)
		assert.Equal(t, expectedHash.RawHash, gotHash.RawHash)
	})
}
