package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticator_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode to exactly same object", func(t *testing.T) {
		// Test data matching TypeScript AuthenticatorTest.ts
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"
		stateHashHex := ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000")

		publicKey, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signature, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		authenticator := &Authenticator{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes(publicKey),
			Signature: NewHexBytes(signature),
			StateHash: stateHashHex,
		}

		jsonBytes, err := json.Marshal(authenticator)
		require.NoError(t, err)

		// Test deserialization
		var deserializedAuth Authenticator
		err = json.Unmarshal(jsonBytes, &deserializedAuth)
		require.NoError(t, err)

		assert.Equal(t, authenticator.Algorithm, deserializedAuth.Algorithm)
		assert.Equal(t, authenticator.PublicKey.String(), deserializedAuth.PublicKey.String())
		assert.Equal(t, authenticator.Signature.String(), deserializedAuth.Signature.String())
		assert.Equal(t, authenticator.StateHash.String(), deserializedAuth.StateHash.String())
	})

	t.Run("should calculate request id correctly", func(t *testing.T) {
		// Use 20 bytes for public key (matches RequestIdTest.ts which uses new Uint8Array(20))
		publicKey := make([]byte, 20)

		// Create RequestID using the same public key and state hash
		requestID, err := CreateRequestID(publicKey, ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000"))
		require.NoError(t, err)

		// Expected result from RequestIdTest.ts (matches our RequestID test)
		expectedRequestID := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		assert.Equal(t, expectedRequestID, requestID.String())
	})

	t.Run("should validate JSON structure", func(t *testing.T) {
		// Valid authenticator JSON
		validJSON := `{
			"algorithm": "secp256k1",
			"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
			"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000"
		}`

		var authenticator Authenticator
		err := json.Unmarshal([]byte(validJSON), &authenticator)
		require.NoError(t, err)

		assert.Equal(t, "secp256k1", authenticator.Algorithm)
		assert.NotEmpty(t, authenticator.PublicKey)
		assert.NotEmpty(t, authenticator.Signature)
		assert.NotEmpty(t, authenticator.StateHash)

		// Test round-trip
		jsonBytes, err := json.Marshal(authenticator)
		require.NoError(t, err)

		var roundTripAuth Authenticator
		err = json.Unmarshal(jsonBytes, &roundTripAuth)
		require.NoError(t, err)

		assert.Equal(t, authenticator.Algorithm, roundTripAuth.Algorithm)
		assert.Equal(t, authenticator.PublicKey.String(), roundTripAuth.PublicKey.String())
		assert.Equal(t, authenticator.Signature.String(), roundTripAuth.Signature.String())
		assert.Equal(t, authenticator.StateHash.String(), roundTripAuth.StateHash.String())
	})
}
