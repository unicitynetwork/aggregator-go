package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmitCommitmentRequest_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode JSON to exactly same object", func(t *testing.T) {
		// Create test data matching TypeScript test
		// From SubmitCommitmentRequestTest.ts

		// Create RequestID
		publicKey := make([]byte, 20) // matches new Uint8Array(20)

		stateHash := ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000")
		requestID, err := CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Create transaction hash (matches new Uint8Array([0x01, ...new Uint8Array(33)]))
		transactionHashBytes := make([]byte, 34)
		transactionHashBytes[0] = 0x01
		transactionHash := ImprintHexString("01000000000000000000000000000000000000000000000000000000000000000000")

		// Create authenticator with test signature
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"

		publicKeyBytes, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signatureBytes, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		authenticator := &Authenticator{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes(publicKeyBytes),
			Signature: NewHexBytes(signatureBytes),
			StateHash: stateHash,
		}

		// Test without receipt
		request1 := &SubmitCommitmentRequest{
			RequestID:       requestID,
			TransactionHash: transactionHash,
			Authenticator:   *authenticator,
		}

		expectedJSON1 := map[string]interface{}{
			"requestId":       requestID.String(),
			"transactionHash": transactionHash.String(),
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": publicKeyHex,
				"signature": signatureHex,
				"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
			},
			// receipt is omitted when false (zero value)
		}

		jsonBytes1, err := json.Marshal(request1)
		require.NoError(t, err)

		var actualJSON1 map[string]interface{}
		err = json.Unmarshal(jsonBytes1, &actualJSON1)
		require.NoError(t, err)

		assert.Equal(t, expectedJSON1, actualJSON1)

		// Test deserialization
		var decodedRequest1 SubmitCommitmentRequest
		err = json.Unmarshal(jsonBytes1, &decodedRequest1)
		require.NoError(t, err)

		assert.Equal(t, request1.RequestID, decodedRequest1.RequestID)
		assert.Equal(t, request1.TransactionHash, decodedRequest1.TransactionHash)
		assert.Equal(t, request1.Authenticator.Algorithm, decodedRequest1.Authenticator.Algorithm)

		// Test with receipt = true
		receiptTrue := true
		request2 := &SubmitCommitmentRequest{
			RequestID:       requestID,
			TransactionHash: transactionHash,
			Authenticator:   *authenticator,
			Receipt:         &receiptTrue,
		}

		expectedJSON2 := map[string]interface{}{
			"requestId":       requestID.String(),
			"transactionHash": transactionHash.String(),
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": publicKeyHex,
				"signature": signatureHex,
				"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
			},
			"receipt": true,
		}

		jsonBytes2, err := json.Marshal(request2)
		require.NoError(t, err)

		var actualJSON2 map[string]interface{}
		err = json.Unmarshal(jsonBytes2, &actualJSON2)
		require.NoError(t, err)

		assert.Equal(t, expectedJSON2, actualJSON2)

		// Test with receipt = false
		receiptFalse := false
		request3 := &SubmitCommitmentRequest{
			RequestID:       requestID,
			TransactionHash: transactionHash,
			Authenticator:   *authenticator,
			Receipt:         &receiptFalse,
		}

		expectedJSON3 := map[string]interface{}{
			"requestId":       requestID.String(),
			"transactionHash": transactionHash.String(),
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": publicKeyHex,
				"signature": signatureHex,
				"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
			},
			"receipt": false,
		}

		jsonBytes3, err := json.Marshal(request3)
		require.NoError(t, err)

		var actualJSON3 map[string]interface{}
		err = json.Unmarshal(jsonBytes3, &actualJSON3)
		require.NoError(t, err)

		assert.Equal(t, expectedJSON3, actualJSON3)
	})

	t.Run("should validate JSON structure correctly", func(t *testing.T) {
		// Valid JSON structure from TypeScript test
		validJSON := `{
			"authenticator": {
				"algorithm": "secp256k1",
				"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
				"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
				"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000"
			},
			"receipt": true,
			"requestId": "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
			"transactionHash": "00010000000000000000000000000000000000000000000000000000000000000000"
		}`

		var request SubmitCommitmentRequest
		err := json.Unmarshal([]byte(validJSON), &request)
		require.NoError(t, err)

		assert.Equal(t, "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40", request.RequestID.String())
		assert.Equal(t, "00010000000000000000000000000000000000000000000000000000000000000000", request.TransactionHash.String())
		assert.Equal(t, "secp256k1", request.Authenticator.Algorithm)
		assert.NotNil(t, request.Receipt)
		assert.True(t, *request.Receipt)

		// Test round-trip
		jsonBytes, err := json.Marshal(request)
		require.NoError(t, err)

		var roundTripRequest SubmitCommitmentRequest
		err = json.Unmarshal(jsonBytes, &roundTripRequest)
		require.NoError(t, err)

		assert.Equal(t, request.RequestID, roundTripRequest.RequestID)
		assert.Equal(t, request.TransactionHash, roundTripRequest.TransactionHash)
		assert.Equal(t, request.Authenticator.Algorithm, roundTripRequest.Authenticator.Algorithm)
		if request.Receipt != nil && roundTripRequest.Receipt != nil {
			assert.Equal(t, *request.Receipt, *roundTripRequest.Receipt)
		} else {
			assert.Equal(t, request.Receipt, roundTripRequest.Receipt)
		}
	})

	t.Run("should handle invalid JSON gracefully", func(t *testing.T) {
		invalidJSONs := []string{
			`{}`,                      // Missing required fields
			`null`,                    // Null
			`"string"`,                // Not an object
			`123`,                     // Not an object
			`{"authenticator": null}`, // Invalid authenticator
		}

		for _, invalidJSON := range invalidJSONs {
			var request SubmitCommitmentRequest
			err := json.Unmarshal([]byte(invalidJSON), &request)
			// Should either error or create a request with empty/invalid fields
			if err == nil {
				// If no error, validate that required fields are missing
				assert.True(t, request.RequestID == "" || request.TransactionHash == "" || request.Authenticator.Algorithm == "")
			}
		}
	})
}
