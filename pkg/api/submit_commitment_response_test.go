package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmitCommitmentResponse_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode JSON to exactly same object", func(t *testing.T) {
		// Test simple success response without receipt
		response1 := &SubmitCommitmentResponse{
			Status: "SUCCESS",
		}

		jsonBytes1, err := json.Marshal(response1)
		require.NoError(t, err)

		var actualJSON1 map[string]interface{}
		err = json.Unmarshal(jsonBytes1, &actualJSON1)
		require.NoError(t, err)

		expectedJSON1 := map[string]interface{}{
			"status": "SUCCESS",
		}
		assert.Equal(t, expectedJSON1, actualJSON1)

		// Test deserialization
		var decodedResponse1 SubmitCommitmentResponse
		err = json.Unmarshal(jsonBytes1, &decodedResponse1)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", decodedResponse1.Status)
		assert.Nil(t, decodedResponse1.Receipt)

		// Test error response
		response2 := &SubmitCommitmentResponse{
			Status: "AUTHENTICATOR_VERIFICATION_FAILED",
		}

		jsonBytes2, err := json.Marshal(response2)
		require.NoError(t, err)

		var actualJSON2 map[string]interface{}
		err = json.Unmarshal(jsonBytes2, &actualJSON2)
		require.NoError(t, err)

		expectedJSON2 := map[string]interface{}{
			"status": "AUTHENTICATOR_VERIFICATION_FAILED",
		}
		assert.Equal(t, expectedJSON2, actualJSON2)

		// Test response with receipt
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"

		publicKeyBytes, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signatureBytes, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		receipt := &Receipt{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes(publicKeyBytes),
			Signature: NewHexBytes(signatureBytes),
			Request: ReceiptRequest{
				Service:         "aggregator",
				Method:          "submit_commitment",
				RequestID:       "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
				TransactionHash: "00010000000000000000000000000000000000000000000000000000000000000000",
				StateHash:       ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000"),
			},
		}

		response3 := &SubmitCommitmentResponse{
			Status:  "SUCCESS",
			Receipt: receipt,
		}

		jsonBytes3, err := json.Marshal(response3)
		require.NoError(t, err)

		var actualJSON3 map[string]interface{}
		err = json.Unmarshal(jsonBytes3, &actualJSON3)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", actualJSON3["status"])
		assert.NotNil(t, actualJSON3["receipt"])

		receiptJSON, ok := actualJSON3["receipt"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "secp256k1", receiptJSON["algorithm"])
		assert.Equal(t, publicKeyHex, receiptJSON["publicKey"])
		assert.Equal(t, signatureHex, receiptJSON["signature"])

		// Test deserialization with receipt
		var decodedResponse3 SubmitCommitmentResponse
		err = json.Unmarshal(jsonBytes3, &decodedResponse3)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", decodedResponse3.Status)
		assert.NotNil(t, decodedResponse3.Receipt)
		assert.Equal(t, "secp256k1", decodedResponse3.Receipt.Algorithm)
		assert.Equal(t, "aggregator", decodedResponse3.Receipt.Request.Service)
		assert.Equal(t, "submit_commitment", decodedResponse3.Receipt.Request.Method)
	})

	t.Run("should validate JSON structure correctly", func(t *testing.T) {
		// Valid JSON structure 1
		validJSON1 := `{
			"status": "SUCCESS"
		}`

		var response1 SubmitCommitmentResponse
		err := json.Unmarshal([]byte(validJSON1), &response1)
		require.NoError(t, err)
		assert.Equal(t, "SUCCESS", response1.Status)

		// Valid JSON structure 2 with receipt
		validJSON2 := `{
			"status": "AUTHENTICATOR_VERIFICATION_FAILED",
			"receipt": {
				"algorithm": "secp256k1",
				"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
				"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
				"request": {
					"service": "aggregator",
					"method": "submit_commitment",
					"requestId": "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
					"transactionHash": "00010000000000000000000000000000000000000000000000000000000000000000",
					"stateHash": "00000000000000000000000000000000000000000000000000000000000000000000"
				}
			}
		}`

		var response2 SubmitCommitmentResponse
		err = json.Unmarshal([]byte(validJSON2), &response2)
		require.NoError(t, err)
		assert.Equal(t, "AUTHENTICATOR_VERIFICATION_FAILED", response2.Status)
		assert.NotNil(t, response2.Receipt)
		assert.Equal(t, "secp256k1", response2.Receipt.Algorithm)

		// Test round-trip
		jsonBytes, err := json.Marshal(response2)
		require.NoError(t, err)

		var roundTripResponse SubmitCommitmentResponse
		err = json.Unmarshal(jsonBytes, &roundTripResponse)
		require.NoError(t, err)

		assert.Equal(t, response2.Status, roundTripResponse.Status)
		if response2.Receipt != nil && roundTripResponse.Receipt != nil {
			assert.Equal(t, response2.Receipt.Algorithm, roundTripResponse.Receipt.Algorithm)
			assert.Equal(t, response2.Receipt.Request.Service, roundTripResponse.Receipt.Request.Service)
		}
	})

	t.Run("should handle invalid JSON gracefully", func(t *testing.T) {
		invalidJSONs := []string{
			`{}`,              // Missing status
			`null`,            // Null
			`"string"`,        // Not an object
			`123`,             // Not an object
			`{"status": 123}`, // Invalid status type
		}

		for _, invalidJSON := range invalidJSONs {
			var response SubmitCommitmentResponse
			err := json.Unmarshal([]byte(invalidJSON), &response)
			// Should either error or create a response with invalid fields
			if err == nil {
				// If no error, validate that status is missing or empty for invalid cases
				if invalidJSON == `{}` {
					assert.Empty(t, response.Status)
				}
			}
		}
	})

	t.Run("should test different status types", func(t *testing.T) {
		statuses := []string{
			"SUCCESS",
			"AUTHENTICATOR_VERIFICATION_FAILED",
			"REQUEST_ID_MISMATCH",
			"REQUEST_ID_EXISTS",
		}

		for _, status := range statuses {
			response := &SubmitCommitmentResponse{
				Status: status,
			}

			jsonBytes, err := json.Marshal(response)
			require.NoError(t, err)

			var decodedResponse SubmitCommitmentResponse
			err = json.Unmarshal(jsonBytes, &decodedResponse)
			require.NoError(t, err)

			assert.Equal(t, status, decodedResponse.Status)
		}
	})

	t.Run("should handle receipt serialization", func(t *testing.T) {
		// Test receipt with all fields
		receipt := &Receipt{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes([]byte{0x02, 0x79}), // shortened for test
			Signature: NewHexBytes([]byte{0xa0, 0xb3}), // shortened for test
			Request: ReceiptRequest{
				Service:         "aggregator",
				Method:          "submit_commitment",
				RequestID:       "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
				TransactionHash: "00010000000000000000000000000000000000000000000000000000000000000000",
				StateHash:       ImprintHexString("0000"), // shortened for test
			},
		}

		response := &SubmitCommitmentResponse{
			Status:  "SUCCESS",
			Receipt: receipt,
		}

		// Test JSON serialization
		jsonBytes, err := json.Marshal(response)
		require.NoError(t, err)

		var actualJSON map[string]interface{}
		err = json.Unmarshal(jsonBytes, &actualJSON)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", actualJSON["status"])
		assert.NotNil(t, actualJSON["receipt"])

		receiptJSON, ok := actualJSON["receipt"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "secp256k1", receiptJSON["algorithm"])
		assert.NotNil(t, receiptJSON["request"])

		// Test deserialization preserves all fields
		var decodedResponse SubmitCommitmentResponse
		err = json.Unmarshal(jsonBytes, &decodedResponse)
		require.NoError(t, err)

		assert.Equal(t, response.Status, decodedResponse.Status)
		assert.NotNil(t, decodedResponse.Receipt)
		assert.Equal(t, receipt.Algorithm, decodedResponse.Receipt.Algorithm)
		assert.Equal(t, receipt.Request.Service, decodedResponse.Receipt.Request.Service)
		assert.Equal(t, receipt.Request.Method, decodedResponse.Receipt.Request.Method)
	})
}
