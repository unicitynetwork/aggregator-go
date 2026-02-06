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

		// Create StateID
		publicKey := make([]byte, 20) // matches new Uint8Array(20)

		stateHash := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000")
		requestID, err := CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Create transaction hash (matches new Uint8Array([0x01, ...new Uint8Array(33)]))
		transactionHashBytes := make([]byte, 34)
		transactionHashBytes[0] = 0x01
		transactionHash := RequireNewImprintV2("01000000000000000000000000000000000000000000000000000000000000000000")

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
				assert.True(t, request.RequestID == nil || request.TransactionHash == nil || request.Authenticator.Algorithm == "")
			}
		}
	})
}

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

		receipt := &ReceiptV1{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes(publicKeyBytes),
			Signature: NewHexBytes(signatureBytes),
			Request: ReceiptRequestV1{
				RequestID:       RequireNewImprintV2("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
				TransactionHash: RequireNewImprintV2("00010000000000000000000000000000000000000000000000000000000000000000"),
				StateHash:       RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000"),
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
		receipt := &ReceiptV1{
			Algorithm: "secp256k1",
			PublicKey: NewHexBytes([]byte{0x02, 0x79}), // shortened for test
			Signature: NewHexBytes([]byte{0xa0, 0xb3}), // shortened for test
			Request: ReceiptRequestV1{
				RequestID:       RequireNewImprintV2("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
				TransactionHash: RequireNewImprintV2("00010000000000000000000000000000000000000000000000000000000000000000"),
				StateHash:       RequireNewImprintV2("000000"), // shortened for test, minimum 3 bytes
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
	})
}

func TestAuthenticator_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode to exactly same object", func(t *testing.T) {
		// Test data matching TypeScript AuthenticatorTest.ts
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"
		stateHashHex := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000")

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

		// Create StateID using the same public key and state hash
		stateHash := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000")
		requestID, err := CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Expected result from RequestIdTest.ts (matches our StateID test)
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

func TestAggregateRequestCountSerialization_V1(t *testing.T) {
	t.Run("SubmitCommitmentRequest JSON serialization", func(t *testing.T) {
		req := &SubmitCommitmentRequest{
			RequestID:       RequireNewImprintV2("0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			TransactionHash: RequireNewImprintV2("0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			Authenticator: Authenticator{
				Algorithm: "secp256k1",
				PublicKey: HexBytes{0x01, 0x02, 0x03},
				Signature: HexBytes{0x04, 0x05, 0x06},
				StateHash: RequireNewImprintV2("0000abcd"),
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
		var decoded SubmitCommitmentRequest
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
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

		record := &AggregatorRecordV1{
			RequestID:       RequireNewImprintV2("0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			TransactionHash: RequireNewImprintV2("0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			Authenticator: Authenticator{
				Algorithm: "secp256k1",
				PublicKey: HexBytes{0x01, 0x02, 0x03},
				Signature: HexBytes{0x04, 0x05, 0x06},
				StateHash: RequireNewImprintV2("0000abcd"),
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
	})
}
