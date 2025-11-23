package api

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCertificationData_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode to exactly same object", func(t *testing.T) {
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"
		sourceStateHashHex := ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000")
		transactionHashHex := ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000001")

		publicKey, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signature, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		certData := &CertificationData{
			PublicKey:       NewHexBytes(publicKey),
			Signature:       NewHexBytes(signature),
			SourceStateHash: sourceStateHashHex,
			TransactionHash: transactionHashHex,
		}

		jsonBytes, err := json.Marshal(certData)
		require.NoError(t, err)

		// Test deserialization
		var deserializedCertData CertificationData
		err = json.Unmarshal(jsonBytes, &deserializedCertData)
		require.NoError(t, err)

		assert.Equal(t, certData.PublicKey.String(), deserializedCertData.PublicKey.String())
		assert.Equal(t, certData.Signature.String(), deserializedCertData.Signature.String())
		assert.Equal(t, certData.SourceStateHash.String(), deserializedCertData.SourceStateHash.String())
		assert.Equal(t, certData.TransactionHash.String(), deserializedCertData.TransactionHash.String())
	})

	t.Run("should validate JSON structure", func(t *testing.T) {
		validJSON := `{
			"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
			"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000001",
			"transactionHash": "00000000000000000000000000000000000000000000000000000000000000000002"
		}`

		var certData CertificationData
		err := json.Unmarshal([]byte(validJSON), &certData)
		require.NoError(t, err)

		assert.NotEmpty(t, certData.PublicKey)
		assert.NotEmpty(t, certData.Signature)
		assert.NotEmpty(t, certData.SourceStateHash)
		assert.NotEmpty(t, certData.TransactionHash)

		// Test round-trip
		jsonBytes, err := json.Marshal(certData)
		require.NoError(t, err)

		var roundTripCertData CertificationData
		err = json.Unmarshal(jsonBytes, &roundTripCertData)
		require.NoError(t, err)

		assert.Equal(t, certData, roundTripCertData)
	})
}

func TestCertificationRequest_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode JSON to exactly same object", func(t *testing.T) {
		// Create StateID
		publicKey := make([]byte, 20) // matches new Uint8Array(20)

		sourceStateHash := ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000")
		stateID, err := CreateStateID(sourceStateHash, publicKey)
		require.NoError(t, err)

		// Create transaction hash (matches new Uint8Array([0x01, ...new Uint8Array(33)]))
		transactionHashBytes := make([]byte, 34)
		transactionHashBytes[0] = 0x01
		transactionHash := ImprintHexString("01000000000000000000000000000000000000000000000000000000000000000000")

		// Create certification request with test signature
		publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
		signatureHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"

		publicKeyBytes, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signatureBytes, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		certificationData := CertificationData{
			PublicKey:       NewHexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
			Signature:       NewHexBytes(signatureBytes),
		}

		// Test without receipt
		request1 := &CertificationRequest{
			StateID:           stateID,
			CertificationData: certificationData,
		}

		expectedJSON1 := map[string]interface{}{
			"stateId": stateID.String(),
			"certificationData": map[string]interface{}{
				"publicKey":       publicKeyHex,
				"signature":       signatureHex,
				"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
				"transactionHash": transactionHash.String(),
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
		var decodedRequest1 CertificationRequest
		err = json.Unmarshal(jsonBytes1, &decodedRequest1)
		require.NoError(t, err)

		assert.Equal(t, request1.StateID, decodedRequest1.StateID)
		assert.Equal(t, request1.CertificationData.PublicKey, decodedRequest1.CertificationData.PublicKey)
		assert.Equal(t, request1.CertificationData.SourceStateHash, decodedRequest1.CertificationData.SourceStateHash)
		assert.Equal(t, request1.CertificationData.TransactionHash, decodedRequest1.CertificationData.TransactionHash)
		assert.Equal(t, request1.CertificationData.Signature, decodedRequest1.CertificationData.Signature)

		// Test with receipt = true
		receiptTrue := true
		request2 := &CertificationRequest{
			StateID:           stateID,
			CertificationData: certificationData,
			Receipt:           &receiptTrue,
		}

		expectedJSON2 := map[string]interface{}{
			"stateId": stateID.String(),
			"certificationData": map[string]interface{}{
				"publicKey":       publicKeyHex,
				"signature":       signatureHex,
				"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
				"transactionHash": transactionHash.String(),
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
		request3 := &CertificationRequest{
			StateID:           stateID,
			CertificationData: certificationData,
			Receipt:           &receiptFalse,
		}

		expectedJSON3 := map[string]interface{}{
			"stateId": stateID.String(),
			"certificationData": map[string]interface{}{
				"publicKey":       publicKeyHex,
				"signature":       signatureHex,
				"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
				"transactionHash": transactionHash.String(),
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
		validJSON := `{
			"certificationData": {
				"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
				"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
				"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000000",
				"transactionHash": "00010000000000000000000000000000000000000000000000000000000000000000"
			},
			"receipt": true,
			"stateId": "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		}`

		var request CertificationRequest
		err := json.Unmarshal([]byte(validJSON), &request)
		require.NoError(t, err)

		assert.Equal(t, "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40", request.StateID.String())
		assert.Equal(t, "00010000000000000000000000000000000000000000000000000000000000000000", request.CertificationData.TransactionHash.String())
		assert.NotNil(t, request.Receipt)
		assert.True(t, *request.Receipt)

		// Test round-trip
		jsonBytes, err := json.Marshal(request)
		require.NoError(t, err)

		var roundTripRequest CertificationRequest
		err = json.Unmarshal(jsonBytes, &roundTripRequest)
		require.NoError(t, err)

		assert.Equal(t, request.StateID, roundTripRequest.StateID)
		assert.Equal(t, request.CertificationData.TransactionHash, roundTripRequest.CertificationData.TransactionHash)
		if request.Receipt != nil && roundTripRequest.Receipt != nil {
			assert.Equal(t, *request.Receipt, *roundTripRequest.Receipt)
		} else {
			assert.Equal(t, request.Receipt, roundTripRequest.Receipt)
		}
	})

	t.Run("should handle invalid JSON gracefully", func(t *testing.T) {
		invalidJSONs := []string{
			`{}`,       // Missing required fields
			`null`,     // Null
			`"string"`, // Not an object
			`123`,      // Not an object
		}

		for _, invalidJSON := range invalidJSONs {
			var request CertificationRequest
			err := json.Unmarshal([]byte(invalidJSON), &request)
			// Should either error or create a request with empty/invalid fields
			if err == nil {
				// If no error, validate that required fields are missing
				assert.True(t, request.StateID == "" || request.CertificationData.TransactionHash == "")
			}
		}
	})
}

func TestCertificationResponse_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode JSON to exactly same object", func(t *testing.T) {
		// Test simple success response without receipt
		response1 := &CertificationResponse{
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
		var decodedResponse1 CertificationResponse
		err = json.Unmarshal(jsonBytes1, &decodedResponse1)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", decodedResponse1.Status)
		assert.Nil(t, decodedResponse1.Receipt)

		// Test error response
		response2 := &CertificationResponse{
			Status: "CERTIFICATION_DATA_VERIFICATION_FAILED",
		}

		jsonBytes2, err := json.Marshal(response2)
		require.NoError(t, err)

		var actualJSON2 map[string]interface{}
		err = json.Unmarshal(jsonBytes2, &actualJSON2)
		require.NoError(t, err)

		expectedJSON2 := map[string]interface{}{
			"status": "CERTIFICATION_DATA_VERIFICATION_FAILED",
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
			PublicKey: NewHexBytes(publicKeyBytes),
			Signature: NewHexBytes(signatureBytes),
			Request: ReceiptRequest{
				StateID:         "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
				TransactionHash: "00010000000000000000000000000000000000000000000000000000000000000000",
				SourceStateHash: ImprintHexString("00000000000000000000000000000000000000000000000000000000000000000000"),
			},
		}

		response3 := &CertificationResponse{
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
		assert.Equal(t, publicKeyHex, receiptJSON["publicKey"])
		assert.Equal(t, signatureHex, receiptJSON["signature"])

		// Test deserialization with receipt
		var decodedResponse3 CertificationResponse
		err = json.Unmarshal(jsonBytes3, &decodedResponse3)
		require.NoError(t, err)

		assert.Equal(t, "SUCCESS", decodedResponse3.Status)
		assert.NotNil(t, decodedResponse3.Receipt)
	})

	t.Run("should validate JSON structure correctly", func(t *testing.T) {
		// Valid JSON structure 1
		validJSON1 := `{
			"status": "SUCCESS"
		}`

		var response1 CertificationResponse
		err := json.Unmarshal([]byte(validJSON1), &response1)
		require.NoError(t, err)
		assert.Equal(t, "SUCCESS", response1.Status)

		// Valid JSON structure 2 with receipt
		validJSON2 := `{
			"status": "CERTIFICATION_DATA_VERIFICATION_FAILED",
			"receipt": {
				"publicKey": "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
				"signature": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
				"request": {
					"service": "aggregator",
					"method": "certification_request",
					"stateId": "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
					"transactionHash": "00010000000000000000000000000000000000000000000000000000000000000000",
					"sourceStateHash": "00000000000000000000000000000000000000000000000000000000000000000000"
				}
			}
		}`

		var response2 CertificationResponse
		err = json.Unmarshal([]byte(validJSON2), &response2)
		require.NoError(t, err)
		assert.Equal(t, "CERTIFICATION_DATA_VERIFICATION_FAILED", response2.Status)
		assert.NotNil(t, response2.Receipt)

		// Test round-trip
		jsonBytes, err := json.Marshal(response2)
		require.NoError(t, err)

		var roundTripResponse CertificationResponse
		err = json.Unmarshal(jsonBytes, &roundTripResponse)
		require.NoError(t, err)

		assert.Equal(t, response2.Status, roundTripResponse.Status)
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
			var response CertificationResponse
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
			"CERTIFICATION_DATA_VERIFICATION_FAILED",
			"STATE_ID_MISMATCH",
			"STATE_ID_EXISTS",
		}

		for _, status := range statuses {
			response := &CertificationResponse{
				Status: status,
			}

			jsonBytes, err := json.Marshal(response)
			require.NoError(t, err)

			var decodedResponse CertificationResponse
			err = json.Unmarshal(jsonBytes, &decodedResponse)
			require.NoError(t, err)

			assert.Equal(t, status, decodedResponse.Status)
		}
	})

	t.Run("should handle receipt serialization", func(t *testing.T) {
		// Test receipt with all fields
		receipt := &Receipt{
			PublicKey: NewHexBytes([]byte{0x02, 0x79}), // shortened for test
			Signature: NewHexBytes([]byte{0xa0, 0xb3}), // shortened for test
			Request: ReceiptRequest{
				StateID:         "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40",
				TransactionHash: "00010000000000000000000000000000000000000000000000000000000000000000",
				SourceStateHash: ImprintHexString("000000"), // shortened for test, minimum 3 bytes
			},
		}

		response := &CertificationResponse{
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
		assert.NotNil(t, receiptJSON["request"])

		// Test deserialization preserves all fields
		var decodedResponse CertificationResponse
		err = json.Unmarshal(jsonBytes, &decodedResponse)
		require.NoError(t, err)

		assert.Equal(t, response.Status, decodedResponse.Status)
		assert.NotNil(t, decodedResponse.Receipt)
	})
}

func TestCertificationRequestJSON(t *testing.T) {
	req := &CertificationRequest{
		StateID: "0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00",
		CertificationData: CertificationData{
			PublicKey:       HexBytes{0x03, 0x20, 0x44, 0xf2},
			SourceStateHash: ImprintHexString("0000cd60"),
			TransactionHash: "00008a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d",
			Signature:       HexBytes{0x41, 0x67, 0x51, 0xe8},
		},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err, "Failed to marshal CertificationRequest")

	var unmarshaledReq CertificationRequest
	err = json.Unmarshal(data, &unmarshaledReq)
	require.NoError(t, err, "Failed to unmarshal CertificationRequest")

	require.Equal(t, req.StateID, unmarshaledReq.StateID, "StateID mismatch")
	require.Equal(t, req.CertificationData, unmarshaledReq.CertificationData, "CertificationData data mismatch")
}
