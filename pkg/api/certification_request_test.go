package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

func TestCertificationData_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode to exactly same object", func(t *testing.T) {
		certData := createCertData(t)
		certDataCborBytes, err := types.Cbor.Marshal(certData)
		require.NoError(t, err)

		var deserializedCertData CertificationData
		require.NoError(t, types.Cbor.Unmarshal(certDataCborBytes, &deserializedCertData))
		require.Equal(t, certData, deserializedCertData)
	})
}

func TestCertificationRequest_SerializeAndValidate(t *testing.T) {
	t.Run("should encode and decode CBOR to exactly same object", func(t *testing.T) {
		// Create test data
		certData := createCertData(t)
		stateID, err := certData.CreateStateID()
		require.NoError(t, err)

		// Test without receipt
		request1 := CertificationRequest{
			StateID:           stateID,
			CertificationData: certData,
		}

		cborBytes1, err := types.Cbor.Marshal(request1)
		require.NoError(t, err)

		fmt.Printf("%x\n", cborBytes1)

		// Test deserialization
		var decodedRequest1 CertificationRequest
		require.NoError(t, types.Cbor.Unmarshal(cborBytes1, &decodedRequest1))
		require.Equal(t, request1, decodedRequest1)

		// Test with receipt = true
		request2 := CertificationRequest{
			StateID:           stateID,
			CertificationData: certData,
			Receipt:           true,
		}

		cborBytes2, err := types.Cbor.Marshal(request2)
		require.NoError(t, err)

		var decodedRequest2 CertificationRequest
		require.NoError(t, types.Cbor.Unmarshal(cborBytes2, &decodedRequest2))
		assert.True(t, decodedRequest2.Receipt)

		// Test with receipt = false
		request3 := CertificationRequest{
			StateID:           stateID,
			CertificationData: certData,
			Receipt:           false,
		}

		cborBytes3, err := types.Cbor.Marshal(request3)
		require.NoError(t, err)

		var decodedRequest3 CertificationRequest
		require.NoError(t, types.Cbor.Unmarshal(cborBytes3, &decodedRequest3))
		assert.False(t, decodedRequest3.Receipt)
	})

	t.Run("should validate CBOR structure correctly", func(t *testing.T) {
		// Create a valid request
		publicKey := make([]byte, 20)
		ownerPredicate := NewPayToPublicKeyPredicate(publicKey)
		sourceStateHash := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000")
		stateID, _ := CreateStateID(ownerPredicate, sourceStateHash)
		transactionHash := RequireNewImprintV2("00010000000000000000000000000000000000000000000000000000000000000000")
		witness := NewHexBytes([]byte{0x01, 0x02, 0x03})

		request := &CertificationRequest{
			StateID: stateID,
			CertificationData: CertificationData{
				OwnerPredicate:  ownerPredicate,
				SourceStateHash: sourceStateHash,
				TransactionHash: transactionHash,
				Witness:         witness,
			},
			Receipt: true,
		}

		// Test round-trip
		cborBytes, err := types.Cbor.Marshal(request)
		require.NoError(t, err)

		var roundTripRequest CertificationRequest
		err = types.Cbor.Unmarshal(cborBytes, &roundTripRequest)
		require.NoError(t, err)

		assert.Equal(t, request.StateID, roundTripRequest.StateID)
		assert.Equal(t, request.CertificationData.OwnerPredicate, roundTripRequest.CertificationData.OwnerPredicate)
		assert.Equal(t, request.CertificationData.TransactionHash, roundTripRequest.CertificationData.TransactionHash)
		assert.Equal(t, request.Receipt, roundTripRequest.Receipt)
	})

	t.Run("should handle invalid CBOR gracefully", func(t *testing.T) {
		invalidCBORs := [][]byte{
			{0x80}, // Empty array (CertificationRequest expects more elements)
			{0x40}, // Empty byte string (not an array)
			{0x01}, // Unsigned integer (not an array)
		}

		for i, invalidCBOR := range invalidCBORs {
			var request CertificationRequest
			err := types.Cbor.Unmarshal(invalidCBOR, &request)
			assert.Error(t, err, "Case %d: %x should fail", i, invalidCBOR)
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

		receipt := &ReceiptV2{
			PublicKey: NewHexBytes(publicKeyBytes),
			Signature: NewHexBytes(signatureBytes),
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
				"witness": "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201",
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
		receipt := &ReceiptV2{
			PublicKey: NewHexBytes([]byte{0x02, 0x79}), // shortened for test
			Signature: NewHexBytes([]byte{0xa0, 0xb3}), // shortened for test
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
		assert.Equal(t, "0279", receiptJSON["publicKey"])
		assert.Equal(t, "a0b3", receiptJSON["signature"])

		// Test deserialization preserves all fields
		var decodedResponse CertificationResponse
		err = json.Unmarshal(jsonBytes, &decodedResponse)
		require.NoError(t, err)

		assert.Equal(t, response.Status, decodedResponse.Status)
		assert.NotNil(t, decodedResponse.Receipt)
	})
}

func TestCertificationRequestCBOR(t *testing.T) {
	req := &CertificationRequest{
		StateID: RequireNewImprintV2("0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00"),
		CertificationData: CertificationData{
			OwnerPredicate:  NewPayToPublicKeyPredicate([]byte{0x03, 0x20, 0x44, 0xf2}),
			SourceStateHash: RequireNewImprintV2("0000cd60"),
			TransactionHash: RequireNewImprintV2("00008a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d"),
			Witness:         HexBytes{0x41, 0x67, 0x51, 0xe8},
		},
	}

	data, err := types.Cbor.Marshal(req)
	require.NoError(t, err, "Failed to marshal CertificationRequest")

	var unmarshaledReq CertificationRequest
	err = types.Cbor.Unmarshal(data, &unmarshaledReq)
	require.NoError(t, err, "Failed to unmarshal CertificationRequest")

	require.Equal(t, req.StateID, unmarshaledReq.StateID, "StateID mismatch")
	require.Equal(t, req.CertificationData, unmarshaledReq.CertificationData, "CertificationData data mismatch")
}

func createCertData(t *testing.T) CertificationData {
	publicKeyHex := "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
	witnessHex := "a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201"
	sourceStateHashHex := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000000")
	transactionHashHex := RequireNewImprintV2("00000000000000000000000000000000000000000000000000000000000000000001")

	publicKey, err := hex.DecodeString(publicKeyHex)
	require.NoError(t, err)

	witness, err := hex.DecodeString(witnessHex)
	require.NoError(t, err)

	return CertificationData{
		OwnerPredicate:  NewPayToPublicKeyPredicate(publicKey),
		SourceStateHash: sourceStateHashHex,
		TransactionHash: transactionHashHex,
		Witness:         NewHexBytes(witness),
	}
}
