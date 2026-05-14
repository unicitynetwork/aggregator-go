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

		// Test serialization
		request1 := CertificationRequest{
			Version:           1,
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
	})

	t.Run("should validate CBOR structure correctly", func(t *testing.T) {
		// Create a valid request
		publicKey := make([]byte, 20)
		ownerPredicate := NewPayToPublicKeyPredicate(publicKey)
		sourceStateHash := RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000")
		stateID, _ := CreateStateID(ownerPredicate, sourceStateHash)
		transactionHash := RequireNewImprintV2("0100000000000000000000000000000000000000000000000000000000000000")
		witness := NewHexBytes([]byte{0x01, 0x02, 0x03})

		request := &CertificationRequest{
			StateID: stateID,
			CertificationData: CertificationData{
				OwnerPredicate:  ownerPredicate,
				SourceStateHash: sourceStateHash,
				TransactionHash: transactionHash,
				Witness:         witness,
			},
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
		// Test simple success response
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

		// Test round-trip
		jsonBytes, err := json.Marshal(response1)
		require.NoError(t, err)

		var roundTripResponse CertificationResponse
		err = json.Unmarshal(jsonBytes, &roundTripResponse)
		require.NoError(t, err)

		assert.Equal(t, response1.Status, roundTripResponse.Status)
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
}

func TestCertificationRequestCBOR(t *testing.T) {
	req := &CertificationRequest{
		Version: 1,
		StateID: RequireNewImprintV2("cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00"),
		CertificationData: CertificationData{
			Version:         1,
			OwnerPredicate:  NewPayToPublicKeyPredicate([]byte{0x03, 0x20, 0x44, 0xf2}),
			SourceStateHash: RequireNewImprintV2("cd60000000000000000000000000000000000000000000000000000000000000"),
			TransactionHash: RequireNewImprintV2("8a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d"),
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
	sourceStateHashHex := RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000")
	transactionHashHex := RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000001")

	publicKey, err := hex.DecodeString(publicKeyHex)
	require.NoError(t, err)

	witness, err := hex.DecodeString(witnessHex)
	require.NoError(t, err)

	return CertificationData{
		Version:         1,
		OwnerPredicate:  NewPayToPublicKeyPredicate(publicKey),
		SourceStateHash: sourceStateHashHex,
		TransactionHash: transactionHashHex,
		Witness:         NewHexBytes(witness),
	}
}

func TestCertificationDataHashing_Compatibility(t *testing.T) {
	t.Run("certification data hash preserves canonical CBOR input bytes", func(t *testing.T) {
		certData := createCertData(t)

		predicateBytes, err := types.Cbor.Marshal(certData.OwnerPredicate)
		require.NoError(t, err)

		expectedBytes := append([]byte{0x84}, predicateBytes...)
		expectedBytes = append(expectedBytes, []byte{0x58, 0x20}...)
		expectedBytes = append(expectedBytes, certData.SourceStateHash...)
		expectedBytes = append(expectedBytes, []byte{0x58, 0x20}...)
		expectedBytes = append(expectedBytes, certData.TransactionHash...)
		expectedBytes = append(expectedBytes, append([]byte{0x58, byte(len(certData.Witness))}, certData.Witness...)...)
		expectedHash := NewDataHasher(SHA256).AddData(expectedBytes).GetHash()

		type certDataInput struct {
			_               struct{} `cbor:",toarray"`
			OwnerPredicate  Predicate
			SourceStateHash []byte
			TransactionHash []byte
			Witness         []byte
		}
		canonicalBytes, err := types.Cbor.Marshal(certDataInput{
			OwnerPredicate:  certData.OwnerPredicate,
			SourceStateHash: certData.SourceStateHash,
			TransactionHash: certData.TransactionHash,
			Witness:         certData.Witness,
		})
		require.NoError(t, err)
		assert.Equal(t, expectedBytes, canonicalBytes)

		gotHash, err := CertDataHash(certData.OwnerPredicate, certData.SourceStateHash, certData.TransactionHash, certData.Witness)
		require.NoError(t, err)
		assert.Equal(t, expectedHash.RawHash, gotHash.RawHash)
	})
}

func TestCertificationDataHashing_InvalidLengths(t *testing.T) {
	validPredicate := NewPayToPublicKeyPredicate([]byte{0x01, 0x02, 0x03})
	validHash := make([]byte, 32)
	shortHash := []byte{0x01, 0x02}
	longHash := make([]byte, 64)

	t.Run("CertDataHash should reject invalid hash lengths", func(t *testing.T) {
		_, err := CertDataHash(validPredicate, shortHash, validHash, []byte{0x00})
		assert.ErrorContains(t, err, "invalid source state hash length")

		_, err = CertDataHash(validPredicate, validHash, longHash, []byte{0x00})
		assert.ErrorContains(t, err, "invalid transaction hash length")

		_, err = CertDataHash(validPredicate, []byte{}, validHash, []byte{0x00})
		assert.ErrorContains(t, err, "invalid source state hash length")
	})

	t.Run("CertificationData.SigDataHash should reject invalid hash lengths", func(t *testing.T) {
		cd := CertificationData{
			OwnerPredicate:  validPredicate,
			SourceStateHash: shortHash,
			TransactionHash: validHash,
		}
		_, err := cd.SigDataHash()
		assert.ErrorContains(t, err, "invalid source state hash length")

		cd.SourceStateHash = validHash
		cd.TransactionHash = longHash
		_, err = cd.SigDataHash()
		assert.ErrorContains(t, err, "invalid transaction hash length")

		cd.TransactionHash = []byte{}
		_, err = cd.SigDataHash()
		assert.ErrorContains(t, err, "invalid transaction hash length")
	})
}

func TestCertificationDataHashing_InvalidHashLengths(t *testing.T) {
	certData := createCertData(t)
	validSource := append([]byte(nil), certData.SourceStateHash...)
	validTransaction := append([]byte(nil), certData.TransactionHash...)

	cases := []struct {
		name              string
		sourceStateHash   []byte
		transactionHash   []byte
		expectedErrorPart string
	}{
		{
			name:              "empty source hash",
			sourceStateHash:   nil,
			transactionHash:   validTransaction,
			expectedErrorPart: "invalid source state hash length",
		},
		{
			name:              "short source hash",
			sourceStateHash:   []byte{0x01, 0x02},
			transactionHash:   validTransaction,
			expectedErrorPart: "invalid source state hash length",
		},
		{
			name:              "large source hash",
			sourceStateHash:   make([]byte, StateTreeKeyLengthBytes+1),
			transactionHash:   validTransaction,
			expectedErrorPart: "invalid source state hash length",
		},
		{
			name:              "empty transaction hash",
			sourceStateHash:   validSource,
			transactionHash:   nil,
			expectedErrorPart: "invalid transaction hash length",
		},
		{
			name:              "short transaction hash",
			sourceStateHash:   validSource,
			transactionHash:   []byte{0x01, 0x02},
			expectedErrorPart: "invalid transaction hash length",
		},
		{
			name:              "large transaction hash",
			sourceStateHash:   validSource,
			transactionHash:   make([]byte, StateTreeKeyLengthBytes+1),
			expectedErrorPart: "invalid transaction hash length",
		},
	}

	for _, tc := range cases {
		t.Run("CertDataHash rejects "+tc.name, func(t *testing.T) {
			_, err := CertDataHash(certData.OwnerPredicate, tc.sourceStateHash, tc.transactionHash, certData.Witness)
			require.ErrorContains(t, err, tc.expectedErrorPart)
		})

		t.Run("CertificationData.SigDataHash rejects "+tc.name, func(t *testing.T) {
			certData := certData
			certData.SourceStateHash = SourceStateHash(tc.sourceStateHash)
			certData.TransactionHash = TransactionHash(tc.transactionHash)

			_, err := certData.SigDataHash()
			require.ErrorContains(t, err, tc.expectedErrorPart)
		})
	}
}
