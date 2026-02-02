package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestCreateLeafValue tests the createLeafValue function to ensure it matches
// the TypeScript LeafValue.create() implementation exactly
func TestCreateLeafValueV1(t *testing.T) {
	t.Run("should create expected leaf value", func(t *testing.T) {
		// Create test data
		publicKeyHex := "02bf8d9e7687f66c7fce1e98edbc05566f7db740030722cf6cf62aca035c5035ea"
		signatureHex := "301c7f19d5e0a7e350012ab7bbaf26a0152a751eec06d18563f96bcf06d2380e7de7ce6cebb8c11479d1bd9c463c3ba47396b5f815c552b344d430b0d011a2e701"
		stateHashHex := "0000f7f53c361c30535ed52b05f24616b5580d562ba7494e352dc2f934a51a78bb0a"
		transactionHashHex := "0000d6035b65700f0af73cc62a580eb833c20f40aaee460087f5fb43ebb3c047f1d4"
		expectedLeafValueHex := "00000777e81da35187bc52073e96a10f89d7fe9aa826693982c8e748a96a3cc7d7b7"

		publicKey, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signature, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		stateHash, err := api.NewImprintHexString(stateHashHex)
		require.NoError(t, err)

		transactionHash, err := api.NewImprintHexString(transactionHashHex)
		require.NoError(t, err)

		requestID, err := api.CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Create authenticator
		authenticator := Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.NewHexBytes(publicKey),
			Signature: api.NewHexBytes(signature),
			StateHash: stateHash,
		}

		// Create commitment
		commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

		// Call createLeafValue
		leafValue, err := commitment.CreateLeafValue()
		require.NoError(t, err)
		require.NotNil(t, leafValue)

		// Verify the result is a 34-byte DataHash imprint (2 bytes algorithm + 32 bytes SHA256)
		assert.Equal(t, 34, len(leafValue))

		// Verify the leaf value matches the expected hex string
		expectedLeafValue, err := hex.DecodeString(expectedLeafValueHex)
		require.NoError(t, err)
		assert.Equal(t, expectedLeafValue, leafValue)
	})

	t.Run("should create consistent leaf value with CBOR encoding", func(t *testing.T) {
		// Create test data
		publicKeyHex := "02bf8d9e7687f66c7fce1e98edbc05566f7db740030722cf6cf62aca035c5035ea"
		signatureHex := "301c7f19d5e0a7e350012ab7bbaf26a0152a751eec06d18563f96bcf06d2380e7de7ce6cebb8c11479d1bd9c463c3ba47396b5f815c552b344d430b0d011a2e701"
		stateHashHex := "0000f7f53c361c30535ed52b05f24616b5580d562ba7494e352dc2f934a51a78bb0a"
		transactionHashHex := "0000d6035b65700f0af73cc62a580eb833c20f40aaee460087f5fb43ebb3c047f1d4"

		publicKey, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)

		signature, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)

		stateHash, err := api.NewImprintHexString(stateHashHex)
		require.NoError(t, err)

		transactionHash, err := api.NewImprintHexString(transactionHashHex)
		require.NoError(t, err)

		requestID, err := api.CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Create authenticator
		authenticator := Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.NewHexBytes(publicKey),
			Signature: api.NewHexBytes(signature),
			StateHash: stateHash,
		}

		// Create commitment
		commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

		// Call createLeafValue
		leafValue, err := commitment.CreateLeafValue()
		require.NoError(t, err)
		require.NotNil(t, leafValue)

		// Verify the result is a 34-byte DataHash imprint (2 bytes algorithm + 32 bytes SHA256)
		assert.Equal(t, 34, len(leafValue))

		// Verify the algorithm prefix is correct (0x00, 0x00 for SHA256)
		assert.Equal(t, byte(0x00), leafValue[0])
		assert.Equal(t, byte(0x00), leafValue[1])

		// Verify the leaf value is deterministic by calling again
		leafValue2, err := commitment.CreateLeafValue()
		require.NoError(t, err)
		assert.Equal(t, leafValue, leafValue2)
	})

	t.Run("should match manual CBOR encoding and hashing", func(t *testing.T) {
		// Create test data
		authenticator := Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.NewHexBytes([]byte{0x02, 0x79, 0xbe, 0x66}),
			Signature: api.NewHexBytes([]byte{0xa0, 0xb3, 0x7f, 0x8f}),
			StateHash: api.ImprintHexString("0000deadbeef"),
		}

		transactionHash, err := api.NewImprintHexString("0000feedcafe")
		require.NoError(t, err)

		requestID, err := api.CreateRequestID(authenticator.PublicKey, authenticator.StateHash)
		require.NoError(t, err)

		commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

		// Get leaf value from our function
		leafValue, err := commitment.CreateLeafValue()
		require.NoError(t, err)

		// Manually replicate the expected process
		// 1. Get state hash imprint for CBOR encoding
		stateHashImprint, err := authenticator.StateHash.Imprint()
		require.NoError(t, err)

		// 2. CBOR encode the authenticator as an array (matching TypeScript)
		authenticatorArray := []interface{}{
			authenticator.Algorithm,         // algorithm as text string
			[]byte(authenticator.PublicKey), // publicKey as byte string
			[]byte(authenticator.Signature), // signature as byte string
			stateHashImprint,                // stateHash.imprint as byte string
		}
		expectedCBOR, err := cbor.Marshal(authenticatorArray)
		require.NoError(t, err)

		// 3. Get transaction hash imprint
		expectedTransactionImprint, err := transactionHash.Imprint()
		require.NoError(t, err)

		// 4. Create SHA256 hash of CBOR + transaction imprint
		expectedHasher := sha256.New()
		expectedHasher.Write(expectedCBOR)
		expectedHasher.Write(expectedTransactionImprint)
		expectedHash := expectedHasher.Sum(nil)

		// 5. Create imprint with SHA256 algorithm prefix (0x00, 0x00)
		expectedImprint := make([]byte, 2+len(expectedHash))
		expectedImprint[0] = 0x00 // SHA256 algorithm high byte
		expectedImprint[1] = 0x00 // SHA256 algorithm low byte
		copy(expectedImprint[2:], expectedHash[:])

		// Verify they match
		assert.Equal(t, expectedImprint, leafValue)
	})

	t.Run("should handle different authenticator data", func(t *testing.T) {
		testCases := []struct {
			name      string
			algorithm string
			publicKey []byte
			signature []byte
			stateHash string
			txHash    string
		}{
			{
				name:      "secp256k1 with normal data",
				algorithm: "secp256k1",
				publicKey: []byte{0x03, 0x01, 0x02, 0x03},
				signature: []byte{0x04, 0x05, 0x06, 0x07},
				stateHash: "0000abcdef123456",
				txHash:    "0000fedcba654321",
			},
			{
				name:      "ed25519 with different data",
				algorithm: "ed25519",
				publicKey: []byte{0x11, 0x22, 0x33, 0x44},
				signature: []byte{0x55, 0x66, 0x77, 0x88},
				stateHash: "0000ffffff000000",
				txHash:    "0000000000ffffff",
			},
			{
				name:      "empty signature",
				algorithm: "test",
				publicKey: []byte{0x99},
				signature: []byte{},
				stateHash: "000001",
				txHash:    "000002",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				stateHash, err := api.NewImprintHexString(tc.stateHash)
				require.NoError(t, err)

				transactionHash, err := api.NewImprintHexString(tc.txHash)
				require.NoError(t, err)

				requestID, err := api.CreateRequestID(tc.publicKey, stateHash)
				require.NoError(t, err)

				authenticator := Authenticator{
					Algorithm: tc.algorithm,
					PublicKey: api.NewHexBytes(tc.publicKey),
					Signature: api.NewHexBytes(tc.signature),
					StateHash: stateHash,
				}

				commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

				leafValue, err := commitment.CreateLeafValue()
				require.NoError(t, err)
				require.NotNil(t, leafValue)
				assert.Equal(t, 34, len(leafValue)) // DataHash imprint: 2 bytes algorithm + 32 bytes SHA256
			})
		}
	})

	t.Run("should handle CBOR encoding errors", func(t *testing.T) {
		// This test would need to create a scenario where CBOR encoding fails
		// Since the current Authenticator struct is well-defined and should always
		// encode properly, this test serves as a placeholder for future edge cases

		// For now, just verify normal operation
		stateHash, err := api.NewImprintHexString("0000deadbeef")
		require.NoError(t, err)

		transactionHash, err := api.NewImprintHexString("0000feedcafe")
		require.NoError(t, err)

		requestID, err := api.CreateRequestID([]byte{0x01}, stateHash)
		require.NoError(t, err)

		authenticator := Authenticator{
			Algorithm: "test",
			PublicKey: api.NewHexBytes([]byte{0x01}),
			Signature: api.NewHexBytes([]byte{0x02}),
			StateHash: stateHash,
		}

		commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

		leafValue, err := commitment.CreateLeafValue()
		require.NoError(t, err)
		require.NotNil(t, leafValue)
	})

	t.Run("should handle transaction hash imprint errors", func(t *testing.T) {
		// Test with invalid transaction hash
		stateHash, err := api.NewImprintHexString("0000deadbeef")
		require.NoError(t, err)

		requestID, err := api.CreateRequestID([]byte{0x01}, stateHash)
		require.NoError(t, err)

		authenticator := Authenticator{
			Algorithm: "test",
			PublicKey: api.NewHexBytes([]byte{0x01}),
			Signature: api.NewHexBytes([]byte{0x02}),
			StateHash: stateHash,
		}

		// Create invalid transaction hash (contains invalid hex characters)
		invalidTransactionHash := api.ImprintHexString("invalid")
		commitment := NewCommitment(requestID, invalidTransactionHash, authenticator).ToAPI()

		leafValue, err := commitment.CreateLeafValue()
		require.Error(t, err)
		require.Nil(t, leafValue)
		assert.Contains(t, err.Error(), "failed to get transaction hash imprint")
	})

	t.Run("should produce different values for different inputs", func(t *testing.T) {
		// Create two different commitments
		stateHash1, err := api.NewImprintHexString("0000deadbeef01")
		require.NoError(t, err)

		stateHash2, err := api.NewImprintHexString("0000deadbeef02")
		require.NoError(t, err)

		transactionHash1, err := api.NewImprintHexString("0000feedcafe01")
		require.NoError(t, err)

		transactionHash2, err := api.NewImprintHexString("0000feedcafe02")
		require.NoError(t, err)

		requestID1, err := api.CreateRequestID([]byte{0x01}, stateHash1)
		require.NoError(t, err)

		requestID2, err := api.CreateRequestID([]byte{0x02}, stateHash2)
		require.NoError(t, err)

		authenticator1 := Authenticator{
			Algorithm: "test",
			PublicKey: api.NewHexBytes([]byte{0x01}),
			Signature: api.NewHexBytes([]byte{0x02}),
			StateHash: stateHash1,
		}

		authenticator2 := Authenticator{
			Algorithm: "test",
			PublicKey: api.NewHexBytes([]byte{0x02}),
			Signature: api.NewHexBytes([]byte{0x03}),
			StateHash: stateHash2,
		}

		commitment1 := NewCommitment(requestID1, transactionHash1, authenticator1).ToAPI()
		commitment2 := NewCommitment(requestID2, transactionHash2, authenticator2).ToAPI()

		leafValue1, err := commitment1.CreateLeafValue()
		require.NoError(t, err)

		leafValue2, err := commitment2.CreateLeafValue()
		require.NoError(t, err)

		// Values should be different
		assert.NotEqual(t, leafValue1, leafValue2)
	})
}

// BenchmarkCreateLeafValue benchmarks the createLeafValue function
func BenchmarkCreateLeafValueV1(b *testing.B) {
	// Setup test data
	publicKey := []byte{0x02, 0x79, 0xbe, 0x66}
	signature := []byte{0xa0, 0xb3, 0x7f, 0x8f}
	stateHash, _ := api.NewImprintHexString("0000deadbeef")
	transactionHash, _ := api.NewImprintHexString("0000feedcafe")
	requestID, _ := api.CreateRequestID(publicKey, stateHash)

	authenticator := Authenticator{
		Algorithm: "secp256k1",
		PublicKey: api.NewHexBytes(publicKey),
		Signature: api.NewHexBytes(signature),
		StateHash: stateHash,
	}

	commitment := NewCommitment(requestID, transactionHash, authenticator).ToAPI()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := commitment.CreateLeafValue()
		if err != nil {
			b.Fatal(err)
		}
	}
}
