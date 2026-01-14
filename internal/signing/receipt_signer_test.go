package signing

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestNewReceiptSigner(t *testing.T) {
	t.Run("valid private key", func(t *testing.T) {
		signingService := NewSigningService()
		privKey, pubKey, err := signingService.GenerateKeyPair()
		require.NoError(t, err)

		signer, err := NewReceiptSigner(privKey)
		require.NoError(t, err)
		assert.NotNil(t, signer)
		assert.Equal(t, "secp256k1", signer.Algorithm())
		assert.Equal(t, pubKey, signer.PublicKey())
	})

	t.Run("invalid private key length", func(t *testing.T) {
		invalidKey := make([]byte, 31) // Should be 32 bytes
		signer, err := NewReceiptSigner(invalidKey)
		assert.Error(t, err)
		assert.Nil(t, signer)
		assert.Contains(t, err.Error(), "private key must be 32 bytes")
	})
}

func TestReceiptSigner_SignReceiptV1(t *testing.T) {
	// Generate a key pair for testing
	signingService := NewSigningService()
	privKey, pubKey, err := signingService.GenerateKeyPair()
	require.NoError(t, err)

	signer, err := NewReceiptSigner(privKey)
	require.NoError(t, err)

	// Create test data
	requestID := api.RequestID("0x0001abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678")
	transactionHash := api.TransactionHash("0x0001fedcba0987654321fedcba0987654321fedcba0987654321fedcba09876543")
	stateHash := api.SourceStateHash("0x0001123456789abcdef123456789abcdef123456789abcdef123456789abcdef12")

	t.Run("sign receipt successfully", func(t *testing.T) {
		receipt, err := signer.SignReceiptV1(requestID, transactionHash, stateHash)
		require.NoError(t, err)
		assert.NotNil(t, receipt)

		// Verify receipt fields
		assert.Equal(t, "secp256k1", receipt.Algorithm)
		assert.Equal(t, pubKey, []byte(receipt.PublicKey))
		assert.Len(t, receipt.Signature, 65) // secp256k1 signature length

		// Verify request fields
		assert.Equal(t, "aggregator", receipt.Request.Service)
		assert.Equal(t, "submit_commitment", receipt.Request.Method)
		assert.Equal(t, requestID, receipt.Request.RequestID)
		assert.Equal(t, transactionHash, receipt.Request.TransactionHash)
		assert.Equal(t, stateHash, receipt.Request.StateHash)
	})

	t.Run("signature is verifiable", func(t *testing.T) {
		receipt, err := signer.SignReceiptV1(requestID, transactionHash, stateHash)
		require.NoError(t, err)

		// Serialize the request data the same way as during signing
		requestBytes, err := json.Marshal(receipt.Request)
		require.NoError(t, err)

		// Verify the signature
		valid, err := signingService.VerifyWithPublicKey(requestBytes, receipt.Signature, receipt.PublicKey)
		require.NoError(t, err)
		assert.True(t, valid, "Receipt signature should be valid")
	})

	t.Run("different inputs produce different signatures", func(t *testing.T) {
		receipt1, err := signer.SignReceiptV1(requestID, transactionHash, stateHash)
		require.NoError(t, err)

		// Use a different request ID
		differentRequestID := api.RequestID("0x0001111111111111111111111111111111111111111111111111111111111111")
		receipt2, err := signer.SignReceiptV1(differentRequestID, transactionHash, stateHash)
		require.NoError(t, err)

		// Signatures should be different
		assert.NotEqual(t, receipt1.Signature, receipt2.Signature)
	})

	t.Run("receipts are deterministic for same input", func(t *testing.T) {
		receipt1, err := signer.SignReceiptV1(requestID, transactionHash, stateHash)
		require.NoError(t, err)

		receipt2, err := signer.SignReceiptV1(requestID, transactionHash, stateHash)
		require.NoError(t, err)

		// Request data should be identical
		assert.Equal(t, receipt1.Request, receipt2.Request)
		assert.Equal(t, receipt1.Algorithm, receipt2.Algorithm)
		assert.Equal(t, receipt1.PublicKey, receipt2.PublicKey)

		// Note: ECDSA signatures may differ due to randomness (k value),
		// but both should verify against the same data
		requestBytes, _ := json.Marshal(receipt1.Request)

		valid1, _ := signingService.VerifyWithPublicKey(requestBytes, receipt1.Signature, receipt1.PublicKey)
		valid2, _ := signingService.VerifyWithPublicKey(requestBytes, receipt2.Signature, receipt2.PublicKey)

		assert.True(t, valid1)
		assert.True(t, valid2)
	})
}

func TestReceiptSigner_SignReceiptV2(t *testing.T) {
	// Generate a key pair for testing
	signingService := NewSigningService()
	privKey, pubKey, err := signingService.GenerateKeyPair()
	require.NoError(t, err)

	signer, err := NewReceiptSigner(privKey)
	require.NoError(t, err)

	// Create test data
	certData := api.CertificationData{
		OwnerPredicate:  api.Predicate{},
		SourceStateHash: "01",
		TransactionHash: "02",
		Witness:         nil,
	}

	t.Run("sign receipt successfully", func(t *testing.T) {
		receipt, err := signer.SignReceiptV2(certData)
		require.NoError(t, err)
		require.NotNil(t, receipt)

		require.Equal(t, pubKey, []byte(receipt.PublicKey))
		require.Len(t, receipt.Signature, 65) // secp256k1 signature length
	})

	t.Run("signature is verifiable", func(t *testing.T) {
		receipt, err := signer.SignReceiptV2(certData)
		require.NoError(t, err)

		// Serialize the request data the same way as during signing
		requestBytes, err := types.Cbor.Marshal(certData)
		require.NoError(t, err)

		// Verify the signature
		valid, err := signingService.VerifyWithPublicKey(requestBytes, receipt.Signature, receipt.PublicKey)
		require.NoError(t, err)
		assert.True(t, valid, "Receipt signature should be valid")
	})
}

func TestReceiptSigner_PublicKeyAndAlgorithm(t *testing.T) {
	signingService := NewSigningService()
	privKey, expectedPubKey, err := signingService.GenerateKeyPair()
	require.NoError(t, err)

	signer, err := NewReceiptSigner(privKey)
	require.NoError(t, err)

	t.Run("PublicKey returns correct key", func(t *testing.T) {
		assert.Equal(t, expectedPubKey, signer.PublicKey())
	})

	t.Run("Algorithm returns secp256k1", func(t *testing.T) {
		assert.Equal(t, "secp256k1", signer.Algorithm())
	})
}
