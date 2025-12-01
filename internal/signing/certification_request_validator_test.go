package signing

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestValidator_Success(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Generate a test key pair for signing
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Create test state hash
	sourceStateHashData := []byte("test-state-hash")
	sourceStateHash := CreateDataHashImprint(sourceStateHashData)
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	require.NoError(t, err)

	// Create state ID using the full imprint bytes (same as what validator will use)
	stateID, err := api.CreateStateIDFromImprint(sourceStateHashImprint, publicKeyBytes)
	require.NoError(t, err, "Failed to create state ID")

	// Create transaction data and sign it
	transactionData := []byte("test-transaction-data")
	transactionDataHash := CreateDataHashImprint(transactionData)
	transactionDataHashImprint, err := transactionDataHash.Imprint()
	require.NoError(t, err)

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHashImprint, transactionDataHashImprint)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction data")

	// Create certification request with valid data
	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionDataHash,
			Signature:       api.HexBytes(signatureBytes),
		},
	}

	// Validate the commitment
	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusSuccess, result.Status, "Expected validation success, got status: %s, error: %v", result.Status.String(), result.Error)
	require.NoError(t, result.Error, "Expected no error")

	_, err = json.Marshal(commitment)
	require.NoError(t, err, "Failed to marshal commitment")
}

func TestValidator_InvalidPublicKeyFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	commitment := &models.CertificationRequest{
		StateID: api.StateID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes("invalid-hex-public-key"), // Invalid hex
			SourceStateHash: CreateDataHashImprint([]byte("test-state")),
			TransactionHash: CreateDataHashImprint([]byte("hello")),
			Signature:       api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusInvalidPublicKeyFormat, result.Status, "Expected invalid public key format status")
	if result.Error == nil {
		t.Error("Expected error for invalid public key format")
	}
}

func TestValidator_InvalidStateHashFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid public key
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	commitment := &models.CertificationRequest{
		StateID: api.StateID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: api.ImprintHexString("invalid-hex-state-hash"), // Invalid hex
			TransactionHash: CreateDataHashImprint([]byte("hello")),
			Signature:       api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusInvalidSourceStateHashFormat, result.Status, "Expected invalid state hash format status")
	if result.Error == nil {
		t.Error("Expected error for invalid state hash format")
	}
}

func TestValidator_StateIDMismatch(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid public key and state hash
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashBytes := []byte("test-state-hash")

	// Create a wrong state ID (not matching the public key + state hash)
	wrongStateID := api.StateID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	commitment := &models.CertificationRequest{
		StateID: wrongStateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: CreateDataHashImprint(stateHashBytes),
			TransactionHash: CreateDataHashImprint([]byte("hello")),
			Signature:       api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusStateIDMismatch, result.Status, "Expected state ID mismatch status")
	if result.Error == nil {
		t.Error("Expected error for state ID mismatch")
	}
}

func TestValidator_ShardID(t *testing.T) {
	tests := []struct {
		commitmentID string
		shardBitmask int
		match        bool
	}{
		// === TWO SHARD CONFIG ===
		// shard1=bitmask 0b10
		// shard2=bitmask 0b11

		// certification request ending with 0b00000000 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b10, true},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b11, false},

		// certification request ending with 0b00000001 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b10, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b11, true},

		// certification request ending with 0b00000010 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b10, true},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b11, false},

		// certification request ending with 0b00000011 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b10, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b11, true},

		// certification request ending with 0b11111111 belongs to shard2
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b10, false},
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b11, true},

		// === END TWO SHARD CONFIG ===

		// === FOUR SHARD CONFIG ===
		// shard1=0b100
		// shard2=0b110
		// shard3=0b101
		// shard4=0b111

		// certification request ending with 0b00000000 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b100, true},

		// certification request ending with 0b00000010 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b100, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b110, true},

		// certification request ending with 0b00000001 belongs to shard3
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b101, true},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b100, false},

		// certification request ending with 0b00000011 belongs to shard4
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b111, true},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b100, false},

		// certification request ending with 0b11111111 belongs to shard4
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b111, true},
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b101, false},
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b110, false},
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b100, false},

		// === END FOUR SHARD CONFIG ===
	}
	for _, tc := range tests {
		match, err := verifyShardID(tc.commitmentID, tc.shardBitmask)
		require.NoError(t, err)
		if match != tc.match {
			t.Errorf("commitmentID=%s shardBitmask=%b expected %v got %v", tc.commitmentID, tc.shardBitmask, tc.match, match)
		}
	}
}

func TestValidator_InvalidSignatureFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid data except signature
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	sourceStateHashImprint := CreateDataHashImprint(stateHashData)
	stateID, err := api.CreateStateID(sourceStateHashImprint, publicKeyBytes)
	require.NoError(t, err)

	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: CreateDataHashImprint([]byte("hello")),
			Signature:       api.HexBytes(make([]byte, 32)), // Invalid length - should be 65 bytes
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusInvalidSignatureFormat, result.Status, "Expected invalid signature format status")
	if result.Error == nil {
		t.Error("Expected error for invalid signature format")
	}
}

func TestValidator_InvalidTransactionHashFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid data except transaction hash
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	sourceStateHashImprint := CreateDataHashImprint(stateHashData)
	stateID, err := api.CreateStateID(sourceStateHashImprint, publicKeyBytes)
	require.NoError(t, err)

	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: api.TransactionHash("invalid-hex-transaction-hash-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), // Invalid hex but 68 chars
			Signature:       api.HexBytes(make([]byte, 65)),                                                         // Valid length signature
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusInvalidTransactionHashFormat, result.Status, "Expected invalid transaction hash format status")
	if result.Error == nil {
		t.Error("Expected error for invalid transaction hash format")
	}
}

func TestValidator_SignatureVerificationFailed(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Generate a test key pair
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	sourceStateHashImprint := CreateDataHashImprint(stateHashData)
	stateID, err := api.CreateStateID(sourceStateHashImprint, publicKeyBytes)
	require.NoError(t, err)

	// Create transaction data
	transactionData := []byte("test-transaction-data")

	// Sign different data (so signature won't match)
	differentData := []byte("different-transaction-data")
	signingService := NewSigningService()
	signatureBytes, _ := signingService.Sign(differentData, privateKey.Serialize())

	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: CreateDataHashImprint(transactionData), // Different from signed data
			Signature:       api.HexBytes(signatureBytes),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusSignatureVerificationFailed, result.Status, "Expected signature verification failed status")
	if result.Error == nil {
		t.Error("Expected error for signature verification failure")
	}
}

func TestValidator_RealSecp256k1Data(t *testing.T) {
	// Test with real secp256k1 cryptographic operations to ensure compatibility
	validator := newDefaultCertificationRequestValidator()

	// Use known test vectors
	privateKeyHex := "c28a9f80738afe1441ba9a68e72033f4c8d52b4f5d6d8f1e6a6b1c4a7b8e9c1f"
	privateKeyBytes, _ := hex.DecodeString(privateKeyHex)

	// Create private key and derive public key
	privKey, _ := btcec.PrivKeyFromBytes(privateKeyBytes)
	publicKeyBytes := privKey.PubKey().SerializeCompressed()

	// Create state hash
	sourceStateData := []byte("real-state-hash-test")
	sourceStateHashImprint := CreateDataHashImprint(sourceStateData)
	sourceStateHashImprintBytes, err := sourceStateHashImprint.Imprint()
	require.NoError(t, err)

	// Create proper state ID
	stateID, err := api.CreateStateIDFromImprint(sourceStateHashImprintBytes, publicKeyBytes)
	require.NoError(t, err)

	// Create transaction data
	transactionData := []byte("real-transaction-data-to-sign")
	transactionHashImprint := CreateDataHashImprint(transactionData)
	transactionHashImprintBytes, err := transactionHashImprint.Imprint()
	require.NoError(t, err)

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHashImprintBytes, transactionHashImprintBytes)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKeyBytes)
	if err != nil {
		t.Fatalf("Failed to sign transaction data: %v", err)
	}

	// Create certification request with all real cryptographic data
	certificationRequest := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.HexBytes(publicKeyBytes),
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: transactionHashImprint,
			Signature:       api.HexBytes(signatureBytes),
		},
	}

	// Validate the certification request - should succeed
	result := validator.Validate(certificationRequest)

	require.Equal(t, ValidationStatusSuccess, result.Status, "Expected validation success with real secp256k1 data, got status: %s, error: %v", result.Status.String(), result.Error)
	if result.Error != nil {
		t.Errorf("Expected no error with real secp256k1 data, got: %v", result.Error)
	}
}

func TestValidator_ValidationStatusString(t *testing.T) {
	tests := []struct {
		status   ValidationStatus
		expected string
	}{
		{ValidationStatusSuccess, "SUCCESS"},
		{ValidationStatusStateIDMismatch, "STATE_ID_MISMATCH"},
		{ValidationStatusSignatureVerificationFailed, "SIGNATURE_VERIFICATION_FAILED"},
		{ValidationStatusInvalidSignatureFormat, "INVALID_SIGNATURE_FORMAT"},
		{ValidationStatusInvalidPublicKeyFormat, "INVALID_PUBLIC_KEY_FORMAT"},
		{ValidationStatusInvalidSourceStateHashFormat, "INVALID_SOURCE_STATE_HASH_FORMAT"},
		{ValidationStatusInvalidTransactionHashFormat, "INVALID_TRANSACTION_HASH_FORMAT"},
		{ValidationStatusShardMismatch, "INVALID_SHARD"},
		{ValidationStatus(999), "UNKNOWN"}, // Test unknown status
	}

	for _, test := range tests {
		result := test.status.String()
		if result != test.expected {
			t.Errorf("Expected status string %s, got %s", test.expected, result)
		}
	}
}

func newDefaultCertificationRequestValidator() *CertificationRequestValidator {
	// use standalone sharding mode to skip shard id validation
	return &CertificationRequestValidator{shardConfig: config.ShardingConfig{Mode: config.ShardingModeStandalone}}
}
