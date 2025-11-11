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
	validator := newDefaultCommitmentValidator()

	// Generate a test key pair for signing
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Create test state hash
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)

	// Create request ID using the full imprint bytes (same as what validator will use)
	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	require.NoError(t, err, "Failed to create request ID")

	// Create transaction data and sign it
	transactionData := []byte("test-transaction-data")
	transactionHashImprint := CreateDataHashImprint(transactionData)

	// Extract the transaction hash bytes from the imprint (what the validator will use for verification)
	transactionHashBytes, err := transactionHashImprint.DataBytes()
	require.NoError(t, err, "Failed to extract transaction hash from imprint")

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction data")

	// Create commitment with valid data
	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: transactionHashImprint,
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(signatureBytes),
			StateHash: stateHashImprint,
		},
	}

	// Validate the commitment
	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusSuccess, result.Status, "Expected validation success, got status: %s, error: %v", result.Status.String(), result.Error)
	require.NoError(t, result.Error, "Expected no error")
}

func TestValidator_UnsupportedAlgorithm(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	commitment := &models.Commitment{
		RequestID:       api.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: api.TransactionHash("000048656c6c6f576f726c640123456789abcdef0123456789abcdef0123456789abcdef"),
		Authenticator: models.Authenticator{
			Algorithm: "unsupported-algorithm",
			PublicKey: api.HexBytes("test-public-key"),
			Signature: api.HexBytes("test-signature"),
			StateHash: CreateDataHashImprint([]byte("test-state-hash")),
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusUnsupportedAlgorithm, result.Status, "Expected unsupported algorithm status")
	require.Error(t, result.Error, "Expected error for unsupported algorithm")
}

func TestValidator_InvalidPublicKeyFormat(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	commitment := &models.Commitment{
		RequestID:       api.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: CreateDataHashImprint([]byte("hello")),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes("invalid-hex-public-key"), // Invalid hex
			Signature: api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: CreateDataHashImprint([]byte("test-state")),
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusInvalidPublicKeyFormat, result.Status, "Expected invalid public key format status")
	if result.Error == nil {
		t.Error("Expected error for invalid public key format")
	}
}

func TestValidator_InvalidStateHashFormat(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	// Create valid public key
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	commitment := &models.Commitment{
		RequestID:       api.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: CreateDataHashImprint([]byte("hello")),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: api.ImprintHexString("invalid-hex-state-hash"), // Invalid hex
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusInvalidStateHashFormat, result.Status, "Expected invalid state hash format status")
	if result.Error == nil {
		t.Error("Expected error for invalid state hash format")
	}
}

func TestValidator_RequestIDMismatch(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	// Create valid public key and state hash
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashBytes := []byte("test-state-hash")

	// Create a wrong request ID (not matching the public key + state hash)
	wrongRequestID := api.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	commitment := &models.Commitment{
		RequestID:       wrongRequestID,
		TransactionHash: CreateDataHashImprint([]byte("hello")),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: CreateDataHashImprint(stateHashBytes),
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusRequestIDMismatch, result.Status, "Expected request ID mismatch status")
	if result.Error == nil {
		t.Error("Expected error for request ID mismatch")
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

		// commitment ending with 0b00000000 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b10, true},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b11, false},

		// commitment ending with 0b00000001 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b10, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b11, true},

		// commitment ending with 0b00000010 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b10, true},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b11, false},

		// commitment ending with 0b00000011 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b10, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b11, true},

		// commitment ending with 0b11111111 belongs to shard2
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b10, false},
		{"000000000000000000000000000000000000000000000000000000000000000000FF", 0b11, true},

		// === END TWO SHARD CONFIG ===

		// === FOUR SHARD CONFIG ===
		// shard1=0b100
		// shard2=0b110
		// shard3=0b101
		// shard4=0b111

		// commitment ending with 0b00000000 belongs to shard1
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000000", 0b100, true},

		// commitment ending with 0b00000010 belongs to shard2
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b100, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000002", 0b110, true},

		// commitment ending with 0b00000001 belongs to shard3
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b111, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b101, true},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000001", 0b100, false},

		// commitment ending with 0b00000011 belongs to shard4
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b111, true},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b101, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b110, false},
		{"00000000000000000000000000000000000000000000000000000000000000000003", 0b100, false},

		// commitment ending with 0b11111111 belongs to shard4
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
	validator := newDefaultCommitmentValidator()

	// Create valid data except signature
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashImprintBytes, _ := stateHashImprint.Bytes()
	requestID, _ := api.CreateRequestIDFromBytes(publicKeyBytes, stateHashImprintBytes)

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: CreateDataHashImprint([]byte("hello")),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(make([]byte, 32)), // Invalid length - should be 65 bytes
			StateHash: stateHashImprint,
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusInvalidSignatureFormat, result.Status, "Expected invalid signature format status")
	if result.Error == nil {
		t.Error("Expected error for invalid signature format")
	}
}

func TestValidator_InvalidTransactionHashFormat(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	// Create valid data except transaction hash
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashImprintBytes, _ := stateHashImprint.Bytes()
	requestID, _ := api.CreateRequestIDFromBytes(publicKeyBytes, stateHashImprintBytes)

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: api.TransactionHash("invalid-hex-transaction-hash-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), // Invalid hex but 68 chars
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(make([]byte, 65)), // Valid length signature
			StateHash: stateHashImprint,
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusInvalidTransactionHashFormat, result.Status, "Expected invalid transaction hash format status")
	if result.Error == nil {
		t.Error("Expected error for invalid transaction hash format")
	}
}

func TestValidator_SignatureVerificationFailed(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	// Generate a test key pair
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashImprintBytes, _ := stateHashImprint.Bytes()
	requestID, _ := api.CreateRequestIDFromBytes(publicKeyBytes, stateHashImprintBytes)

	// Create transaction data
	transactionData := []byte("test-transaction-data")

	// Sign different data (so signature won't match)
	differentData := []byte("different-transaction-data")
	signingService := NewSigningService()
	signatureBytes, _ := signingService.Sign(differentData, privateKey.Serialize())

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: CreateDataHashImprint(transactionData), // Different from signed data
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(signatureBytes),
			StateHash: stateHashImprint,
		},
	}

	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusSignatureVerificationFailed, result.Status, "Expected signature verification failed status")
	if result.Error == nil {
		t.Error("Expected error for signature verification failure")
	}
}

func TestValidator_RealSecp256k1Data(t *testing.T) {
	// Test with real secp256k1 cryptographic operations to ensure compatibility
	validator := newDefaultCommitmentValidator()

	// Use known test vectors
	privateKeyHex := "c28a9f80738afe1441ba9a68e72033f4c8d52b4f5d6d8f1e6a6b1c4a7b8e9c1f"
	privateKeyBytes, _ := hex.DecodeString(privateKeyHex)

	// Create private key and derive public key
	privKey, _ := btcec.PrivKeyFromBytes(privateKeyBytes)
	publicKeyBytes := privKey.PubKey().SerializeCompressed()

	// Create state hash
	stateHashData := []byte("real-state-hash-test")
	stateHashImprint := CreateDataHashImprint(stateHashData)

	// Create proper request ID
	stateHashImprintBytes, _ := stateHashImprint.Bytes()
	requestID, _ := api.CreateRequestIDFromBytes(publicKeyBytes, stateHashImprintBytes)

	// Create transaction data
	transactionData := []byte("real-transaction-data-to-sign")
	transactionHashImprint := CreateDataHashImprint(transactionData)

	// Extract the transaction hash bytes from the imprint (what the validator will use for verification)
	transactionHashBytes, err := transactionHashImprint.DataBytes()
	if err != nil {
		t.Fatalf("Failed to extract transaction hash from imprint: %v", err)
	}

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKeyBytes)
	if err != nil {
		t.Fatalf("Failed to sign transaction data: %v", err)
	}

	// Create commitment with all real cryptographic data
	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: transactionHashImprint,
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(signatureBytes),
			StateHash: stateHashImprint,
		},
	}

	// Validate the commitment - should succeed
	result := validator.ValidateCommitment(commitment)

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
		{ValidationStatusRequestIDMismatch, "REQUEST_ID_MISMATCH"},
		{ValidationStatusSignatureVerificationFailed, "SIGNATURE_VERIFICATION_FAILED"},
		{ValidationStatusInvalidSignatureFormat, "INVALID_SIGNATURE_FORMAT"},
		{ValidationStatusInvalidPublicKeyFormat, "INVALID_PUBLIC_KEY_FORMAT"},
		{ValidationStatusInvalidStateHashFormat, "INVALID_STATE_HASH_FORMAT"},
		{ValidationStatusInvalidTransactionHashFormat, "INVALID_TRANSACTION_HASH_FORMAT"},
		{ValidationStatusUnsupportedAlgorithm, "UNSUPPORTED_ALGORITHM"},
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

func TestValidator_vsTS(t *testing.T) {
	validator := newDefaultCommitmentValidator()

	requestJson := `{
	  "authenticator": {
		"algorithm": "secp256k1",
		"publicKey": "02bf8d9e7687f66c7fce1e98edbc05566f7db740030722cf6cf62aca035c5035ea",
		"signature": "301c7f19d5e0a7e350012ab7bbaf26a0152a751eec06d18563f96bcf06d2380e7de7ce6cebb8c11479d1bd9c463c3ba47396b5f815c552b344d430b0d011a2e701",
		"stateHash": "0000f7f53c361c30535ed52b05f24616b5580d562ba7494e352dc2f934a51a78bb0a"
	  },
	  "receipt": false,
	  "requestId": "00009399ada3bd4dfa4bce4787bbc416be1e617a734efeb9c4d70a70d4503d5637b0",
	  "transactionHash": "0000d6035b65700f0af73cc62a580eb833c20f40aaee460087f5fb43ebb3c047f1d4"
	}`
	var request api.SubmitCommitmentRequest
	err := json.Unmarshal([]byte(requestJson), &request)
	require.NoError(t, err)

	// Create commitment with all real cryptographic data
	commitment := &models.Commitment{
		RequestID:       request.RequestID,
		TransactionHash: request.TransactionHash,
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: request.Authenticator.PublicKey,
			Signature: request.Authenticator.Signature,
			StateHash: request.Authenticator.StateHash,
		},
	}

	// Validate the commitment - should succeed
	result := validator.ValidateCommitment(commitment)

	require.Equal(t, ValidationStatusSuccess, result.Status, "Expected validation success with real secp256k1 data, got status: %s, error: %v", result.Status.String(), result.Error)
	if result.Error != nil {
		t.Errorf("Expected no error with real secp256k1 data, got: %v", result.Error)
	}
}

func newDefaultCommitmentValidator() *CommitmentValidator {
	// use standalone sharding mode to skip shard id validation
	return &CommitmentValidator{shardConfig: config.ShardingConfig{Mode: config.ShardingModeStandalone}}
}
