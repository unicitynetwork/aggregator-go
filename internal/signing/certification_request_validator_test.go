package signing

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	// Create test state hash
	sourceStateHashData := []byte("test-state-hash")
	sourceStateHash := CreateDataHash(sourceStateHashData)

	// Create state ID using the raw v2 hash bytes (same as what validator will use)
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	require.NoError(t, err, "Failed to create state ID")

	// Create transaction data and sign it
	transactionData := []byte("test-transaction-data")
	transactionDataHash := CreateDataHash(transactionData)
	transactionHash := transactionDataHash.Imprint()

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHash, transactionHash)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction data")

	// Create certification request with valid data
	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionDataHash,
			Witness:         api.HexBytes(signatureBytes),
		},
	}

	// Validate the commitment
	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusSuccess, result.Status, "Expected validation success, got status: %s, error: %v", result.Status.String(), result.Error)
	require.NoError(t, result.Error, "Expected no error")

	jsonBytes, err := json.Marshal(commitment)
	require.NoError(t, err, "Failed to marshal commitment")
	fmt.Println(string(jsonBytes))
}

func TestValidator_InvalidOwnerPredicateFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	commitment := &models.CertificationRequest{
		StateID: api.RequireNewImprintV2("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		CertificationData: models.CertificationData{
			OwnerPredicate: api.Predicate{
				Engine: 1,
				Code:   []byte{2}, // invalid code
				Params: publicKeyBytes,
			},
			SourceStateHash: CreateDataHash([]byte("test-state")),
			TransactionHash: CreateDataHash([]byte("hello")),
			Witness:         api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusInvalidOwnerPredicate, result.Status, "Expected invalid owner predicate format status: %v", result.Error)
	if result.Error == nil {
		t.Error("Expected error for invalid owner predicate format")
	}
}

func TestValidator_InvalidStateHashFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid public key
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	commitment := &models.CertificationRequest{
		StateID: api.RequireNewImprintV2("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: api.ImprintV2("invalid-hex-state-hash"), // Invalid hex
			TransactionHash: CreateDataHash([]byte("hello")),
			Witness:         api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
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
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	// Create a wrong state ID (not matching the public key + state hash)
	wrongStateID := api.RequireNewImprintV2("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	commitment := &models.CertificationRequest{
		StateID: wrongStateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: CreateDataHash(stateHashBytes),
			TransactionHash: CreateDataHash([]byte("hello")),
			Witness:         api.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
		},
	}

	result := validator.Validate(commitment)

	require.Equal(t, ValidationStatusStateIDMismatch, result.Status, "Expected state ID mismatch status")
	if result.Error == nil {
		t.Error("Expected error for state ID mismatch")
	}
}

func TestValidator_ShardID(t *testing.T) {
	makeShardTestID := func(firstByte, lastByte byte) string {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		key[0] = firstByte
		key[len(key)-1] = lastByte
		return hex.EncodeToString(key)
	}

	tests := []struct {
		commitmentID string
		shardBitmask int
		match        bool
	}{
		// === TWO SHARD CONFIG ===
		// shard1=bitmask 0b10
		// shard2=bitmask 0b11

		// certification request with key bit 0 = 0 belongs to shard1
		{makeShardTestID(0x00, 0x00), 0b10, true},
		{makeShardTestID(0x00, 0x00), 0b11, false},

		// certification request with key bit 0 = 1 belongs to shard2
		{makeShardTestID(0x01, 0x00), 0b10, false},
		{makeShardTestID(0x01, 0x00), 0b11, true},

		// certification request with first byte 0b00000010 still belongs to shard1
		{makeShardTestID(0x02, 0x00), 0b10, true},
		{makeShardTestID(0x02, 0x00), 0b11, false},

		// certification request with first byte 0b00000011 belongs to shard2
		{makeShardTestID(0x03, 0x00), 0b10, false},
		{makeShardTestID(0x03, 0x00), 0b11, true},

		// certification request with first byte 0b11111111 belongs to shard2
		{makeShardTestID(0xFF, 0x00), 0b10, false},
		{makeShardTestID(0xFF, 0x00), 0b11, true},

		// the last byte no longer affects shard routing under LSB-first byte order
		{makeShardTestID(0x00, 0xFF), 0b10, true},
		{makeShardTestID(0x00, 0xFF), 0b11, false},

		// === END TWO SHARD CONFIG ===

		// === FOUR SHARD CONFIG ===
		// shard1=0b100
		// shard2=0b110
		// shard3=0b101
		// shard4=0b111

		// key bits 1:0 = 00 belong to shard1
		{makeShardTestID(0x00, 0x00), 0b111, false},
		{makeShardTestID(0x00, 0x00), 0b101, false},
		{makeShardTestID(0x00, 0x00), 0b110, false},
		{makeShardTestID(0x00, 0x00), 0b100, true},

		// key bits 1:0 = 10 belong to shard2
		{makeShardTestID(0x02, 0x00), 0b111, false},
		{makeShardTestID(0x02, 0x00), 0b100, false},
		{makeShardTestID(0x02, 0x00), 0b101, false},
		{makeShardTestID(0x02, 0x00), 0b110, true},

		// key bits 1:0 = 01 belong to shard3
		{makeShardTestID(0x01, 0x00), 0b111, false},
		{makeShardTestID(0x01, 0x00), 0b101, true},
		{makeShardTestID(0x01, 0x00), 0b110, false},
		{makeShardTestID(0x01, 0x00), 0b100, false},

		// key bits 1:0 = 11 belong to shard4
		{makeShardTestID(0x03, 0x00), 0b111, true},
		{makeShardTestID(0x03, 0x00), 0b101, false},
		{makeShardTestID(0x03, 0x00), 0b110, false},
		{makeShardTestID(0x03, 0x00), 0b100, false},

		// key bits 1:0 = 11 still belong to shard4 when the whole first byte is set
		{makeShardTestID(0xFF, 0x00), 0b111, true},
		{makeShardTestID(0xFF, 0x00), 0b101, false},
		{makeShardTestID(0xFF, 0x00), 0b110, false},
		{makeShardTestID(0xFF, 0x00), 0b100, false},

		// === END FOUR SHARD CONFIG ===
	}
	for _, tc := range tests {
		match, err := api.MatchesShardPrefixFromHex(tc.commitmentID, tc.shardBitmask)
		require.NoError(t, err)
		if match != tc.match {
			t.Errorf("commitmentID=%s shardBitmask=%b expected %v got %v", tc.commitmentID, tc.shardBitmask, tc.match, match)
		}
	}
}

func TestValidator_ShardIDRejectsPrefixedStateID(t *testing.T) {
	rawKey := make([]byte, api.StateTreeKeyLengthBytes)
	rawKey[0] = 0x01
	prefixed := append([]byte{0x00, 0x00}, rawKey...)

	match, err := api.MatchesShardPrefixFromHex(hex.EncodeToString(prefixed), 0b11)
	require.Error(t, err)
	require.False(t, match)
	require.Contains(t, err.Error(), "must be exactly 32 bytes")
}

func TestValidator_InvalidSignatureFormat(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	// Create valid data except signature
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)
	stateHashData := []byte("test-state-hash")
	sourceStateHash := CreateDataHash(stateHashData)
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	require.NoError(t, err)

	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: CreateDataHash([]byte("hello")),
			Witness:         api.HexBytes(make([]byte, 32)), // Invalid length - should be 65 bytes
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
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)
	stateHashData := []byte("test-state-hash")
	sourceStateHash := CreateDataHash(stateHashData)
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	require.NoError(t, err)

	commitment := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: api.ImprintV2("invalid-hex-transaction-hash-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), // Invalid hex but 68 chars
			Witness:         api.HexBytes(make([]byte, 65)),                                                   // Valid length signature
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
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)
	stateHashData := []byte("test-state-hash")
	sourceStateHash := CreateDataHash(stateHashData)
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
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
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: CreateDataHash(transactionData), // Different from signed data
			Witness:         api.HexBytes(signatureBytes),
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
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	// Create state hash
	sourceStateData := []byte("real-state-hash-test")
	sourceStateHash := CreateDataHash(sourceStateData)

	// Create proper state ID
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	require.NoError(t, err)

	// Create transaction data
	transactionData := []byte("real-transaction-data-to-sign")
	transactionHash := CreateDataHash(transactionData)

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHash, transactionHash)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKeyBytes)
	if err != nil {
		t.Fatalf("Failed to sign transaction data: %v", err)
	}

	// Create certification request with all real cryptographic data
	certificationRequest := &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
			Witness:         api.HexBytes(signatureBytes),
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
