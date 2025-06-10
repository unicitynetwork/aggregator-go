package signing

import (
	"encoding/hex"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/unicitynetwork/aggregator-go/internal/models"
)

// Helper function to convert hex string to HexBytes for tests
func hexStringToHexBytes(hexStr string) models.HexBytes {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		panic(err)
	}
	return models.HexBytes(data)
}

func TestCommitmentValidator_ValidateCommitment_Success(t *testing.T) {
	validator := NewCommitmentValidator()

	// Generate a test key pair for signing
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Create test state hash
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	
	// Extract the actual hash bytes from the imprint (what the validator will use)
	stateHashBytes, err := ExtractDataFromImprint(stateHashImprint)
	if err != nil {
		t.Fatalf("Failed to extract state hash from imprint: %v", err)
	}

	// Create request ID using the extracted state hash bytes (same as what validator will use)
	requestIDGenerator := NewRequestIDGenerator()
	requestID, err := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)
	if err != nil {
		t.Fatalf("Failed to create request ID: %v", err)
	}

	// Create transaction data and sign it
	transactionData := []byte("test-transaction-data")
	transactionHashImprint := CreateDataHashImprint(transactionData)
	
	// Extract the transaction hash bytes from the imprint (what the validator will use for verification)
	transactionHashBytes, err := ExtractDataFromImprint(transactionHashImprint)
	if err != nil {
		t.Fatalf("Failed to extract transaction hash from imprint: %v", err)
	}

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	signatureBytes, err := signingService.Sign(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		t.Fatalf("Failed to sign transaction data: %v", err)
	}

	// Create commitment with valid data  
	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: models.TransactionHash(transactionHashImprint),
		Authenticator: models.Authenticator{
			Algorithm:   AlgorithmSecp256k1,
			PublicKey:   models.HexBytes(publicKeyBytes),
			Signature:   models.HexBytes(signatureBytes),
			StateHash:   hexStringToHexBytes(stateHashImprint),
		},
	}

	// Validate the commitment
	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusSuccess {
		t.Errorf("Expected validation success, got status: %s, error: %v", result.Status.String(), result.Error)
	}
	if result.Error != nil {
		t.Errorf("Expected no error, got: %v", result.Error)
	}
}

func TestCommitmentValidator_ValidateCommitment_UnsupportedAlgorithm(t *testing.T) {
	validator := NewCommitmentValidator()

	commitment := &models.Commitment{
		RequestID:       models.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: models.TransactionHash("000048656c6c6f576f726c640123456789abcdef0123456789abcdef0123456789abcdef"),
		Authenticator: models.Authenticator{
			Algorithm: "unsupported-algorithm",
			PublicKey: models.HexBytes("test-public-key"),
			Signature: models.HexBytes("test-signature"),
			StateHash: hexStringToHexBytes(CreateDataHashImprint([]byte("test-state-hash"))),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusUnsupportedAlgorithm {
		t.Errorf("Expected unsupported algorithm status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for unsupported algorithm")
	}
}

func TestCommitmentValidator_ValidateCommitment_InvalidPublicKeyFormat(t *testing.T) {
	validator := NewCommitmentValidator()

	commitment := &models.Commitment{
		RequestID:       models.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: models.TransactionHash(CreateDataHashImprint([]byte("hello"))),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes("invalid-hex-public-key"), // Invalid hex
			Signature: models.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: hexStringToHexBytes(CreateDataHashImprint([]byte("test-state"))),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusInvalidPublicKeyFormat {
		t.Errorf("Expected invalid public key format status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for invalid public key format")
	}
}

func TestCommitmentValidator_ValidateCommitment_InvalidStateHashFormat(t *testing.T) {
	validator := NewCommitmentValidator()

	// Create valid public key
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	commitment := &models.Commitment{
		RequestID:       models.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		TransactionHash: models.TransactionHash(CreateDataHashImprint([]byte("hello"))),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: models.HexBytes("invalid-hex-state-hash"), // Invalid hex
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusInvalidStateHashFormat {
		t.Errorf("Expected invalid state hash format status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for invalid state hash format")
	}
}

func TestCommitmentValidator_ValidateCommitment_RequestIDMismatch(t *testing.T) {
	validator := NewCommitmentValidator()

	// Create valid public key and state hash
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashBytes := []byte("test-state-hash")

	// Create a wrong request ID (not matching the public key + state hash)
	wrongRequestID := models.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	commitment := &models.Commitment{
		RequestID:       wrongRequestID,
		TransactionHash: models.TransactionHash(CreateDataHashImprint([]byte("hello"))),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01"),
			StateHash: hexStringToHexBytes(CreateDataHashImprint(stateHashBytes)),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusRequestIDMismatch {
		t.Errorf("Expected request ID mismatch status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for request ID mismatch")
	}
}

func TestCommitmentValidator_ValidateCommitment_InvalidSignatureFormat(t *testing.T) {
	validator := NewCommitmentValidator()

	// Create valid data except signature
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashBytes, _ := ExtractDataFromImprint(stateHashImprint)

	requestIDGenerator := NewRequestIDGenerator()
	requestID, _ := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: models.TransactionHash(CreateDataHashImprint([]byte("hello"))),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes(make([]byte, 32)), // Invalid length - should be 65 bytes
			StateHash: hexStringToHexBytes(stateHashImprint),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusInvalidSignatureFormat {
		t.Errorf("Expected invalid signature format status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for invalid signature format")
	}
}

func TestCommitmentValidator_ValidateCommitment_InvalidTransactionHashFormat(t *testing.T) {
	validator := NewCommitmentValidator()

	// Create valid data except transaction hash
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashBytes, _ := ExtractDataFromImprint(stateHashImprint)

	requestIDGenerator := NewRequestIDGenerator()
	requestID, _ := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: models.TransactionHash("invalid-hex-transaction-hash-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), // Invalid hex but 68 chars
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes(make([]byte, 65)), // Valid length signature
			StateHash: hexStringToHexBytes(stateHashImprint),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusInvalidTransactionHashFormat {
		t.Errorf("Expected invalid transaction hash format status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for invalid transaction hash format")
	}
}

func TestCommitmentValidator_ValidateCommitment_SignatureVerificationFailed(t *testing.T) {
	validator := NewCommitmentValidator()

	// Generate a test key pair
	privateKey, _ := btcec.NewPrivateKey()
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	stateHashData := []byte("test-state-hash")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashBytes, _ := ExtractDataFromImprint(stateHashImprint)

	requestIDGenerator := NewRequestIDGenerator()
	requestID, _ := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)

	// Create transaction data
	transactionData := []byte("test-transaction-data")

	// Sign different data (so signature won't match)
	differentData := []byte("different-transaction-data")
	signingService := NewSigningService()
	signatureBytes, _ := signingService.Sign(differentData, privateKey.Serialize())

	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: models.TransactionHash(CreateDataHashImprint(transactionData)), // Different from signed data
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes(signatureBytes),
			StateHash: hexStringToHexBytes(stateHashImprint),
		},
	}

	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusSignatureVerificationFailed {
		t.Errorf("Expected signature verification failed status, got: %s", result.Status.String())
	}
	if result.Error == nil {
		t.Error("Expected error for signature verification failure")
	}
}

func TestCommitmentValidator_ValidateCommitment_RealSecp256k1Data(t *testing.T) {
	// Test with real secp256k1 cryptographic operations to ensure compatibility
	validator := NewCommitmentValidator()

	// Use known test vectors
	privateKeyHex := "c28a9f80738afe1441ba9a68e72033f4c8d52b4f5d6d8f1e6a6b1c4a7b8e9c1f"
	privateKeyBytes, _ := hex.DecodeString(privateKeyHex)

	// Create private key and derive public key
	privKey, _ := btcec.PrivKeyFromBytes(privateKeyBytes)
	publicKeyBytes := privKey.PubKey().SerializeCompressed()

	// Create state hash
	stateHashData := []byte("real-state-hash-test")
	stateHashImprint := CreateDataHashImprint(stateHashData)
	stateHashBytes, _ := ExtractDataFromImprint(stateHashImprint)

	// Create proper request ID
	requestIDGenerator := NewRequestIDGenerator()
	requestID, _ := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)

	// Create transaction data
	transactionData := []byte("real-transaction-data-to-sign")
	transactionHashImprint := CreateDataHashImprint(transactionData)
	
	// Extract the transaction hash bytes from the imprint (what the validator will use for verification)
	transactionHashBytes, err := ExtractDataFromImprint(transactionHashImprint)
	if err != nil {
		t.Fatalf("Failed to extract transaction hash from imprint: %v", err)
	}

	// Sign the actual transaction hash bytes (what the validator expects)
	signingService := NewSigningService()
	signatureBytes, err := signingService.Sign(transactionHashBytes, privateKeyBytes)
	if err != nil {
		t.Fatalf("Failed to sign transaction data: %v", err)
	}

	// Create commitment with all real cryptographic data
	commitment := &models.Commitment{
		RequestID:       requestID,
		TransactionHash: models.TransactionHash(transactionHashImprint),
		Authenticator: models.Authenticator{
			Algorithm: AlgorithmSecp256k1,
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes(signatureBytes),
			StateHash: hexStringToHexBytes(stateHashImprint),
		},
	}

	// Validate the commitment - should succeed
	result := validator.ValidateCommitment(commitment)

	if result.Status != ValidationStatusSuccess {
		t.Errorf("Expected validation success with real secp256k1 data, got status: %s, error: %v", result.Status.String(), result.Error)
	}
	if result.Error != nil {
		t.Errorf("Expected no error with real secp256k1 data, got: %v", result.Error)
	}
}

func TestCommitmentValidator_ValidationStatusString(t *testing.T) {
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
		{ValidationStatus(999), "UNKNOWN"}, // Test unknown status
	}

	for _, test := range tests {
		result := test.status.String()
		if result != test.expected {
			t.Errorf("Expected status string %s, got %s", test.expected, result)
		}
	}
}