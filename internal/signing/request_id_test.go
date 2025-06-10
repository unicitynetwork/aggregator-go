package signing

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

func TestRequestIDGenerator_CreateRequestID(t *testing.T) {
	generator := NewRequestIDGenerator()

	// Test with known values
	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	stateHash := []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64} // "Hello World"

	requestID, err := generator.CreateRequestID(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID: %v", err)
	}

	// Verify the result is deterministic by creating it again
	requestID2, err := generator.CreateRequestID(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID second time: %v", err)
	}

	if requestID != requestID2 {
		t.Errorf("Request ID should be deterministic. Got different values:\n%s\n%s", requestID, requestID2)
	}

	// Verify the request ID is a valid hex string (68 characters: 4 for algorithm + 64 for SHA256)
	requestIDStr := string(requestID)
	if len(requestIDStr) != 68 {
		t.Errorf("Expected request ID length 68, got %d", len(requestIDStr))
	}

	// Verify it's valid hex
	_, err = hex.DecodeString(requestIDStr)
	if err != nil {
		t.Errorf("Request ID should be valid hex: %v", err)
	}
}

func TestRequestIDGenerator_CreateRequestIDCompatibility(t *testing.T) {
	// Test that matches the TypeScript implementation
	generator := NewRequestIDGenerator()

	// Manually compute expected value using the same algorithm as TypeScript
	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	stateHashBytes := []byte("test-state-hash")

	// Calculate expected value manually
	hasher := sha256.New()
	hasher.Write(publicKey)
	hasher.Write(stateHashBytes)
	expectedHash := hasher.Sum(nil)
	// Add algorithm prefix as per the RequestID format
	algorithmImprint := "0000" // SHA256 algorithm identifier (HashAlgorithm.SHA256 = 0)
	expectedRequestID := models.RequestID(algorithmImprint + hex.EncodeToString(expectedHash))

	// Generate using our function
	actualRequestID, err := generator.CreateRequestID(publicKey, stateHashBytes)
	if err != nil {
		t.Fatalf("Failed to create request ID: %v", err)
	}

	if actualRequestID != expectedRequestID {
		t.Errorf("Request ID doesn't match expected value.\nExpected: %s\nActual: %s", expectedRequestID, actualRequestID)
	}
}

func TestRequestIDGenerator_ValidateRequestID(t *testing.T) {
	generator := NewRequestIDGenerator()

	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	stateHash := []byte("test-state-for-validation")

	// Create a valid request ID
	validRequestID, err := generator.CreateRequestID(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create valid request ID: %v", err)
	}

	// Test valid request ID validation
	isValid, err := generator.ValidateRequestID(validRequestID, publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to validate request ID: %v", err)
	}
	if !isValid {
		t.Error("Valid request ID should pass validation")
	}

	// Test invalid request ID validation
	invalidRequestID := models.RequestID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	isValid, err = generator.ValidateRequestID(invalidRequestID, publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to validate invalid request ID: %v", err)
	}
	if isValid {
		t.Error("Invalid request ID should fail validation")
	}

	// Test with different public key (should be invalid)
	differentPublicKey := make([]byte, len(publicKey))
	copy(differentPublicKey, publicKey)
	differentPublicKey[0] = 0x02 // Change first byte

	isValid, err = generator.ValidateRequestID(validRequestID, differentPublicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to validate request ID with different public key: %v", err)
	}
	if isValid {
		t.Error("Request ID should be invalid with different public key")
	}

	// Test with different state hash (should be invalid)
	differentStateHash := []byte("different-state-hash")
	isValid, err = generator.ValidateRequestID(validRequestID, publicKey, differentStateHash)
	if err != nil {
		t.Fatalf("Failed to validate request ID with different state hash: %v", err)
	}
	if isValid {
		t.Error("Request ID should be invalid with different state hash")
	}
}

func TestRequestIDGenerator_EmptyInputs(t *testing.T) {
	generator := NewRequestIDGenerator()

	// Test with empty public key
	emptyPublicKey := []byte{}
	stateHash := []byte("test-state")

	requestID, err := generator.CreateRequestID(emptyPublicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID with empty public key: %v", err)
	}

	// Should still be deterministic
	requestID2, err := generator.CreateRequestID(emptyPublicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID with empty public key second time: %v", err)
	}

	if requestID != requestID2 {
		t.Error("Request ID should be deterministic even with empty public key")
	}

	// Test with empty state hash
	publicKey := []byte{0x03, 0xd8}
	emptyStateHash := []byte{}

	requestID, err = generator.CreateRequestID(publicKey, emptyStateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID with empty state hash: %v", err)
	}

	// Test with both empty
	requestID, err = generator.CreateRequestID([]byte{}, []byte{})
	if err != nil {
		t.Fatalf("Failed to create request ID with both inputs empty: %v", err)
	}

	// Should still produce valid hex string
	requestIDStr := string(requestID)
	if len(requestIDStr) != 68 {
		t.Errorf("Expected request ID length 68 even with empty inputs, got %d", len(requestIDStr))
	}
}

func TestRequestIDGenerator_LargeInputs(t *testing.T) {
	generator := NewRequestIDGenerator()

	// Test with large public key (larger than typical 33 bytes)
	largePublicKey := make([]byte, 1024)
	for i := range largePublicKey {
		largePublicKey[i] = byte(i % 256)
	}

	// Test with large state hash
	largeStateHash := make([]byte, 2048)
	for i := range largeStateHash {
		largeStateHash[i] = byte((i * 7) % 256)
	}

	requestID, err := generator.CreateRequestID(largePublicKey, largeStateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID with large inputs: %v", err)
	}

	// Verify deterministic behavior
	requestID2, err := generator.CreateRequestID(largePublicKey, largeStateHash)
	if err != nil {
		t.Fatalf("Failed to create request ID with large inputs second time: %v", err)
	}

	if requestID != requestID2 {
		t.Error("Request ID should be deterministic with large inputs")
	}

	// Verify validation works with large inputs
	isValid, err := generator.ValidateRequestID(requestID, largePublicKey, largeStateHash)
	if err != nil {
		t.Fatalf("Failed to validate request ID with large inputs: %v", err)
	}
	if !isValid {
		t.Error("Request ID should be valid with large inputs")
	}
}