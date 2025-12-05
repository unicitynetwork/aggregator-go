package signing

import (
	"encoding/hex"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestStateIDGenerator_CreateStateID(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	// Test with known values
	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	stateHash := []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64} // "Hello World"

	stateID, err := api.CreateStateIDFromImprint(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID: %v", err)
	}

	// Verify the result is deterministic by creating it again
	stateID2, err := api.CreateStateIDFromImprint(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID second time: %v", err)
	}

	if stateID != stateID2 {
		t.Errorf("State ID should be deterministic. Got different values:\n%s\n%s", stateID, stateID2)
	}

	// Verify the state ID is a valid hex string (68 characters: 4 for algorithm + 64 for SHA256)
	stateIDStr := string(stateID)
	if len(stateIDStr) != 68 {
		t.Errorf("Expected state ID length 68, got %d", len(stateIDStr))
	}

	// Verify it's valid hex
	_, err = hex.DecodeString(stateIDStr)
	if err != nil {
		t.Errorf("State ID should be valid hex: %v", err)
	}
}

func TestStateIDGenerator_CreateStateIDCompatibility(t *testing.T) {
	// Test that matches the TypeScript implementation
	// Using api.CreateStateID and api.ValidateStateID

	// Manually compute expected value using the same algorithm as TypeScript
	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	sourceStateHashBytes := []byte("test-state-hash")
	stateIDDataHash := api.StateIDDataHash(publicKey, sourceStateHashBytes)
	expectedStateID := api.StateID(stateIDDataHash.ToHex())

	// Generate using our function
	actualStateID, err := api.CreateStateIDFromImprint(publicKey, sourceStateHashBytes)
	if err != nil {
		t.Fatalf("Failed to create state ID: %v", err)
	}

	if actualStateID != expectedStateID {
		t.Errorf("State ID doesn't match expected value.\nExpected: %s\nActual: %s", expectedStateID, actualStateID)
	}
}

func TestStateIDGenerator_ValidateStateID(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	sourceStateHash := []byte("test-state-for-validation")

	// Create a valid state ID
	validStateID, err := api.CreateStateIDFromImprint(publicKey, sourceStateHash)
	if err != nil {
		t.Fatalf("Failed to create valid state ID: %v", err)
	}

	// Test valid state ID validation
	isValid, err := api.ValidateStateID(validStateID, sourceStateHash, publicKey)
	if err != nil {
		t.Fatalf("Failed to validate state ID: %v", err)
	}
	if !isValid {
		t.Error("Valid state ID should pass validation")
	}

	// Test invalid state ID validation
	invalidStateID := api.StateID("00000123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	isValid, err = api.ValidateStateID(invalidStateID, sourceStateHash, publicKey)
	if err != nil {
		t.Fatalf("Failed to validate invalid state ID: %v", err)
	}
	if isValid {
		t.Error("Invalid state ID should fail validation")
	}

	// Test with different public key (should be invalid)
	differentPublicKey := make([]byte, len(publicKey))
	copy(differentPublicKey, publicKey)
	differentPublicKey[0] = 0x02 // Change first byte

	isValid, err = api.ValidateStateID(validStateID, sourceStateHash, differentPublicKey)
	if err != nil {
		t.Fatalf("Failed to validate state ID with different public key: %v", err)
	}
	if isValid {
		t.Error("State ID should be invalid with different public key")
	}

	// Test with different state hash (should be invalid)
	differentStateHash := []byte("different-state-hash")
	isValid, err = api.ValidateStateID(validStateID, differentStateHash, publicKey)
	if err != nil {
		t.Fatalf("Failed to validate state ID with different state hash: %v", err)
	}
	if isValid {
		t.Error("State ID should be invalid with different state hash")
	}
}

func TestStateIDGenerator_EmptyInputs(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	// Test with empty public key
	emptyPublicKey := []byte{}
	stateHash := []byte("test-state")

	stateID, err := api.CreateStateIDFromImprint(emptyPublicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with empty public key: %v", err)
	}

	// Should still be deterministic
	stateID2, err := api.CreateStateIDFromImprint(emptyPublicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with empty public key second time: %v", err)
	}

	if stateID != stateID2 {
		t.Error("State ID should be deterministic even with empty public key")
	}

	// Test with empty state hash
	publicKey := []byte{0x03, 0xd8}
	emptyStateHash := api.SourceStateHash([]byte{})

	stateID, err = api.CreateStateID(publicKey, emptyStateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with empty state hash: %v", err)
	}

	// Test with both empty
	stateID, err = api.CreateStateID([]byte{}, api.SourceStateHash([]byte{}))
	if err != nil {
		t.Fatalf("Failed to create state ID with both inputs empty: %v", err)
	}

	// Should still produce valid hex string
	stateIDStr := string(stateID)
	if len(stateIDStr) != 68 {
		t.Errorf("Expected state ID length 68 even with empty inputs, got %d", len(stateIDStr))
	}
}

func TestStateIDGenerator_LargeInputs(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

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

	stateID, err := api.CreateStateIDFromImprint(largePublicKey, largeStateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with large inputs: %v", err)
	}

	// Verify deterministic behavior
	stateID2, err := api.CreateStateIDFromImprint(largePublicKey, largeStateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with large inputs second time: %v", err)
	}

	if stateID != stateID2 {
		t.Error("State ID should be deterministic with large inputs")
	}

	// Verify validation works with large inputs
	isValid, err := api.ValidateStateID(stateID, largeStateHash, largePublicKey)
	if err != nil {
		t.Fatalf("Failed to validate state ID with large inputs: %v", err)
	}
	if !isValid {
		t.Error("State ID should be valid with large inputs")
	}
}
