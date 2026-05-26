package signing

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestStateIDGenerator_CreateStateID(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	// Test with known values
	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKey)
	stateHash := make([]byte, 32)
	copy(stateHash, []byte("Hello World"))

	stateID, err := api.CreateStateID(ownerPredicate, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID: %v", err)
	}

	// Verify the result is deterministic by creating it again
	stateID2, err := api.CreateStateID(ownerPredicate, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID second time: %v", err)
	}

	if !bytes.Equal(stateID, stateID2) {
		t.Errorf("State ID should be deterministic. Got different values:\n%s\n%s", stateID, stateID2)
	}

	// Verify the state ID length
	if len(stateID) != 32 {
		t.Errorf("Expected state ID length 32, got %d", len(stateID))
	}

	// Verify it's valid hex
	_, err = hex.DecodeString(stateID.String())
	if err != nil {
		t.Errorf("State ID should be valid hex: %v", err)
	}
}

func TestStateIDGenerator_ValidateStateID(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	publicKey := []byte{0x03, 0xd8, 0xe2, 0xb2, 0xff, 0x8a, 0xc4, 0xf0, 0x2b, 0x2b, 0x5c, 0x45, 0x12, 0xc5, 0xe4, 0xe6, 0xb1, 0xc7, 0xd2, 0xe3, 0xa8, 0xb9, 0xc1, 0xf8, 0xe9, 0xd1, 0xc2, 0xa3, 0xb4, 0xe5, 0xf6, 0xa7}
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKey)
	sourceStateHash := make([]byte, 32)
	copy(sourceStateHash, []byte("test-state-for-validation"))

	// Create a valid state ID
	validStateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	if err != nil {
		t.Fatalf("Failed to create valid state ID: %v", err)
	}

	// Test valid state ID validation
	isValid, err := api.ValidateStateID(validStateID, sourceStateHash, ownerPredicate)
	if err != nil {
		t.Fatalf("Failed to validate state ID: %v", err)
	}
	if !isValid {
		t.Error("Valid state ID should pass validation")
	}

	// Test invalid state ID validation
	invalidStateID := api.RequireNewImprintV2("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	isValid, err = api.ValidateStateID(invalidStateID, sourceStateHash, ownerPredicate)
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
	differentOwnerPredicate := api.NewPayToPublicKeyPredicate(differentPublicKey)

	isValid, err = api.ValidateStateID(validStateID, sourceStateHash, differentOwnerPredicate)
	if err != nil {
		t.Fatalf("Failed to validate state ID with different public key: %v", err)
	}
	if isValid {
		t.Error("State ID should be invalid with different public key")
	}

	// Test with different state hash (should be invalid)
	differentStateHash := make([]byte, 32)
	copy(differentStateHash, []byte("different-state-hash"))
	isValid, err = api.ValidateStateID(validStateID, differentStateHash, ownerPredicate)
	if err != nil {
		t.Fatalf("Failed to validate state ID with different state hash: %v", err)
	}
	if isValid {
		t.Error("State ID should be invalid with different state hash")
	}
}

func TestStateIDGenerator_EmptyInputs(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	// Test with empty public key (this is still allowed by the predicate marshal)
	emptyPublicKey := []byte{}
	emptyOwnerPredicate := api.NewPayToPublicKeyPredicate(emptyPublicKey)
	stateHash := make([]byte, 32)
	copy(stateHash, []byte("test-state"))

	stateID, err := api.CreateStateID(emptyOwnerPredicate, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with empty public key: %v", err)
	}

	// Should still be deterministic
	stateID2, err := api.CreateStateID(emptyOwnerPredicate, stateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with empty public key second time: %v", err)
	}

	if !bytes.Equal(stateID, stateID2) {
		t.Error("State ID should be deterministic even with empty public key")
	}

	// Test with invalid state hash length (too short)
	publicKey := []byte{0x03, 0xd8}
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKey)
	invalidStateHash := []byte{0x01, 0x02}

	_, err = api.CreateStateID(ownerPredicate, invalidStateHash)
	if err == nil {
		t.Error("Expected error for too short state hash, got nil")
	}

	// Test with empty state hash
	emptyStateHash := []byte{}
	_, err = api.CreateStateID(ownerPredicate, emptyStateHash)
	if err == nil {
		t.Error("Expected error for empty state hash, got nil")
	}
}

func TestStateIDGenerator_LargeInputs(t *testing.T) {
	// Using api.CreateStateID and api.ValidateStateID

	// Test with large public key (larger than typical 33 bytes)
	largePublicKey := make([]byte, 1024)
	for i := range largePublicKey {
		largePublicKey[i] = byte(i % 256)
	}
	largeOwnerPredicate := api.NewPayToPublicKeyPredicate(largePublicKey)

	// Test with large state hash (invalid length for v2)
	largeStateHash := make([]byte, 2048)
	for i := range largeStateHash {
		largeStateHash[i] = byte((i * 7) % 256)
	}

	_, err := api.CreateStateID(largeOwnerPredicate, largeStateHash)
	if err == nil {
		t.Error("Expected error for too large state hash, got nil")
	}

	// Verify deterministic behavior with VALID 32-byte hash but large predicate
	validStateHash := make([]byte, 32)
	stateID, err := api.CreateStateID(largeOwnerPredicate, validStateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID with large predicate: %v", err)
	}

	stateID2, err := api.CreateStateID(largeOwnerPredicate, validStateHash)
	if err != nil {
		t.Fatalf("Failed to create state ID second time: %v", err)
	}

	if !bytes.Equal(stateID, stateID2) {
		t.Error("State ID should be deterministic with large predicate")
	}
}
