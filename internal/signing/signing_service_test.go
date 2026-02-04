package signing

import (
	"encoding/hex"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
)

func TestSigningService_GenerateKeyPair(t *testing.T) {
	service := NewSigningService()

	privateKey, publicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Validate private key length
	if len(privateKey) != 32 {
		t.Errorf("Expected private key length 32, got %d", len(privateKey))
	}

	// Validate public key length (compressed)
	if len(publicKey) != 33 {
		t.Errorf("Expected compressed public key length 33, got %d", len(publicKey))
	}

	// Validate public key format
	if err := service.ValidatePublicKey(publicKey); err != nil {
		t.Errorf("Generated public key is invalid: %v", err)
	}
}

func TestSigningService_SignAndVerify(t *testing.T) {
	service := NewSigningService()

	// Generate test key pair
	privateKey, publicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Test data to sign
	testData := []byte("hello world")

	// Sign the data
	signature, err := service.Sign(testData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign data: %v", err)
	}

	// Verify signature length
	if len(signature) != 65 {
		t.Errorf("Expected signature length 65, got %d", len(signature))
	}

	// Verify the signature
	isValid, err := service.VerifyWithPublicKey(testData, signature, publicKey)
	if err != nil {
		t.Fatalf("Failed to verify signature: %v", err)
	}

	if !isValid {
		t.Error("Witness verification failed")
	}
}

func TestSigningService_VerifyWithWrongData(t *testing.T) {
	service := NewSigningService()

	// Generate test key pair
	privateKey, publicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Original data and different data
	originalData := []byte("hello world")
	differentData := []byte("hello mars")

	// Sign the original data
	signature, err := service.Sign(originalData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign data: %v", err)
	}

	// Try to verify with different data (should fail)
	isValid, err := service.VerifyWithPublicKey(differentData, signature, publicKey)
	if err != nil {
		t.Fatalf("Failed to verify signature: %v", err)
	}

	if isValid {
		t.Error("Witness verification should have failed with different data")
	}
}

func TestSigningService_RecoverPublicKey(t *testing.T) {
	service := NewSigningService()

	// Generate test key pair
	privateKey, expectedPublicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	// Test data to sign
	testData := []byte("hello world")

	// Sign the data
	signature, err := service.Sign(testData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign data: %v", err)
	}

	// Recover the public key
	recoveredPublicKey, err := service.RecoverPublicKey(testData, signature)
	if err != nil {
		t.Fatalf("Failed to recover public key: %v", err)
	}

	// Compare with expected public key
	if hex.EncodeToString(recoveredPublicKey) != hex.EncodeToString(expectedPublicKey) {
		t.Errorf("Recovered public key doesn't match expected.\nExpected: %s\nGot: %s",
			hex.EncodeToString(expectedPublicKey),
			hex.EncodeToString(recoveredPublicKey))
	}
}

func TestSigningService_ValidatePublicKey(t *testing.T) {
	service := NewSigningService()

	// Test valid public key
	_, validPublicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	if err := service.ValidatePublicKey(validPublicKey); err != nil {
		t.Errorf("Valid public key should pass validation: %v", err)
	}

	// Test invalid public key (wrong length)
	invalidPublicKey := make([]byte, 32) // Should be 33 for compressed
	if err := service.ValidatePublicKey(invalidPublicKey); err == nil {
		t.Error("Invalid public key should fail validation")
	}

	// Test invalid public key (invalid format)
	invalidFormatKey := make([]byte, 33)
	invalidFormatKey[0] = 0xFF // Invalid prefix for compressed key
	if err := service.ValidatePublicKey(invalidFormatKey); err == nil {
		t.Error("Invalid format public key should fail validation")
	}
}

func TestSigningService_RealSecp256k1Signature(t *testing.T) {
	// This test uses real secp256k1 values to ensure compatibility
	service := NewSigningService()

	// Generate a known test vector by creating one from scratch
	testDataHex := "48656c6c6f20576f726c64" // "Hello World"

	// Generate a key pair first
	privateKey, expectedPublicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	testData, err := hex.DecodeString(testDataHex)
	if err != nil {
		t.Fatalf("Failed to decode test data: %v", err)
	}

	// Derive public key from private key to verify consistency
	privKey, _ := btcec.PrivKeyFromBytes(privateKey)
	actualPublicKey := privKey.PubKey().SerializeCompressed()

	if hex.EncodeToString(actualPublicKey) != hex.EncodeToString(expectedPublicKey) {
		t.Errorf("Public key derivation mismatch.\nExpected: %s\nGot: %s",
			hex.EncodeToString(expectedPublicKey), hex.EncodeToString(actualPublicKey))
	}

	// Sign the test data
	signature, err := service.Sign(testData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign test data: %v", err)
	}

	// Verify the signature with the expected public key
	isValid, err := service.VerifyWithPublicKey(testData, signature, expectedPublicKey)
	if err != nil {
		t.Fatalf("Failed to verify signature: %v", err)
	}

	if !isValid {
		t.Error("Real secp256k1 signature verification failed")
	}

	// Also test recovery
	recoveredPublicKey, err := service.RecoverPublicKey(testData, signature)
	if err != nil {
		t.Fatalf("Failed to recover public key: %v", err)
	}

	if hex.EncodeToString(recoveredPublicKey) != hex.EncodeToString(expectedPublicKey) {
		t.Errorf("Public key recovery mismatch.\nExpected: %s\nGot: %s",
			hex.EncodeToString(expectedPublicKey), hex.EncodeToString(recoveredPublicKey))
	}
}

func TestSigningService_EdgeCases(t *testing.T) {
	service := NewSigningService()

	// Test empty data
	privateKey, publicKey, err := service.GenerateKeyPair()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	emptyData := []byte{}
	signature, err := service.Sign(emptyData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign empty data: %v", err)
	}

	isValid, err := service.VerifyWithPublicKey(emptyData, signature, publicKey)
	if err != nil {
		t.Fatalf("Failed to verify empty data signature: %v", err)
	}

	if !isValid {
		t.Error("Empty data signature verification failed")
	}

	// Test large data
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	signature, err = service.Sign(largeData, privateKey)
	if err != nil {
		t.Fatalf("Failed to sign large data: %v", err)
	}

	isValid, err = service.VerifyWithPublicKey(largeData, signature, publicKey)
	if err != nil {
		t.Fatalf("Failed to verify large data signature: %v", err)
	}

	if !isValid {
		t.Error("Large data signature verification failed")
	}
}

func TestSigningService_ErrorCases(t *testing.T) {
	service := NewSigningService()

	// Test invalid private key length
	invalidPrivateKey := make([]byte, 31) // Should be 32
	testData := []byte("test")

	_, err := service.Sign(testData, invalidPrivateKey)
	if err == nil {
		t.Error("Should fail with invalid private key length")
	}

	// Test invalid signature length for verification
	privateKey, publicKey, _ := service.GenerateKeyPair()
	invalidSignature := make([]byte, 64) // Should be 65

	_, err = service.VerifyWithPublicKey(testData, invalidSignature, publicKey)
	if err == nil {
		t.Error("Should fail with invalid signature length")
	}

	// Test invalid public key length for verification
	validSignature, _ := service.Sign(testData, privateKey)
	invalidPublicKey := make([]byte, 32) // Should be 33

	_, err = service.VerifyWithPublicKey(testData, validSignature, invalidPublicKey)
	if err == nil {
		t.Error("Should fail with invalid public key length")
	}
}
