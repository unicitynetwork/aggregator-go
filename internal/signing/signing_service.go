package signing

import (
	"crypto/sha256"
	"fmt"

	"github.com/alphabill-org/alphabill-go-base/crypto"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
)

// Algorithm constants
const (
	AlgorithmSecp256k1 = "secp256k1"
)

// SigningService provides cryptographic signing and verification operations
type SigningService struct{}

// NewSigningService creates a new signing service instance
func NewSigningService() *SigningService {
	return &SigningService{}
}

// Sign signs the given data with the private key and returns the signature
func (s *SigningService) Sign(data []byte, privateKey []byte) ([]byte, error) {
	if len(privateKey) != 32 {
		return nil, fmt.Errorf("private key must be 32 bytes, got %d", len(privateKey))
	}

	// Create private key object
	privKey, _ := btcec.PrivKeyFromBytes(privateKey)

	// Hash the data
	hash := sha256.Sum256(data)

	// Sign the hash  
	signature := ecdsa.SignCompact(privKey, hash[:], true) // true for compressed public key recovery

	return signature, nil
}

// VerifyWithPublicKey verifies a signature against data using a public key
func (s *SigningService) VerifyWithPublicKey(data []byte, signature []byte, publicKey []byte) (bool, error) {
	hash := sha256.Sum256(data)
	return s.VerifyHashWithPublicKey(hash[:], signature, publicKey)
}

func (s *SigningService) VerifyHashWithPublicKey(dataHash []byte, signature []byte, publicKey []byte) (bool, error) {
	if len(signature) != 65 {
		return false, fmt.Errorf("signature must be 65 bytes (64 bytes + recovery), got %d", len(signature))
	}

	if len(publicKey) != 33 {
		return false, fmt.Errorf("compressed public key must be 33 bytes, got %d", len(publicKey))
	}

	verifier, err := crypto.NewVerifierSecp256k1(publicKey)
	if err != nil {
		return false, fmt.Errorf("failed to create verifier: %w", err)
	}

	if err := verifier.VerifyHash(signature, dataHash); err != nil {
		return false, fmt.Errorf("signature verification failed: %w", err)
	}

	return true, nil
}

// RecoverPublicKey recovers the public key from a signature and the original data
func (s *SigningService) RecoverPublicKey(data []byte, signature []byte) ([]byte, error) {
	if len(signature) != 65 {
		return nil, fmt.Errorf("signature must be 65 bytes (64 bytes + recovery), got %d", len(signature))
	}

	// Hash the data
	hash := sha256.Sum256(data)

	// Recover the public key using compact recovery
	pubKey, _, err := ecdsa.RecoverCompact(signature, hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to recover public key: %w", err)
	}

	// Return compressed public key
	return pubKey.SerializeCompressed(), nil
}

// GenerateKeyPair generates a new secp256k1 key pair
func (s *SigningService) GenerateKeyPair() (privateKey []byte, publicKey []byte, err error) {
	// Generate private key
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Get private key bytes
	privateKey = privKey.Serialize()

	// Get compressed public key
	publicKey = privKey.PubKey().SerializeCompressed()

	return privateKey, publicKey, nil
}

// ValidatePublicKey validates that a public key is valid for secp256k1
func (s *SigningService) ValidatePublicKey(publicKey []byte) error {
	if len(publicKey) != 33 {
		return fmt.Errorf("compressed public key must be 33 bytes, got %d", len(publicKey))
	}

	_, err := btcec.ParsePubKey(publicKey)
	if err != nil {
		return fmt.Errorf("invalid public key: %w", err)
	}

	return nil
}
