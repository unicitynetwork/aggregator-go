package signing

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// SigningService provides cryptographic signing and verification operations
type SigningService struct{}

// NewSigningService creates a new signing service instance
func NewSigningService() *SigningService {
	return &SigningService{}
}

// Sign signs the given data with the private key and returns the signature
func (s *SigningService) Sign(data []byte, privateKey []byte) ([]byte, error) {
	// Hash the data
	hash := sha256.Sum256(data)
	return s.SignHash(hash[:], privateKey)
}

// convertBtcecToUnicity converts a signature from btcec's [V || R || S] format
// to Unicity's [R || S || V] format
func convertBtcecToUnicity(compactSig []byte) []byte {
	// For compressed keys, btcec's V is 31-34. We normalize it to 0 or 1.
	v := compactSig[0] - 31
	r := compactSig[1:33]
	sigS := compactSig[33:65]

	signature := make([]byte, 65)
	copy(signature[0:32], r)
	copy(signature[32:64], sigS)
	signature[64] = v

	return signature
}

// convertUnicityToBtcec converts a signature from Unicity's [R || S || V] format
// to btcec's [V || R || S] format
func convertUnicityToBtcec(signature []byte) []byte {
	// Unicity uses recovery ID 0 or 1, we need to convert to btcec's 31-34 for compressed keys
	r := signature[0:32]
	sigS := signature[32:64]
	v := signature[64] + 31 // Convert 0/1 to 31/32 for compressed keys

	compactSig := make([]byte, 65)
	compactSig[0] = v
	copy(compactSig[1:33], r)
	copy(compactSig[33:65], sigS)

	return compactSig
}

// SignCertData signs the given certification request data and stores the signature to the Witness field.
func (s *SigningService) SignCertData(certData *api.CertificationData, privateKey []byte) error {
	if certData == nil {
		return errors.New("certification data is nil")
	}
	sigDataHash, err := certData.SigDataHash()
	if err != nil {
		return fmt.Errorf("failed to generate signature bytes: %w", err)
	}
	sig, err := s.SignDataHash(sigDataHash, privateKey)
	if err != nil {
		return fmt.Errorf("error generating signature: %w", err)
	}
	certData.Witness = sig
	return nil
}

// SignDataHash signs the given data hash with the private key and returns the signature
func (s *SigningService) SignDataHash(dataHash *api.DataHash, privateKey []byte) ([]byte, error) {
	return s.SignHash(dataHash.RawHash, privateKey)
}

// SignHash signs the given hash with the private key and returns the signature
func (s *SigningService) SignHash(dataHash []byte, privateKey []byte) ([]byte, error) {
	if len(privateKey) != 32 {
		return nil, fmt.Errorf("private key must be 32 bytes, got %d", len(privateKey))
	}

	// Create private key object
	privKey, _ := btcec.PrivKeyFromBytes(privateKey)

	// Sign the hash
	compactSig := ecdsa.SignCompact(privKey, dataHash, true) // true for compressed public key recovery

	// Convert from btcec's [V || R || S] format to Unicity's [R || S || V] format
	return convertBtcecToUnicity(compactSig), nil
}

// VerifyWithPublicKey verifies a signature against data using a public key
func (s *SigningService) VerifyWithPublicKey(data []byte, signature []byte, publicKey []byte) (bool, error) {
	hash := sha256.Sum256(data)
	return s.VerifyHashWithPublicKey(hash[:], signature, publicKey)
}

// VerifyDataHashWithPublicKey verifies a signature against data hash using a public key
func (s *SigningService) VerifyDataHashWithPublicKey(dataHash *api.DataHash, signature []byte, publicKey []byte) (bool, error) {
	return s.VerifyHashWithPublicKey(dataHash.RawHash, signature, publicKey)
}

// VerifyHashWithPublicKey verifies a signature against data hash using a public key
func (s *SigningService) VerifyHashWithPublicKey(dataHash []byte, signature []byte, publicKey []byte) (bool, error) {
	if len(signature) != 65 {
		return false, fmt.Errorf("signature must be 65 bytes (64 bytes + recovery), got %d", len(signature))
	}

	if len(publicKey) != 33 {
		return false, fmt.Errorf("compressed public key must be 33 bytes, got %d", len(publicKey))
	}

	// Parse the public key
	pubKey, err := btcec.ParsePubKey(publicKey)
	if err != nil {
		return false, fmt.Errorf("failed to parse public key: %w", err)
	}

	// Convert from Unicity's [R || S || V] format to btcec's [V || R || S] format
	compactSig := convertUnicityToBtcec(signature)

	// For compact signatures, we need to recover the public key and verify it matches
	// Recover public key from signature
	recoveredPubKey, _, err := ecdsa.RecoverCompact(compactSig, dataHash)
	if err != nil {
		return false, fmt.Errorf("failed to recover public key from signature: %w", err)
	}

	// Check if the recovered public key matches the provided public key
	if !recoveredPubKey.IsEqual(pubKey) {
		return false, nil // Witness doesn't match the public key
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

	// Convert from Unicity's [R || S || V] format to btcec's [V || R || S] format
	compactSig := convertUnicityToBtcec(signature)

	// Recover the public key using compact recovery
	pubKey, _, err := ecdsa.RecoverCompact(compactSig, hash[:])
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
