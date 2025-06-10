package signing

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RequestIDGenerator provides functionality to generate and validate request IDs
type RequestIDGenerator struct{}

// NewRequestIDGenerator creates a new request ID generator
func NewRequestIDGenerator() *RequestIDGenerator {
	return &RequestIDGenerator{}
}

// CreateRequestID creates a deterministic request ID from a public key and state hash
// This mirrors the TypeScript RequestId.create(publicKey, stateHash) implementation
func (g *RequestIDGenerator) CreateRequestID(publicKey []byte, stateHashBytes []byte) (api.RequestID, error) {
	// Initialize SHA256 hasher
	hasher := sha256.New()

	// Update with public key first
	hasher.Write(publicKey)

	// Update with state hash imprint  
	hasher.Write(stateHashBytes)

	// Generate final hash
	hashResult := hasher.Sum(nil)

	// Create RequestID from the hash bytes with SHA256 algorithm prefix (0000)
	// Following the TypeScript DataHash format: algorithm imprint (4 chars) + hash (64 chars)
	algorithmImprint := "0000" // SHA256 algorithm identifier (HashAlgorithm.SHA256 = 0)
	requestIDStr := algorithmImprint + hex.EncodeToString(hashResult)
	requestID := api.RequestID(requestIDStr)

	return requestID, nil
}

// ValidateRequestID validates that a request ID matches the expected value for the given public key and state hash
func (g *RequestIDGenerator) ValidateRequestID(requestID api.RequestID, publicKey []byte, stateHashBytes []byte) (bool, error) {
	expectedRequestID, err := g.CreateRequestID(publicKey, stateHashBytes)
	if err != nil {
		return false, err
	}

	return requestID == expectedRequestID, nil
}
