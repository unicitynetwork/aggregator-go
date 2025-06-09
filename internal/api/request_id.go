package api

import (
	"crypto/sha256"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

// CreateRequestID creates a RequestID from public key and state hash
// This must match the TypeScript implementation exactly
func CreateRequestID(publicKey []byte, stateHash models.StateHash) (models.RequestID, error) {
	// Convert state hash to bytes
	stateHashBytes, err := stateHash.Bytes()
	if err != nil {
		return "", fmt.Errorf("failed to convert state hash to bytes: %w", err)
	}

	// Create the data to hash: publicKey + stateHash
	data := make([]byte, 0, len(publicKey)+len(stateHashBytes))
	data = append(data, publicKey...)
	data = append(data, stateHashBytes...)

	// Calculate SHA256 hash
	hash := sha256.Sum256(data)

	// Convert to hex string with SHA256 algorithm imprint (0000)
	requestIDStr := fmt.Sprintf("0000%x", hash)

	// Create and validate RequestID
	return models.NewRequestID(requestIDStr)
}

