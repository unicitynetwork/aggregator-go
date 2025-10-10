package redis

import (
	"crypto/rand"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// createTestCommitment creates a valid commitment for testing purposes
func createTestCommitment() *models.Commitment {
	// Generate a real secp256k1 key pair
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Generate random state data
	stateData := make([]byte, 32)
	rand.Read(stateData)
	stateHashImprint := signing.CreateDataHashImprint(stateData)

	// Create RequestID deterministically like the performance test
	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	if err != nil {
		panic(fmt.Sprintf("Failed to create request ID: %v", err))
	}

	// Generate transaction data
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	// Extract transaction hash bytes for signing
	transactionHashBytes, err := transactionHashImprint.DataBytes()
	if err != nil {
		panic(fmt.Sprintf("Failed to extract transaction hash: %v", err))
	}

	// Sign the transaction hash bytes
	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	return &models.Commitment{
		RequestID: api.RequestID(requestID),
		Authenticator: models.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.NewHexBytes(publicKeyBytes),
			Signature: api.NewHexBytes(signatureBytes),
			StateHash: api.StateHash(stateHashImprint),
		},
		TransactionHash:       api.TransactionHash(transactionHashImprint),
		AggregateRequestCount: 1,
	}
}
