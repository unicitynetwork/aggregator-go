package redis

import (
	"crypto/rand"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/predicates"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// createTestCommitment creates a valid certification request for testing purposes
func createTestCommitment() *models.CertificationRequest {
	// Generate a real secp256k1 key pair
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicateBytes, err := predicates.NewPayToPublicKeyPredicateBytes(publicKeyBytes)
	if err != nil {
		panic(fmt.Sprintf("Failed to create owner predicate: %v", err))
	}

	// Generate random state data
	stateData := make([]byte, 32)
	rand.Read(stateData)
	sourceStateHash := signing.CreateDataHashImprint(stateData)
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	if err != nil {
		panic(fmt.Sprintf("Failed to create source state imprint: %v", err))
	}

	// Create StateID deterministically like the performance test
	stateID, err := api.CreateStateID(ownerPredicateBytes, sourceStateHash)
	if err != nil {
		panic(fmt.Sprintf("Failed to create state ID: %v", err))
	}

	// Generate transaction data
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHash := signing.CreateDataHashImprint(transactionData)
	transactionHashImprint, err := transactionHash.Imprint()
	if err != nil {
		panic(fmt.Sprintf("Failed to extract transaction hash: %v", err))
	}

	// Sign the transaction
	signingService := signing.NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHashImprint, transactionHashImprint)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	return &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.NewHexBytes(ownerPredicateBytes),
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
			Witness:         api.NewHexBytes(signatureBytes),
		},
		AggregateRequestCount: 1,
	}
}

func toAckEntries(commitments []*models.CertificationRequest) []interfaces.CertificationRequestAck {
	acks := make([]interfaces.CertificationRequestAck, len(commitments))
	for i, commitment := range commitments {
		acks[i] = interfaces.CertificationRequestAck{
			RequestID: commitment.StateID,
			StreamID:  commitment.StreamID,
		}
	}
	return acks
}
