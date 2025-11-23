package redis

import (
	"crypto/rand"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/models"
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

	// Generate random state data
	stateData := make([]byte, 32)
	rand.Read(stateData)
	sourceStateHash := signing.CreateDataHashImprint(stateData)
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	if err != nil {
		panic(fmt.Sprintf("Failed to create source state imprint: %v", err))
	}

	// Create StateID deterministically like the performance test
	stateID, err := api.CreateStateID(sourceStateHash, publicKeyBytes)
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
	sigData := api.SigHashData{
		SourceStateHashImprint: sourceStateHashImprint,
		TransactionHashImprint: transactionHashImprint,
	}
	sigDataCBOR, err := types.Cbor.Marshal(sigData)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode signature data: %v", err))
	}
	signatureBytes, err := signingService.Sign(sigDataCBOR, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	return &models.CertificationRequest{
		StateID: stateID,
		CertificationData: models.CertificationData{
			PublicKey:       api.NewHexBytes(publicKeyBytes),
			Signature:       api.NewHexBytes(signatureBytes),
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
		},
		AggregateRequestCount: 1,
	}
}

func toAckEntries(commitments []*models.CertificationRequest) []interfaces.CertificationRequestAck {
	acks := make([]interfaces.CertificationRequestAck, len(commitments))
	for i, commitment := range commitments {
		acks[i] = interfaces.CertificationRequestAck{
			StateID:  commitment.StateID,
			StreamID: commitment.StreamID,
		}
	}
	return acks
}
