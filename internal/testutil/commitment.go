package testutil

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CreateTestCertificationRequest creates a valid, signed CertificationRequest for testing
func CreateTestCertificationRequest(t *testing.T, baseData string) *models.CertificationRequest {
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	stateData := make([]byte, 32)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		require.NoError(t, err, "Failed to generate random state data")
	}
	sourceStateHash := signing.CreateDataHash(stateData)

	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	require.NoError(t, err, "Failed to create state ID")

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_%s", baseData)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		require.NoError(t, err, "Failed to generate random transaction data")
	}
	transactionHash := signing.CreateDataHash(transactionData)
	signingService := signing.NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHash, transactionHash)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction")

	certData := models.CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         signatureBytes,
	}
	return models.NewCertificationRequest(stateID, certData)
}

// CreateTestCertificationRequests creates multiple test commitments with unique IDs.
func CreateTestCertificationRequests(t *testing.T, count int, prefix string) []*models.CertificationRequest {
	commitments := make([]*models.CertificationRequest, count)
	for i := 0; i < count; i++ {
		commitments[i] = CreateTestCertificationRequest(t, fmt.Sprintf("%s_%d", prefix, i))
	}
	return commitments
}
