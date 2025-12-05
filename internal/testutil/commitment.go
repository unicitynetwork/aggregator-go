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

	stateData := make([]byte, 32)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		require.NoError(t, err, "Failed to generate random state data")
	}
	sourceStateHash := signing.CreateDataHashImprint(stateData)
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	require.NoError(t, err)

	stateID, err := api.CreateStateID(publicKeyBytes, sourceStateHash)
	require.NoError(t, err, "Failed to create state ID")

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_%s", baseData)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		require.NoError(t, err, "Failed to generate random transaction data")
	}
	transactionHash := signing.CreateDataHashImprint(transactionData)
	transactionHashImprint, err := transactionHash.Bytes()
	require.NoError(t, err, "Failed to extract transaction hash")

	signingService := signing.NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHashImprint, transactionHashImprint)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction")

	certData := models.CertificationData{
		PublicKey:       publicKeyBytes,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Signature:       signatureBytes,
	}
	return models.NewCertificationRequest(stateID, certData)
}
