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

// CreateTestCommitment creates a valid, signed commitment for testing
func CreateTestCommitment(t *testing.T, baseData string) *models.Commitment {
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	stateData := make([]byte, 32)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		require.NoError(t, err, "Failed to generate random state data")
	}
	stateHashImprint := signing.CreateDataHashImprint(stateData)

	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	require.NoError(t, err, "Failed to create request ID")

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_%s", baseData)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		require.NoError(t, err, "Failed to generate random transaction data")
	}
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	transactionHashBytes, err := transactionHashImprint.DataBytes()
	require.NoError(t, err, "Failed to extract transaction hash")

	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction")

	authenticator := models.Authenticator{
		Algorithm: "secp256k1",
		PublicKey: publicKeyBytes,
		Signature: signatureBytes,
		StateHash: stateHashImprint,
	}

	return models.NewCommitment(requestID, transactionHashImprint, authenticator)
}
