package testutil

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	v1 "github.com/unicitynetwork/aggregator-go/internal/models/v1"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	signingV1 "github.com/unicitynetwork/aggregator-go/internal/signing/v1"
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

// CreateTestCommitment creates a valid, signed commitment for testing
func CreateTestCommitment(t *testing.T, baseData string) *v1.Commitment {
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "Failed to generate private key")
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	stateData := make([]byte, 32)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		require.NoError(t, err, "Failed to generate random state data")
	}
	stateHashImprint := signingV1.CreateDataHashImprint(stateData)

	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	require.NoError(t, err, "Failed to create request ID")

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_%s", baseData)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		require.NoError(t, err, "Failed to generate random transaction data")
	}
	transactionHashImprint := signingV1.CreateDataHashImprint(transactionData)
	transactionHashBytes := transactionHashImprint.DataBytes()
	require.NoError(t, err, "Failed to extract transaction hash")

	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	require.NoError(t, err, "Failed to sign transaction")

	authenticator := v1.Authenticator{
		Algorithm: "secp256k1",
		PublicKey: publicKeyBytes,
		Signature: signatureBytes,
		StateHash: stateHashImprint,
	}

	return v1.NewCommitment(requestID, transactionHashImprint, authenticator)
}
