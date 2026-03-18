package gateway

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestDocumentationExamplePayload verifies that the example payload in the documentation
// contains a valid signature that would pass validation
func TestDocumentationExamplePayload(t *testing.T) {
	// Extract the example payload from the documentation
	// This is the exact payload shown in the docs
	exampleJSON, err := hex.DecodeString("83582032bbbc306e3a101e8fa1e4349631864957c7d84167238c85cba972943ce05ccd8483014101582102cbbbe7dc6d51dea5c5fb4d7e7da3416e5914b989c303399f31b51db090981cfa58208126936ae7bcd660a93368a8c83951e01ccbcd4af093769b1cc14f942e2a9ca85820ad8f039daf3827446a0af7ccf31b438aea079440406a07cca7374b52b4e84c2c584177ff8a78a8e59f71d03e687fa14851327babb6a4f5cafcdfc56b16b2267284247734523df030465e249734aab146ae18d65bb3c7aee570c4f53e56e779e0c4200100")
	require.NoError(t, err)

	// Parse the CBOR
	var certRequest *api.CertificationRequest
	err = types.Cbor.Unmarshal(exampleJSON, &certRequest)
	require.NoError(t, err, "Failed to parse example CBOR")

	// Verify DataHash imprint format (should start with 0000 for SHA256)
	certData := certRequest.CertificationData
	require.Equal(t, len(certData.SourceStateHash.Imprint()), 32, "State hash should be 32 bytes")
	require.GreaterOrEqual(t, len(certData.TransactionHash.Imprint()), 32, "Transaction hash should be 32 bytes")

	// Verify using our signing service
	signingService := signing.NewSigningService()

	// 1. Verify public key format
	err = signingService.ValidatePublicKey(certData.OwnerPredicate.Params)
	require.NoError(t, err, "Invalid public key format")

	// 2. Verify signature format
	require.Equal(t, 65, len(certData.Witness), "Expected signature length 65")

	// 3. Verify state ID
	expectedStateID, err := certData.CreateStateID()
	require.NoError(t, err, "Failed to create expected state ID")

	require.Equal(t, string(expectedStateID), string(certRequest.StateID), "State ID mismatch")

	// 4. Verify signature
	// The signature is over the raw transaction hash bytes (no additional hashing)
	sigDataHash, err := certData.SigDataHash()
	require.NoError(t, err, "Failed to create signature data hash")

	isValid, err := signingService.VerifyDataHashWithPublicKey(sigDataHash, certData.Witness, certData.OwnerPredicate.Params)
	require.NoError(t, err, "Failed to verify signature")
	require.True(t, isValid, "Witness verification failed - the documentation example has an invalid signature!")

	// 5. Test with certification request validator (end-to-end validation)
	request := models.CertificationRequest{
		StateID: certRequest.StateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  certData.OwnerPredicate,
			SourceStateHash: certData.SourceStateHash,
			TransactionHash: certData.TransactionHash,
			Witness:         certData.Witness,
		},
	}
	validator := signing.NewCertificationRequestValidator(config.ShardingConfig{Mode: config.ShardingModeStandalone})
	result := validator.Validate(&request)

	require.Equal(t, signing.ValidationStatusSuccess, result.Status,
		"CertificationData validation failed with status: %s, error: %v", result.Status.String(), result.Error)

	t.Logf("✅ Documentation example payload passes all validation checks!")
}
