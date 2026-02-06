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
	exampleJSON, err := hex.DecodeString("8458203e331d1f9c0265105db6173d932a419049fdbdfdd32f9e488fe48e8598a23eed8483014101582102d1a5a7bebd4118d9a46feb00686f2236bc745c8378eb54078c43003799e042395820b54d5ddf1772aca589c8fbfad03eca8d6ffaca0e1f1c1cd89ecd0afe3a73b6f05820f5c5161937726e0f24b074115e4752899a2f4d3f154764209ae11d05ec63c72e5841a000a2c83ddf683f8ca388cbd0b1bfd52bcd919641a0774d09459ca97aeeaf4e3682d863ac77afb71dc8efe0b8c1b983ee9b5231adad9adc5c27a0b97be6147c00f500")
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
