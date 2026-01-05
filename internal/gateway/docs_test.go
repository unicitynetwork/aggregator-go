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
	exampleJSON, err := hex.DecodeString("8458220000b1333daf3261d9bfa9d6dd98f170c0e756c26dbe284b5f90b27df900f6a77c04848301410158210299de0a2414a39fc981694b40bcb7006c6a3c70da7097a9a02877469fe1d2a62b582200002dc34763859638857585ce6aa30a43d3d7a342b51e6caee408888f3ab1c9e84b582200004c3b2c6fce3a19589cb219a0c18281696fedcbab1f28afd8aecc830cff55dacb584103ce4ef0fe3b4f53f5264daee6930c5e7a3b60f4dfd102b4d8f2420d8bbba17e446b0f855ad402437f14d00c1f27752e9aa802301ca42a57a80cb1f6f57e03eb00f500")
	require.NoError(t, err)

	// Parse the CBOR
	var certRequest *api.CertificationRequest
	err = types.Cbor.Unmarshal(exampleJSON, &certRequest)
	require.NoError(t, err, "Failed to parse example CBOR")

	// Verify DataHash imprint format (should start with 0000 for SHA256)
	certData := certRequest.CertificationData
	sourceStateHashImprint, err := certData.SourceStateHash.Imprint()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(sourceStateHashImprint), 3, "State hash should be at least 3 bytes")
	require.Equal(t, byte(0), sourceStateHashImprint[0], "State hash should start with 0x00 (SHA256 prefix)")
	require.Equal(t, byte(0), sourceStateHashImprint[1], "State hash should start with 0x00 (SHA256 prefix)")

	transactionHashImprint, err := certData.TransactionHash.Imprint()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(transactionHashImprint), 3, "Transaction hash should be at least 3 bytes")
	require.Equal(t, byte(0), transactionHashImprint[0], "Transaction hash should start with 0x00 (SHA256 prefix)")
	require.Equal(t, byte(0), transactionHashImprint[1], "Transaction hash should start with 0x00 (SHA256 prefix)")

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
