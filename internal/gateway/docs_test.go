package gateway

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

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
	exampleJSON := `{
  	"stateId": "00008b21e56c7240474646033765f7768966f8265c673f35a42f42557b67b9ff353f",
  	"certificationData": {
    	"publicKey": "03fdf2cad258e66f01ed8d0265e0b69c95de62ece8c29a1114c3b291a15f2b410e",
    	"sourceStateHash": "0000fc30e421e001d3c6a846749847b6a8e514d8d90dead42d6f245f1a4d74a24085",
    	"transactionHash": "000050a6635ff03e99d297b0802a14f0723f5246c555740d683ab0466b079ee421a5",
    	"signature": "cd7ea4da76def516b100da2478769a73458c53d9b5564bd282f77d4b4150f49a0e05f4cd2cd91b6bc64529095b6b7614bef739ecef9b250b964f5e0fba4dce3501"
  	}
}`

	// Parse the JSON
	var payload map[string]interface{}
	err := json.Unmarshal([]byte(exampleJSON), &payload)
	require.NoError(t, err, "Failed to parse example JSON")

	// Extract fields
	stateID := api.StateID(payload["stateId"].(string))
	certData := payload["certificationData"].(map[string]interface{})

	publicKeyHex := certData["publicKey"].(string)
	signatureHex := certData["signature"].(string)
	sourceStateHashHex := certData["sourceStateHash"].(string)
	transactionHashHex := certData["transactionHash"].(string)

	// Decode hex values
	publicKey, err := hex.DecodeString(publicKeyHex)
	require.NoError(t, err, "Failed to decode public key")

	signature, err := hex.DecodeString(signatureHex)
	require.NoError(t, err, "Failed to decode signature")

	sourceStateHashImprint, err := hex.DecodeString(sourceStateHashHex)
	require.NoError(t, err, "Failed to decode state hash")

	transactionHashImprint, err := hex.DecodeString(transactionHashHex)
	require.NoError(t, err, "Failed to decode transaction hash")

	// Verify DataHash imprint format (should start with 0000 for SHA256)
	require.GreaterOrEqual(t, len(sourceStateHashImprint), 3, "State hash should be at least 3 bytes")
	require.Equal(t, byte(0), sourceStateHashImprint[0], "State hash should start with 0x00 (SHA256 prefix)")
	require.Equal(t, byte(0), sourceStateHashImprint[1], "State hash should start with 0x00 (SHA256 prefix)")

	require.GreaterOrEqual(t, len(transactionHashImprint), 3, "Transaction hash should be at least 3 bytes")
	require.Equal(t, byte(0), transactionHashImprint[0], "Transaction hash should start with 0x00 (SHA256 prefix)")
	require.Equal(t, byte(0), transactionHashImprint[1], "Transaction hash should start with 0x00 (SHA256 prefix)")

	// Verify using our signing service
	signingService := signing.NewSigningService()

	// 1. Verify public key format
	err = signingService.ValidatePublicKey(publicKey)
	require.NoError(t, err, "Invalid public key format")

	// 2. Verify signature format
	require.Equal(t, 65, len(signature), "Expected signature length 65")

	// 3. Verify state ID
	expectedStateID, err := api.CreateStateIDFromImprint(publicKey, sourceStateHashImprint)
	require.NoError(t, err, "Failed to create expected state ID")

	require.Equal(t, string(expectedStateID), string(stateID), "State ID mismatch")

	// 4. Verify signature
	// The signature is over the raw transaction hash bytes (no additional hashing)
	sigDataHash := api.SigDataHash(sourceStateHashImprint, transactionHashImprint)
	isValid, err := signingService.VerifyDataHashWithPublicKey(sigDataHash, signature, publicKey)
	require.NoError(t, err, "Failed to verify signature")
	require.True(t, isValid, "Signature verification failed - the documentation example has an invalid signature!")

	// 5. Test with certification request validator (end-to-end validation)
	// Create certification request by unmarshaling the JSON directly to simulate real usage
	var request models.CertificationRequest
	err = json.Unmarshal([]byte(exampleJSON), &request)
	require.NoError(t, err, "Failed to unmarshal request")

	validator := signing.NewCertificationRequestValidator(config.ShardingConfig{Mode: config.ShardingModeStandalone})
	result := validator.Validate(&request)

	require.Equal(t, signing.ValidationStatusSuccess, result.Status,
		"CertificationData validation failed with status: %s, error: %v", result.Status.String(), result.Error)

	t.Logf("✅ Documentation example payload passes all validation checks!")
}
