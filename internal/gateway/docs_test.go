package gateway

import (
	"encoding/hex"
	"encoding/json"
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
	exampleJSON := `{
  "stateId": "0000316f0853596601083c6d5695a417cb0539ed9692f3366d125aaacaa8b69cbe31",
  "certificationData": {
    "publicKey": "02b9f440bbaa0c9784c10c59c6e19b7296dee36a78a8cc5feb649e278178cd19ce",
    "signature": "81e10fba935b6a48aee0712cf373ed1940c385e1b77c7ddfb71d7f0eb6449151071a07043e975174358ac2e7cb2d3a7f3dc2b7f5cbe70cf3964ad1fe0abcd2be01",
    "sourceStateHash": "0000fc30e421e001d3c6a846749847b6a8e514d8d90dead42d6f245f1a4d74a24085",
    "transactionHash": "000050a6635ff03e99d297b0802a14f0723f5246c555740d683ab0466b079ee421a5"
  },
  "receipt": true
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
	expectedStateID, err := api.CreateStateIDFromImprint(sourceStateHashImprint, publicKey)
	require.NoError(t, err, "Failed to create expected state ID")

	require.Equal(t, string(expectedStateID), string(stateID), "State ID mismatch")

	// 4. Verify signature
	// The signature is over the raw transaction hash bytes (no additional hashing)
	sigData := api.SigHashData{
		SourceStateHashImprint: sourceStateHashImprint,
		TransactionHashImprint: transactionHashImprint,
	}
	sigDataCBOR, err := types.Cbor.Marshal(sigData)
	require.NoError(t, err, "Failed to marshal signature data")
	isValid, err := signingService.VerifyWithPublicKey(sigDataCBOR, signature, publicKey)
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
