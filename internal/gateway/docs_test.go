package gateway

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/predicates"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestDocumentationExamplePayload verifies that the example payload in the documentation
// contains a valid signature that would pass validation
func TestDocumentationExamplePayload(t *testing.T) {
	// Extract the example payload from the documentation
	// This is the exact payload shown in the docs
	exampleJSON := `{
  "stateId": "0000ea8412d69949eb70b7597b990b744661fcc3e9a209643309e703554baab55941",
  "certificationData": {
    "ownerPredicate": "830141015821036d8d8083dc71afded4453392aa45a29b65b9bc56287c8cefb246902282dca299",
    "sourceStateHash": "0000a973ad8c4813422205e833ede9a2762b4a8ee7a70274193a984bf3143f1b69f1",
    "transactionHash": "00009a64be27e8a92b42dc2ce33c8aecb62c2d364b15080cc6a61ed3e0d7836580a9",
    "witness": "d04617557ee8562b28b04ea8c7b53566f886d53d1fd0ed953342031292e94bf204c6596b66f19fe6a5f681ebe129d139222b14547578b4e4bf930270ac54aaa401"
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

	ownerPredicateHex := certData["ownerPredicate"].(string)
	witnessHex := certData["witness"].(string)
	sourceStateHashHex := certData["sourceStateHash"].(string)
	transactionHashHex := certData["transactionHash"].(string)

	// Decode hex values
	ownerPredicate, err := hex.DecodeString(ownerPredicateHex)
	require.NoError(t, err, "Failed to decode owner predicate")

	signature, err := hex.DecodeString(witnessHex)
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
	var pred predicates.Predicate
	require.NoError(t, types.Cbor.Unmarshal(ownerPredicate, &pred))
	err = signingService.ValidatePublicKey(pred.Params)
	require.NoError(t, err, "Invalid public key format")

	// 2. Verify signature format
	require.Equal(t, 65, len(signature), "Expected signature length 65")

	// 3. Verify state ID
	expectedStateID, err := api.CreateStateIDFromImprint(ownerPredicate, sourceStateHashImprint)
	require.NoError(t, err, "Failed to create expected state ID")

	require.Equal(t, string(expectedStateID), string(stateID), "State ID mismatch")

	// 4. Verify signature
	// The signature is over the raw transaction hash bytes (no additional hashing)
	sigDataHash := api.SigDataHash(sourceStateHashImprint, transactionHashImprint)
	isValid, err := signingService.VerifyDataHashWithPublicKey(sigDataHash, signature, pred.Params)
	require.NoError(t, err, "Failed to verify signature")
	require.True(t, isValid, "Witness verification failed - the documentation example has an invalid signature!")

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
