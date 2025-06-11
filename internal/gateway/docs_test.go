package gateway

import (
	"encoding/hex"
	"encoding/json"
	"testing"

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
  "requestId": "0000220207629fb5d473755e804685971f946eb2ebddc54cbced5b34bc67911bf98e",
  "transactionHash": "00003055507c62716fc2e38af9326f3d894eb0813b8e66b8fea45da4012f36560a9b",
  "authenticator": {
    "algorithm": "secp256k1",
    "publicKey": "02bdc93b5b700fc0562b6b4606bb57bde490cbc5e429913cfdadcb5c35ae02ba49",
    "signature": "1f4ed5ce831d95e8f3f3325f72f3f361435a64bb6969cf032d78618cf191625a3b326cd9fdac2972b11605be401df498acd644f4e9ef9e39257ad3a4aeff649fc8",
    "stateHash": "0000abac20b3ac6a9dfd4e0b645e1e584e3efe81a3dc6d0a55fb543226d94c96ef2a"
  },
  "receipt": true
}`

	// Parse the JSON
	var payload map[string]interface{}
	err := json.Unmarshal([]byte(exampleJSON), &payload)
	if err != nil {
		t.Fatalf("Failed to parse example JSON: %v", err)
	}

	// Extract fields
	requestID := api.RequestID(payload["requestId"].(string))
	transactionHashHex := payload["transactionHash"].(string)
	authenticator := payload["authenticator"].(map[string]interface{})

	algorithm := authenticator["algorithm"].(string)
	publicKeyHex := authenticator["publicKey"].(string)
	signatureHex := authenticator["signature"].(string)
	stateHashHex := authenticator["stateHash"].(string)

	// Verify algorithm
	if algorithm != "secp256k1" {
		t.Errorf("Expected algorithm secp256k1, got %s", algorithm)
	}

	// Decode hex values
	publicKey, err := hex.DecodeString(publicKeyHex)
	if err != nil {
		t.Fatalf("Failed to decode public key: %v", err)
	}

	signature, err := hex.DecodeString(signatureHex)
	if err != nil {
		t.Fatalf("Failed to decode signature: %v", err)
	}

	stateHashImprint, err := hex.DecodeString(stateHashHex)
	if err != nil {
		t.Fatalf("Failed to decode state hash: %v", err)
	}

	transactionHashImprint, err := hex.DecodeString(transactionHashHex)
	if err != nil {
		t.Fatalf("Failed to decode transaction hash: %v", err)
	}

	// Verify DataHash imprint format (should start with 0000 for SHA256)
	if len(stateHashImprint) < 3 || stateHashImprint[0] != 0 || stateHashImprint[1] != 0 {
		t.Errorf("State hash should be a SHA256 DataHash imprint starting with 0000")
	}

	if len(transactionHashImprint) < 3 || transactionHashImprint[0] != 0 || transactionHashImprint[1] != 0 {
		t.Errorf("Transaction hash should be a SHA256 DataHash imprint starting with 0000")
	}

	transactionHash := transactionHashImprint[2:]

	// Verify using our signing service
	signingService := signing.NewSigningService()

	// 1. Verify public key format
	if err := signingService.ValidatePublicKey(publicKey); err != nil {
		t.Errorf("Invalid public key format: %v", err)
	}

	// 2. Verify signature format
	if len(signature) != 65 {
		t.Errorf("Expected signature length 65, got %d", len(signature))
	}

	// 3. Verify request ID
	expectedRequestID, err := api.CreateRequestIDFromBytes(publicKey, stateHashImprint)
	if err != nil {
		t.Fatalf("Failed to create expected request ID: %v", err)
	}

	if string(expectedRequestID) != string(requestID) {
		t.Errorf("Request ID mismatch.\nExpected: %s\nActual: %s",
			string(expectedRequestID), string(requestID))
	}

	// 4. Verify signature
	// The signature is over the raw transaction hash bytes (no additional hashing)
	isValid, err := signingService.VerifyHashWithPublicKey(transactionHash, signature, publicKey)
	if err != nil {
		t.Fatalf("Failed to verify signature: %v", err)
	}

	if !isValid {
		t.Error("Signature verification failed - the documentation example has an invalid signature!")
	}

	// 5. Test with commitment validator (end-to-end validation)
	// Create commitment by unmarshaling the JSON directly to simulate real usage
	var commitment models.Commitment
	err = json.Unmarshal([]byte(exampleJSON), &commitment)
	if err != nil {
		t.Fatalf("Failed to unmarshal commitment: %v", err)
	}

	validator := signing.NewCommitmentValidator()
	result := validator.ValidateCommitment(&commitment)

	if result.Status != signing.ValidationStatusSuccess {
		t.Errorf("Commitment validation failed with status: %s, error: %v",
			result.Status.String(), result.Error)
	}

	t.Logf("✅ Documentation example payload passes all validation checks!")
}
