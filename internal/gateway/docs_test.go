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
  "requestId": "0000981012b1c865f65d3d5523819cb34fa2c6827e792efd4579b4927144eb243122",
  "transactionHash": "0000c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297",
  "authenticator": {
    "algorithm": "secp256k1",
    "publicKey": "027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
    "signature": "65ed0261e093aa2df02c0e8fb0aa46144e053ea705ce7053023745b3626c60550b2a5e90eacb93416df116af96872547608a31de1f8ef25dc5a79104e6b69c8d00",
    "stateHash": "0000539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647"
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
