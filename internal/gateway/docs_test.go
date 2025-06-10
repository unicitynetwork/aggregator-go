package gateway

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
)

// TestDocumentationExamplePayload verifies that the example payload in the documentation
// contains a valid signature that would pass validation
func TestDocumentationExamplePayload(t *testing.T) {
	// Extract the example payload from the documentation
	// This is the exact payload shown in the docs
	exampleJSON := `{
  "requestId": "00004d1b938134c52340952357dd89c4c270b9b0b523bd69c03c1774fed907f1ebb5",
  "transactionHash": "0000d89cdfd6716717577adeb4149e22646cca3b4daf76632d35e97bd19642f8478a",
  "authenticator": {
    "algorithm": "secp256k1",
    "publicKey": "033cf8de37cec427b5e3d782e5fc516dcc43f8e9c7bc03530833879f6ee7987d4e",
    "signature": "2061590eeaf9c3fc3e894454b43410d0410f37ab17e5104a08db3d018d072880f9715dc3b60989cf9cc4589850edecac344702594aa264b2789792bb855a30f39c",
    "stateHash": "0000026581b5546639dc5110634df8cbbdf4150f3583fc54a0db98ef413574396dd0"
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
	requestID := models.RequestID(payload["requestId"].(string))
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

	// Extract actual data from imprints (skip first 2 bytes)
	stateHash := stateHashImprint[2:]
	transactionHash := transactionHashImprint[2:]

	// Verify using our signing service
	signingService := signing.NewSigningService()
	requestIDGenerator := signing.NewRequestIDGenerator()

	// 1. Verify public key format
	if err := signingService.ValidatePublicKey(publicKey); err != nil {
		t.Errorf("Invalid public key format: %v", err)
	}

	// 2. Verify signature format
	if len(signature) != 65 {
		t.Errorf("Expected signature length 65, got %d", len(signature))
	}

	// 3. Verify request ID
	expectedRequestID, err := requestIDGenerator.CreateRequestID(publicKey, stateHash)
	if err != nil {
		t.Fatalf("Failed to create expected request ID: %v", err)
	}

	if string(expectedRequestID) != string(requestID) {
		t.Errorf("Request ID mismatch.\nExpected: %s\nActual: %s", 
			string(expectedRequestID), string(requestID))
	}

	// 4. Verify signature
	isValid, err := signingService.VerifyWithPublicKey(transactionHash, signature, publicKey)
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