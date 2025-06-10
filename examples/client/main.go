// Package main demonstrates how to use the public API types for client implementations
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const aggregatorURL = "http://localhost:3000"

// JSON-RPC request/response types
type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data,omitempty"`
	} `json:"error,omitempty"`
	ID int `json:"id"`
}

func main() {
	fmt.Println("Unicity Aggregator Client Example")
	fmt.Println("=================================")

	// Example 1: Create a cryptographically valid commitment
	fmt.Println("1. Creating a valid commitment...")
	commitment := createValidCommitment()
	fmt.Printf("   Request ID: %s\n", commitment.RequestID)
	fmt.Printf("   Public Key: %x\n", commitment.Authenticator.PublicKey)
	fmt.Printf("   Signature: %x\n", commitment.Authenticator.Signature)

	// Example 2: Submit the commitment (commented out since server might not be running)
	// fmt.Println("\n2. Submitting commitment...")
	// response, err := submitCommitment(commitment)
	// if err != nil {
	// 	log.Printf("   Error: %v", err)
	// } else {
	// 	fmt.Printf("   Status: %s\n", response.Status)
	// }

	// Example 3: Show other API types usage
	fmt.Println("\n3. Other API types available:")
	fmt.Println("   - api.GetInclusionProofRequest")
	fmt.Println("   - api.GetInclusionProofResponse") 
	fmt.Println("   - api.GetBlockRequest")
	fmt.Println("   - api.GetBlockResponse")
	fmt.Println("   - api.GetBlockCommitmentsRequest")
	fmt.Println("   - api.GetBlockCommitmentsResponse")
	fmt.Println("   - api.GetBlockHeightResponse")
	fmt.Println("   - api.GetNoDeletionProofResponse")

	fmt.Println("\nAll types are available in the 'github.com/unicitynetwork/aggregator-go/pkg/api' package")
}

// createValidCommitment demonstrates how to create a cryptographically valid commitment
func createValidCommitment() *api.SubmitCommitmentRequest {
	// Generate a real secp256k1 key pair
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Generate random state data and create DataHash imprint
	stateData := make([]byte, 32)
	rand.Read(stateData)
	stateHashImprint := signing.CreateDataHashImprint(stateData)

	// Extract actual state hash bytes for RequestID calculation
	stateHashBytes, err := signing.ExtractDataFromImprint(stateHashImprint)
	if err != nil {
		panic(fmt.Sprintf("Failed to extract state hash: %v", err))
	}

	// Create RequestID deterministically 
	requestIDGenerator := signing.NewRequestIDGenerator()
	requestID, err := requestIDGenerator.CreateRequestID(publicKeyBytes, stateHashBytes)
	if err != nil {
		panic(fmt.Sprintf("Failed to create request ID: %v", err))
	}

	// Generate random transaction data and create DataHash imprint
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	// Extract transaction hash bytes for signing
	transactionHashBytes, err := signing.ExtractDataFromImprint(transactionHashImprint)
	if err != nil {
		panic(fmt.Sprintf("Failed to extract transaction hash: %v", err))
	}

	// Sign the transaction hash bytes
	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.Sign(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	// Create receipt flag
	receipt := true
	
	return &api.SubmitCommitmentRequest{
		RequestID:       models.RequestID(requestID),
		TransactionHash: models.TransactionHash(transactionHashImprint),
		Authenticator: models.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: models.HexBytes(publicKeyBytes),
			Signature: models.HexBytes(signatureBytes),
			StateHash: models.HexBytes(stateHashImprint),
		},
		Receipt: &receipt,
	}
}

// submitCommitment demonstrates how to submit a commitment via JSON-RPC
func submitCommitment(req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	// Create JSON-RPC request
	rpcReq := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "submit_commitment",
		Params:  req,
		ID:      1,
	}

	// Marshal to JSON
	reqBody, err := json.Marshal(rpcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Send HTTP request
	resp, err := http.Post(aggregatorURL, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Parse JSON-RPC response
	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	// Parse the result as SubmitCommitmentResponse
	var result api.SubmitCommitmentResponse
	resultBytes, _ := json.Marshal(rpcResp.Result)
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return nil, fmt.Errorf("failed to parse result: %w", err)
	}

	return &result, nil
}