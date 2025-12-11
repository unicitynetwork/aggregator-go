package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type (
	jsonRPCRequest struct {
		JSONRPC string      `json:"jsonrpc"`
		Method  string      `json:"method"`
		Params  interface{} `json:"params"`
		ID      int         `json:"id"`
	}

	jsonRPCResponse struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result,omitempty"`
		Error   *jsonRPCError   `json:"error,omitempty"`
		ID      int             `json:"id"`
	}

	jsonRPCError struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	}
)

var (
	flagURL          = flag.String("url", "https://localhost:3002", "Aggregator JSON-RPC endpoint")
	flagAuth         = flag.String("auth", "", "Optional Authorization header value")
	flagTimeout      = flag.Duration("timeout", 45*time.Second, "Maximum time to wait for inclusion proof")
	flagPollInterval = flag.Duration("poll-interval", time.Second, "Polling interval for inclusion proof checks")
	flagVerbose      = flag.Bool("v", true, "Log request and response payloads")
)

func main() {
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	ctx, cancel := context.WithTimeout(context.Background(), *flagTimeout)
	defer cancel()

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	commitReq := generateCommitmentRequest()
	if *flagVerbose {
		if payload, err := json.MarshalIndent(commitReq, "", "  "); err == nil {
			logger.Printf("submit_commitment request:\n%s", payload)
		}
	}

	logger.Printf("Submitting commitment to URL: %s", *flagURL)

	submitResp, err := callJSONRPC(ctx, client, *flagURL, *flagAuth, "submit_commitment", commitReq)
	if err != nil {
		logger.Fatalf("submit_commitment call failed: %v", err)
	}

	if *flagVerbose {
		if payload, err := json.MarshalIndent(submitResp, "", "  "); err == nil {
			logger.Printf("submit_commitment response:\n%s", payload)
		}
	}

	var submitResult api.SubmitCommitmentResponse
	if submitResp.Error != nil {
		logger.Fatalf("submit_commitment returned error: %s (code %d)", submitResp.Error.Message, submitResp.Error.Code)
	}
	if err := json.Unmarshal(submitResp.Result, &submitResult); err != nil {
		logger.Fatalf("failed to decode submit_commitment result: %v", err)
	}
	if submitResult.Status != "SUCCESS" {
		logger.Fatalf("submit_commitment status was %q", submitResult.Status)
	}

	logger.Printf("Commitment %s accepted. Polling for inclusion proof...", commitReq.RequestID)

	path, err := commitReq.RequestID.GetPath()
	if err != nil {
		logger.Fatalf("failed to derive SMT path: %v", err)
	}

	submittedAt := time.Now()
	inclusionProof, verification, attempts, err := waitForInclusionProof(ctx, client, commitReq.RequestID, path, logger)
	if err != nil {
		logger.Fatalf("failed to retrieve inclusion proof after %d attempt(s): %v", attempts, err)
	}

	if *flagVerbose {
		if payload, err := json.MarshalIndent(inclusionProof, "", "  "); err == nil {
			logger.Printf("get_inclusion_proof response:\n%s", payload)
		}
	}

	logger.Printf("Proof verification result: pathValid=%t pathIncluded=%t overall=%t",
		verification.PathValid, verification.PathIncluded, verification.Result)

	elapsed := time.Since(submittedAt)
	logger.Printf("Valid inclusion proof received in %s after %d attempt(s).", elapsed.Round(time.Millisecond), attempts)

	logger.Printf("Commitment %s successfully submitted and verified.", commitReq.RequestID)
}

func generateCommitmentRequest() *api.SubmitCommitmentRequest {
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("failed to generate private key: %v", err))
	}

	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	stateData := make([]byte, 32)
	if _, err := rand.Read(stateData); err != nil {
		panic(fmt.Sprintf("failed to read random state bytes: %v", err))
	}

	stateHashImprint := signing.CreateDataHashImprint(stateData)

	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	if err != nil {
		panic(fmt.Sprintf("failed to create request ID: %v", err))
	}

	transactionData := make([]byte, 32)
	if _, err := rand.Read(transactionData); err != nil {
		panic(fmt.Sprintf("failed to read random transaction bytes: %v", err))
	}

	transactionHashImprint := signing.CreateDataHashImprint(transactionData)
	transactionHashBytes, err := transactionHashImprint.DataBytes()
	if err != nil {
		panic(fmt.Sprintf("failed to extract transaction hash bytes: %v", err))
	}

	signature, err := signing.NewSigningService().SignHash(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("failed to sign transaction hash: %v", err))
	}

	receipt := false
	return &api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(requestID),
		TransactionHash: api.TransactionHash(transactionHashImprint),
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(signature),
			StateHash: api.StateHash(stateHashImprint),
		},
		Receipt: &receipt,
	}
}

func callJSONRPC(ctx context.Context, client *http.Client, url, authHeader, method string, params interface{}) (*jsonRPCResponse, error) {
	body, err := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	/*if authHeader != "" {
		req.Header.Set("Authorization", "supersecret")
	}*/
	req.Header.Set("Authorization", "Bearer supersecret")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp jsonRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &rpcResp, nil
}

func waitForInclusionProof(ctx context.Context, client *http.Client, requestID api.RequestID, requestPath *big.Int, logger *log.Logger) (*api.GetInclusionProofResponse, *api.PathVerificationResult, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(45 * time.Second)
	}

	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		select {
		case <-ctx.Done():
			return nil, nil, attempts, ctx.Err()
		default:
		}

		proofResp, err := callJSONRPC(ctx, client, *flagURL, *flagAuth, "get_inclusion_proof", api.GetInclusionProofRequest{
			RequestID: requestID,
		})
		if err != nil {
			logger.Printf("get_inclusion_proof attempt %d failed: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		if proofResp.Error != nil {
			logger.Printf("get_inclusion_proof attempt %d returned error: %s (code %d)", attempts, proofResp.Error.Message, proofResp.Error.Code)
			time.Sleep(*flagPollInterval)
			continue
		}

		var payload api.GetInclusionProofResponse
		if err := json.Unmarshal(proofResp.Result, &payload); err != nil {
			logger.Printf("get_inclusion_proof attempt %d decode error: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		if payload.InclusionProof == nil || payload.InclusionProof.MerkleTreePath == nil {
			logger.Printf("get_inclusion_proof attempt %d: proof payload incomplete, retrying...", attempts)
			time.Sleep(*flagPollInterval)
			continue
		}

		result, err := verifyProof(&payload, requestPath)
		if err != nil {
			logger.Printf("get_inclusion_proof attempt %d verification error: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		if result.PathIncluded {
			return &payload, result, attempts, nil
		}

		logger.Printf("get_inclusion_proof attempt %d: proof returned but path not included yet (pathValid=%t). Waiting...",
			attempts, result.PathValid)
		time.Sleep(*flagPollInterval)
	}

	return nil, nil, attempts, fmt.Errorf("timed out waiting for inclusion proof for request %s", requestID)
}

func verifyProof(resp *api.GetInclusionProofResponse, path *big.Int) (*api.PathVerificationResult, error) {
	if resp == nil || resp.InclusionProof == nil {
		return nil, fmt.Errorf("inclusion proof payload was empty")
	}

	if resp.InclusionProof.MerkleTreePath == nil {
		return nil, fmt.Errorf("merkle tree path missing from inclusion proof")
	}

	return resp.InclusionProof.MerkleTreePath.Verify(path)
}
