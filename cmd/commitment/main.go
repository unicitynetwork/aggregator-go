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
	flagURL          = flag.String("url", "http://localhost:3002", "Aggregator JSON-RPC endpoint")
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

	certReq := generateCertificationRequest()
	if *flagVerbose {
		if payload, err := json.MarshalIndent(certReq, "", "  "); err == nil {
			logger.Printf("certification_request request:\n%s", payload)
		}
	}

	logger.Printf("Submitting certification request to URL: %s", *flagURL)

	submitResp, err := callJSONRPC(ctx, client, *flagURL, *flagAuth, "certification_request", certReq)
	if err != nil {
		logger.Fatalf("certification_request call failed: %v", err)
	}

	if *flagVerbose {
		if payload, err := json.MarshalIndent(submitResp, "", "  "); err == nil {
			logger.Printf("certification_request response:\n%s", payload)
		}
	}

	var submitResult api.CertificationResponse
	if submitResp.Error != nil {
		logger.Fatalf("certification_request returned error: %s (code %d)", submitResp.Error.Message, submitResp.Error.Code)
	}
	if err := json.Unmarshal(submitResp.Result, &submitResult); err != nil {
		logger.Fatalf("failed to decode certification_request result: %v", err)
	}
	if submitResult.Status != "SUCCESS" && submitResult.Status != "STATE_ID_EXISTS" {
		logger.Fatalf("certification_request status was %q", submitResult.Status)
	}
	logger.Printf("Certification request %s accepted. Polling for inclusion proof...", certReq.StateID)

	submittedAt := time.Now()
	inclusionProof, attempts, err := waitForInclusionProof(ctx, client, certReq, logger)
	if err != nil {
		logger.Fatalf("failed to retrieve inclusion proof after %d attempt(s): %v", attempts, err)
	}

	if *flagVerbose {
		if payload, err := json.MarshalIndent(inclusionProof, "", "  "); err == nil {
			logger.Printf("get_inclusion_proof.v2 response:\n%s", payload)
		}
	}

	if err := verifyInclusionProofLocal(inclusionProof.InclusionProof, certReq); err != nil {
		logger.Fatalf("proof verification failed: %v", err)
	}
	logger.Printf("Proof verified successfully against block %d.", inclusionProof.BlockNumber)

	elapsed := time.Since(submittedAt)
	logger.Printf("Valid inclusion proof received in %s after %d attempt(s).", elapsed.Round(time.Millisecond), attempts)

	logger.Printf("Certification request %s successfully submitted and verified.", certReq.StateID)
}

func generateCertificationRequest() *api.CertificationRequest {
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("failed to generate private key: %v", err))
	}

	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	stateData := make([]byte, 32)
	if _, err := rand.Read(stateData); err != nil {
		panic(fmt.Sprintf("failed to read random state bytes: %v", err))
	}

	sourceStateHash := signing.CreateDataHash(stateData)

	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	if err != nil {
		panic(fmt.Sprintf("failed to create state ID: %v", err))
	}

	transactionData := make([]byte, 32)
	if _, err := rand.Read(transactionData); err != nil {
		panic(fmt.Sprintf("failed to read random transaction bytes: %v", err))
	}

	transactionHash := signing.CreateDataHash(transactionData)
	certData := api.CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
	}
	if err := signing.NewSigningService().SignCertData(&certData, privateKey.Serialize()); err != nil {
		panic(fmt.Sprintf("failed to sign certification data: %v", err))
	}

	return &api.CertificationRequest{
		StateID:               stateID,
		CertificationData:     certData,
		AggregateRequestCount: 1,
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
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

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

func waitForInclusionProof(ctx context.Context, client *http.Client, req *api.CertificationRequest, logger *log.Logger) (*api.GetInclusionProofResponseV2, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(45 * time.Second)
	}

	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		select {
		case <-ctx.Done():
			return nil, attempts, ctx.Err()
		default:
		}

		proofResp, err := callJSONRPC(ctx, client, *flagURL, *flagAuth, "get_inclusion_proof.v2", api.GetInclusionProofRequestV2{
			StateID: req.StateID,
		})
		if err != nil {
			logger.Printf("get_inclusion_proof.v2 attempt %d failed: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		if proofResp.Error != nil {
			logger.Printf("get_inclusion_proof.v2 attempt %d returned error: %s (code %d)", attempts, proofResp.Error.Message, proofResp.Error.Code)
			time.Sleep(*flagPollInterval)
			continue
		}

		var payload api.GetInclusionProofResponseV2
		if err := json.Unmarshal(proofResp.Result, &payload); err != nil {
			logger.Printf("get_inclusion_proof.v2 attempt %d decode error: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		if payload.InclusionProof == nil || len(payload.InclusionProof.UnicityCertificate) == 0 {
			logger.Printf("get_inclusion_proof.v2 attempt %d: proof payload incomplete, retrying...", attempts)
			time.Sleep(*flagPollInterval)
			continue
		}

		if err := verifyInclusionProofLocal(payload.InclusionProof, req); err != nil {
			logger.Printf("get_inclusion_proof.v2 attempt %d verification error: %v", attempts, err)
			time.Sleep(*flagPollInterval)
			continue
		}

		return &payload, attempts, nil
	}

	return nil, attempts, fmt.Errorf("timed out waiting for inclusion proof for state ID %s", req.StateID)
}

// verifyInclusionProofLocal runs the SMT-path portion of v2 proof verification
// without checking UC signatures. For certified verification use
// api.InclusionProofV2.Verify.
func verifyInclusionProofLocal(p *api.InclusionProofV2, req *api.CertificationRequest) error {
	if p == nil {
		return fmt.Errorf("nil inclusion proof")
	}
	if p.CertificationData == nil {
		return api.ErrExclusionNotImpl
	}
	if !bytes.Equal(
		p.CertificationData.TransactionHash.DataBytes(),
		req.CertificationData.TransactionHash.DataBytes(),
	) {
		return fmt.Errorf("proof certification data transaction hash does not match request")
	}
	rootRaw, err := p.UCInputRecordHashRaw()
	if err != nil {
		return err
	}
	var cert api.InclusionCert
	if err := cert.UnmarshalBinary(p.CertificateBytes); err != nil {
		return fmt.Errorf("failed to decode inclusion cert: %w", err)
	}
	key, err := req.StateID.GetTreeKey()
	if err != nil {
		return fmt.Errorf("failed to derive SMT key: %w", err)
	}
	return cert.Verify(key, req.CertificationData.TransactionHash.DataBytes(), rootRaw, api.InclusionProofV2HashAlgorithm)
}
