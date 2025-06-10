package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
)

// JSON-RPC types
type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
	ID      int           `json:"id"`
}

type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

// Request/Response types
type SubmitCommitmentRequest struct {
	RequestID       string        `json:"requestId"`
	TransactionHash string        `json:"transactionHash"`
	Authenticator   Authenticator `json:"authenticator"`
}

type Authenticator struct {
	Algorithm string `json:"algorithm"`
	PublicKey string `json:"publicKey"`
	Signature string `json:"signature"`
	StateHash string `json:"stateHash"`
}

type SubmitCommitmentResponse struct {
	Status string `json:"status"`
}

type GetBlockResponse struct {
	Block Block `json:"block"`
}

type Block struct {
	Index     string `json:"index"`
	Timestamp string `json:"timestamp"`
}

type GetBlockCommitmentsRequest struct {
	BlockNumber string `json:"blockNumber"`
}

type GetBlockCommitmentsResponse struct {
	Commitments []AggregatorRecord `json:"commitments"`
}

type AggregatorRecord struct {
	RequestID   string `json:"requestId"`
	BlockNumber string `json:"blockNumber"`
}

type GetBlockHeightResponse struct {
	BlockNumber string `json:"blockNumber"`
}

// Test configuration
const (
	aggregatorURL  = "http://localhost:3000"
	testDuration   = 10 * time.Second
	workerCount    = 100  // Number of concurrent workers (increased)
	requestsPerSec = 5000 // Target requests per second (increased)
	samplingRounds = 10   // Number of recent rounds to sample for average
)

// Metrics
type Metrics struct {
	totalRequests         int64
	successfulRequests    int64
	failedRequests        int64
	requestIdExistsErr    int64
	startTime             time.Time
	blockCommitmentCounts []int
	mutex                 sync.RWMutex
}

func (m *Metrics) addBlockCommitmentCount(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.blockCommitmentCounts = append(m.blockCommitmentCounts, count)
}

func (m *Metrics) getAverageCommitments() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if len(m.blockCommitmentCounts) == 0 {
		return 0
	}

	// Take last N rounds for average
	start := 0
	if len(m.blockCommitmentCounts) > samplingRounds {
		start = len(m.blockCommitmentCounts) - samplingRounds
	}

	total := 0
	count := 0
	for i := start; i < len(m.blockCommitmentCounts); i++ {
		total += m.blockCommitmentCounts[i]
		count++
	}

	if count == 0 {
		return 0
	}

	return float64(total) / float64(count)
}

// HTTP client for JSON-RPC calls
type JSONRPCClient struct {
	httpClient *http.Client
	url        string
	requestID  int64
}

func NewJSONRPCClient(url string) *JSONRPCClient {
	return &JSONRPCClient{
		httpClient: &http.Client{Timeout: 5 * time.Second},
		url:        url,
	}
}

func (c *JSONRPCClient) call(method string, params interface{}) (*JSONRPCResponse, error) {
	id := atomic.AddInt64(&c.requestID, 1)

	request := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      int(id),
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(c.url, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	var response JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// Generate random hex string
func generateRandomHex(length int) string {
	bytes := make([]byte, length/2)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// Generate a cryptographically valid commitment request
func generateCommitmentRequest() *SubmitCommitmentRequest {
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

	return &SubmitCommitmentRequest{
		RequestID:       string(requestID),
		TransactionHash: transactionHashImprint,
		Authenticator: Authenticator{
			Algorithm: "secp256k1",
			PublicKey: hex.EncodeToString(publicKeyBytes),
			Signature: hex.EncodeToString(signatureBytes),
			StateHash: stateHashImprint,
		},
	}
}

// Worker function that continuously submits commitments
func commitmentWorker(ctx context.Context, client *JSONRPCClient, metrics *Metrics, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second / time.Duration(requestsPerSec/workerCount))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Generate and submit commitment
			req := generateCommitmentRequest()

			atomic.AddInt64(&metrics.totalRequests, 1)

			resp, err := client.call("submit_commitment", req)
			if err != nil {
				atomic.AddInt64(&metrics.failedRequests, 1)
				continue
			}

			if resp.Error != nil {
				atomic.AddInt64(&metrics.failedRequests, 1)
				if resp.Error.Message == "REQUEST_ID_EXISTS" {
					atomic.AddInt64(&metrics.requestIdExistsErr, 1)
				}
				continue
			}

			// Parse response
			var submitResp SubmitCommitmentResponse
			respBytes, _ := json.Marshal(resp.Result)
			if err := json.Unmarshal(respBytes, &submitResp); err != nil {
				atomic.AddInt64(&metrics.failedRequests, 1)
				continue
			}

			if submitResp.Status == "SUCCESS" {
				atomic.AddInt64(&metrics.successfulRequests, 1)
			} else if submitResp.Status == "REQUEST_ID_EXISTS" {
				atomic.AddInt64(&metrics.requestIdExistsErr, 1)
			} else {
				atomic.AddInt64(&metrics.failedRequests, 1)
			}
		}
	}
}

// Monitor blocks and count commitments
func blockMonitor(ctx context.Context, client *JSONRPCClient, metrics *Metrics, wg *sync.WaitGroup) {
	defer wg.Done()

	// Get the starting block number to only monitor new blocks created during the test
	resp, err := client.call("get_block_height", nil)
	if err != nil {
		fmt.Printf("Failed to get initial block height: %v\n", err)
		return
	}
	
	if resp.Error != nil {
		fmt.Printf("Error getting initial block height: %v\n", resp.Error.Message)
		return
	}
	
	var heightResp GetBlockHeightResponse
	respBytes, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(respBytes, &heightResp); err != nil {
		fmt.Printf("Failed to parse initial block height: %v\n", err)
		return
	}
	
	// Parse starting block number
	var startingBlockNumber int64
	if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
		fmt.Printf("Failed to parse starting block number: %v\n", err)
		return
	}
	
	fmt.Printf("Starting block monitoring from block %d\n", startingBlockNumber+1)
	lastBlockNumber := startingBlockNumber
	
	ticker := time.NewTicker(100 * time.Millisecond) // Check for new blocks frequently
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current block height
			resp, err := client.call("get_block_height", nil)
			if err != nil {
				continue
			}

			if resp.Error != nil {
				continue
			}

			var heightResp GetBlockHeightResponse
			respBytes, _ := json.Marshal(resp.Result)
			if err := json.Unmarshal(respBytes, &heightResp); err != nil {
				continue
			}

			// Parse block number
			var currentBlockNumber int64
			if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &currentBlockNumber); err != nil {
				continue
			}

			// Only check new blocks created during the test
			for blockNum := lastBlockNumber + 1; blockNum <= currentBlockNumber; blockNum++ {

				// Get commitments for this block
				commitReq := GetBlockCommitmentsRequest{
					BlockNumber: fmt.Sprintf("%d", blockNum),
				}

				commitResp, err := client.call("get_block_commitments", commitReq)
				if err != nil {
					continue
				}

				if commitResp.Error != nil {
					continue
				}

				var commitsResp GetBlockCommitmentsResponse
				commitRespBytes, _ := json.Marshal(commitResp.Result)
				if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
					continue
				}

				commitmentCount := len(commitsResp.Commitments)
				metrics.addBlockCommitmentCount(commitmentCount)

				fmt.Printf("Block %d: %d commitments\n", blockNum, commitmentCount)
			}

			lastBlockNumber = currentBlockNumber
		}
	}
}

func main() {
	fmt.Printf("Starting aggregator performance test...\n")
	fmt.Printf("Target: %s\n", aggregatorURL)
	fmt.Printf("Duration: %v\n", testDuration)
	fmt.Printf("Workers: %d\n", workerCount)
	fmt.Printf("Target RPS: %d\n", requestsPerSec)
	fmt.Printf("----------------------------------------\n")

	// Initialize metrics
	metrics := &Metrics{
		startTime: time.Now(),
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	// Create JSON-RPC client
	client := NewJSONRPCClient(aggregatorURL)

	// Test connectivity
	fmt.Printf("Testing connectivity to %s...\n", aggregatorURL)
	if _, err := client.call("get_block_height", nil); err != nil {
		log.Fatalf("Failed to connect to aggregator: %v", err)
	}
	fmt.Printf("✓ Connected successfully\n")

	var wg sync.WaitGroup

	// Start block monitor
	wg.Add(1)
	go blockMonitor(ctx, client, metrics, &wg)

	// Start commitment workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go commitmentWorker(ctx, client, metrics, &wg)
	}

	// Progress reporting
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(metrics.startTime)
				total := atomic.LoadInt64(&metrics.totalRequests)
				successful := atomic.LoadInt64(&metrics.successfulRequests)
				failed := atomic.LoadInt64(&metrics.failedRequests)
				exists := atomic.LoadInt64(&metrics.requestIdExistsErr)

				rps := float64(total) / elapsed.Seconds()
				fmt.Printf("[%v] Total: %d, Success: %d, Failed: %d, Exists: %d, RPS: %.1f\n",
					elapsed.Truncate(time.Second), total, successful, failed, exists, rps)
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Final metrics
	elapsed := time.Since(metrics.startTime)
	total := atomic.LoadInt64(&metrics.totalRequests)
	successful := atomic.LoadInt64(&metrics.successfulRequests)
	failed := atomic.LoadInt64(&metrics.failedRequests)
	exists := atomic.LoadInt64(&metrics.requestIdExistsErr)

	fmt.Printf("\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("REQUEST_ID_EXISTS: %d\n", exists)
	fmt.Printf("Average RPS: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("Success rate: %.2f%%\n", float64(successful)/float64(total)*100)

	avgCommitments := metrics.getAverageCommitments()
	fmt.Printf("\nBLOCK THROUGHPUT:\n")
	fmt.Printf("Average commitments per block (last %d rounds): %.2f\n", samplingRounds, avgCommitments)
	fmt.Printf("Blocks processed per second: %.2f\n", 1.0) // 1-second rounds
	fmt.Printf("Effective commitment throughput: %.2f commitments/sec\n", avgCommitments)

	if len(metrics.blockCommitmentCounts) > 0 {
		fmt.Printf("\nBlock commitment counts: %v\n", metrics.blockCommitmentCounts)
	}

	fmt.Printf("========================================\n")
}
