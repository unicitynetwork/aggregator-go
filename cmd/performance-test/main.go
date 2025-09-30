package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
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

// Use the public API types

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
	defaultAggregatorURL = "http://localhost:3000"
	testDuration         = 30 * time.Second
	workerCount          = 30   // Number of concurrent workers
	requestsPerSec       = 5000 // Target requests per second
)

// BlockCommitmentInfo stores block number and commitment count
type BlockCommitmentInfo struct {
	BlockNumber     int64
	CommitmentCount int
}

// Metrics
type Metrics struct {
	totalRequests         int64
	successfulRequests    int64
	failedRequests        int64
	requestIdExistsErr    int64
	startTime             time.Time
	blockCommitmentCounts []int
	blockCommitmentInfo   []BlockCommitmentInfo // Track block numbers with counts
	totalBlockCommitments int64                 // Track total commitments processed in blocks
	startingBlockNumber   int64                 // Store starting block number
	submittedRequestIDs   sync.Map              // Thread-safe map to track which request IDs we submitted
	mutex                 sync.RWMutex          // For protecting blockCommitmentCounts slice
}

func (m *Metrics) addBlockCommitmentCount(blockNumber int64, count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Track all blocks including empty ones to show the real pattern
	m.blockCommitmentCounts = append(m.blockCommitmentCounts, count)
	m.blockCommitmentInfo = append(m.blockCommitmentInfo, BlockCommitmentInfo{
		BlockNumber:     blockNumber,
		CommitmentCount: count,
	})
	atomic.AddInt64(&m.totalBlockCommitments, int64(count))
}

func (m *Metrics) getAverageCommitments() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if len(m.blockCommitmentCounts) == 0 {
		return 0
	}

	// Calculate average across ALL blocks, not just a sample
	total := 0
	count := 0
	for i := 0; i < len(m.blockCommitmentCounts); i++ {
		if m.blockCommitmentCounts[i] > 0 {
			total += m.blockCommitmentCounts[i]
			count++
		}
	}

	if count == 0 {
		return 0
	}

	return float64(total) / float64(count)
}

// APIClient interface for submitting commitments
type APIClient interface {
	SubmitCommitment(req *api.SubmitCommitmentRequest) error
	GetBlockHeight() (*GetBlockHeightResponse, error)
	GetBlock(blockNumber string) (*GetBlockResponse, error)
	GetBlockCommitments(blockNumber string) (*GetBlockCommitmentsResponse, error)
}

// JSONRPCClient implements APIClient for JSON-RPC
type JSONRPCClient struct {
	httpClient *http.Client
	url        string
	authHeader string
	requestID  int64
}

// RESTClient implements APIClient for REST API
type RESTClient struct {
	httpClient *http.Client
	baseURL    string
	authHeader string
	jsonRPCClient *JSONRPCClient // Cache for non-submission endpoints
}

// NewRESTClient creates a new REST API client
func NewRESTClient(url string, authHeader string) *RESTClient {
	// Configure transport with higher connection limits
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
	}

	return &RESTClient{
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		baseURL:    url,
		authHeader: authHeader,
		jsonRPCClient: NewJSONRPCClient(url, authHeader), // Create once and reuse
	}
}

// SubmitCommitment submits a commitment via REST API
func (c *RESTClient) SubmitCommitment(req *api.SubmitCommitmentRequest) error {
	url := fmt.Sprintf("%s/commitments/%s", c.baseURL, req.RequestID)

	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	if c.authHeader != "" {
		httpReq.Header.Set("Authorization", c.authHeader)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("REQUEST_ID_EXISTS")
	}
	if resp.StatusCode != http.StatusOK {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		if msg, ok := errResp["error"].(string); ok {
			return fmt.Errorf("REST API error: %s", msg)
		}
		return fmt.Errorf("request failed with status %d", resp.StatusCode)
	}

	return nil
}

// GetBlockHeight gets the current block height via REST API (uses JSON-RPC endpoint)
func (c *RESTClient) GetBlockHeight() (*GetBlockHeightResponse, error) {
	// For non-submission endpoints, we still use JSON-RPC
	return c.jsonRPCClient.GetBlockHeight()
}

// GetBlock gets a block by number via REST API (uses JSON-RPC endpoint)
func (c *RESTClient) GetBlock(blockNumber string) (*GetBlockResponse, error) {
	// For non-submission endpoints, we still use JSON-RPC
	return c.jsonRPCClient.GetBlock(blockNumber)
}

// GetBlockCommitments gets commitments in a block via REST API (uses JSON-RPC endpoint)
func (c *RESTClient) GetBlockCommitments(blockNumber string) (*GetBlockCommitmentsResponse, error) {
	// For non-submission endpoints, we still use JSON-RPC
	return c.jsonRPCClient.GetBlockCommitments(blockNumber)
}

func NewJSONRPCClient(url string, authHeader string) *JSONRPCClient {
	// Configure transport with higher connection limits
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
	}

	return &JSONRPCClient{
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		url:        url,
		authHeader: authHeader,
	}
}

// SubmitCommitment submits a commitment via JSON-RPC
func (c *JSONRPCClient) SubmitCommitment(req *api.SubmitCommitmentRequest) error {
	resp, err := c.call("submit_commitment", req)
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	return nil
}

// GetBlockHeight gets the current block height via JSON-RPC
func (c *JSONRPCClient) GetBlockHeight() (*GetBlockHeightResponse, error) {
	resp, err := c.call("get_block_height", nil)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	var result GetBlockHeightResponse
	respBytes, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetBlock gets a block by number via JSON-RPC
func (c *JSONRPCClient) GetBlock(blockNumber string) (*GetBlockResponse, error) {
	params := map[string]interface{}{"blockNumber": blockNumber}
	resp, err := c.call("get_block", params)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	var result GetBlockResponse
	respBytes, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetBlockCommitments gets commitments in a block via JSON-RPC
func (c *JSONRPCClient) GetBlockCommitments(blockNumber string) (*GetBlockCommitmentsResponse, error) {
	params := GetBlockCommitmentsRequest{BlockNumber: blockNumber}
	resp, err := c.call("get_block_commitments", params)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("JSON-RPC error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	var result GetBlockCommitmentsResponse
	respBytes, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, err
	}
	return &result, nil
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

	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add auth header if provided
	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}

	resp, err := c.httpClient.Do(req)
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
func generateCommitmentRequest() *api.SubmitCommitmentRequest {
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

	// Create RequestID deterministically
	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	if err != nil {
		panic(fmt.Sprintf("Failed to create request ID: %v", err))
	}

	// Generate random transaction data and create DataHash imprint
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	// Extract transaction hash bytes for signing
	transactionHashBytes, err := transactionHashImprint.DataBytes()
	if err != nil {
		panic(fmt.Sprintf("Failed to extract transaction hash: %v", err))
	}

	// Sign the transaction hash bytes
	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	// Create receipt flag
	receipt := false

	return &api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(requestID),
		TransactionHash: api.TransactionHash(transactionHashImprint),
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes(publicKeyBytes),
			Signature: api.HexBytes(signatureBytes),
			StateHash: api.StateHash(stateHashImprint),
		},
		Receipt: &receipt,
	}
}

// Worker function that continuously submits commitments
func commitmentWorker(ctx context.Context, client APIClient, metrics *Metrics, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second / time.Duration(requestsPerSec/workerCount))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Generate and submit commitment asynchronously
			go func() {
				req := generateCommitmentRequest()

				atomic.AddInt64(&metrics.totalRequests, 1)

				err := client.SubmitCommitment(req)
				if err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if strings.Contains(err.Error(), "REQUEST_ID_EXISTS") {
						atomic.AddInt64(&metrics.requestIdExistsErr, 1)
						// Track this ID - it exists so it will be in blocks!
						metrics.submittedRequestIDs.Store(string(req.RequestID), true)
					}
					return
				}

				// Success
				atomic.AddInt64(&metrics.successfulRequests, 1)
				// Track this request ID as submitted by us (normalized to lowercase)
				metrics.submittedRequestIDs.Store(strings.ToLower(string(req.RequestID)), true)
			}()
		}
	}
}

func main() {
	// Parse command-line flags
	var useREST bool
	flag.BoolVar(&useREST, "rest", false, "Use REST API instead of JSON-RPC for submitting commitments")
	flag.Parse()

	// Get URL and auth header from environment variables
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	if aggregatorURL == "" {
		aggregatorURL = defaultAggregatorURL
	}

	authHeader := os.Getenv("AUTH_HEADER")

	fmt.Printf("Starting aggregator performance test...\n")
	fmt.Printf("Target: %s\n", aggregatorURL)
	fmt.Printf("API Mode: %s\n", func() string {
		if useREST {
			return "REST"
		}
		return "JSON-RPC"
	}())
	if authHeader != "" {
		fmt.Printf("Authorization: [configured]\n")
	}
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

	// Create API client based on flag
	var client APIClient
	if useREST {
		client = NewRESTClient(aggregatorURL, authHeader)
	} else {
		client = NewJSONRPCClient(aggregatorURL, authHeader)
	}

	// Test connectivity and get starting block number
	fmt.Printf("Testing connectivity to %s...\n", aggregatorURL)
	heightResp, err := client.GetBlockHeight()
	if err != nil {
		log.Fatalf("Failed to connect to aggregator: %v", err)
	}

	// Parse starting block number
	var startingBlockNumber int64
	if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
		log.Fatalf("Failed to parse starting block number: %v", err)
	}

	fmt.Printf("✓ Connected successfully\n")
	fmt.Printf("✓ Starting block number: %d\n", startingBlockNumber)
	metrics.startingBlockNumber = startingBlockNumber

	var wg sync.WaitGroup

	// Start commitment workers (no separate block monitor)
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

	// Give a moment for any in-flight requests to complete
	time.Sleep(1 * time.Second)

	// Stop submission phase and get counts
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Submission completed. Now checking blocks for all commitments...\n")

	successful := atomic.LoadInt64(&metrics.successfulRequests)
	fmt.Printf("Total successful submissions: %d\n", successful)
	fmt.Printf("Starting from block %d\n", startingBlockNumber+1)

	// First, get the latest block number to know the range
	// Create a separate client for waiting/monitoring (always use same type as main client)
	var waitClient APIClient
	if useREST {
		waitClient = NewRESTClient(aggregatorURL, authHeader)
	} else {
		waitClient = NewJSONRPCClient(aggregatorURL, authHeader)
	}

	var latestBlockNumber int64
	heightResp, err = waitClient.GetBlockHeight()
	if err == nil {
		fmt.Sscanf(heightResp.BlockNumber, "%d", &latestBlockNumber)
	}

	fmt.Printf("Latest block: %d\n", latestBlockNumber)

	// Check all blocks from start to latest-1 (since latest might not be fully persisted)
	processedCount := int64(0)
	currentCheckBlock := startingBlockNumber + 1
	blocksWithOurCommitments := 0

	// Only check blocks that are guaranteed to be fully persisted (N-1)
	safeBlockNumber := latestBlockNumber - 1
	if safeBlockNumber < currentCheckBlock {
		fmt.Printf("\nWaiting for more blocks to be created...\n")
		safeBlockNumber = currentCheckBlock
	}

	fmt.Printf("\nChecking blocks %d to %d (block %d exists, ensuring previous blocks are fully persisted)...\n",
		currentCheckBlock, safeBlockNumber, latestBlockNumber)

	for currentCheckBlock <= safeBlockNumber && processedCount < successful {
		// Try to get commitments for this block
		commitsResp, err := waitClient.GetBlockCommitments(fmt.Sprintf("%d", currentCheckBlock))
		if err != nil {
			// Network or API error, skip this block
			currentCheckBlock++
			continue
		}

		// Count only commitments that we submitted
		ourCommitmentCount := 0
		notOurs := 0
		for _, commitment := range commitsResp.Commitments {
			// Normalize the request ID to ensure consistent format
			requestIDStr := strings.ToLower(commitment.RequestID)
			if _, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists {
				ourCommitmentCount++
				// Mark this ID as found
				metrics.submittedRequestIDs.Store(requestIDStr, "found")
			} else {
				notOurs++
			}
		}

		// Debug: if block has many commitments not from us, log it
		if notOurs > 100 {
			fmt.Printf("  [DEBUG] Block %d has %d commitments not from our test\n", currentCheckBlock, notOurs)
		}

		// Track block and update counts
		metrics.addBlockCommitmentCount(currentCheckBlock, ourCommitmentCount)
		processedCount += int64(ourCommitmentCount)

		if ourCommitmentCount > 0 {
			fmt.Printf("Block %d: %d our commitments (total in block: %d, running total: %d/%d)\n",
				currentCheckBlock, ourCommitmentCount, len(commitsResp.Commitments), processedCount, successful)
			blocksWithOurCommitments++
		} else if len(commitsResp.Commitments) > 0 {
			// Block has commitments but none are ours
			fmt.Printf("Block %d: %d commitments from other sources\n", currentCheckBlock, len(commitsResp.Commitments))
		}

		currentCheckBlock++

		// Brief pause to avoid hammering the API
		time.Sleep(20 * time.Millisecond)
	}

	// If we haven't found all commitments yet, keep checking for a bit more
	if processedCount < successful {
		fmt.Printf("\nContinuing to check for remaining %d commitments...\n", successful-processedCount)
		fmt.Printf("Will check for up to 3 minutes for new blocks...\n")

		// Continue checking for up to 3 minutes or until we find all commitments
		timeoutTime := time.Now().Add(3 * time.Minute)
		lastProgressTime := time.Now()
		lastReportTime := time.Now()
		blocksCheckedInWait := 0

		// Track blocks that had commitments but none were ours (might need retry due to race condition)
		type blockRetryInfo struct {
			blockNumber      int64
			totalCommitments int
			lastChecked      time.Time
			retryCount       int
		}
		blocksToRetry := make(map[int64]*blockRetryInfo)

		// Helper function to check a block and handle retry logic
		checkBlock := func(blockNum int64) bool {
			commitsResp, err := waitClient.GetBlockCommitments(fmt.Sprintf("%d", blockNum))
			if err != nil {
				// Only log real errors, not "block doesn't exist yet"
				if !strings.Contains(err.Error(), "does not exist") {
					fmt.Printf("Block %d: error: %v\n", blockNum, err)
				}
				return false
			}

			// Count only commitments that we submitted and haven't counted yet
			ourCommitmentCount := 0
			for _, commitment := range commitsResp.Commitments {
				// Normalize the request ID to ensure consistent format
				requestIDStr := strings.ToLower(commitment.RequestID)
				if val, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists && val != "found" {
					ourCommitmentCount++
					// Mark this ID as found
					metrics.submittedRequestIDs.Store(requestIDStr, "found")
				}
			}

			if ourCommitmentCount > 0 {
				metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount)
				processedCount += int64(ourCommitmentCount)
				fmt.Printf("Block %d: %d our commitments (total in block: %d, running total: %d/%d)\n",
					blockNum, ourCommitmentCount, len(commitsResp.Commitments), processedCount, successful)
				lastProgressTime = time.Now()
				// Remove from retry list if it was there
				delete(blocksToRetry, blockNum)
				return true
			} else if len(commitsResp.Commitments) > 0 {
				// Block has commitments but none are ours - might be race condition
				// Track for retry in case aggregator records are still being written
				if _, exists := blocksToRetry[blockNum]; !exists {
					blocksToRetry[blockNum] = &blockRetryInfo{
						blockNumber:      blockNum,
						totalCommitments: len(commitsResp.Commitments),
						lastChecked:      time.Now(),
						retryCount:       0,
					}
					fmt.Printf("Block %d: 0 our commitments yet (will retry, total: %d)\n",
						blockNum, len(commitsResp.Commitments))
				}
			} else {
				// Block exists but has 0 commitments total
				// Don't print anything for empty blocks to reduce noise
			}
			return false
		}

		for processedCount < successful && time.Now().Before(timeoutTime) {
			// Get current block height
			heightResult, err := waitClient.GetBlockHeight()
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var latestBlock int64
			if _, err := fmt.Sscanf(heightResult.BlockNumber, "%d", &latestBlock); err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Check any new blocks that we haven't checked yet
			// Only check blocks up to latestBlock-1 to ensure they're fully persisted
			safeLatestBlock := latestBlock - 1
			for currentCheckBlock <= safeLatestBlock && processedCount < successful {
				if checkBlock(currentCheckBlock) {
					blocksCheckedInWait++
				}
				currentCheckBlock++
			}

			// Retry blocks that had commitments but we didn't recognize them (race condition with aggregator record writes)
			// Be more aggressive with retries to ensure we find all commitments
			for blockNum, info := range blocksToRetry {
				// Retry more frequently (every 500ms) and don't give up until we timeout
				if time.Since(info.lastChecked) > 500*time.Millisecond {
					// Retry this block
					if commitsResp, err := waitClient.GetBlockCommitments(fmt.Sprintf("%d", blockNum)); err == nil {
						ourCommitmentCount := 0
						for _, commitment := range commitsResp.Commitments {
							// Normalize the request ID to ensure consistent format
							requestIDStr := strings.ToLower(commitment.RequestID)
							if val, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists && val != "found" {
								ourCommitmentCount++
								metrics.submittedRequestIDs.Store(requestIDStr, "found")
							}
						}

						if ourCommitmentCount > 0 {
							// Found some! Remove from retry list
							metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount)
							processedCount += int64(ourCommitmentCount)
							fmt.Printf("Block %d (retry #%d): FOUND %d our commitments! (running total: %d/%d)\n",
								blockNum, info.retryCount+1, ourCommitmentCount, processedCount, successful)
							delete(blocksToRetry, blockNum)
							lastProgressTime = time.Now()
						} else {
							// Still none, update retry info
							info.retryCount++
							info.lastChecked = time.Now()

							// Don't give up - keep retrying until timeout
							// Only log every 10 retries to reduce noise
							if info.retryCount%10 == 0 {
								fmt.Printf("Block %d: retry #%d, still waiting for aggregator records (block has %d total commitments)\n",
									blockNum, info.retryCount, info.totalCommitments)
							}
						}
					}
				}
			}

			// Update progress if we found any
			if processedCount > 0 {
				lastProgressTime = time.Now()
			}

			// Report status periodically
			if time.Since(lastReportTime) > 5*time.Second {
				retryCount := len(blocksToRetry)
				if retryCount > 0 {
					fmt.Printf("Still checking... waiting for block %d (latest: %d), found %d/%d commitments, %d blocks pending retry...\n",
						currentCheckBlock, latestBlock, processedCount, successful, retryCount)
				} else {
					fmt.Printf("Still checking... waiting for block %d (latest: %d), found %d/%d commitments...\n",
						currentCheckBlock, latestBlock, processedCount, successful)
				}
				lastReportTime = time.Now()
			}

			// If no progress for 90 seconds AND we have no blocks to retry, stop
			if time.Since(lastProgressTime) > 90*time.Second && len(blocksToRetry) == 0 {
				fmt.Printf("\nNo new commitments found for 90 seconds and no blocks pending retry (checked %d additional blocks), stopping...\n", blocksCheckedInWait)
				break
			}

			// Keep trying if we still have blocks to retry (eventual consistency)
			if len(blocksToRetry) > 0 && time.Since(lastProgressTime) > 120*time.Second {
				fmt.Printf("\nNo progress for 120 seconds despite %d blocks still pending retry, stopping...\n", len(blocksToRetry))
				break
			}

			// Brief pause before checking again for new blocks
			time.Sleep(200 * time.Millisecond)
		}

		// Final aggressive retry pass for any remaining blocks
		if len(blocksToRetry) > 0 && processedCount < successful {
			fmt.Printf("\nPerforming final retry pass on %d blocks...\n", len(blocksToRetry))
			finalRetryCount := 0
			maxFinalRetries := 10

			for finalRetryCount < maxFinalRetries && len(blocksToRetry) > 0 && processedCount < successful {
				finalRetryCount++
				foundInRound := 0

				for blockNum := range blocksToRetry {
					if commitsResp, err := waitClient.GetBlockCommitments(fmt.Sprintf("%d", blockNum)); err == nil {
						ourCommitmentCount := 0
						for _, commitment := range commitsResp.Commitments {
							requestIDStr := strings.ToLower(commitment.RequestID)
							if val, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists && val != "found" {
								ourCommitmentCount++
								metrics.submittedRequestIDs.Store(requestIDStr, "found")
							}
						}

						if ourCommitmentCount > 0 {
							metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount)
							processedCount += int64(ourCommitmentCount)
							foundInRound += ourCommitmentCount
							fmt.Printf("  Block %d (final retry): FOUND %d commitments! (running total: %d/%d)\n",
								blockNum, ourCommitmentCount, processedCount, successful)
							delete(blocksToRetry, blockNum)
						}
					}
				}

				if foundInRound > 0 {
					fmt.Printf("Final retry round %d: found %d commitments\n", finalRetryCount, foundInRound)
				}

				// Brief pause before next retry
				time.Sleep(1 * time.Second)
			}

			if len(blocksToRetry) > 0 {
				fmt.Printf("Final retry pass complete, %d blocks still pending\n", len(blocksToRetry))
			}
		}
	}

	// Update the final count
	atomic.StoreInt64(&metrics.totalBlockCommitments, processedCount)

	// Debug: count tracked IDs and find missing ones
	trackedCount := 0
	foundCount := 0
	var sampleMissingIDs []string

	metrics.submittedRequestIDs.Range(func(key, value interface{}) bool {
		trackedCount++
		requestID := key.(string)
		// Check if this ID was found in blocks
		if value == "found" {
			foundCount++
		} else if len(sampleMissingIDs) < 5 {
			// This is a missing ID
			sampleMissingIDs = append(sampleMissingIDs, requestID)
		}
		return true
	})

	fmt.Printf("\nDebug: Tracked %d request IDs, found in blocks: %d, missing: %d\n", trackedCount, foundCount, trackedCount-foundCount)
	if len(sampleMissingIDs) > 0 {
		fmt.Printf("Sample missing IDs:\n")
		for i, id := range sampleMissingIDs {
			fmt.Printf("  %d. %s\n", i+1, id)
		}
	}

	// Additional debug: check if we have the right block range
	if len(metrics.blockCommitmentInfo) > 0 {
		firstBlock := metrics.blockCommitmentInfo[0].BlockNumber
		lastBlock := metrics.blockCommitmentInfo[len(metrics.blockCommitmentInfo)-1].BlockNumber
		fmt.Printf("Checked blocks from %d to %d (total: %d blocks)\n", firstBlock, lastBlock, len(metrics.blockCommitmentInfo))
	}

	if processedCount < successful {
		fmt.Printf("\nFinished checking. Found %d/%d commitments\n", processedCount, successful)
	} else {
		fmt.Printf("\nAll %d commitments have been found in blocks!\n", successful)
	}
	// Final metrics
	elapsed := time.Since(metrics.startTime)
	total := atomic.LoadInt64(&metrics.totalRequests)
	successful = atomic.LoadInt64(&metrics.successfulRequests)
	failed := atomic.LoadInt64(&metrics.failedRequests)
	exists := atomic.LoadInt64(&metrics.requestIdExistsErr)
	processedInBlocks := atomic.LoadInt64(&metrics.totalBlockCommitments)

	fmt.Printf("\n\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("REQUEST_ID_EXISTS: %d\n", exists)
	fmt.Printf("Average RPS: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("Success rate: %.2f%%\n", float64(successful)/float64(total)*100)

	fmt.Printf("\nBLOCK PROCESSING:\n")
	fmt.Printf("Total commitments in blocks: %d\n", processedInBlocks)
	pendingCommitments := successful - processedInBlocks
	fmt.Printf("Commitments pending: %d\n", pendingCommitments)

	if pendingCommitments > 0 {
		percentage := float64(pendingCommitments) / float64(successful) * 100
		fmt.Printf("\n⚠️  WARNING: %d commitments (%.1f%%) not found in blocks!\n", pendingCommitments, percentage)
		fmt.Printf("\nNote: Blocks may contain commitments from other sources (previous tests, etc.)\n")
		fmt.Printf("The test correctly tracks only commitments submitted in this run.\n")
		if percentage < 5.0 {
			fmt.Printf("\nWith only %.1f%% missing, this is likely due to processing delays or queue limits.\n", percentage)
		}
	} else {
		fmt.Printf("\n✅ SUCCESS: All %d commitments were found in blocks!\n", successful)
	}

	fmt.Printf("\nBLOCK THROUGHPUT:\n")
	fmt.Printf("Total blocks checked: %d\n", len(metrics.blockCommitmentCounts))

	// Count empty vs non-empty blocks
	emptyBlocks := 0
	nonEmptyBlocks := 0
	for _, count := range metrics.blockCommitmentCounts {
		if count == 0 {
			emptyBlocks++
		} else {
			nonEmptyBlocks++
		}
	}

	if nonEmptyBlocks > 0 {
		fmt.Printf("Non-empty blocks: %d (average %.1f commitments/block)\n",
			nonEmptyBlocks, float64(processedInBlocks)/float64(nonEmptyBlocks))
	}
	if emptyBlocks > 0 {
		fmt.Printf("Empty blocks: %d\n", emptyBlocks)
	}

	// Calculate block creation rate
	if len(metrics.blockCommitmentCounts) > 0 {
		totalBlockTime := float64(len(metrics.blockCommitmentCounts))
		blockCreationRate := float64(len(metrics.blockCommitmentCounts)) / totalBlockTime
		fmt.Printf("Block creation rate: %.1f blocks/sec\n", blockCreationRate)

		// Calculate throughput based on non-empty blocks only
		if nonEmptyBlocks > 0 {
			fmt.Printf("Effective commitment throughput: %.1f commitments/sec\n",
				float64(processedInBlocks)/totalBlockTime)
		}
	}

	if len(metrics.blockCommitmentInfo) > 0 {
		fmt.Printf("\nBlock details:\n")
		if len(metrics.blockCommitmentInfo) <= 20 {
			// Print all blocks if 20 or fewer
			for _, info := range metrics.blockCommitmentInfo {
				fmt.Printf("  Block %d: %d commitments\n", info.BlockNumber, info.CommitmentCount)
			}
		} else {
			// Print first 10 and last 10 if more than 20
			fmt.Printf("  First 10 blocks:\n")
			for i := 0; i < 10; i++ {
				info := metrics.blockCommitmentInfo[i]
				fmt.Printf("    Block %d: %d commitments\n", info.BlockNumber, info.CommitmentCount)
			}
			fmt.Printf("  ...\n")
			fmt.Printf("  Last 10 blocks:\n")
			for i := len(metrics.blockCommitmentInfo) - 10; i < len(metrics.blockCommitmentInfo); i++ {
				info := metrics.blockCommitmentInfo[i]
				fmt.Printf("    Block %d: %d commitments\n", info.BlockNumber, info.CommitmentCount)
			}
		}

		// Check for gaps in block numbers
		var gaps []string
		for i := 1; i < len(metrics.blockCommitmentInfo); i++ {
			expected := metrics.blockCommitmentInfo[i-1].BlockNumber + 1
			actual := metrics.blockCommitmentInfo[i].BlockNumber
			if actual != expected {
				gaps = append(gaps, fmt.Sprintf("Gap between blocks %d and %d (missing %d blocks)",
					metrics.blockCommitmentInfo[i-1].BlockNumber, actual, actual-expected))
			}
		}
		if len(gaps) > 0 {
			fmt.Printf("\nBlock gaps detected (possibly due to repeat UCs):\n")
			for _, gap := range gaps {
				fmt.Printf("  %s\n", gap)
			}
		}
	}

	fmt.Printf("========================================\n")
}
