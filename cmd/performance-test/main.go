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
	"os"
	"strconv"
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

type GetBlockRecordsRequest struct {
	BlockNumber string `json:"blockNumber"`
}

type GetBlockRecordsResponse struct {
	AggregatorRecords []AggregatorRecord `json:"aggregatorRecords"`
}

type AggregatorRecord struct {
	StateID     string `json:"stateId"`
	BlockNumber string `json:"blockNumber"`
}

type GetBlockHeightResponse struct {
	BlockNumber string `json:"blockNumber"`
}

type GetBlockRequest struct {
	BlockNumber interface{} `json:"blockNumber"` // Can be number, string, or "latest"
}

type GetBlockResponse struct {
	Block BlockInfo `json:"block"`
}

type BlockInfo struct {
	Index     string `json:"index"`
	CreatedAt string `json:"createdAt"` // Unix timestamp in milliseconds as string
}

type GetInclusionProofRequest struct {
	StateID string `json:"stateId"`
}

type GetInclusionProofResponse struct {
	InclusionProof *InclusionProof `json:"inclusionProof"`
}

type InclusionProof struct {
	CertificationData  *api.CertificationData `json:"certificationData"`
	MerkleTreePath     *api.MerkleTreePath    `json:"merkleTreePath"`
	UnicityCertificate string                 `json:"unicityCertificate"`
}

// Test configuration
const (
	defaultAggregatorURL = "http://localhost:3000"
	testDuration         = 10 * time.Second
	workerCount          = 10   // Number of concurrent workers
	requestsPerSec       = 1000 // Target requests per second
)

// BlockCommitmentInfo stores block number, certification request count, and timestamp
type BlockCommitmentInfo struct {
	BlockNumber     int64
	CommitmentCount int
	Timestamp       time.Time
}

// Metrics
type Metrics struct {
	totalRequests         int64
	successfulRequests    int64
	failedRequests        int64
	stateIdExistsErr      int64
	startTime             time.Time
	blockCommitmentCounts []int
	blockCommitmentInfo   []BlockCommitmentInfo // Track block numbers with counts
	totalBlockCommitments int64                 // Track total commitments processed in blocks
	startingBlockNumber   int64                 // Store starting block number
	submittedStateIDs     sync.Map              // Thread-safe map to track which state IDs we submitted
	mutex                 sync.RWMutex          // For protecting blockCommitmentCounts slice

	// Proof verification metrics
	proofAttempts       int64           // Total attempts to get proof
	proofSuccess        int64           // Successfully retrieved proofs
	proofFailed         int64           // Failed to get proof (after retries)
	proofVerified       int64           // Successfully verified proofs
	proofVerifyFailed   int64           // Failed proof verification
	proofLatencies      []time.Duration // Track latencies for successful proofs
	proofLatenciesMutex sync.RWMutex    // Protect latencies slice
}

func (m *Metrics) addBlockCommitmentCount(blockNumber int64, count int, timestamp time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Track all blocks including empty ones to show the real pattern
	m.blockCommitmentCounts = append(m.blockCommitmentCounts, count)
	m.blockCommitmentInfo = append(m.blockCommitmentInfo, BlockCommitmentInfo{
		BlockNumber:     blockNumber,
		CommitmentCount: count,
		Timestamp:       timestamp,
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

func (m *Metrics) addProofLatency(latency time.Duration) {
	m.proofLatenciesMutex.Lock()
	defer m.proofLatenciesMutex.Unlock()
	m.proofLatencies = append(m.proofLatencies, latency)
}

func (m *Metrics) getAverageProofLatency() time.Duration {
	m.proofLatenciesMutex.RLock()
	defer m.proofLatenciesMutex.RUnlock()

	if len(m.proofLatencies) == 0 {
		return 0
	}

	var total time.Duration
	for _, latency := range m.proofLatencies {
		total += latency
	}
	return total / time.Duration(len(m.proofLatencies))
}

// HTTP client for JSON-RPC calls
type JSONRPCClient struct {
	httpClient *http.Client
	url        string
	authHeader string
	stateID    int64
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

func (c *JSONRPCClient) call(method string, params interface{}) (*JSONRPCResponse, error) {
	id := atomic.AddInt64(&c.stateID, 1)

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

// Helper function to get block timestamp
func getBlockTimestamp(client *JSONRPCClient, blockNumber int64) time.Time {
	blockReq := GetBlockRequest{
		BlockNumber: fmt.Sprintf("%d", blockNumber),
	}
	blockResp, blockErr := client.call("get_block", blockReq)
	if blockErr == nil && blockResp.Error == nil {
		var blockInfo GetBlockResponse
		blockInfoBytes, _ := json.Marshal(blockResp.Result)
		if err := json.Unmarshal(blockInfoBytes, &blockInfo); err == nil {
			// Parse timestamp (milliseconds as string)
			if millis, err := strconv.ParseInt(blockInfo.Block.CreatedAt, 10, 64); err == nil {
				return time.UnixMilli(millis)
			}
		}
	}
	return time.Now() // Fallback
}

// Generate random hex string
func generateRandomHex(length int) string {
	bytes := make([]byte, length/2)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// Generate a cryptographically valid certification request request
func generateCommitmentRequest() *api.CertificationRequest {
	// Generate a real secp256k1 key pair
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	// Generate random state data and create DataHash imprint
	stateData := make([]byte, 32)
	rand.Read(stateData)
	sourceStateHashImprint := signing.CreateDataHashImprint(stateData)

	// Create StateID deterministically
	stateID, err := api.CreateStateID(sourceStateHashImprint, publicKeyBytes)
	if err != nil {
		panic(fmt.Sprintf("Failed to create state ID: %v", err))
	}

	// Generate random transaction data and create DataHash imprint
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	signingService := signing.NewSigningService()
	certData := &api.CertificationData{
		PublicKey:       publicKeyBytes,
		SourceStateHash: sourceStateHashImprint,
		TransactionHash: transactionHashImprint,
	}
	if err = signingService.SignCertData(certData, privateKey.Serialize()); err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	// Create receipt flag
	receipt := false

	return &api.CertificationRequest{
		StateID:           stateID,
		CertificationData: *certData,
		Receipt:           &receipt,
	}
}

// Worker function that continuously submits commitments
func commitmentWorker(ctx context.Context, client *JSONRPCClient, metrics *Metrics, proofQueue chan string) {
	ticker := time.NewTicker(time.Second / time.Duration(requestsPerSec/workerCount))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Generate and submit certification request asynchronously
			go func() {
				req := generateCommitmentRequest()

				atomic.AddInt64(&metrics.totalRequests, 1)

				resp, err := client.call("certification_request", req)
				if err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					// Don't print network errors - too noisy
					return
				}

				if resp.Error != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if resp.Error.Message == "STATE_ID_EXISTS" {
						atomic.AddInt64(&metrics.stateIdExistsErr, 1)
						// Track this ID - it exists so it will be in blocks!
						metrics.submittedStateIDs.Store(string(req.StateID), true)
					}
					return
				}

				// Parse response
				var submitResp api.CertificationResponse
				respBytes, _ := json.Marshal(resp.Result)
				if err := json.Unmarshal(respBytes, &submitResp); err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					return
				}

				if submitResp.Status == "SUCCESS" {
					atomic.AddInt64(&metrics.successfulRequests, 1)
					// Track this state ID as submitted by us (normalized to lowercase)
					stateIDStr := strings.ToLower(string(req.StateID))
					metrics.submittedStateIDs.Store(stateIDStr, true)

					// Queue for proof verification
					select {
					case proofQueue <- stateIDStr:
					default:
						// Queue full, skip proof verification for this one
					}
				} else if submitResp.Status == "STATE_ID_EXISTS" {
					atomic.AddInt64(&metrics.stateIdExistsErr, 1)
					atomic.AddInt64(&metrics.successfulRequests, 1) // Count as successful - it will be in blocks!
					// Also track this ID - it exists so it will be in blocks! (normalized to lowercase)
					stateIDStr := strings.ToLower(string(req.StateID))
					metrics.submittedStateIDs.Store(stateIDStr, true)

					// Queue for proof verification
					select {
					case proofQueue <- stateIDStr:
					default:
						// Queue full, skip proof verification for this one
					}
				} else {
					atomic.AddInt64(&metrics.failedRequests, 1)
					// Log unexpected status
					if submitResp.Status != "" {
						fmt.Printf("Unexpected status '%s' for request %s\n", submitResp.Status, req.StateID)
					}
				}
			}()
		}
	}
}

// Worker function that continuously verifies inclusion proofs
//
// Note: Inclusion proofs require two stages to be complete:
// 1. CertificationData must be in a block and aggregator record must be stored
// 2. SMT snapshot must be committed to the main tree
//
// There's a small window between these stages where:
// - get_inclusion_proof returns an inclusion proof (has CertificationData)
// - But verification fails with PathIncluded=false (commitment not in main tree yet)
//
// The retry logic handles this by waiting for the snapshot to be committed.
// Typical latency: 1-5 seconds from submission to verified proof.
func proofVerificationWorker(ctx context.Context, client *JSONRPCClient, metrics *Metrics, proofQueue chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case stateID := <-proofQueue:
			go func(stateID string) {
				// Try to get and verify the proof with retries
				// Proof won't be available immediately - it's only available after certification request is in a block
				maxRetries := 30 // 30 retries * 500ms = 15 seconds max wait
				retryDelay := 500 * time.Millisecond
				startTime := time.Now()

				for attempt := 0; attempt < maxRetries; attempt++ {
					atomic.AddInt64(&metrics.proofAttempts, 1)

					proofReq := GetInclusionProofRequest{
						StateID: stateID,
					}

					resp, err := client.call("get_inclusion_proof", proofReq)
					if err != nil {
						// Network error, retry
						time.Sleep(retryDelay)
						continue
					}

					if resp.Error != nil {
						// Proof not available yet, retry
						time.Sleep(retryDelay)
						continue
					}

					// Parse response
					var proofResp GetInclusionProofResponse
					respBytes, _ := json.Marshal(resp.Result)
					if err := json.Unmarshal(respBytes, &proofResp); err != nil {
						atomic.AddInt64(&metrics.proofFailed, 1)
						return
					}

					if proofResp.InclusionProof == nil {
						// Proof not available yet, retry
						time.Sleep(retryDelay)
						continue
					}

					// Check if this is an inclusion proof (has TransactionHashImprint and CertificationData)
					// vs a non-inclusion proof (which doesn't have these fields)
					if proofResp.InclusionProof.CertificationData == nil || proofResp.InclusionProof.CertificationData.TransactionHash == "" {
						// This is a non-inclusion proof - certification request not in tree yet, retry
						time.Sleep(retryDelay)
						continue
					}

					// Successfully retrieved inclusion proof
					latency := time.Since(startTime)
					atomic.AddInt64(&metrics.proofSuccess, 1)
					metrics.addProofLatency(latency)

					// Verify the proof
					if proofResp.InclusionProof.MerkleTreePath != nil {
						// Use GetPath() method to properly convert StateID to big.Int
						stateIDPath, err := api.StateID(stateID).GetPath()
						if err != nil {
							atomic.AddInt64(&metrics.proofVerifyFailed, 1)
							return
						}

						// Verify the merkle path
						result, err := proofResp.InclusionProof.MerkleTreePath.Verify(stateIDPath)
						if err != nil {
							atomic.AddInt64(&metrics.proofVerifyFailed, 1)
							fmt.Printf("\n[ERROR] Proof verification error for %s: %v\n", stateID, err)
							return
						}

						if result.Result {
							atomic.AddInt64(&metrics.proofVerified, 1)
						} else {
							// If PathValid is true but PathIncluded is false, this means:
							// - The aggregator record exists (commitment is in a block)
							// - But the SMT snapshot hasn't been committed to the main tree yet
							// This is expected during the window between storing aggregator records
							// and committing the snapshot. Retry to get an updated proof.
							if result.PathValid && !result.PathIncluded && attempt < maxRetries-1 {
								time.Sleep(retryDelay)
								continue // Retry getting the proof
							}

							// Final verification failure after all retries
							atomic.AddInt64(&metrics.proofVerifyFailed, 1)
							fmt.Printf("\n[ERROR] Proof verification failed for %s after %d attempts - PathValid: %v, PathIncluded: %v\n",
								stateID, attempt+1, result.PathValid, result.PathIncluded)
						}
					} else {
						atomic.AddInt64(&metrics.proofVerifyFailed, 1)
					}

					return
				}

				// Failed after all retries
				atomic.AddInt64(&metrics.proofFailed, 1)
			}(stateID)
		}
	}
}

func main() {
	// Get URL and auth header from environment variables
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	if aggregatorURL == "" {
		aggregatorURL = defaultAggregatorURL
	}

	authHeader := os.Getenv("AUTH_HEADER")

	fmt.Printf("Starting aggregator performance test...\n")
	fmt.Printf("Target: %s\n", aggregatorURL)
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

	// Create context with timeout for certification request submission
	submitCtx, submitCancel := context.WithTimeout(context.Background(), testDuration)
	defer submitCancel()

	// Create longer context for proof verification (allows extra time for late submissions)
	proofTimeout := testDuration + 20*time.Second
	proofCtx, proofCancel := context.WithTimeout(context.Background(), proofTimeout)
	defer proofCancel()

	// Create JSON-RPC client
	client := NewJSONRPCClient(aggregatorURL, authHeader)

	// Test connectivity and get starting block number
	fmt.Printf("Testing connectivity to %s...\n", aggregatorURL)
	resp, err := client.call("get_block_height", nil)
	if err != nil {
		log.Fatalf("Failed to connect to aggregator: %v", err)
	}

	if resp.Error != nil {
		log.Fatalf("Error getting block height: %v", resp.Error.Message)
	}

	var heightResp GetBlockHeightResponse
	respBytes, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(respBytes, &heightResp); err != nil {
		log.Fatalf("Failed to parse block height: %v", err)
	}

	// Parse starting block number
	var startingBlockNumber int64
	if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
		log.Fatalf("Failed to parse starting block number: %v", err)
	}

	fmt.Printf("✓ Connected successfully\n")
	fmt.Printf("✓ Starting block number: %d\n", startingBlockNumber)
	metrics.startingBlockNumber = startingBlockNumber

	// Create proof verification queue (buffered channel)
	proofQueue := make(chan string, 10000)

	var wg sync.WaitGroup

	// Start certification request workers (use submitCtx - stops after testDuration)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commitmentWorker(submitCtx, client, metrics, proofQueue)
		}()
	}

	// Start proof verification workers (use proofCtx - runs longer)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proofVerificationWorker(proofCtx, client, metrics, proofQueue)
		}()
	}

	// Progress reporting (only during submission phase)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-submitCtx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(metrics.startTime)
				total := atomic.LoadInt64(&metrics.totalRequests)
				successful := atomic.LoadInt64(&metrics.successfulRequests)
				failed := atomic.LoadInt64(&metrics.failedRequests)
				exists := atomic.LoadInt64(&metrics.stateIdExistsErr)
				proofSuccess := atomic.LoadInt64(&metrics.proofSuccess)
				proofVerified := atomic.LoadInt64(&metrics.proofVerified)

				rps := float64(total) / elapsed.Seconds()
				fmt.Printf("[%v] Total: %d, Success: %d, Failed: %d, Exists: %d, RPS: %.1f, Proofs: %d (verified: %d)\n",
					elapsed.Truncate(time.Second), total, successful, failed, exists, rps, proofSuccess, proofVerified)
			}
		}
	}()

	// Wait for all workers to complete (both submission and proof verification)
	// Note: Proof workers continue running for up to 20 seconds after submissions stop
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Submission phase completed. Waiting for proof verification to complete...\n")

	// Monitor proof verification progress
	lastProofCount := int64(0)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-proofCtx.Done():
				return
			case <-ticker.C:
				currentProofs := atomic.LoadInt64(&metrics.proofVerified)
				if currentProofs > lastProofCount {
					successful := atomic.LoadInt64(&metrics.successfulRequests)
					fmt.Printf("  Proofs verified: %d/%d\n", currentProofs, successful)
					lastProofCount = currentProofs
				}
			}
		}
	}()

	wg.Wait()

	// Stop submission phase and get counts
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Submission completed. Now checking blocks for all commitments...\n")

	successful := atomic.LoadInt64(&metrics.successfulRequests)
	fmt.Printf("Total successful submissions: %d\n", successful)
	fmt.Printf("Starting from block %d\n", startingBlockNumber+1)

	// First, get the latest block number to know the range
	waitClient := NewJSONRPCClient(aggregatorURL, authHeader)
	var latestBlockNumber int64
	blockHeightResp, blockHeightErr := waitClient.call("get_block_height", nil)
	if blockHeightErr == nil && blockHeightResp.Error == nil {
		var heightResult GetBlockHeightResponse
		respBytes, _ := json.Marshal(blockHeightResp.Result)
		if err := json.Unmarshal(respBytes, &heightResult); err == nil {
			fmt.Sscanf(heightResult.BlockNumber, "%d", &latestBlockNumber)
		}
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
		req := GetBlockRecordsRequest{
			BlockNumber: fmt.Sprintf("%d", currentCheckBlock),
		}

		resp, err := waitClient.call("get_block_records", req)
		if err != nil {
			// Network error, skip this block
			currentCheckBlock++
			continue
		}

		if resp.Error != nil {
			// Block doesn't exist (could be skipped round due to repeat UC)
			currentCheckBlock++
			continue
		}

		// Parse the response
		var commitsResp GetBlockRecordsResponse
		commitRespBytes, err := json.Marshal(resp.Result)
		if err != nil {
			currentCheckBlock++
			continue
		}
		if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
			currentCheckBlock++
			continue
		}

		// Count only commitments that we submitted
		ourCommitmentCount := 0
		notOurs := 0
		for _, commitment := range commitsResp.AggregatorRecords {
			// Normalize the state ID to ensure consistent format
			stateIDStr := strings.ToLower(commitment.StateID)
			if _, exists := metrics.submittedStateIDs.Load(stateIDStr); exists {
				ourCommitmentCount++
				// Mark this ID as found
				metrics.submittedStateIDs.Store(stateIDStr, "found")
			} else {
				notOurs++
			}
		}

		// Debug: if block has many commitments not from us, log it
		if notOurs > 100 {
			fmt.Printf("  [DEBUG] Block %d has %d commitments not from our test\n", currentCheckBlock, notOurs)
		}

		// Get block timestamp
		blockTimestamp := getBlockTimestamp(waitClient, currentCheckBlock)

		// Track block and update counts
		metrics.addBlockCommitmentCount(currentCheckBlock, ourCommitmentCount, blockTimestamp)
		processedCount += int64(ourCommitmentCount)

		if ourCommitmentCount > 0 {
			fmt.Printf("Block %d: %d our commitments (total in block: %d, running total: %d/%d)\n",
				currentCheckBlock, ourCommitmentCount, len(commitsResp.AggregatorRecords), processedCount, successful)
			blocksWithOurCommitments++
		} else if len(commitsResp.AggregatorRecords) > 0 {
			// Block has commitments but none are ours
			fmt.Printf("Block %d: %d commitments from other sources\n", currentCheckBlock, len(commitsResp.AggregatorRecords))
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
			blockNumber int64
			totalCount  int
			lastChecked time.Time
			retryCount  int
		}
		blocksToRetry := make(map[int64]*blockRetryInfo)

		// Helper function to check a block and handle retry logic
		checkBlock := func(blockNum int64) bool {
			commitReq := GetBlockRecordsRequest{
				BlockNumber: fmt.Sprintf("%d", blockNum),
			}

			commitResp, err := waitClient.call("get_block_records", commitReq)
			if err != nil {
				fmt.Printf("Block %d: network error: %v\n", blockNum, err)
				return false
			}

			if commitResp.Error != nil {
				// Only log real errors, not "block doesn't exist yet"
				if commitResp.Error.Code != -32602 {
					fmt.Printf("Block %d: error %d: %s\n", blockNum, commitResp.Error.Code, commitResp.Error.Message)
				}
				return false
			}

			// Parse the response
			var commitsResp GetBlockRecordsResponse
			commitRespBytes, _ := json.Marshal(commitResp.Result)
			if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
				fmt.Printf("Block %d: failed to parse response: %v\n", blockNum, err)
				return false
			}

			// Count only commitments that we submitted and haven't counted yet
			ourCommitmentCount := 0
			for _, commitment := range commitsResp.AggregatorRecords {
				// Normalize the state ID to ensure consistent format
				stateIDStr := strings.ToLower(commitment.StateID)
				if val, exists := metrics.submittedStateIDs.Load(stateIDStr); exists && val != "found" {
					ourCommitmentCount++
					// Mark this ID as found
					metrics.submittedStateIDs.Store(stateIDStr, "found")
				}
			}

			if ourCommitmentCount > 0 {
				blockTimestamp := getBlockTimestamp(waitClient, blockNum)
				metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount, blockTimestamp)
				processedCount += int64(ourCommitmentCount)
				fmt.Printf("Block %d: %d our commitments (total in block: %d, running total: %d/%d)\n",
					blockNum, ourCommitmentCount, len(commitsResp.AggregatorRecords), processedCount, successful)
				lastProgressTime = time.Now()
				// Remove from retry list if it was there
				delete(blocksToRetry, blockNum)
				return true
			} else if len(commitsResp.AggregatorRecords) > 0 {
				// Block has commitments but none are ours - might be race condition
				// Track for retry in case aggregator records are still being written
				if _, exists := blocksToRetry[blockNum]; !exists {
					blocksToRetry[blockNum] = &blockRetryInfo{
						blockNumber: blockNum,
						totalCount:  len(commitsResp.AggregatorRecords),
						lastChecked: time.Now(),
						retryCount:  0,
					}
					fmt.Printf("Block %d: 0 our commitments yet (will retry, total: %d)\n",
						blockNum, len(commitsResp.AggregatorRecords))
				}
			} else {
				// Block exists but has 0 commitments total
				// Don't print anything for empty blocks to reduce noise
			}
			return false
		}

		for processedCount < successful && time.Now().Before(timeoutTime) {
			// Get current block height
			heightResp, err := waitClient.call("get_block_height", nil)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var heightResult GetBlockHeightResponse
			heightRespBytes, _ := json.Marshal(heightResp.Result)
			if err := json.Unmarshal(heightRespBytes, &heightResult); err != nil {
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
					commitReq := GetBlockRecordsRequest{
						BlockNumber: fmt.Sprintf("%d", blockNum),
					}

					if commitResp, err := waitClient.call("get_block_records", commitReq); err == nil && commitResp.Error == nil {
						var commitsResp GetBlockRecordsResponse
						commitRespBytes, _ := json.Marshal(commitResp.Result)
						if err := json.Unmarshal(commitRespBytes, &commitsResp); err == nil {
							ourCommitmentCount := 0
							for _, commitment := range commitsResp.AggregatorRecords {
								// Normalize the state ID to ensure consistent format
								stateIDStr := strings.ToLower(commitment.StateID)
								if val, exists := metrics.submittedStateIDs.Load(stateIDStr); exists && val != "found" {
									ourCommitmentCount++
									metrics.submittedStateIDs.Store(stateIDStr, "found")
								}
							}

							if ourCommitmentCount > 0 {
								// Found some! Remove from retry list
								blockTimestamp := getBlockTimestamp(waitClient, blockNum)
								metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount, blockTimestamp)
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
										blockNum, info.retryCount, info.totalCount)
								}
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
					commitReq := GetBlockRecordsRequest{
						BlockNumber: fmt.Sprintf("%d", blockNum),
					}

					if commitResp, err := waitClient.call("get_block_records", commitReq); err == nil && commitResp.Error == nil {
						var commitsResp GetBlockRecordsResponse
						commitRespBytes, _ := json.Marshal(commitResp.Result)
						if err := json.Unmarshal(commitRespBytes, &commitsResp); err == nil {
							ourCommitmentCount := 0
							for _, commitment := range commitsResp.AggregatorRecords {
								stateIDStr := strings.ToLower(commitment.StateID)
								if val, exists := metrics.submittedStateIDs.Load(stateIDStr); exists && val != "found" {
									ourCommitmentCount++
									metrics.submittedStateIDs.Store(stateIDStr, "found")
								}
							}

							if ourCommitmentCount > 0 {
								blockTimestamp := getBlockTimestamp(waitClient, blockNum)
								metrics.addBlockCommitmentCount(blockNum, ourCommitmentCount, blockTimestamp)
								processedCount += int64(ourCommitmentCount)
								foundInRound += ourCommitmentCount
								fmt.Printf("  Block %d (final retry): FOUND %d commitments! (running total: %d/%d)\n",
									blockNum, ourCommitmentCount, processedCount, successful)
								delete(blocksToRetry, blockNum)
							}
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

	metrics.submittedStateIDs.Range(func(key, value interface{}) bool {
		trackedCount++
		stateID := key.(string)
		// Check if this ID was found in blocks
		if value == "found" {
			foundCount++
		} else if len(sampleMissingIDs) < 5 {
			// This is a missing ID
			sampleMissingIDs = append(sampleMissingIDs, stateID)
		}
		return true
	})

	fmt.Printf("\nDebug: Tracked %d state IDs, found in blocks: %d, missing: %d\n", trackedCount, foundCount, trackedCount-foundCount)
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
	exists := atomic.LoadInt64(&metrics.stateIdExistsErr)
	processedInBlocks := atomic.LoadInt64(&metrics.totalBlockCommitments)

	fmt.Printf("\n\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("STATE_ID_EXISTS: %d\n", exists)
	fmt.Printf("Average RPS: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("Success rate: %.2f%%\n", float64(successful)/float64(total)*100)

	fmt.Printf("\nBLOCK PROCESSING:\n")
	fmt.Printf("Total commitments in blocks: %d\n", processedInBlocks)
	pendingCommitments := successful - processedInBlocks
	fmt.Printf("AggregatorRecords pending: %d\n", pendingCommitments)

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

	// Calculate average block finalization time using actual timestamps
	if len(metrics.blockCommitmentInfo) > 1 {
		firstBlockInfo := metrics.blockCommitmentInfo[0]
		lastBlockInfo := metrics.blockCommitmentInfo[len(metrics.blockCommitmentInfo)-1]

		// Calculate actual time span from block timestamps
		if !firstBlockInfo.Timestamp.IsZero() && !lastBlockInfo.Timestamp.IsZero() {
			actualTimeSpan := lastBlockInfo.Timestamp.Sub(firstBlockInfo.Timestamp)
			blockNumberSpan := lastBlockInfo.BlockNumber - firstBlockInfo.BlockNumber

			if actualTimeSpan > 0 && blockNumberSpan > 0 {
				// Average time per block
				avgBlockTime := actualTimeSpan.Seconds() / float64(blockNumberSpan)

				fmt.Printf("Block range: blocks %d to %d (%d blocks) over %.3f seconds (measured from timestamps)\n",
					firstBlockInfo.BlockNumber, lastBlockInfo.BlockNumber, len(metrics.blockCommitmentInfo), actualTimeSpan.Seconds())
				fmt.Printf("Average block finalization time: %.3f seconds/block\n", avgBlockTime)
				fmt.Printf("Effective certification request throughput: %.1f commitments/sec\n",
					float64(processedInBlocks)/actualTimeSpan.Seconds())
			}
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

	// Proof verification metrics
	proofAttempts := atomic.LoadInt64(&metrics.proofAttempts)
	proofSuccess := atomic.LoadInt64(&metrics.proofSuccess)
	proofFailed := atomic.LoadInt64(&metrics.proofFailed)
	proofVerified := atomic.LoadInt64(&metrics.proofVerified)
	proofVerifyFailed := atomic.LoadInt64(&metrics.proofVerifyFailed)

	fmt.Printf("\nINCLUSION PROOF VERIFICATION:\n")
	fmt.Printf("Total proof attempts: %d\n", proofAttempts)
	fmt.Printf("Proofs retrieved: %d\n", proofSuccess)
	fmt.Printf("Proofs failed to retrieve: %d\n", proofFailed)
	fmt.Printf("Proofs verified successfully: %d\n", proofVerified)
	fmt.Printf("Proofs failed verification: %d\n", proofVerifyFailed)

	if proofSuccess > 0 {
		avgLatency := metrics.getAverageProofLatency()
		fmt.Printf("Average proof retrieval latency: %v\n", avgLatency.Truncate(time.Millisecond))
	}

	// Calculate verification rate based on successful submissions, not retrieved proofs
	// (proofs may be retrieved multiple times due to retries)
	successful = atomic.LoadInt64(&metrics.successfulRequests)
	if successful > 0 {
		verificationRate := float64(proofVerified) / float64(successful) * 100
		fmt.Printf("Proof verification rate: %.2f%% (%d/%d submissions)\n", verificationRate, proofVerified, successful)
	}

	if proofVerified == successful {
		fmt.Printf("\n✅ SUCCESS: All %d commitments have verified inclusion proofs!\n", proofVerified)
	} else if proofVerified > 0 {
		fmt.Printf("\n⚠️  Verified %d/%d proofs (%.1f%%)\n", proofVerified, successful, float64(proofVerified)/float64(successful)*100)
	}

	fmt.Printf("========================================\n")
}
