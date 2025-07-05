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
	aggregatorURL  = "http://localhost:3000"
	testDuration   = 10 * time.Second
	workerCount    = 100  // Number of concurrent workers (increased)
	requestsPerSec = 1000 // Target requests per second (increased)
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
	totalBlockCommitments int64 // Track total commitments processed in blocks
	startingBlockNumber   int64 // Store starting block number
	mutex                 sync.RWMutex
}

func (m *Metrics) addBlockCommitmentCount(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// Track all blocks including empty ones to show the real pattern
	m.blockCommitmentCounts = append(m.blockCommitmentCounts, count)
	atomic.AddInt64(&m.totalBlockCommitments, int64(count))
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
	requestID, err := api.CreateRequestID(publicKeyBytes, signing.CreateDataHashImprint(stateData))
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
			var submitResp api.SubmitCommitmentResponse
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

	// Stop submission phase and get counts
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Submission completed. Now checking blocks for all commitments...\n")
	
	successful := atomic.LoadInt64(&metrics.successfulRequests)
	fmt.Printf("Total successful submissions: %d\n", successful)
	fmt.Printf("Starting from block %d\n", startingBlockNumber + 1)
	
	// Continue monitoring blocks until all successful commitments are processed
	// Add a timeout to prevent infinite waiting (5 minutes should be enough for most tests)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer waitCancel()
	
	waitClient := NewJSONRPCClient(aggregatorURL)
	processedCount := int64(0)
	lastCheckTime := time.Now()
	currentCheckBlock := startingBlockNumber + 1
	emptyBlocksFound := 0
	lastEmptyBlockCheck := int64(0)
	var emptyBlockCheckStart time.Time
	
	for processedCount < successful {
		select {
		case <-waitCtx.Done():
			fmt.Printf("\nTimeout waiting for commitments to be processed\n")
			goto finalize
		default:
			// Get current block height
			resp, err := waitClient.call("get_block_height", nil)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			
			if resp.Error != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			
			var heightResp GetBlockHeightResponse
			respBytes, _ := json.Marshal(resp.Result)
			if err := json.Unmarshal(respBytes, &heightResp); err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			
			// Parse block number
			var currentBlockNumber int64
			if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &currentBlockNumber); err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			
			// Check all blocks from currentCheckBlock to currentBlockNumber
			newCommitmentsFound := false
			if currentCheckBlock <= currentBlockNumber {
				fmt.Printf("\nChecking blocks %d to %d\n", currentCheckBlock, currentBlockNumber)
			}
			for blockNum := currentCheckBlock; blockNum <= currentBlockNumber; blockNum++ {
				// Get commitments for this block
				commitReq := GetBlockCommitmentsRequest{
					BlockNumber: fmt.Sprintf("%d", blockNum),
				}
				
				commitResp, err := waitClient.call("get_block_commitments", commitReq)
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
				
				// Always add the block to our stats
				metrics.addBlockCommitmentCount(commitmentCount)
				processedCount += int64(commitmentCount)
				
				if commitmentCount > 0 {
					fmt.Printf("\nBlock %d: %d commitments (total: %d/%d)", 
						blockNum, commitmentCount, processedCount, successful)
					newCommitmentsFound = true
					lastCheckTime = time.Now()
				}
				
				currentCheckBlock = blockNum + 1
			}
			
			// Check for timeout if no progress
			if !newCommitmentsFound && time.Since(lastCheckTime) > 30*time.Second {
				fmt.Printf("\nNo new commitments for 30 seconds, stopping check\n")
				goto finalize
			}
			
			// Progress indicator
			if !newCommitmentsFound {
				fmt.Printf("\rChecking blocks... %d/%d commitments found", processedCount, successful)
			}
			
			time.Sleep(100 * time.Millisecond)
		}
	}
	
	fmt.Printf("\nAll %d commitments have been processed into blocks\n", successful)
	
	// Now verify that empty blocks continue to be produced
	fmt.Printf("\nVerifying empty block production for 5 seconds...\n")
	emptyBlockCheckStart = time.Now()
	lastEmptyBlockCheck = currentCheckBlock
	emptyBlocksFound = 0
	
	for time.Since(emptyBlockCheckStart) < 5*time.Second {
		resp, err := waitClient.call("get_block_height", nil)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		if resp.Error != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		var heightResp GetBlockHeightResponse
		respBytes, _ := json.Marshal(resp.Result)
		if err := json.Unmarshal(respBytes, &heightResp); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		var currentHeight int64
		if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &currentHeight); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		// Check any new blocks
		for blockNum := lastEmptyBlockCheck; blockNum <= currentHeight; blockNum++ {
			commitReq := GetBlockCommitmentsRequest{
				BlockNumber: fmt.Sprintf("%d", blockNum),
			}
			
			commitResp, err := waitClient.call("get_block_commitments", commitReq)
			if err != nil || commitResp.Error != nil {
				continue
			}
			
			var commitsResp GetBlockCommitmentsResponse
			commitRespBytes, _ := json.Marshal(commitResp.Result)
			if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
				continue
			}
			
			commitmentCount := len(commitsResp.Commitments)
			metrics.addBlockCommitmentCount(commitmentCount)
			
			if commitmentCount == 0 {
				emptyBlocksFound++
				fmt.Printf("Empty block %d created (total empty blocks: %d)\n", blockNum, emptyBlocksFound)
			} else {
				// Unexpected: found commitments after we thought we were done
				processedCount += int64(commitmentCount)
				fmt.Printf("WARNING: Block %d has %d unexpected commitments\n", blockNum, commitmentCount)
			}
			
			lastEmptyBlockCheck = blockNum + 1
		}
		
		time.Sleep(100 * time.Millisecond)
	}
	
	if emptyBlocksFound == 0 {
		fmt.Printf("WARNING: No empty blocks created in 5 seconds - round manager may have stopped!\n")
	} else {
		avgEmptyBlockRate := float64(emptyBlocksFound) / 5.0
		fmt.Printf("Empty block creation rate: %.1f blocks/sec\n", avgEmptyBlockRate)
	}

finalize:
	// Do one final check for any blocks we might have missed
	if processedCount < successful {
		resp, err := waitClient.call("get_block_height", nil)
		if err == nil && resp.Error == nil {
			var heightResp GetBlockHeightResponse
			respBytes, _ := json.Marshal(resp.Result)
			if err := json.Unmarshal(respBytes, &heightResp); err == nil {
				var finalBlockNumber int64
				if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &finalBlockNumber); err == nil {
					if finalBlockNumber >= currentCheckBlock {
						fmt.Printf("\nDoing final check for blocks %d to %d\n", currentCheckBlock, finalBlockNumber)
						for blockNum := currentCheckBlock; blockNum <= finalBlockNumber; blockNum++ {
							commitReq := GetBlockCommitmentsRequest{
								BlockNumber: fmt.Sprintf("%d", blockNum),
							}
							
							commitResp, err := waitClient.call("get_block_commitments", commitReq)
							if err != nil || commitResp.Error != nil {
								continue
							}
							
							var commitsResp GetBlockCommitmentsResponse
							commitRespBytes, _ := json.Marshal(commitResp.Result)
							if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
								continue
							}
							
							commitmentCount := len(commitsResp.Commitments)
							if commitmentCount > 0 {
								metrics.addBlockCommitmentCount(commitmentCount)
								processedCount += int64(commitmentCount)
								fmt.Printf("Block %d: %d commitments (final total: %d/%d)\n", 
									blockNum, commitmentCount, processedCount, successful)
							}
						}
					}
				}
			}
		}
		// Update the final count
		atomic.StoreInt64(&metrics.totalBlockCommitments, processedCount)
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
	fmt.Printf("Commitments pending: %d\n", successful-processedInBlocks)
	
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

	if len(metrics.blockCommitmentCounts) > 0 && len(metrics.blockCommitmentCounts) <= 20 {
		fmt.Printf("\nBlock commitment counts: %v\n", metrics.blockCommitmentCounts)
	} else if len(metrics.blockCommitmentCounts) > 20 {
		fmt.Printf("\nFirst 10 blocks: %v\n", metrics.blockCommitmentCounts[:10])
		fmt.Printf("Last 10 blocks: %v\n", metrics.blockCommitmentCounts[len(metrics.blockCommitmentCounts)-10:])
	}

	fmt.Printf("========================================\n")
}
