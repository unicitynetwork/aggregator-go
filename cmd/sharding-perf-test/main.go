package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Test configuration
const (
	testDuration   = 30 * time.Second
	workerCount    = 30   // Number of concurrent workers
	requestsPerSec = 5000 // Target requests per second
)

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
func commitmentWorker(ctx context.Context, clients []*JSONRPCClient, metrics *Metrics) {
	var wg sync.WaitGroup
	ticker := time.NewTicker(time.Second / time.Duration(requestsPerSec/workerCount))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait() // Wait for in-flight requests to complete
			return
		case <-ticker.C:
			wg.Add(1)
			// Generate and submit commitment asynchronously
			go func() {
				defer wg.Done()
				req := generateCommitmentRequest()

				// choose correct client based on generated request ID
				reqBytes, _ := req.RequestID.Bytes()
				lsb := reqBytes[len(reqBytes)-1]
				var client *JSONRPCClient
				if lsb&1 == 0 {
					client = clients[1]
				} else {
					client = clients[0]
				}

				shardM := metrics.shardMetrics[client.url]
				atomic.AddInt64(&shardM.totalRequests, 1)

				resp, err := client.call("submit_commitment", req)
				if err != nil {
					atomic.AddInt64(&shardM.failedRequests, 1)
					// Don't print network errors - too noisy
					return
				}

				requestIDStr := strings.ToLower(string(req.RequestID))
				if resp.Error != nil {
					atomic.AddInt64(&shardM.failedRequests, 1)
					if resp.Error.Message == "REQUEST_ID_EXISTS" {
						atomic.AddInt64(&shardM.requestIdExistsErr, 1)
						// Track this ID - it exists so it will be in blocks!
						metrics.submittedRequestIDs.Store(requestIDStr, &requestInfo{URL: client.url, Found: 0})
					}
					return
				}

				// Parse response
				var submitResp api.SubmitCommitmentResponse
				respBytes, _ := json.Marshal(resp.Result)
				if err := json.Unmarshal(respBytes, &submitResp); err != nil {
					atomic.AddInt64(&shardM.failedRequests, 1)
					return
				}

				if submitResp.Status == "SUCCESS" {
					atomic.AddInt64(&shardM.successfulRequests, 1)
					// Track this request ID as submitted by us (normalized to lowercase)
					metrics.submittedRequestIDs.Store(requestIDStr, &requestInfo{URL: client.url, Found: 0})
				} else if submitResp.Status == "REQUEST_ID_EXISTS" {
					atomic.AddInt64(&shardM.requestIdExistsErr, 1)
					atomic.AddInt64(&shardM.successfulRequests, 1) // Count as successful - it will be in blocks!
					// Also track this ID - it exists so it will be in blocks! (normalized to lowercase)
					metrics.submittedRequestIDs.Store(requestIDStr, &requestInfo{URL: client.url, Found: 0})
				} else {
					atomic.AddInt64(&shardM.failedRequests, 1)
					// Log unexpected status
					if submitResp.Status != "" {
						fmt.Printf("Unexpected status '%s' for request %s\n", submitResp.Status, req.RequestID)
					}
				}
			}()
		}
	}
}

func main() {
	// Get URL and auth header from environment variables
	authHeader := os.Getenv("AUTH_HEADER")

	fmt.Printf("Starting sharded aggregator performance test...\n")
	fmt.Printf("Duration: %v\n", testDuration)
	fmt.Printf("Workers: %d\n", workerCount)
	fmt.Printf("Target RPS: %d\n", requestsPerSec)
	fmt.Printf("----------------------------------------\n")

	// Initialize metrics
	metrics := &Metrics{
		startTime:            time.Now(),
		startingBlockNumbers: make(map[string]int64),
		shardMetrics:         make(map[string]*ShardMetrics),
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	// Create JSON-RPC clients
	s1Client := NewJSONRPCClient("http://localhost:3001", authHeader)
	s2Client := NewJSONRPCClient("http://localhost:3002", authHeader)
	clients := []*JSONRPCClient{s1Client, s2Client}

	// Test connectivity and get starting block number for both shards
	for _, client := range clients {
		metrics.shardMetrics[client.url] = &ShardMetrics{}
		fmt.Printf("Testing connectivity to %s...\n", client.url)
		resp, err := client.call("get_block_height", nil)
		if err != nil {
			log.Fatalf("Failed to connect to aggregator at %s: %v", client.url, err)
		}

		if resp.Error != nil {
			log.Fatalf("Error getting block height from %s: %v", client.url, resp.Error.Message)
		}

		var heightResp GetBlockHeightResponse
		respBytes, _ := json.Marshal(resp.Result)
		if err := json.Unmarshal(respBytes, &heightResp); err != nil {
			log.Fatalf("Failed to parse block height from %s: %v", client.url, err)
		}

		var startingBlockNumber int64
		if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
			log.Fatalf("Failed to parse starting block number from %s: %v", client.url, err)
		}

		fmt.Printf("✓ Connected successfully to %s\n", client.url)
		fmt.Printf("✓ Starting block number for %s: %d\n", client.url, startingBlockNumber)
		metrics.startingBlockNumbers[client.url] = startingBlockNumber
	}

	var wg sync.WaitGroup

	// Start commitment workers
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			commitmentWorker(ctx, clients, metrics)
		}()
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
				var total, successful, failed, exists int64
				for _, sm := range metrics.shardMetrics {
					total += atomic.LoadInt64(&sm.totalRequests)
					successful += atomic.LoadInt64(&sm.successfulRequests)
					failed += atomic.LoadInt64(&sm.failedRequests)
					exists += atomic.LoadInt64(&sm.requestIdExistsErr)
				}

				rps := float64(total) / elapsed.Seconds()
				fmt.Printf("[%v] Total: %d, Success: %d, Failed: %d, Exists: %d, RPS: %.1f\n", elapsed.Truncate(time.Second), total, successful, failed, exists, rps)
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

	var totalSuccessful int64
	for _, sm := range metrics.shardMetrics {
		totalSuccessful += atomic.LoadInt64(&sm.successfulRequests)
	}
	fmt.Printf("Total successful submissions: %d\n", totalSuccessful)

	for _, waitClient := range clients {
		shardM := metrics.shardMetrics[waitClient.url]
		shardSuccessful := atomic.LoadInt64(&shardM.successfulRequests)
		startingBlockNumber := metrics.startingBlockNumbers[waitClient.url]
		fmt.Printf("\n--- Checking shard %s ---\n", waitClient.url)
		fmt.Printf("Starting from block %d\n", startingBlockNumber+1)

		var latestBlockNumber int64
		var blockHeightResp *JSONRPCResponse
		var blockHeightErr error

		for i := 0; i < 5; i++ { // Retry up to 5 times
			blockHeightResp, blockHeightErr = waitClient.call("get_block_height", nil)
			if blockHeightErr == nil && blockHeightResp.Error == nil {
				break // Success
			}
			fmt.Printf("Retrying get_block_height for %s... (%d/5)\n", waitClient.url, i+1)
			time.Sleep(1 * time.Second)
		}

		if blockHeightErr == nil && blockHeightResp.Error == nil {
			var heightResult GetBlockHeightResponse
			respBytes, _ := json.Marshal(blockHeightResp.Result)
			if err := json.Unmarshal(respBytes, &heightResult); err == nil {
				fmt.Sscanf(heightResult.BlockNumber, "%d", &latestBlockNumber)
			}
		} else {
			log.Printf("Could not get block height for %s after retries. Proceeding with latestBlock=0.", waitClient.url)
		}

		fmt.Printf("Latest block: %d\n", latestBlockNumber)

		currentCheckBlock := startingBlockNumber + 1

		safeBlockNumber := latestBlockNumber - 1
		if safeBlockNumber < currentCheckBlock {
			fmt.Printf("\nWaiting for more blocks to be created...\n")
			safeBlockNumber = currentCheckBlock
		}

		fmt.Printf("\nChecking blocks %d to %d...\n", currentCheckBlock, safeBlockNumber)

		for currentCheckBlock <= safeBlockNumber && atomic.LoadInt64(&shardM.totalBlockCommitments) < shardSuccessful {
			commitReq := GetBlockCommitmentsRequest{
				BlockNumber: fmt.Sprintf("%d", currentCheckBlock),
			}

			commitResp, err := waitClient.call("get_block_commitments", commitReq)
			if err != nil {
				currentCheckBlock++
				continue
			}

			if commitResp.Error != nil {
				currentCheckBlock++
				continue
			}

			var commitsResp GetBlockCommitmentsResponse
			commitRespBytes, err := json.Marshal(commitResp.Result)
			if err != nil {
				currentCheckBlock++
				continue
			}
			if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
				currentCheckBlock++
				continue
			}

			ourCommitmentCount := 0
			notOurs := 0
			for _, commitment := range commitsResp.Commitments {
				requestIDStr := strings.ToLower(commitment.RequestID)
				if val, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists {
					info := val.(*requestInfo)
					if info.URL == waitClient.url {
						if atomic.CompareAndSwapInt32(&info.Found, 0, 1) {
							ourCommitmentCount++
						}
					}
				} else {
					notOurs++
				}
			}

			if notOurs > 100 {
				fmt.Printf("  [DEBUG] Block %d has %d commitments not from our test\n", currentCheckBlock, notOurs)
			}

			shardM.addBlockCommitmentCount(ourCommitmentCount)
			if ourCommitmentCount > 0 {
				atomic.AddInt64(&shardM.totalBlockCommitments, int64(ourCommitmentCount))
				fmt.Printf("Block %d: %d our commitments (total in block: %d, shard total: %d/%d)\n", currentCheckBlock, ourCommitmentCount, len(commitsResp.Commitments), atomic.LoadInt64(&shardM.totalBlockCommitments), shardSuccessful)
			} else if len(commitsResp.Commitments) > 0 {
				fmt.Printf("Block %d: %d commitments from other sources\n", currentCheckBlock, len(commitsResp.Commitments))
			}

			currentCheckBlock++
			time.Sleep(20 * time.Millisecond)
		}

		if atomic.LoadInt64(&shardM.totalBlockCommitments) < shardSuccessful {
			fmt.Printf("\nContinuing to check for remaining commitments on %s...\n", waitClient.url)
			fmt.Printf("Will check for up to 3 minutes for new blocks...\n")

			timeoutTime := time.Now().Add(3 * time.Minute)
			lastProgressTime := time.Now()
			lastReportTime := time.Now()

			type blockRetryInfo struct {
				blockNumber      int64
				totalCommitments int
				lastChecked      time.Time
				retryCount       int
			}
			blocksToRetry := make(map[int64]*blockRetryInfo)

			checkBlock := func(blockNum int64) bool {
				commitReq := GetBlockCommitmentsRequest{
					BlockNumber: fmt.Sprintf("%d", blockNum),
				}

				commitResp, err := waitClient.call("get_block_commitments", commitReq)
				if err != nil {
					fmt.Printf("Block %d: network error: %v\n", blockNum, err)
					return false
				}

				if commitResp.Error != nil {
					if commitResp.Error.Code != -32602 {
						fmt.Printf("Block %d: error %d: %s\n", blockNum, commitResp.Error.Code, commitResp.Error.Message)
					}
					return false
				}

				var commitsResp GetBlockCommitmentsResponse
				commitRespBytes, _ := json.Marshal(commitResp.Result)
				if err := json.Unmarshal(commitRespBytes, &commitsResp); err != nil {
					fmt.Printf("Block %d: failed to parse response: %v\n", blockNum, err)
					return false
				}

				ourCommitmentCount := 0
				for _, commitment := range commitsResp.Commitments {
					requestIDStr := strings.ToLower(commitment.RequestID)
					if val, exists := metrics.submittedRequestIDs.Load(requestIDStr); exists {
						info := val.(*requestInfo)
						if info.URL == waitClient.url {
							if atomic.CompareAndSwapInt32(&info.Found, 0, 1) {
								ourCommitmentCount++
							}
						}
					}
				}

				if ourCommitmentCount > 0 {
					shardM.addBlockCommitmentCount(ourCommitmentCount)
					atomic.AddInt64(&shardM.totalBlockCommitments, int64(ourCommitmentCount))
					fmt.Printf("Block %d: %d our commitments (total in block: %d, shard total: %d/%d)\n", blockNum, ourCommitmentCount, len(commitsResp.Commitments), atomic.LoadInt64(&shardM.totalBlockCommitments), shardSuccessful)
					lastProgressTime = time.Now()
					delete(blocksToRetry, blockNum)
					return true
				} else if len(commitsResp.Commitments) > 0 {
					if _, exists := blocksToRetry[blockNum]; !exists {
						blocksToRetry[blockNum] = &blockRetryInfo{
							blockNumber:      blockNum,
							totalCommitments: len(commitsResp.Commitments),
							lastChecked:      time.Now(),
							retryCount:       0,
						}
						fmt.Printf("Block %d: 0 our commitments yet (will retry, total: %d)\n", blockNum, len(commitsResp.Commitments))
					}
				}
				return false
			}

			for atomic.LoadInt64(&shardM.totalBlockCommitments) < shardSuccessful && time.Now().Before(timeoutTime) {
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

				safeLatestBlock := latestBlock - 1
				for currentCheckBlock <= safeLatestBlock && atomic.LoadInt64(&shardM.totalBlockCommitments) < shardSuccessful {
					checkBlock(currentCheckBlock)
					currentCheckBlock++
				}

				for blockNum, info := range blocksToRetry {
					if time.Since(info.lastChecked) > 500*time.Millisecond {
						checkBlock(blockNum)
						info.lastChecked = time.Now()
						info.retryCount++
					}
				}

				if time.Since(lastReportTime) > 5*time.Second {
					fmt.Printf("Still checking %s... found %d/%d commitments, %d blocks pending retry...\n", waitClient.url, atomic.LoadInt64(&shardM.totalBlockCommitments), shardSuccessful, len(blocksToRetry))
					lastReportTime = time.Now()
				}

				if time.Since(lastProgressTime) > 90*time.Second && len(blocksToRetry) == 0 {
					fmt.Printf("\nNo new commitments found for 90 seconds on %s, stopping...\n", waitClient.url)
					break
				}

				time.Sleep(200 * time.Millisecond)
			}
		}
	}

	// Debug: count tracked IDs and find missing ones
	trackedCount := 0
	foundCount := 0
	var sampleMissingIDs []string

	metrics.submittedRequestIDs.Range(func(key, value interface{}) bool {
		trackedCount++
		requestID := key.(string)
		info := value.(*requestInfo)
		if info.Found == 1 {
			foundCount++
		} else if len(sampleMissingIDs) < 5 {
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

	if foundCount < trackedCount {
		fmt.Printf("\nFinished checking. Found %d/%d commitments\n", foundCount, trackedCount)
	} else {
		fmt.Printf("\nAll %d commitments have been found in blocks!\n", trackedCount)
	}
	// Final metrics
	elapsed := time.Since(metrics.startTime)

	fmt.Printf("\n\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Duration: %v\n", elapsed.Truncate(time.Millisecond))

	var total, successful, failed, exists, processedInBlocks int64

	// Per-shard results
	for url, shardM := range metrics.shardMetrics {
		fmt.Printf("\n--- SHARD: %s ---\n", url)

		shardTotal := atomic.LoadInt64(&shardM.totalRequests)
		shardSuccessful := atomic.LoadInt64(&shardM.successfulRequests)
		shardFailed := atomic.LoadInt64(&shardM.failedRequests)
		shardExists := atomic.LoadInt64(&shardM.requestIdExistsErr)
		shardProcessedInBlocks := atomic.LoadInt64(&shardM.totalBlockCommitments)

		total += shardTotal
		successful += shardSuccessful
		failed += shardFailed
		exists += shardExists
		processedInBlocks += shardProcessedInBlocks

		fmt.Printf("Total requests: %d\n", shardTotal)
		fmt.Printf("Successful requests: %d\n", shardSuccessful)
		fmt.Printf("Failed requests: %d\n", shardFailed)
		fmt.Printf("REQUEST_ID_EXISTS: %d\n", shardExists)
		if elapsed.Seconds() > 0 {
			fmt.Printf("Average RPS: %.2f\n", float64(shardTotal)/elapsed.Seconds())
		}
		if shardTotal > 0 {
			fmt.Printf("Success rate: %.2f%%\n", float64(shardSuccessful)/float64(shardTotal)*100)
		}

		fmt.Printf("\nBLOCK PROCESSING:\n")
		fmt.Printf("Total commitments in blocks: %d\n", shardProcessedInBlocks)

		pendingCommitments := shardSuccessful - shardProcessedInBlocks
		if pendingCommitments > 0 {
			if shardSuccessful > 0 {
				percentage := float64(pendingCommitments) / float64(shardSuccessful) * 100
				fmt.Printf("\n⚠️  WARNING: %d commitments (%.1f%%) not found in blocks!\n", pendingCommitments, percentage)
			}
		} else if shardSuccessful > 0 {
			fmt.Printf("\n✅ SUCCESS: All %d commitments were found in blocks!\n", shardSuccessful)
		}

		fmt.Printf("\nBLOCK THROUGHPUT:\n")
		shardM.mutex.RLock()
		fmt.Printf("Total blocks checked: %d\n", len(shardM.blockCommitmentCounts))

		emptyBlocks := 0
		nonEmptyBlocks := 0
		for _, count := range shardM.blockCommitmentCounts {
			if count == 0 {
				emptyBlocks++
			} else {
				nonEmptyBlocks++
			}
		}
		shardM.mutex.RUnlock()

		if nonEmptyBlocks > 0 {
			fmt.Printf("Non-empty blocks: %d (average %.1f commitments/block)\n", nonEmptyBlocks, float64(shardProcessedInBlocks)/float64(nonEmptyBlocks))
		}
		if emptyBlocks > 0 {
			fmt.Printf("Empty blocks: %d\n", emptyBlocks)
		}
	}

	// Aggregate results
	fmt.Printf("\n\n--- AGGREGATE RESULTS ---\n")
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("REQUEST_ID_EXISTS: %d\n", exists)
	if elapsed.Seconds() > 0 {
		fmt.Printf("Average RPS: %.2f\n", float64(total)/elapsed.Seconds())
	}
	if total > 0 {
		fmt.Printf("Success rate: %.2f%%\n", float64(successful)/float64(total)*100)
	}

	fmt.Printf("\nBLOCK PROCESSING:\n")
	fmt.Printf("Total commitments in blocks: %d\n", processedInBlocks)

	pendingCommitments := successful - processedInBlocks
	if pendingCommitments > 0 {
		if successful > 0 {
			percentage := float64(pendingCommitments) / float64(successful) * 100
			fmt.Printf("\n⚠️  WARNING: %d commitments (%.1f%%) not found in blocks!\n", pendingCommitments, percentage)
		}
	} else if successful > 0 {
		fmt.Printf("\n✅ SUCCESS: All %d commitments were found in blocks!\n", successful)
	}

	fmt.Printf("========================================\n")
}
