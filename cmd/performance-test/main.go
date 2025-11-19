package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	defaultAggregatorURL = "http://localhost:3000"
	testDuration         = 30 * time.Second
	workerCount          = 20
	httpClientPoolSize   = 12
	requestsPerSec       = 1000
	aggregatorLogPath    = "logs/aggregator.log"
	proofMaxRetries      = 10
	proofRetryDelay      = 1000 * time.Millisecond
	proofInitialDelay    = 3500 * time.Millisecond
)

// Optional shard targets (leave empty for single-aggregator mode)
var shardTargets = []shardTarget{
	{name: "shard-3", url: "http://localhost:3001", shardMask: 3},
	{name: "shard-2", url: "http://localhost:3002", shardMask: 2},
}

func normalizeRequestID(id string) string {
	return strings.ToLower(id)
}

func buildShardClients(aggregatorURL, authHeader string, metrics *Metrics) []*ShardClient {
	if len(shardTargets) == 0 {
		return []*ShardClient{
			{
				name:   "default",
				url:    aggregatorURL,
				client: NewJSONRPCClient(aggregatorURL, authHeader, metrics),
			},
		}
	}

	clients := make([]*ShardClient, 0, len(shardTargets))
	for i, target := range shardTargets {
		trimmed := strings.TrimSpace(target.url)
		if trimmed == "" {
			continue
		}
		name := target.name
		if name == "" {
			name = fmt.Sprintf("shard-%d", i)
		}
		clients = append(clients, &ShardClient{
			name:      name,
			url:       trimmed,
			shardMask: target.shardMask,
			client:    NewJSONRPCClient(trimmed, authHeader, metrics),
		})
	}

	if len(clients) == 0 {
		return []*ShardClient{
			{
				name:   "default",
				url:    aggregatorURL,
				client: NewJSONRPCClient(aggregatorURL, authHeader, metrics),
			},
		}
	}

	return clients
}

func selectShardIndex(requestID api.RequestID, shardClients []*ShardClient) int {
	shardCount := len(shardClients)
	if shardCount <= 1 {
		return 0
	}

	reqHex := requestID.String()
	for idx, sc := range shardClients {
		if sc == nil || sc.shardMask <= 0 {
			continue
		}
		match, err := matchesShardMask(reqHex, sc.shardMask)
		if err == nil && match {
			return idx
		}
	}

	imprint, err := requestID.Imprint()
	if err != nil || len(imprint) == 0 {
		return 0
	}
	return int(imprint[len(imprint)-1]) % shardCount
}

func matchesShardMask(requestIDHex string, shardMask int) (bool, error) {
	if shardMask <= 0 {
		return false, nil
	}

	bytes, err := hex.DecodeString(requestIDHex)
	if err != nil {
		return false, fmt.Errorf("failed to decode request ID: %w", err)
	}

	requestBig := new(big.Int).SetBytes(bytes)
	maskBig := new(big.Int).SetInt64(int64(shardMask))

	msbPos := maskBig.BitLen() - 1
	if msbPos < 0 {
		return false, fmt.Errorf("invalid shard mask: %d", shardMask)
	}

	compareMask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(msbPos)), big.NewInt(1))
	expected := new(big.Int).And(maskBig, compareMask)
	requestLowBits := new(big.Int).And(requestBig, compareMask)

	return requestLowBits.Cmp(expected) == 0, nil
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
func commitmentWorker(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, proofQueue chan proofJob, commitmentPool []*api.SubmitCommitmentRequest, poolIndex *atomic.Int64, counters *RequestRateCounters) {
	requestsPerWorker := float64(requestsPerSec) / float64(workerCount)
	if requestsPerWorker <= 0 {
		requestsPerWorker = 1
	}
	interval := time.Duration(float64(time.Second) / requestsPerWorker)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get pre-generated commitment from pool (round-robin)
			go func() {
				idx := poolIndex.Add(1) % int64(len(commitmentPool))
				req := commitmentPool[idx]

				shardIdx := selectShardIndex(req.RequestID, shardClients)
				client := shardClients[shardIdx].client

				if counters != nil {
					counters.IncSubmitStarted()
				}

				atomic.AddInt64(&metrics.totalRequests, 1)
				if sm := metrics.shard(shardIdx); sm != nil {
					sm.totalRequests.Add(1)
				}

				resp, err := client.call("submit_commitment", req)
				if counters != nil {
					counters.IncSubmitCompleted()
				}
				if err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					return
				}

				if resp.Error != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.failedRequests.Add(1)
					}
					if resp.Error.Message == "REQUEST_ID_EXISTS" {
						atomic.AddInt64(&metrics.requestIdExistsErr, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.requestIdExistsErr.Add(1)
						}
						requestIDStr := normalizeRequestID(string(req.RequestID))
						metrics.submittedRequestIDs.Store(requestIDStr, true)
					}
					return
				}

				// Parse response
				var submitResp api.SubmitCommitmentResponse
				respBytes, _ := json.Marshal(resp.Result)
				if err := json.Unmarshal(respBytes, &submitResp); err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					return
				}

				switch submitResp.Status {
				case "SUCCESS", "REQUEST_ID_EXISTS":
					if submitResp.Status == "REQUEST_ID_EXISTS" {
						atomic.AddInt64(&metrics.requestIdExistsErr, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.requestIdExistsErr.Add(1)
						}
					}
					atomic.AddInt64(&metrics.successfulRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.successfulRequests.Add(1)
					}
					requestIDStr := normalizeRequestID(string(req.RequestID))
					metrics.submittedRequestIDs.Store(requestIDStr, true)

					if proofQueue != nil {
						metrics.recordSubmissionTimestamp(requestIDStr)
						select {
						case proofQueue <- proofJob{shardIdx: shardIdx, requestID: requestIDStr}:
						default:
							// Queue full, skip proof verification for this one
						}
					}
				default:
					atomic.AddInt64(&metrics.failedRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.failedRequests.Add(1)
					}
					if submitResp.Status != "" {
						fmt.Printf("Unexpected status '%s' for request %s\n", submitResp.Status, req.RequestID)
					}
				}
			}()
		}
	}
}

// Worker function that continuously verifies inclusion proofs in a sharded setup.
func proofVerificationWorker(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, proofQueue chan proofJob, cancelTest context.CancelFunc, counters *RequestRateCounters) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-proofQueue:
			go func(reqID string, shardIdx int) {
				time.Sleep(proofInitialDelay)
				startTime := time.Now()
				normalizedID := normalizeRequestID(reqID)
				client := shardClients[shardIdx].client

				for attempt := 0; attempt < proofMaxRetries; attempt++ {
					atomic.AddInt64(&metrics.proofAttempts, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.proofAttempts.Add(1)
					}
					if attempt > 0 {
						atomic.AddInt64(&metrics.proofRetries, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofRetries.Add(1)
						}
						if counters != nil {
							counters.IncProofRetries()
						}
					}

					proofReq := GetInclusionProofRequest{RequestID: reqID}

					atomic.AddInt64(&metrics.proofActiveRequests, 1)
					if counters != nil {
						counters.IncProofStarted()
					}
					requestStart := time.Now()
					resp, err := client.call("get_inclusion_proof", proofReq)
					if counters != nil {
						counters.IncProofCompleted()
					}
					requestDuration := time.Since(requestStart)
					atomic.AddInt64(&metrics.proofActiveRequests, -1)

					if err != nil {
						if attempt >= 3 {
							metrics.recordError(fmt.Sprintf("Network error getting proof after %d attempts: %v", attempt+1, err))
							atomic.AddInt64(&metrics.proofFailed, 1)
							if sm := metrics.shard(shardIdx); sm != nil {
								sm.proofFailed.Add(1)
							}
							return
						}
						time.Sleep(proofRetryDelay)
						continue
					}

					if resp.Error != nil {
						if attempt >= proofMaxRetries-1 {
							metrics.recordError(fmt.Sprintf("API error getting proof (code %d): %s", resp.Error.Code, resp.Error.Message))
							atomic.AddInt64(&metrics.proofFailed, 1)
							if sm := metrics.shard(shardIdx); sm != nil {
								sm.proofFailed.Add(1)
							}
							return
						}
						time.Sleep(proofRetryDelay)
						continue
					}

					var proofResp GetInclusionProofResponse
					respBytes, _ := json.Marshal(resp.Result)
					if err := json.Unmarshal(respBytes, &proofResp); err != nil {
						metrics.recordError(fmt.Sprintf("Failed to parse proof response: %v", err))
						atomic.AddInt64(&metrics.proofFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofFailed.Add(1)
						}
						return
					}

					if proofResp.InclusionProof == nil || proofResp.InclusionProof.TransactionHash == "" || proofResp.InclusionProof.Authenticator == nil {
						time.Sleep(proofRetryDelay)
						continue
					}

					atomic.AddInt64(&metrics.proofSuccess, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.proofSuccess.Add(1)
					}
					metrics.recordProofSuccessAttempt(attempt)
					metrics.addProofRequestDuration(requestDuration)

					submittedAt, hasSubmission := metrics.getSubmissionTimestamp(normalizedID)
					var totalLatency time.Duration
					if hasSubmission {
						totalLatency = time.Since(submittedAt)
						metrics.clearSubmissionTimestamp(normalizedID)
					} else {
						totalLatency = time.Since(startTime) + proofInitialDelay
					}
					metrics.addProofLatency(totalLatency)

					apiPath := &api.MerkleTreePath{
						Root:  proofResp.InclusionProof.MerkleTreePath.Root,
						Steps: make([]api.MerkleTreeStep, len(proofResp.InclusionProof.MerkleTreePath.Steps)),
					}
					for i, step := range proofResp.InclusionProof.MerkleTreePath.Steps {
						apiPath.Steps[i] = api.MerkleTreeStep{
							Path: step.Path,
							Data: step.Data,
						}
					}

					requestIDPath, err := api.RequestID(reqID).GetPath()
					if err != nil {
						metrics.recordError(fmt.Sprintf("Failed to get path for request ID: %v", err))
						atomic.AddInt64(&metrics.proofVerifyFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofVerifyFailed.Add(1)
						}
						return
					}

					result, err := apiPath.Verify(requestIDPath)
					if err != nil {
						metrics.recordError(fmt.Sprintf("Proof verification error: %v", err))
						atomic.AddInt64(&metrics.proofVerifyFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofVerifyFailed.Add(1)
						}
						return
					}

					if result.Result {
						atomic.AddInt64(&metrics.proofVerified, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofVerified.Add(1)
						}
					} else {
						if !result.PathIncluded && attempt < proofMaxRetries-1 {
							time.Sleep(proofRetryDelay)
							continue
						}
						fmt.Printf("\n\n[FATAL ERROR] Proof verification failed for request %s after %d attempts\n", reqID, attempt+1)
						fmt.Printf("PathValid: %v\n", result.PathValid)
						fmt.Printf("PathIncluded: %v\n", result.PathIncluded)
						fmt.Printf("Stopping test due to proof verification failure.\n\n")
						atomic.AddInt64(&metrics.proofVerifyFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofVerifyFailed.Add(1)
						}
						cancelTest()
						return
					}

					return
				}

				metrics.recordError(fmt.Sprintf("Timeout getting proof after %d attempts", proofMaxRetries))
				atomic.AddInt64(&metrics.proofFailed, 1)
				if sm := metrics.shard(shardIdx); sm != nil {
					sm.proofFailed.Add(1)
				}
			}(job.requestID, job.shardIdx)
		}
	}
}

func logClientPerfRates(ctx context.Context, metrics *Metrics, counters *RequestRateCounters, totalPlanned int64, shardClients []*ShardClient) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prevSubmitStart, prevSubmitComplete, prevProofStart, prevProofComplete, prevProofRetries int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			submitStarted := counters.submitStarted.Load()
			submitCompleted := counters.submitCompleted.Load()
			proofStarted := counters.proofStarted.Load()
			proofCompleted := counters.proofCompleted.Load()
			proofRetries := counters.proofRetries.Load()
			total := atomic.LoadInt64(&metrics.totalRequests)
			successful := atomic.LoadInt64(&metrics.successfulRequests)
			failed := atomic.LoadInt64(&metrics.failedRequests)
			elapsed := time.Since(metrics.submissionStartTime)
			if elapsed <= 0 {
				elapsed = time.Second
			}
			overallRPS := float64(total) / math.Max(elapsed.Seconds(), 0.001)

			submitStartRate := submitStarted - prevSubmitStart
			submitCompleteRate := submitCompleted - prevSubmitComplete
			proofStartRate := proofStarted - prevProofStart
			proofCompleteRate := proofCompleted - prevProofComplete
			proofRetryRate := proofRetries - prevProofRetries

			prevSubmitStart = submitStarted
			prevSubmitComplete = submitCompleted
			prevProofStart = proofStarted
			prevProofComplete = proofCompleted
			prevProofRetries = proofRetries

			progress := float64(successful)
			if totalPlanned > 0 {
				progress = progress / float64(totalPlanned) * 100
			} else {
				progress = 100
			}
			if progress > 100 {
				progress = 100
			}

			timestamp := time.Now().Format(time.RFC3339)
			successRate := float64(0)
			if total > 0 {
				successRate = float64(successful) / float64(total) * 100
			}
			verified := atomic.LoadInt64(&metrics.proofVerified)
			lag := successful - verified
			verificationPct := float64(0)
			if successful > 0 {
				verificationPct = float64(verified) / float64(successful) * 100
			}
			activeConns := metrics.currentActiveConnections()
			totalConn := metrics.totalConnectionAttempts()
			failedConn := metrics.totalFailedConnections()
			fmt.Printf("%s submitted=%d/%d (%.1f%%) fail=%d progress=%.1f%% rps=%.1f submit_started=%d/s submit_completed=%d/s proof_started=%d/s proof_completed=%d/s proof_retries=%d/s verified=%d/%d (%.1f%%) proof_lag=%d conns_active=%d conns_total=%d conns_failed=%d\n",
				timestamp, successful, total, successRate, failed, progress, overallRPS, submitStartRate, submitCompleteRate, proofStartRate, proofCompleteRate, proofRetryRate, verified, successful, verificationPct, lag, activeConns, totalConn, failedConn)

			if len(shardClients) > 1 && len(metrics.shardMetrics) > 1 {
				for idx, sc := range shardClients {
					if sm := metrics.shard(idx); sm != nil {
						shardTotal := sm.totalRequests.Load()
						shardSuccess := sm.successfulRequests.Load()
						shardFailed := sm.failedRequests.Load()
						shardProofVerified := sm.proofVerified.Load()
						shardProofSuccess := sm.proofSuccess.Load()
						successPct := 0.0
						if shardTotal > 0 {
							successPct = float64(shardSuccess) / float64(shardTotal) * 100
						}
						proofPct := 0.0
						if shardSuccess > 0 {
							proofPct = float64(shardProofVerified) / float64(shardSuccess) * 100
						}
						fmt.Printf("  - %s submitted=%d/%d (%.1f%%) fail=%d proofs=%d/%d (%.1f%%)\n",
							sc.name, shardSuccess, shardTotal, successPct, shardFailed, shardProofVerified, shardProofSuccess, proofPct)
					}
				}
			}
		}
	}
}

func printShardFinalReport(metrics *Metrics, shardClients []*ShardClient) {
	if len(shardClients) <= 1 || len(metrics.shardMetrics) <= 1 {
		return
	}

	fmt.Printf("\nPer-shard submission stats:\n")
	for idx, sc := range shardClients {
		sm := metrics.shard(idx)
		if sm == nil {
			continue
		}
		total := sm.totalRequests.Load()
		success := sm.successfulRequests.Load()
		failed := sm.failedRequests.Load()
		exists := sm.requestIdExistsErr.Load()
		successPct := 0.0
		if total > 0 {
			successPct = float64(success) / float64(total) * 100
		}
		fmt.Printf("  - %s total=%d success=%d failed=%d request_id_exists=%d success_rate=%.2f%%\n",
			sc.name, total, success, failed, exists, successPct)
	}

	fmt.Printf("\nPer-shard proof stats:\n")
	for idx, sc := range shardClients {
		sm := metrics.shard(idx)
		if sm == nil {
			continue
		}
		attempts := sm.proofAttempts.Load()
		success := sm.proofSuccess.Load()
		failed := sm.proofFailed.Load()
		verified := sm.proofVerified.Load()
		verifyFailed := sm.proofVerifyFailed.Load()
		retries := sm.proofRetries.Load()
		proofSuccessPct := 0.0
		if attempts > 0 {
			proofSuccessPct = float64(success) / float64(attempts) * 100
		}
		verificationPct := 0.0
		submissions := sm.successfulRequests.Load()
		if submissions > 0 {
			verificationPct = float64(verified) / float64(submissions) * 100
		}
		fmt.Printf("  - %s attempts=%d success=%d failed=%d retries=%d verified=%d verify_failed=%d verification_rate=%.2f%% proof_success_rate=%.2f%%\n",
			sc.name, attempts, success, failed, retries, verified, verifyFailed, verificationPct, proofSuccessPct)
	}
}

func parseAggregatorRoundLogs(path string, start, end time.Time) ([]aggregatorRoundSummary, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if end.Before(start) {
		start, end = end, start
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	var summaries []aggregatorRoundSummary
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var raw aggregatorLogRaw
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		if raw.Msg != "PERF: Round completed" {
			continue
		}

		timestamp, err := time.Parse(time.RFC3339Nano, raw.Time)
		if err != nil {
			continue
		}
		if timestamp.Before(start) || timestamp.After(end) {
			continue
		}

		roundDur, err := time.ParseDuration(raw.RoundTime)
		if err != nil {
			continue
		}
		procDur, err := time.ParseDuration(raw.Processing)
		if err != nil {
			continue
		}
		bftDur, err := time.ParseDuration(raw.BftWait)
		if err != nil {
			continue
		}
		finalDur, err := time.ParseDuration(raw.Finalization)
		if err != nil {
			continue
		}
		medianDur, err := time.ParseDuration(raw.ProofReadyMedian)
		if err != nil {
			continue
		}
		p95Dur, err := time.ParseDuration(raw.ProofReadyP95)
		if err != nil {
			continue
		}
		p99Dur, err := time.ParseDuration(raw.ProofReadyP99)
		if err != nil {
			continue
		}

		summaries = append(summaries, aggregatorRoundSummary{
			Timestamp:    timestamp,
			Block:        raw.Block,
			Commitments:  raw.Commitments,
			RoundTime:    roundDur,
			Processing:   procDur,
			BftWait:      bftDur,
			Finalization: finalDur,
			ProofMedian:  medianDur,
			ProofP95:     p95Dur,
			ProofP99:     p99Dur,
			RedisTotal:   raw.RedisTotal,
			RedisPending: raw.RedisPending,
		})
	}

	if err := scanner.Err(); err != nil {
		return summaries, err
	}

	return summaries, nil
}

func reportAggregatorServerStats(start, end time.Time) {
	summaries, err := parseAggregatorRoundLogs(aggregatorLogPath, start, end)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("\nAggregator log file %s not found; skipping server stats.\n", aggregatorLogPath)
		} else {
			fmt.Printf("\nFailed to read aggregator logs (%s): %v\n", aggregatorLogPath, err)
		}
		return
	}

	if len(summaries) == 0 {
		fmt.Printf("\nNo aggregator round logs found in %s between %s and %s.\n",
			aggregatorLogPath,
			start.Format(time.RFC3339),
			end.Format(time.RFC3339))
		return
	}

	usable := summaries
	if len(summaries) > 2 {
		usable = summaries[1 : len(summaries)-1]
	}

	var finalSum, roundSum time.Duration
	var procSum, bftSum time.Duration
	var proofMedSum, proofP95Sum, proofP99Sum time.Duration
	totalCommitments := 0
	for _, entry := range usable {
		finalSum += entry.Finalization
		roundSum += entry.RoundTime
		procSum += entry.Processing
		bftSum += entry.BftWait
		proofMedSum += entry.ProofMedian
		proofP95Sum += entry.ProofP95
		proofP99Sum += entry.ProofP99
		totalCommitments += entry.Commitments
	}

	count := len(usable)
	if count == 0 {
		fmt.Printf("\nNo aggregator rounds available after filtering; showing raw entries only.\n")
		usable = summaries
		count = len(usable)
	}

	avgFinal := time.Duration(0)
	avgProcessing := time.Duration(0)
	avgBft := time.Duration(0)
	avgProofMedian := time.Duration(0)
	avgProofP95 := time.Duration(0)
	avgProofP99 := time.Duration(0)
	avgCommit := 0.0
	if count > 0 {
		avgFinal = finalSum / time.Duration(count)
		avgProcessing = procSum / time.Duration(count)
		avgBft = bftSum / time.Duration(count)
		avgProofMedian = proofMedSum / time.Duration(count)
		avgProofP95 = proofP95Sum / time.Duration(count)
		avgProofP99 = proofP99Sum / time.Duration(count)
		avgCommit = float64(totalCommitments) / float64(count)
	}

	finalPct := 0.0
	procPct := 0.0
	bftPct := 0.0
	if roundSum > 0 {
		finalPct = float64(finalSum) / float64(roundSum) * 100
		procPct = float64(procSum) / float64(roundSum) * 100
		bftPct = float64(bftSum) / float64(roundSum) * 100
	}

	fmt.Printf("\nAGGREGATOR SERVER STATS (%d rounds, averages exclude first/last when possible)\n", len(summaries))
	fmt.Printf("Average finalization time: %v (%.1f%% of round time)\n",
		avgFinal.Truncate(time.Millisecond), finalPct)
	fmt.Printf("Average commitments per round: %.0f\n", avgCommit)
	fmt.Printf("Average processing time: %v (%.1f%% of round time)\n",
		avgProcessing.Truncate(time.Millisecond), procPct)
	fmt.Printf("Average BFT wait: %v (%.1f%% of round time)\n",
		avgBft.Truncate(time.Millisecond), bftPct)
	fmt.Printf("Average proof readiness: median %v, p95 %v, p99 %v\n",
		avgProofMedian.Truncate(time.Millisecond),
		avgProofP95.Truncate(time.Millisecond),
		avgProofP99.Truncate(time.Millisecond))
	fmt.Printf("Log window: %s to %s\n",
		summaries[0].Timestamp.Format(time.RFC3339),
		summaries[len(summaries)-1].Timestamp.Format(time.RFC3339))
}

func main() {
	// Get URL and auth header from environment variables
	aggregatorURL := os.Getenv("AGGREGATOR_URL")
	if aggregatorURL == "" {
		aggregatorURL = defaultAggregatorURL
	}

	authHeader := os.Getenv("AUTH_HEADER")

	fmt.Printf("Starting aggregator performance test...\n")
	if len(shardTargets) == 0 {
		fmt.Printf("Target: %s\n", aggregatorURL)
	} else {
		fmt.Printf("Targets (%d shards):\n", len(shardTargets))
		for _, target := range shardTargets {
			label := target.url
			if target.name != "" {
				label = fmt.Sprintf("%s (%s)", target.name, target.url)
			}
			if target.shardMask > 0 {
				fmt.Printf("  - %s shardMask=%d\n", label, target.shardMask)
			} else {
				fmt.Printf("  - %s\n", label)
			}
		}
	}
	if authHeader != "" {
		fmt.Printf("Authorization: [configured]\n")
	}
	fmt.Printf("Duration: %v\n", testDuration)
	fmt.Printf("Workers: %d\n", workerCount)
	fmt.Printf("HTTP client pool size: %d\n", httpClientPoolSize)
	fmt.Printf("Target RPS: %d\n", requestsPerSec)
	fmt.Printf("----------------------------------------\n")

	testWindowStart := time.Now()

	// Initialize metrics
	metrics := &Metrics{
		startTime: time.Now(),
	}
	rateCounters := &RequestRateCounters{}

	// Create context with timeout for commitment submission
	submitCtx, submitCancel := context.WithTimeout(context.Background(), testDuration)
	defer submitCancel()

	// Create longer context for proof verification (allows extra time for late submissions)
	proofTimeout := testDuration + 20*time.Second
	proofCtx, proofCancel := context.WithTimeout(context.Background(), proofTimeout)
	defer proofCancel()

	shardClients := buildShardClients(aggregatorURL, authHeader, metrics)
	metrics.initShardMetrics(len(shardClients))

	// Test connectivity and get starting block number for each shard
	for _, sc := range shardClients {
		fmt.Printf("Testing connectivity to %s...\n", sc.url)
		resp, err := sc.client.call("get_block_height", nil)
		if err != nil {
			log.Fatalf("Failed to connect to aggregator at %s: %v", sc.url, err)
		}

		if resp.Error != nil {
			log.Fatalf("Error getting block height from %s: %v", sc.url, resp.Error.Message)
		}

		var heightResp GetBlockHeightResponse
		respBytes, _ := json.Marshal(resp.Result)
		if err := json.Unmarshal(respBytes, &heightResp); err != nil {
			log.Fatalf("Failed to parse block height from %s: %v", sc.url, err)
		}

		var startingBlockNumber int64
		if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
			log.Fatalf("Failed to parse starting block number from %s: %v", sc.url, err)
		}
		sc.startingBlock = startingBlockNumber

		fmt.Printf("✓ Connected successfully to %s\n", sc.url)
		fmt.Printf("✓ Starting block number for %s: %d\n", sc.url, startingBlockNumber)
	}

	// Create proof verification queue (buffered channel)
	proofQueue := make(chan proofJob, 10000)

	// Pre-generate commitment pool to eliminate client-side crypto overhead
	// Calculate pool size: total requests needed + 10% buffer
	poolSize := int(float64(requestsPerSec) * testDuration.Seconds() * 1.1)
	fmt.Printf("\nPre-generating %d commitments (%d RPS × %v + 10%% buffer)...\n", poolSize, requestsPerSec, testDuration)
	commitmentPool := make([]*api.SubmitCommitmentRequest, poolSize)
	workerCountPreGen := runtime.NumCPU()
	if workerCountPreGen < 1 {
		workerCountPreGen = 1
	}
	if workerCountPreGen > 32 {
		workerCountPreGen = 32
	}
	jobs := make(chan int, workerCountPreGen*2)
	var preGenWG sync.WaitGroup
	for w := 0; w < workerCountPreGen; w++ {
		preGenWG.Add(1)
		go func() {
			defer preGenWG.Done()
			for idx := range jobs {
				commitmentPool[idx] = generateCommitmentRequest()
			}
		}()
	}
	for i := 0; i < poolSize; i++ {
		jobs <- i
		if (i+1)%10000 == 0 {
			fmt.Printf("  Generated %d/%d commitments...\n", i+1, poolSize)
		}
	}
	close(jobs)
	preGenWG.Wait()
	fmt.Printf("✓ Pre-generated %d commitments\n\n", poolSize)

	var poolIndex atomic.Int64
	var wg sync.WaitGroup

	// Record when submission actually starts
	metrics.submissionStartTime = time.Now()

	// Start commitment workers (use submitCtx - stops after testDuration)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commitmentWorker(submitCtx, shardClients, metrics, proofQueue, commitmentPool, &poolIndex, rateCounters)
		}()
	}

	// Start proof verification workers (use proofCtx - runs longer)
	// Note: We don't close the proof queue manually; workers exit when proofCtx is cancelled
	// after all proofs are verified or timeout is reached
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proofVerificationWorker(proofCtx, shardClients, metrics, proofQueue, proofCancel, rateCounters)
		}()
	}

	perfLogCtx := proofCtx
	plannedRequests := int64(float64(requestsPerSec) * testDuration.Seconds())
	if plannedRequests <= 0 {
		plannedRequests = int64(poolSize)
	}
	go logClientPerfRates(perfLogCtx, metrics, rateCounters, plannedRequests, shardClients)

	// Monitor when submission phase completes
	go func() {
		<-submitCtx.Done()
		metrics.submissionEndTime = time.Now()
	}()

	// Wait for all workers to complete (both submission and proof verification)
	// Note: Proof workers continue running for up to 20 seconds after submissions stop
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Commitment submissions complete; waiting for proof verification...\n")

	// Monitor proof verification progress and cancel early when all proofs are verified
	lastProofCount := int64(0)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		noProgressCount := 0
		for {
			select {
			case <-proofCtx.Done():
				return
			case <-ticker.C:
				currentProofs := atomic.LoadInt64(&metrics.proofVerified)
				proofFailed := atomic.LoadInt64(&metrics.proofFailed)
				successful := atomic.LoadInt64(&metrics.successfulRequests)

				// Check if all proofs are done (verified + failed = expected)
				totalProcessed := currentProofs + proofFailed
				if totalProcessed >= successful {
					fmt.Printf("  All proofs processed (%d verified, %d failed). Finishing early...\n", currentProofs, proofFailed)
					proofCancel() // Cancel early since we're done
					return
				}

				// Also check if we haven't made progress in several seconds and queue is likely drained
				if currentProofs > 0 && currentProofs == lastProofCount {
					noProgressCount++
					// If no progress for 5 seconds and we've verified most proofs, likely done
					if noProgressCount >= 5 && totalProcessed >= successful*95/100 {
						fmt.Printf("  No progress for 5s and %d%% complete. Finishing early...\n", totalProcessed*100/successful)
						proofCancel()
						return
					}
				} else {
					noProgressCount = 0
				}
				lastProofCount = currentProofs
			}
		}
	}()

	wg.Wait()

	// Stop submission phase and get counts
	fmt.Printf("\n----------------------------------------\n")
	successful := atomic.LoadInt64(&metrics.successfulRequests)
	verified := atomic.LoadInt64(&metrics.proofVerified)
	proofFailedCount := atomic.LoadInt64(&metrics.proofFailed)
	if verified+proofFailedCount < successful {
		fmt.Printf("Warning: proof verification ended early (%d verified, %d failed, %d expected)\n",
			verified, proofFailedCount, successful)
	} else if verified == successful {
		fmt.Printf("All proofs verified successfully.\n")
	} else {
		fmt.Printf("Proof verification completed with failures (%d verified, %d failed).\n", verified, proofFailedCount)
	}

	// Final metrics
	elapsed := time.Since(metrics.startTime)
	submissionDuration := metrics.submissionEndTime.Sub(metrics.submissionStartTime)
	total := atomic.LoadInt64(&metrics.totalRequests)
	successful = atomic.LoadInt64(&metrics.successfulRequests)
	failed := atomic.LoadInt64(&metrics.failedRequests)
	exists := atomic.LoadInt64(&metrics.requestIdExistsErr)

	fmt.Printf("\n\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Total duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Submission duration: %v\n", submissionDuration.Truncate(time.Millisecond))
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("REQUEST_ID_EXISTS: %d\n", exists)
	fmt.Printf("Average RPS: %.2f\n", float64(total)/submissionDuration.Seconds())
	fmt.Printf("Success rate: %.2f%%\n", float64(successful)/float64(total)*100)
	if failed > 0 {
		failurePct := float64(failed) / math.Max(float64(total), 1)
		fmt.Printf("⚠️  WARNING: %d submissions failed (%.2f%% of total).\n",
			failed, failurePct*100)
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
		fmt.Printf("Proof retrieval attempt distribution:\n")
		for attempt, count := range metrics.proofSuccessAttemptBuckets() {
			fmt.Printf("  Attempt %d: %d\n", attempt+1, count)
		}
	}

	if proofSuccess > 0 {
		medianLatency, p95Latency, p99Latency := metrics.getProofLatencyStats()
		fmt.Printf("Proof retrieval latency (submission to proof): median %v, p95 %v, p99 %v\n",
			medianLatency.Truncate(time.Millisecond),
			p95Latency.Truncate(time.Millisecond),
			p99Latency.Truncate(time.Millisecond))

		// Display proof request duration statistics
		avg, min, max, p50, p95, p99 := metrics.getProofRequestStats()
		if avg > 0 {
			fmt.Printf("\nProof Request Duration Statistics:\n")
			fmt.Printf("  Average: %v\n", avg.Truncate(time.Microsecond))
			fmt.Printf("  Median (p50): %v\n", p50.Truncate(time.Microsecond))
			fmt.Printf("  p95: %v\n", p95.Truncate(time.Microsecond))
			fmt.Printf("  p99: %v\n", p99.Truncate(time.Microsecond))
			fmt.Printf("  Min: %v\n", min.Truncate(time.Microsecond))
			fmt.Printf("  Max: %v\n\n", max.Truncate(time.Microsecond))
		}
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

	printShardFinalReport(metrics, shardClients)

	// Print error summary
	metrics.printErrorSummary()

	reportAggregatorServerStats(testWindowStart, time.Now())
}
