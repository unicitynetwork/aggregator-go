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
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofverify"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	defaultAggregatorURL     = "https://localhost:3000"
	defaultTestDuration      = 30 * time.Second
	workerCount              = 20
	proofWorkerCount         = 10 // Separate worker pool for proof requests
	httpClientPoolSize       = 4
	defaultRequestsPerSec    = 2000
	aggregatorLogPath        = "logs/aggregator.log"
	aggregatorLogPathsEnv    = "AGGREGATOR_LOG_PATHS"
	proofMaxRetries          = 10
	proofRetryDelay          = 1000 * time.Millisecond
	defaultProofInitialDelay = 2500 * time.Millisecond
)

// Sharding modes for routing generated state IDs to shard endpoints.
// In app mode the SHARD_TARGETS field is an LSB-first sentinel-int mask;
// in bft-shard mode it is an MSB-first binary bit string (e.g. "0", "10", "101").
const (
	shardingModeApp = "app"
	shardingModeBFT = "bft-shard"
)

// Configurable via environment variables
var (
	testDuration      = getEnvDuration("TEST_DURATION", defaultTestDuration)
	requestsPerSec    = getEnvInt("REQUESTS_PER_SEC", defaultRequestsPerSec)
	proofInitialDelay = getEnvDuration("PROOF_INITIAL_DELAY", defaultProofInitialDelay)
	shardingMode      = getShardingMode()
	shardTargets      = getEnvShardTargets()
	enableH2C         = os.Getenv("ENABLE_H2C") != "false"
)

func getShardingMode() string {
	val := strings.ToLower(strings.TrimSpace(os.Getenv("SHARDING_MODE")))
	switch val {
	case "", shardingModeApp:
		return shardingModeApp
	case shardingModeBFT:
		return shardingModeBFT
	default:
		log.Fatalf("invalid SHARDING_MODE=%q (expected 'app' or 'bft-shard')", val)
		return shardingModeApp
	}
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
		// Also try parsing as seconds (integer)
		if secs, err := strconv.Atoi(val); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultVal
}

// getEnvShardTargets parses SHARD_TARGETS. Each comma-separated entry is
// URL:mask where mask is interpreted by the active sharding mode (decimal
// sentinel-int in app mode, MSB-first binary in bft-shard mode). The last colon
// in each entry is the delimiter, so URLs with ports still parse correctly.
func getEnvShardTargets() []shardTarget {
	val := os.Getenv("SHARD_TARGETS")
	if val == "" {
		// Default targets are for app-mode local runs. In bft-shard mode the
		// user must set SHARD_TARGETS explicitly; there is no useful default
		// bit pattern without knowing the partition's shard scheme.
		if shardingMode == shardingModeBFT {
			log.Fatal("SHARDING_MODE=bft-shard requires SHARD_TARGETS (e.g. 'https://localhost:3001:0,https://localhost:3002:1')")
		}
		return []shardTarget{
			{name: "shard-7", url: "https://localhost:3001", shardMask: 7},
		}
	}

	var targets []shardTarget
	parts := strings.Split(val, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lastColon := strings.LastIndex(part, ":")
		if lastColon == -1 {
			log.Printf("Warning: invalid shard target format (missing mask/bits): %s", part)
			continue
		}
		url := part[:lastColon]
		field := part[lastColon+1:]

		switch shardingMode {
		case shardingModeApp:
			mask, err := strconv.Atoi(field)
			if err != nil {
				log.Printf("Warning: invalid shard mask '%s': %v", field, err)
				continue
			}
			targets = append(targets, shardTarget{
				name:      fmt.Sprintf("shard-%d", mask),
				url:       url,
				shardMask: mask,
			})
		case shardingModeBFT:
			// Validate the bit string is only 0s and 1s. Empty string means
			// "single-shard partition" (accepts every stateID).
			for _, r := range field {
				if r != '0' && r != '1' {
					log.Printf("Warning: invalid shard bits '%s' (bft-shard mode expects binary 0/1 string)", field)
					field = "__invalid__"
					break
				}
			}
			if field == "__invalid__" {
				continue
			}
			name := field
			if name == "" {
				name = "single"
			}
			targets = append(targets, shardTarget{
				name:      fmt.Sprintf("shard-%s", name),
				url:       url,
				shardBits: field,
			})
		}
	}

	if len(targets) == 0 {
		log.Fatal("No valid shard targets found in SHARD_TARGETS")
	}

	return targets
}

// Default shard configs for reference:
// 4 shards (2-bit): "https://localhost:3001:7,https://localhost:3002:6,https://localhost:3003:5,https://localhost:3004:4"
// 2 shards (1-bit): "https://localhost:3001:3,https://localhost:3002:2"
//
// Example hardcoded config (for reference)
//var shardTargets = []shardTarget{
//	{name: "shard-7", url: "https://localhost:3001", shardMask: 7}, // 0b111
//	{name: "shard-6", url: "https://localhost:3002", shardMask: 6}, // 0b110
//	{name: "shard-5", url: "https://localhost:3003", shardMask: 5}, // 0b101
//	{name: "shard-4", url: "https://localhost:3004", shardMask: 4}, // 0b100
//}
//	{name: "shard-2", url: "https://localhost:3002", shardMask: 2},
//}

func normalizeStateID(id string) string {
	return strings.ToLower(id)
}

func buildShardClients(aggregatorURL, authHeader string, metrics *Metrics) []*ShardClient {
	if len(shardTargets) == 0 {
		return []*ShardClient{
			{
				name:        "default",
				url:         aggregatorURL,
				client:      NewJSONRPCClient(aggregatorURL, authHeader, metrics),
				proofClient: NewJSONRPCClient(aggregatorURL, authHeader, metrics), // Separate pool for proofs
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
			name:        name,
			url:         trimmed,
			shardMask:   target.shardMask,
			shardBits:   target.shardBits,
			client:      NewJSONRPCClient(trimmed, authHeader, metrics),
			proofClient: NewJSONRPCClient(trimmed, authHeader, metrics), // Separate pool for proofs
		})
	}

	if len(clients) == 0 {
		return []*ShardClient{
			{
				name:        "default",
				url:         aggregatorURL,
				client:      NewJSONRPCClient(aggregatorURL, authHeader, metrics),
				proofClient: NewJSONRPCClient(aggregatorURL, authHeader, metrics), // Separate pool for proofs
			},
		}
	}

	return clients
}

func selectShardIndex(stateID api.StateID, shardClients []*ShardClient) int {
	shardCount := len(shardClients)
	if shardCount <= 1 {
		return 0
	}

	stateIDHex := stateID.String()
	for idx, sc := range shardClients {
		if sc == nil {
			continue
		}
		match, err := matchesShardTarget(stateIDHex, sc.shardMask, sc.shardBits)
		if err == nil && match {
			return idx
		}
	}

	imprint := stateID.Imprint()
	if len(imprint) == 0 {
		return 0
	}
	keyBytes := stateID.DataBytes()
	if len(keyBytes) == 0 {
		return 0
	}
	// Fallback: distribute over shards when no explicit predicate matched.
	// Bucket selection stays stable across modes; the top byte happens to be
	// where both matchers look, so hashing on it is fine for load-balancing.
	return int(keyBytes[0]) % shardCount
}

// matchesShardTarget dispatches to the active sharding mode's prefix matcher.
func matchesShardTarget(stateIDHex string, shardMask int, shardBits string) (bool, error) {
	switch shardingMode {
	case shardingModeBFT:
		return matchesShardBits(stateIDHex, shardBits)
	default:
		return matchesShardMask(stateIDHex, shardMask)
	}
}

func matchesShardMask(stateIDHex string, shardMask int) (bool, error) {
	if shardMask <= 0 {
		return false, nil
	}
	return api.MatchesShardPrefixFromHex(stateIDHex, shardMask)
}

// matchesShardBits is the BFT-mode prefix check. An empty bits string means
// "single-shard partition, accepts all".
func matchesShardBits(stateIDHex, shardBits string) (bool, error) {
	if shardBits == "" {
		return true, nil
	}
	keyBytes, err := hex.DecodeString(stateIDHex)
	if err != nil {
		return false, fmt.Errorf("decode stateId hex: %w", err)
	}
	if len(keyBytes) != api.StateTreeKeyLengthBytes {
		return false, fmt.Errorf("stateId must be %d bytes, got %d", api.StateTreeKeyLengthBytes, len(keyBytes))
	}
	sid, err := shardIDFromBitString(shardBits)
	if err != nil {
		return false, err
	}
	return sid.Comparator()(keyBytes), nil
}

// shardIDFromBitString constructs a types.ShardID from an MSB-first binary
// string. Encoding: bits packed MSB-first, followed by a single 1 as end
// marker, zero-padded to ceil((b+1)/8) bytes.
func shardIDFromBitString(bits string) (bfttypes.ShardID, error) {
	if bits == "" {
		return bfttypes.ShardID{}, nil
	}
	byteLen := (len(bits) + 1 + 7) / 8
	buf := make([]byte, byteLen)
	for i, r := range bits {
		if r == '1' {
			buf[i/8] |= 1 << (7 - uint(i%8))
		}
	}
	// End marker: set the bit immediately after the content.
	buf[len(bits)/8] |= 1 << (7 - uint(len(bits)%8))
	hexStr := "0x" + hex.EncodeToString(buf)
	var sid bfttypes.ShardID
	if err := sid.UnmarshalText([]byte(hexStr)); err != nil {
		return bfttypes.ShardID{}, fmt.Errorf("encode shard bits %q: %w", bits, err)
	}
	return sid, nil
}

// matchesAnyShardTarget checks if a state ID matches any of the configured shard targets.
func matchesAnyShardTarget(stateIDHex string) bool {
	if len(shardTargets) == 0 {
		return true // No targets configured, accept all
	}
	for _, target := range shardTargets {
		if shardingMode == shardingModeApp && target.shardMask <= 0 {
			continue
		}
		ok, err := matchesShardTarget(stateIDHex, target.shardMask, target.shardBits)
		if err == nil && ok {
			return true
		}
	}
	return false
}

// Generate a cryptographically valid certification request request
func generateCommitmentRequest() *api.CertificationRequest {
	// Generate a real secp256k1 key pair
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		panic(fmt.Sprintf("Failed to generate private key: %v", err))
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	// Generate random state data and hash it.
	stateData := make([]byte, 32)
	rand.Read(stateData)
	sourceStateHash := signing.CreateDataHash(stateData)

	var stateID api.StateID

	// Only rejection-sample when at least one target actually constrains the
	// state-ID prefix. In bft-shard mode an empty shardBits string means
	// "single-shard partition", which accepts everything and therefore
	// imposes no constraint.
	hasActiveShardPredicate := false
	for _, target := range shardTargets {
		switch shardingMode {
		case shardingModeApp:
			if target.shardMask > 0 {
				hasActiveShardPredicate = true
			}
		case shardingModeBFT:
			if target.shardBits != "" {
				hasActiveShardPredicate = true
			}
		}
		if hasActiveShardPredicate {
			break
		}
	}

	for {
		calculated, err := api.CreateStateID(ownerPredicate, sourceStateHash)
		if err != nil {
			panic(fmt.Sprintf("Failed to create state ID: %v", err))
		}
		// If no shard predicate constrains us, accept any state ID.
		if !hasActiveShardPredicate {
			stateID = calculated
			break
		}
		// Check if the state ID matches any of the configured shard targets.
		if matchesAnyShardTarget(calculated.String()) {
			stateID = calculated
			break
		}
		// Regenerate state hash and try again
		rand.Read(stateData)
		sourceStateHash = signing.CreateDataHash(stateData)
	}

	// Generate random transaction data and hash it.
	transactionData := make([]byte, 32)
	rand.Read(transactionData)
	transactionHash := signing.CreateDataHash(transactionData)

	signingService := signing.NewSigningService()
	certData := &api.CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
	}
	if err = signingService.SignCertData(certData, privateKey.Serialize()); err != nil {
		panic(fmt.Sprintf("Failed to sign transaction: %v", err))
	}

	return &api.CertificationRequest{
		StateID:           stateID,
		CertificationData: *certData,
	}
}

// Worker function that continuously submits commitments
func commitmentWorker(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, proofQueue chan proofJob, commitmentPool []*api.CertificationRequest, poolIndex *atomic.Int64, counters *RequestRateCounters, submissionWg *sync.WaitGroup) {
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
			submissionWg.Add(1)
			go func() {
				defer submissionWg.Done()

				idx := poolIndex.Add(1) % int64(len(commitmentPool))
				req := commitmentPool[idx]

				shardIdx := selectShardIndex(req.StateID, shardClients)
				client := shardClients[shardIdx].client

				if counters != nil {
					counters.IncSubmitStarted()
				}

				atomic.AddInt64(&metrics.totalRequests, 1)
				if sm := metrics.shard(shardIdx); sm != nil {
					sm.totalRequests.Add(1)
				}

				// Create a context with 3 second timeout for submission
				submitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				resp, err := client.callWithContext(submitCtx, "certification_request", req)

				if counters != nil {
					counters.IncSubmitCompleted()
				}
				if err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.failedRequests.Add(1)
					}
					// Log timeout/error details
					metrics.recordError(fmt.Sprintf("submit failed: %v", err))
					return
				}

				if resp.Error != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.failedRequests.Add(1)
					}
					if resp.Error.Message == "STATE_ID_EXISTS" {
						atomic.AddInt64(&metrics.stateIdExistsErr, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.stateIdExistsErr.Add(1)
						}
						stateIDStr := normalizeStateID(req.StateID.String())
						metrics.submittedStateIDs.Store(stateIDStr, true)
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

				switch submitResp.Status {
				case "SUCCESS", "STATE_ID_EXISTS":
					if submitResp.Status == "STATE_ID_EXISTS" {
						atomic.AddInt64(&metrics.stateIdExistsErr, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.stateIdExistsErr.Add(1)
						}
					}
					atomic.AddInt64(&metrics.successfulRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.successfulRequests.Add(1)
					}
					stateIDStr := normalizeStateID(req.StateID.String())
					metrics.submittedStateIDs.Store(stateIDStr, true)

					if proofQueue != nil {
						metrics.recordSubmissionTimestamp(stateIDStr)
						select {
						case proofQueue <- proofJob{shardIdx: shardIdx, request: req}:
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
						fmt.Printf("Unexpected status '%s' for request %s\n", submitResp.Status, req.StateID)
					}
				}
			}()
		}
	}
}

// Worker function that continuously verifies inclusion proofs in a sharded setup.
func proofVerificationWorker(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, proofQueue chan proofJob, counters *RequestRateCounters) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-proofQueue:
			go func(job proofJob) {
				if job.request == nil {
					metrics.recordError("Missing original request for proof verification")
					atomic.AddInt64(&metrics.proofVerifyFailed, 1)
					if sm := metrics.shard(job.shardIdx); sm != nil {
						sm.proofVerifyFailed.Add(1)
					}
					return
				}

				stateID := normalizeStateID(job.request.StateID.String())
				shardIdx := job.shardIdx
				time.Sleep(proofInitialDelay)
				startTime := time.Now()
				normalizedID := stateID
				client := shardClients[shardIdx].proofClient // Use separate proof client pool

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

					proofReq := GetInclusionProofRequestV2{StateID: stateID}

					atomic.AddInt64(&metrics.proofActiveRequests, 1)
					if counters != nil {
						counters.IncProofStarted()
					}
					requestStart := time.Now()

					// Create a context with 5 second timeout for proof retrieval
					proofCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					resp, err := client.callWithContext(proofCtx, "get_inclusion_proof.v2", proofReq)
					cancel()

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

					var proofResp api.GetInclusionProofResponseV2
					respBytes, _ := json.Marshal(resp.Result)
					if err := json.Unmarshal(respBytes, &proofResp); err != nil {
						metrics.recordError(fmt.Sprintf("Failed to parse proof response: %v", err))
						atomic.AddInt64(&metrics.proofFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofFailed.Add(1)
						}
						return
					}

					if proofResp.InclusionProof == nil || proofResp.InclusionProof.CertificationData == nil || proofResp.InclusionProof.CertificationData.TransactionHash == nil {
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

					if err := proofverify.VerifyInclusionProofLocal(proofResp.InclusionProof, job.request); err != nil {
						if attempt < proofMaxRetries-1 {
							time.Sleep(proofRetryDelay)
							continue
						}
						metrics.recordError(fmt.Sprintf("Proof verification failed for state ID %s: %v", stateID, err))
						atomic.AddInt64(&metrics.proofVerifyFailed, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.proofVerifyFailed.Add(1)
						}
						return
					}
					atomic.AddInt64(&metrics.proofVerified, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.proofVerified.Add(1)
					}

					return
				}

				metrics.recordError(fmt.Sprintf("Timeout getting proof after %d attempts", proofMaxRetries))
				atomic.AddInt64(&metrics.proofFailed, 1)
				if sm := metrics.shard(shardIdx); sm != nil {
					sm.proofFailed.Add(1)
				}
			}(job)
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
			outstanding := submitStarted - submitCompleted
			fmt.Printf("%s submitted=%d/%d (%.1f%%) fail=%d progress=%.1f%% rps=%.1f submit_started=%d/s submit_completed=%d/s outstanding=%d proof_started=%d/s proof_completed=%d/s proof_retries=%d/s verified=%d/%d (%.1f%%) proof_lag=%d conns_active=%d conns_total=%d conns_failed=%d\n",
				timestamp, successful, total, successRate, failed, progress, overallRPS, submitStartRate, submitCompleteRate, outstanding, proofStartRate, proofCompleteRate, proofRetryRate, verified, successful, verificationPct, lag, activeConns, totalConn, failedConn)

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
		exists := sm.stateIdExistsErr.Load()
		successPct := 0.0
		if total > 0 {
			successPct = float64(success) / float64(total) * 100
		}
		fmt.Printf("  - %s total=%d success=%d failed=%d state_id_exists=%d success_rate=%.2f%%\n",
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

type aggregatorLogSource struct {
	label string
	path  string
}

func parseAggregatorLogSourcesOverride(raw string) []aggregatorLogSource {
	entries := strings.Split(raw, ",")
	sources := make([]aggregatorLogSource, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i, entry := range entries {
		token := strings.TrimSpace(entry)
		if token == "" {
			continue
		}

		label := ""
		path := token
		if idx := strings.Index(token, "="); idx > 0 {
			label = strings.TrimSpace(token[:idx])
			path = strings.TrimSpace(token[idx+1:])
		}
		if path == "" {
			continue
		}

		cleanPath := filepath.Clean(path)
		if _, ok := seen[cleanPath]; ok {
			continue
		}
		seen[cleanPath] = struct{}{}

		if label == "" {
			label = filepath.Base(filepath.Dir(cleanPath))
			if label == "." || label == "" {
				label = fmt.Sprintf("source-%d", i+1)
			}
		}
		sources = append(sources, aggregatorLogSource{
			label: label,
			path:  cleanPath,
		})
	}
	return sources
}

func discoverAggregatorLogSources(shardClients []*ShardClient) []aggregatorLogSource {
	if override := strings.TrimSpace(os.Getenv(aggregatorLogPathsEnv)); override != "" {
		return parseAggregatorLogSourcesOverride(override)
	}

	if len(shardClients) <= 1 {
		return []aggregatorLogSource{
			{label: "aggregator", path: aggregatorLogPath},
		}
	}

	// Preferred convention in sharded local setup:
	// logs/shard1/aggregator.log, logs/shard2/aggregator.log, ...
	preferred := make([]aggregatorLogSource, 0, len(shardClients))
	for idx, sc := range shardClients {
		candidate := filepath.Join("logs", fmt.Sprintf("shard%d", idx+1), "aggregator.log")
		if _, err := os.Stat(candidate); err == nil {
			preferred = append(preferred, aggregatorLogSource{
				label: sc.name,
				path:  filepath.Clean(candidate),
			})
		}
	}
	if len(preferred) > 0 {
		return preferred
	}

	matches, err := filepath.Glob(filepath.Join("logs", "shard*", "aggregator.log"))
	if err != nil || len(matches) == 0 {
		return []aggregatorLogSource{
			{label: "aggregator", path: aggregatorLogPath},
		}
	}
	sort.Strings(matches)

	sources := make([]aggregatorLogSource, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		cleanPath := filepath.Clean(match)
		if _, ok := seen[cleanPath]; ok {
			continue
		}
		seen[cleanPath] = struct{}{}
		label := filepath.Base(filepath.Dir(cleanPath))
		if label == "." || label == "" {
			label = cleanPath
		}
		sources = append(sources, aggregatorLogSource{
			label: label,
			path:  cleanPath,
		})
	}
	return sources
}

func printAggregatorServerStatsSummary(header string, summaries []aggregatorRoundSummary) {
	if len(summaries) == 0 {
		return
	}
	ordered := append([]aggregatorRoundSummary(nil), summaries...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Timestamp.Before(ordered[j].Timestamp)
	})

	usable := ordered
	if len(ordered) > 2 {
		usable = ordered[1 : len(ordered)-1]
	}
	if len(usable) == 0 {
		usable = ordered
	}

	var roundSum time.Duration
	var finalSum time.Duration
	var procSum, bftSum time.Duration
	var proofMedSum, proofP95Sum, proofP99Sum time.Duration
	totalCommitments := 0
	for _, entry := range usable {
		roundSum += entry.RoundTime
		finalSum += entry.Finalization
		procSum += entry.Processing
		bftSum += entry.BftWait
		proofMedSum += entry.ProofMedian
		proofP95Sum += entry.ProofP95
		proofP99Sum += entry.ProofP99
		totalCommitments += entry.Commitments
	}

	count := len(usable)

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

	fmt.Printf("\nAGGREGATOR SERVER STATS [%s] (%d rounds, averages exclude first/last when possible)\n", header, len(ordered))
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
		ordered[0].Timestamp.Format(time.RFC3339),
		ordered[len(ordered)-1].Timestamp.Format(time.RFC3339))
}

func reportAggregatorServerStats(start, end time.Time, shardClients []*ShardClient) {
	sources := discoverAggregatorLogSources(shardClients)
	if len(sources) == 0 {
		fmt.Printf("\nNo aggregator log sources configured; skipping server stats.\n")
		return
	}

	foundSources := 0
	combined := make([]aggregatorRoundSummary, 0)
	readErrors := make([]string, 0)
	noDataSources := make([]aggregatorLogSource, 0)
	for _, source := range sources {
		summaries, err := parseAggregatorRoundLogs(source.path, start, end)
		if err != nil {
			if os.IsNotExist(err) {
				readErrors = append(readErrors, fmt.Sprintf("log file not found: %s (%s)", source.path, source.label))
			} else {
				readErrors = append(readErrors, fmt.Sprintf("failed to read %s (%s): %v", source.path, source.label, err))
			}
			continue
		}

		if len(summaries) == 0 {
			noDataSources = append(noDataSources, source)
			continue
		}

		foundSources++
		combined = append(combined, summaries...)
		printAggregatorServerStatsSummary(fmt.Sprintf("%s (%s)", source.label, source.path), summaries)
	}

	if foundSources == 0 {
		for _, msg := range readErrors {
			fmt.Printf("\n%s\n", msg)
		}
		for _, source := range noDataSources {
			fmt.Printf("\nNo aggregator round logs found in %s (%s) between %s and %s.\n",
				source.path,
				source.label,
				start.Format(time.RFC3339),
				end.Format(time.RFC3339))
		}
	} else {
		skippedSources := len(readErrors) + len(noDataSources)
		if skippedSources > 0 {
			fmt.Printf("\nSkipped %d log source(s) with no matching round data in the test window.\n", skippedSources)
		}
	}

	if foundSources == 0 && len(shardClients) > 1 && os.Getenv(aggregatorLogPathsEnv) == "" {
		fmt.Printf("\nSet %s to a comma-separated list of shard log files to override auto-discovery.\n", aggregatorLogPathsEnv)
	}

	if foundSources > 1 {
		printAggregatorServerStatsSummary("combined", combined)
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
	fmt.Printf("Sharding mode: %s\n", shardingMode)
	if len(shardTargets) == 0 {
		fmt.Printf("Target: %s\n", aggregatorURL)
	} else {
		fmt.Printf("Targets (%d shards):\n", len(shardTargets))
		for _, target := range shardTargets {
			label := target.url
			if target.name != "" {
				label = fmt.Sprintf("%s (%s)", target.name, target.url)
			}
			switch shardingMode {
			case shardingModeBFT:
				if target.shardBits != "" {
					fmt.Printf("  - %s shardBits=%s (MSB-first)\n", label, target.shardBits)
				} else {
					fmt.Printf("  - %s (single-shard partition)\n", label)
				}
			default:
				if target.shardMask > 0 {
					fmt.Printf("  - %s shardMask=%d\n", label, target.shardMask)
				} else {
					fmt.Printf("  - %s\n", label)
				}
			}
		}
	}
	if authHeader != "" {
		fmt.Printf("Authorization: [configured]\n")
	}
	fmt.Printf("Duration: %v\n", testDuration)
	fmt.Printf("Submission workers: %d\n", workerCount)
	fmt.Printf("Proof workers: %d\n", proofWorkerCount)
	fmt.Printf("HTTP client pool size: %d\n", httpClientPoolSize)
	if enableH2C {
		fmt.Printf("H2C: enabled (HTTP/2 cleartext for plain HTTP)\n")
	} else {
		fmt.Printf("H2C: disabled (HTTP/1.1 for plain HTTP)\n")
	}
	fmt.Printf("Target RPS: %d\n", requestsPerSec)
	fmt.Printf("----------------------------------------\n")

	testWindowStart := time.Now()

	// Initialize metrics
	metrics := &Metrics{
		startTime: time.Now(),
	}
	rateCounters := &RequestRateCounters{}

	shardClients := buildShardClients(aggregatorURL, authHeader, metrics)
	metrics.initShardMetrics(len(shardClients))

	// Test connectivity and get starting block number for each shard
	for _, sc := range shardClients {
		fmt.Printf("Testing connectivity to %s...\n", sc.url)
		// Include shardId param so gateway proxies can route the request
		var blockHeightParams interface{}
		if sc.shardMask > 0 {
			blockHeightParams = map[string]interface{}{"shardId": fmt.Sprintf("%d", sc.shardMask)}
		}
		resp, err := sc.client.call("get_block_height", blockHeightParams)
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
	commitmentPool := make([]*api.CertificationRequest, poolSize)
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

	// === WARMUP PHASE ===
	fmt.Printf("========================================\n")
	fmt.Printf("WARMUP PHASE\n")
	fmt.Printf("========================================\n")
	warmupDuration := 3 * time.Second
	warmupPoolSize := int(float64(requestsPerSec) * warmupDuration.Seconds() * 1.1)
	fmt.Printf("Generating %d warmup commitments (%d RPS × %v + 10%% buffer)...\n", warmupPoolSize, requestsPerSec, warmupDuration)

	// Generate separate warmup pool
	warmupPool := make([]*api.CertificationRequest, warmupPoolSize)
	warmupJobs := make(chan int, workerCountPreGen*2)
	var warmupPreGenWG sync.WaitGroup
	for w := 0; w < workerCountPreGen; w++ {
		warmupPreGenWG.Add(1)
		go func() {
			defer warmupPreGenWG.Done()
			for idx := range warmupJobs {
				warmupPool[idx] = generateCommitmentRequest()
			}
		}()
	}
	for i := 0; i < warmupPoolSize; i++ {
		warmupJobs <- i
	}
	close(warmupJobs)
	warmupPreGenWG.Wait()
	fmt.Printf("✓ Generated %d warmup commitments\n", warmupPoolSize)
	fmt.Printf("Warming up servers...\n")

	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), warmupDuration)

	var warmupWg sync.WaitGroup
	var warmupSubmissionWg sync.WaitGroup
	var warmupPoolIndex atomic.Int64
	var warmupMetrics Metrics
	warmupMetrics.submittedStateIDs.Store("init", true) // Initialize map
	warmupMetrics.submissionStartTime = time.Now()

	// Start warmup workers with separate warmup pool
	for i := 0; i < workerCount; i++ {
		warmupWg.Add(1)
		go func() {
			defer warmupWg.Done()
			commitmentWorker(warmupCtx, shardClients, &warmupMetrics, nil, warmupPool, &warmupPoolIndex, nil, &warmupSubmissionWg)
		}()
	}

	// Wait for warmup to complete
	<-warmupCtx.Done()
	warmupCancel()

	// Wait for outstanding warmup requests
	warmupSubmissionWg.Wait()
	warmupWg.Wait()

	warmupTotal := atomic.LoadInt64(&warmupMetrics.totalRequests)
	warmupSuccess := atomic.LoadInt64(&warmupMetrics.successfulRequests)
	warmupFailed := atomic.LoadInt64(&warmupMetrics.failedRequests)
	warmupDurationActual := time.Since(warmupMetrics.submissionStartTime)

	// Calculate average submission latency during warmup
	var warmupAvgLatency time.Duration
	if warmupSuccess > 0 {
		warmupAvgLatency = time.Duration(atomic.LoadInt64(&warmupMetrics.failedRequests) / warmupSuccess)
	}

	fmt.Printf("✓ Warmup complete: %d submitted, %d successful, %d failed\n", warmupTotal, warmupSuccess, warmupFailed)
	fmt.Printf("  Duration: %v\n", warmupDurationActual.Round(time.Millisecond))
	fmt.Printf("  Submission latency - Avg: %v\n", warmupAvgLatency.Round(time.Millisecond))
	fmt.Printf("Waiting 5 seconds before starting actual test...\n\n")
	time.Sleep(5 * time.Second)

	// === ACTUAL TEST ===
	fmt.Printf("========================================\n")
	fmt.Printf("STARTING ACTUAL TEST\n")
	fmt.Printf("========================================\n\n")

	// Create contexts for actual test (AFTER warmup completes)
	submitCtx, submitCancel := context.WithTimeout(context.Background(), testDuration)
	defer submitCancel()

	proofTimeout := testDuration + 20*time.Second
	proofCtx, proofCancel := context.WithTimeout(context.Background(), proofTimeout)
	defer proofCancel()

	var poolIndex atomic.Int64
	var wg sync.WaitGroup
	var submissionWg sync.WaitGroup // Track outstanding submission requests

	// Record when submission actually starts
	metrics.submissionStartTime = time.Now()

	// Start commitment workers (use submitCtx - stops after testDuration)
	// These workers ONLY handle submissions - no proof requests
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commitmentWorker(submitCtx, shardClients, metrics, proofQueue, commitmentPool, &poolIndex, rateCounters, &submissionWg)
		}()
	}

	// Start SEPARATE proof verification workers (use proofCtx - runs longer)
	// These workers are dedicated to proof requests, so they don't compete with submissions
	var proofWg sync.WaitGroup
	for i := 0; i < proofWorkerCount; i++ {
		proofWg.Add(1)
		go func() {
			defer proofWg.Done()
			proofVerificationWorker(proofCtx, shardClients, metrics, proofQueue, rateCounters)
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

		// Wait for all outstanding submissions to complete
		fmt.Printf("\n----------------------------------------\n")
		fmt.Printf("Submission window closed; waiting for outstanding requests to complete...\n")
		submissionWg.Wait()

		metrics.submissionEndTime = time.Now()
		fmt.Printf("All submissions completed.\n")
	}()

	// Wait for all workers to complete (both submission and proof verification)
	// Note: Proof workers continue running for up to 20 seconds after submissions stop
	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Commitment submissions in progress; will verify proofs after...\n")

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

	wg.Wait()      // Wait for submission workers
	proofWg.Wait() // Wait for proof workers

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
	exists := atomic.LoadInt64(&metrics.stateIdExistsErr)

	fmt.Printf("\n\n========================================\n")
	fmt.Printf("PERFORMANCE TEST RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Total duration: %v\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("Submission duration: %v\n", submissionDuration.Truncate(time.Millisecond))
	fmt.Printf("Total requests: %d\n", total)
	fmt.Printf("Successful requests: %d\n", successful)
	fmt.Printf("Failed requests: %d\n", failed)
	fmt.Printf("STATE_ID_EXISTS: %d\n", exists)
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

	reportAggregatorServerStats(testWindowStart, time.Now(), shardClients)
}
