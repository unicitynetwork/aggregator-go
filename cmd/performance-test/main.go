package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
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
	defaultWorkerCount       = 20
	defaultProofWorkerCount  = 10 // Separate worker pool for proof requests
	defaultHTTPClientPool    = 4
	defaultRequestsPerSec    = 2000
	aggregatorLogPath        = "logs/aggregator.log"
	aggregatorLogPathsEnv    = "AGGREGATOR_LOG_PATHS"
	proofMaxRetries          = 10
	defaultProofRetryDelay   = 1000 * time.Millisecond
	defaultProofInitialDelay = 2500 * time.Millisecond
	defaultStartupProbeWait  = 60 * time.Second
	startupProbeInterval     = 250 * time.Millisecond
	defaultBufferSeconds     = 10
	defaultProofReadyMetric  = "aggregator_proof_readiness_seconds"
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
	testDuration       = getEnvDuration("TEST_DURATION", defaultTestDuration)
	requestsPerSec     = getEnvInt("REQUESTS_PER_SEC", defaultRequestsPerSec)
	workerCount        = getEnvInt("SUBMISSION_WORKERS", defaultWorkerCount)
	proofWorkerCount   = getEnvInt("PROOF_WORKERS", defaultProofWorkerCount)
	httpClientPoolSize = getEnvInt("HTTP_CLIENT_POOL_SIZE", defaultHTTPClientPool)
	proofRetryDelay    = getEnvDuration("PROOF_RETRY_DELAY", defaultProofRetryDelay)
	proofInitialDelay  = getEnvDuration("PROOF_INITIAL_DELAY", defaultProofInitialDelay)
	startupProbeWait   = getEnvDuration("STARTUP_PROBE_WAIT", defaultStartupProbeWait)
	requestWireFormat  = strings.ToLower(strings.TrimSpace(os.Getenv("CERTIFICATION_REQUEST_WIRE_FORMAT")))
	commitmentBuffer   = getCommitmentBufferSize()
	verifyProofs       = getEnvBool("VERIFY_PROOFS", true)
	verifyProofCrypto  = getEnvBool("VERIFY_PROOF_CRYPTO", true)
	shardingMode       = getShardingMode()
	shardTargets       = getEnvShardTargets()
	enableH2C          = os.Getenv("ENABLE_H2C") != "false"
	proofReadyMetric   = getEnvString("SERVER_PROOF_READINESS_METRIC", defaultProofReadyMetric)
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

func getCommitmentBufferSize() int {
	defaultVal := requestsPerSec * defaultBufferSeconds
	if defaultVal < 1000 {
		defaultVal = 1000
	}
	return getEnvInt("COMMITMENT_BUFFER_SIZE", defaultVal)
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

func getEnvBool(key string, defaultVal bool) bool {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}

func getEnvString(key string, defaultVal string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return defaultVal
}

func waitForStartingBlock(sc *ShardClient, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for attempt := 1; ; attempt++ {
		startingBlock, err := getStartingBlock(sc)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("✓ get_block_height succeeded for %s after %d attempts\n", sc.url, attempt)
			}
			return startingBlock, nil
		}

		lastErr = err
		if time.Now().Add(startupProbeInterval).After(deadline) {
			return 0, lastErr
		}

		if attempt == 1 || attempt%8 == 0 {
			fmt.Printf("Waiting for get_block_height from %s: %v\n", sc.url, err)
		}
		time.Sleep(startupProbeInterval)
	}
}

func getStartingBlock(sc *ShardClient) (int64, error) {
	// Include shardId param so gateway proxies can route the request.
	var blockHeightParams interface{}
	if shardingMode == shardingModeBFT && sc.shardBits != "" {
		blockHeightParams = map[string]interface{}{"shardId": sc.shardBits}
	} else if sc.shardMask > 0 {
		blockHeightParams = map[string]interface{}{"shardId": fmt.Sprintf("%d", sc.shardMask)}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := sc.client.callWithContext(ctx, "get_block_height", blockHeightParams)
	if err != nil {
		return 0, err
	}
	if resp.Error != nil {
		return 0, fmt.Errorf("JSON-RPC error: %s", resp.Error.Message)
	}

	var heightResp GetBlockHeightResponse
	respBytes, err := json.Marshal(resp.Result)
	if err != nil {
		return 0, fmt.Errorf("marshal block height response: %w", err)
	}
	if err := json.Unmarshal(respBytes, &heightResp); err != nil {
		return 0, fmt.Errorf("parse block height response: %w", err)
	}

	var startingBlockNumber int64
	if _, err := fmt.Sscanf(heightResp.BlockNumber, "%d", &startingBlockNumber); err != nil {
		return 0, fmt.Errorf("parse starting block number %q: %w", heightResp.BlockNumber, err)
	}
	return startingBlockNumber, nil
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

type proofReadinessHistogramSnapshot struct {
	metricName string
	buckets    map[float64]float64
	targets    int
}

func metricsEndpoint(target string) (string, error) {
	parsed, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("target URL must include scheme and host: %q", target)
	}
	parsed.Path = "/metrics"
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func scrapeProofReadinessHistogram(ctx context.Context, shardClients []*ShardClient, authHeader string) (*proofReadinessHistogramSnapshot, error) {
	if len(shardClients) == 0 {
		return nil, fmt.Errorf("no shard clients configured")
	}

	snapshot := &proofReadinessHistogramSnapshot{
		metricName: proofReadyMetric,
		buckets:    make(map[float64]float64),
	}
	var errors []string
	for _, sc := range shardClients {
		endpoint, err := metricsEndpoint(sc.url)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", sc.url, err))
			continue
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", endpoint, err))
			continue
		}
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}

		client := newHTTPClient(sc.url, nil)
		resp, err := client.Do(req)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", endpoint, err))
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", endpoint, readErr))
			continue
		}
		if closeErr != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", endpoint, closeErr))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			errors = append(errors, fmt.Sprintf("%s: HTTP %d", endpoint, resp.StatusCode))
			continue
		}

		buckets, err := parseProofReadinessBuckets(body, proofReadyMetric)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%s: %v", endpoint, err))
			continue
		}
		for le, value := range buckets {
			snapshot.buckets[le] += value
		}
		snapshot.targets++
	}

	if snapshot.targets == 0 {
		if len(errors) == 0 {
			return nil, fmt.Errorf("no %s_bucket metrics found", proofReadyMetric)
		}
		return nil, fmt.Errorf("no %s_bucket metrics found (%s)", proofReadyMetric, strings.Join(errors, "; "))
	}
	if len(errors) > 0 {
		fmt.Printf("Warning: skipped %d metrics target(s): %s\n", len(errors), strings.Join(errors, "; "))
	}
	return snapshot, nil
}

func parseProofReadinessBuckets(body []byte, metricName string) (map[float64]float64, error) {
	bucketMetric := metricName + "_bucket"
	buckets := make(map[float64]float64)
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, bucketMetric+"{") && !strings.HasPrefix(line, bucketMetric+" ") {
			continue
		}

		le, ok := parseHistogramLE(line)
		if !ok {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		buckets[le] += value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("metric %s not present", bucketMetric)
	}
	return buckets, nil
}

func parseHistogramLE(line string) (float64, bool) {
	idx := strings.Index(line, `le="`)
	if idx < 0 {
		return 0, false
	}
	rest := line[idx+len(`le="`):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return 0, false
	}
	raw := rest[:end]
	if raw == "+Inf" {
		return math.Inf(1), true
	}
	le, err := strconv.ParseFloat(raw, 64)
	return le, err == nil
}

func proofReadinessHistogramDelta(before, after *proofReadinessHistogramSnapshot) (*proofReadinessHistogramSnapshot, error) {
	if before == nil || after == nil {
		return nil, fmt.Errorf("missing before/after histogram snapshot")
	}
	if before.metricName != after.metricName {
		return nil, fmt.Errorf("metric changed from %s to %s", before.metricName, after.metricName)
	}
	delta := &proofReadinessHistogramSnapshot{
		metricName: after.metricName,
		buckets:    make(map[float64]float64),
		targets:    after.targets,
	}
	for le, afterValue := range after.buckets {
		value := afterValue - before.buckets[le]
		if value < 0 {
			return nil, fmt.Errorf("histogram bucket %v decreased from %.0f to %.0f", le, before.buckets[le], afterValue)
		}
		delta.buckets[le] = value
	}
	return delta, nil
}

func histogramTotal(buckets map[float64]float64) float64 {
	if total, ok := buckets[math.Inf(1)]; ok {
		return total
	}
	var total float64
	for le, value := range buckets {
		if !math.IsInf(le, 1) && value > total {
			total = value
		}
	}
	return total
}

func histogramCountLE(buckets map[float64]float64, target float64) float64 {
	var bestLE float64
	var bestValue float64
	found := false
	for le, value := range buckets {
		if math.IsInf(le, 1) || le > target {
			continue
		}
		if !found || le > bestLE {
			bestLE = le
			bestValue = value
			found = true
		}
	}
	if !found {
		return 0
	}
	return bestValue
}

func histogramQuantile(q float64, buckets map[float64]float64) float64 {
	total := histogramTotal(buckets)
	if total <= 0 {
		return math.NaN()
	}
	rank := q * total
	bounds := make([]float64, 0, len(buckets))
	for le := range buckets {
		if !math.IsInf(le, 1) {
			bounds = append(bounds, le)
		}
	}
	sort.Float64s(bounds)

	var prevBound float64
	var prevCount float64
	for _, bound := range bounds {
		count := buckets[bound]
		if count >= rank {
			bucketCount := count - prevCount
			if bucketCount <= 0 {
				return bound
			}
			fraction := (rank - prevCount) / bucketCount
			return prevBound + (bound-prevBound)*fraction
		}
		prevBound = bound
		prevCount = count
	}
	if len(bounds) > 0 {
		return bounds[len(bounds)-1]
	}
	return math.NaN()
}

func formatSeconds(seconds float64) string {
	if math.IsNaN(seconds) {
		return "n/a"
	}
	if math.IsInf(seconds, 1) {
		return "+Inf"
	}
	return time.Duration(seconds * float64(time.Second)).Truncate(time.Millisecond).String()
}

func printServerProofReadinessHistogram(before, after *proofReadinessHistogramSnapshot, beforeErr, afterErr error) {
	fmt.Printf("\nSERVER PROOF READINESS HISTOGRAM:\n")
	if beforeErr != nil {
		fmt.Printf("  unavailable before test: %v\n", beforeErr)
		return
	}
	if afterErr != nil {
		fmt.Printf("  unavailable after test: %v\n", afterErr)
		return
	}

	delta, err := proofReadinessHistogramDelta(before, after)
	if err != nil {
		fmt.Printf("  unavailable: %v\n", err)
		return
	}

	total := histogramTotal(delta.buckets)
	if total <= 0 {
		fmt.Printf("  no samples in test window for %s_bucket\n", delta.metricName)
		return
	}
	within1s := histogramCountLE(delta.buckets, 1.0)
	fmt.Printf("  Source: direct /metrics scrape, %s_bucket, %d target(s)\n", delta.metricName, delta.targets)
	fmt.Printf("  Samples: %.0f\n", total)
	fmt.Printf("  <=1s: %.0f/%.0f (%.1f%%)\n", within1s, total, within1s/total*100)
	fmt.Printf("  Global server proof readiness: p50 %s, p95 %s, p99 %s\n",
		formatSeconds(histogramQuantile(0.50, delta.buckets)),
		formatSeconds(histogramQuantile(0.95, delta.buckets)),
		formatSeconds(histogramQuantile(0.99, delta.buckets)))
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
		calculated, err := createStateIDForWireFormat(ownerPredicate, sourceStateHash)
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

// Wire-format hooks allow optional local overrides of request/response
// encoding. When no hook is registered, the standard JSON/CBOR path is used.
var (
	stateIDWireFormatHook func(ownerPredicate api.Predicate, sourceStateHash api.SourceStateHash) (api.StateID, bool, error)
	encodeParamsHook      func(method string, params interface{}) (interface{}, bool, error)
	decodeProofHook       func(respBytes []byte) (*api.GetInclusionProofResponseV2, bool, error)
)

func createStateIDForWireFormat(ownerPredicate api.Predicate, sourceStateHash api.SourceStateHash) (api.StateID, error) {
	if stateIDWireFormatHook != nil {
		if id, handled, err := stateIDWireFormatHook(ownerPredicate, sourceStateHash); handled {
			return id, err
		}
	}
	return api.CreateStateID(ownerPredicate, sourceStateHash)
}

func encodeJSONRPCParams(method string, params interface{}) (interface{}, error) {
	if encodeParamsHook != nil {
		if out, handled, err := encodeParamsHook(method, params); handled {
			return out, err
		}
	}
	return params, nil
}

func decodeProofResponse(result interface{}) (*api.GetInclusionProofResponseV2, error) {
	respBytes, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal proof response: %w", err)
	}
	if decodeProofHook != nil {
		if resp, handled, err := decodeProofHook(respBytes); handled {
			return resp, err
		}
	}
	var proofResp api.GetInclusionProofResponseV2
	if err := json.Unmarshal(respBytes, &proofResp); err != nil {
		return nil, err
	}
	return &proofResp, nil
}

func startCommitmentGenerator(ctx context.Context, total, bufferSize, workers, progressEvery int, label string) <-chan *api.CertificationRequest {
	if bufferSize < 1 {
		bufferSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	out := make(chan *api.CertificationRequest, bufferSize)
	jobs := make(chan struct{}, workers*2)
	var generated atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req := generateCommitmentRequest()
				select {
				case out <- req:
					count := generated.Add(1)
					if progressEvery > 0 && count%int64(progressEvery) == 0 {
						fmt.Printf("  Generated %d/%d %s commitments...\n", count, total, label)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for i := 0; i < total; i++ {
			select {
			case jobs <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Worker function that continuously submits commitments
func commitmentWorker(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, proofQueue chan proofJob, commitmentRequests <-chan *api.CertificationRequest, counters *RequestRateCounters, submissionWg *sync.WaitGroup) {
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
			var req *api.CertificationRequest
			var ok bool
			select {
			case <-ctx.Done():
				return
			case req, ok = <-commitmentRequests:
				if !ok {
					metrics.recordError("commitment generator exhausted")
					return
				}
				if req == nil {
					metrics.recordError("commitment generator returned nil request")
					return
				}
			}

			submissionWg.Add(1)
			go func(req *api.CertificationRequest) {
				defer submissionWg.Done()

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
					metrics.recordError(fmt.Sprintf("submit API error (code %d): %s", resp.Error.Code, resp.Error.Message))
					if resp.Error.Message == "STATE_ID_EXISTS" {
						atomic.AddInt64(&metrics.stateIdExistsErr, 1)
						if sm := metrics.shard(shardIdx); sm != nil {
							sm.stateIdExistsErr.Add(1)
						}
					}
					return
				}

				// Parse response
				var submitResp api.CertificationResponse
				respBytes, err := json.Marshal(resp.Result)
				if err != nil {
					atomic.AddInt64(&metrics.failedRequests, 1)
					if sm := metrics.shard(shardIdx); sm != nil {
						sm.failedRequests.Add(1)
					}
					metrics.recordError(fmt.Sprintf("submit failed: marshal response result: %v", err))
					return
				}
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

					if proofQueue != nil {
						submittedAt := time.Now()
						firstProofAt := submittedAt.Add(proofInitialDelay)
						job := proofJob{shardIdx: shardIdx, request: req, submittedAt: submittedAt, firstProofAt: firstProofAt}
						select {
						case proofQueue <- job:
							metrics.recordSubmissionTimestamp(stateIDStr, submittedAt)
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
			}(req)
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func verifyProofJob(ctx context.Context, shardClients []*ShardClient, metrics *Metrics, job proofJob, counters *RequestRateCounters) {
	if job.request == nil {
		metrics.recordError("Missing original request for proof verification")
		atomic.AddInt64(&metrics.proofVerifyFailed, 1)
		if sm := metrics.shard(job.shardIdx); sm != nil {
			sm.proofVerifyFailed.Add(1)
		}
		return
	}

	stateID := normalizeStateID(job.request.StateID.String())
	defer metrics.clearSubmissionTimestamp(stateID)
	shardIdx := job.shardIdx
	startTime := time.Now()
	client := shardClients[shardIdx].proofClient // Use separate proof client pool

	for attempt := 0; attempt < proofMaxRetries; attempt++ {
		if ctx.Err() != nil {
			return
		}

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

		requestStart := time.Now()
		if attempt == 0 && !job.submittedAt.IsZero() {
			firstStartLag := requestStart.Sub(job.submittedAt)
			schedulerLag := time.Duration(0)
			if !job.firstProofAt.IsZero() && requestStart.After(job.firstProofAt) {
				schedulerLag = requestStart.Sub(job.firstProofAt)
			}
			metrics.addProofStartTiming(firstStartLag, schedulerLag)
		}

		atomic.AddInt64(&metrics.proofActiveRequests, 1)
		if counters != nil {
			counters.IncProofStarted()
		}

		proofCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
			if !sleepOrDone(ctx, proofRetryDelay) {
				return
			}
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
			if !sleepOrDone(ctx, proofRetryDelay) {
				return
			}
			continue
		}

		proofResp, err := decodeProofResponse(resp.Result)
		if err != nil {
			metrics.recordError(fmt.Sprintf("Failed to parse proof response: %v", err))
			atomic.AddInt64(&metrics.proofFailed, 1)
			if sm := metrics.shard(shardIdx); sm != nil {
				sm.proofFailed.Add(1)
			}
			return
		}

		if proofResp.InclusionProof == nil || proofResp.InclusionProof.CertificationData == nil || proofResp.InclusionProof.CertificationData.TransactionHash == nil {
			if !sleepOrDone(ctx, proofRetryDelay) {
				return
			}
			continue
		}

		atomic.AddInt64(&metrics.proofSuccess, 1)
		if sm := metrics.shard(shardIdx); sm != nil {
			sm.proofSuccess.Add(1)
		}
		metrics.recordProofSuccessAttempt(attempt)
		metrics.addProofRequestDuration(requestDuration)

		submittedAt, hasSubmission := metrics.getSubmissionTimestamp(stateID)
		var totalLatency time.Duration
		if !job.submittedAt.IsZero() {
			totalLatency = time.Since(job.submittedAt)
		} else if hasSubmission {
			totalLatency = time.Since(submittedAt)
		} else {
			totalLatency = time.Since(startTime) + proofInitialDelay
		}
		metrics.addProofLatency(totalLatency)

		if !verifyProofCrypto {
			atomic.AddInt64(&metrics.proofVerified, 1)
			if sm := metrics.shard(shardIdx); sm != nil {
				sm.proofVerified.Add(1)
			}
			return
		}

		if err := proofverify.VerifyInclusionProofLocal(proofResp.InclusionProof, job.request); err != nil {
			if attempt < proofMaxRetries-1 {
				if !sleepOrDone(ctx, proofRetryDelay) {
					return
				}
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
	if sm := metrics.shard(job.shardIdx); sm != nil {
		sm.proofFailed.Add(1)
	}
}

func sleepUntilOrDone(ctx context.Context, deadline time.Time) bool {
	if deadline.IsZero() {
		return true
	}
	return sleepOrDone(ctx, time.Until(deadline))
}

func scheduleProofJobs(ctx context.Context, proofQueue <-chan proofJob, shardClients []*ShardClient, metrics *Metrics, counters *RequestRateCounters, proofJobsWg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-proofQueue:
			if !ok {
				return
			}

			proofJobsWg.Add(1)
			go func(job proofJob) {
				defer proofJobsWg.Done()
				if !sleepUntilOrDone(ctx, job.firstProofAt) {
					return
				}
				verifyProofJob(ctx, shardClients, metrics, job, counters)
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

func parseOptionalLogDuration(raw string) (time.Duration, bool, error) {
	if raw == "" {
		return 0, false, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, true, err
	}
	return duration, true, nil
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

		timestamp, err := time.Parse(time.RFC3339Nano, raw.Time)
		if err != nil {
			continue
		}
		if timestamp.Before(start) || timestamp.After(end) || raw.Block == "" {
			continue
		}

		if raw.Msg != "Round completed" {
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

		finalizeScan, hasFinalizeScan, err := parseOptionalLogDuration(raw.FinalizeScan)
		if err != nil {
			continue
		}
		finalizeConvert, hasFinalizeConvert, err := parseOptionalLogDuration(raw.FinalizeConvert)
		if err != nil {
			continue
		}
		finalizeStoreBlock, hasFinalizeStoreBlock, err := parseOptionalLogDuration(raw.FinalizeStoreBlock)
		if err != nil {
			continue
		}
		finalizeStoreBlockDoc, hasFinalizeStoreBlockDoc, err := parseOptionalLogDuration(raw.FinalizeStoreBlockDoc)
		if err != nil {
			continue
		}
		finalizeStoreBlockRecords, hasFinalizeStoreBlockRecords, err := parseOptionalLogDuration(raw.FinalizeStoreBlockRecords)
		if err != nil {
			continue
		}
		finalizeStoreData, hasFinalizeStoreData, err := parseOptionalLogDuration(raw.FinalizeStoreData)
		if err != nil {
			continue
		}
		finalizeStoreSmt, hasFinalizeStoreSmt, err := parseOptionalLogDuration(raw.FinalizeStoreSmt)
		if err != nil {
			continue
		}
		finalizeStoreRecords, hasFinalizeStoreRecords, err := parseOptionalLogDuration(raw.FinalizeStoreRecords)
		if err != nil {
			continue
		}
		finalizeLockWait, hasFinalizeLockWait, err := parseOptionalLogDuration(raw.FinalizeLockWait)
		if err != nil {
			continue
		}
		finalizeSmtCommit, hasFinalizeSmtCommit, err := parseOptionalLogDuration(raw.FinalizeSmtCommit)
		if err != nil {
			continue
		}
		finalizeSmtCommitCollect, hasFinalizeSmtCommitCollect, err := parseOptionalLogDuration(raw.FinalizeSmtCommitCollect)
		if err != nil {
			continue
		}
		finalizeSmtCommitTombstone, hasFinalizeSmtCommitTombstone, err := parseOptionalLogDuration(raw.FinalizeSmtCommitTombstone)
		if err != nil {
			continue
		}
		finalizeSmtCommitBatchBuild, hasFinalizeSmtCommitBatchBuild, err := parseOptionalLogDuration(raw.FinalizeSmtCommitBatchBuild)
		if err != nil {
			continue
		}
		finalizeSmtCommitRootHash, hasFinalizeSmtCommitRootHash, err := parseOptionalLogDuration(raw.FinalizeSmtCommitRootHash)
		if err != nil {
			continue
		}
		finalizeSmtCommitEngineWrite, hasFinalizeSmtCommitEngineWrite, err := parseOptionalLogDuration(raw.FinalizeSmtCommitEngineWrite)
		if err != nil {
			continue
		}
		finalizeSmtCommitCacheUpdate, hasFinalizeSmtCommitCacheUpdate, err := parseOptionalLogDuration(raw.FinalizeSmtCommitCacheUpdate)
		if err != nil {
			continue
		}
		finalizeSetFinalized, hasFinalizeSetFinalized, err := parseOptionalLogDuration(raw.FinalizeSetFinalized)
		if err != nil {
			continue
		}
		finalizeAck, hasFinalizeAck, err := parseOptionalLogDuration(raw.FinalizeAck)
		if err != nil {
			continue
		}
		hasFinalizationBreakdown := hasFinalizeScan || hasFinalizeConvert || hasFinalizeStoreBlock || hasFinalizeStoreBlockDoc || hasFinalizeStoreBlockRecords || hasFinalizeStoreData || hasFinalizeStoreSmt || hasFinalizeStoreRecords || hasFinalizeLockWait || hasFinalizeSmtCommit || hasFinalizeSmtCommitCollect || hasFinalizeSmtCommitTombstone || hasFinalizeSmtCommitBatchBuild || hasFinalizeSmtCommitRootHash || hasFinalizeSmtCommitEngineWrite || hasFinalizeSmtCommitCacheUpdate || hasFinalizeSetFinalized || hasFinalizeAck

		proposalToProofReadyDur, hasProposalToProofReady, err := parseOptionalLogDuration(raw.ProposalToProofReady)
		if err != nil {
			continue
		}

		medianDur, hasMedian, err := parseOptionalLogDuration(raw.ProofReadyMedian)
		if err != nil {
			continue
		}
		p95Dur, hasP95, err := parseOptionalLogDuration(raw.ProofReadyP95)
		if err != nil {
			continue
		}
		p99Dur, hasP99, err := parseOptionalLogDuration(raw.ProofReadyP99)
		if err != nil {
			continue
		}
		hasProofReady := hasMedian || hasP95 || hasP99
		if hasProofReady && (!hasMedian || !hasP95 || !hasP99) {
			continue
		}

		summary := aggregatorRoundSummary{
			Timestamp:                    timestamp,
			Block:                        raw.Block,
			Commitments:                  raw.Commitments,
			RoundTime:                    roundDur,
			Processing:                   procDur,
			BftWait:                      bftDur,
			Finalization:                 finalDur,
			HasFinalizationBreakdown:     hasFinalizationBreakdown,
			FinalizeScan:                 finalizeScan,
			FinalizeConvert:              finalizeConvert,
			FinalizeStoreBlock:           finalizeStoreBlock,
			FinalizeStoreBlockDoc:        finalizeStoreBlockDoc,
			FinalizeStoreBlockRecords:    finalizeStoreBlockRecords,
			FinalizeStoreData:            finalizeStoreData,
			FinalizeStoreSmt:             finalizeStoreSmt,
			FinalizeStoreRecords:         finalizeStoreRecords,
			FinalizeLockWait:             finalizeLockWait,
			FinalizeSmtCommit:            finalizeSmtCommit,
			FinalizeSmtCommitCollect:     finalizeSmtCommitCollect,
			FinalizeSmtCommitTombstone:   finalizeSmtCommitTombstone,
			FinalizeSmtCommitBatchBuild:  finalizeSmtCommitBatchBuild,
			FinalizeSmtCommitRootHash:    finalizeSmtCommitRootHash,
			FinalizeSmtCommitEngineWrite: finalizeSmtCommitEngineWrite,
			FinalizeSmtCommitCacheUpdate: finalizeSmtCommitCacheUpdate,
			FinalizeSmtCommitNodeWrites:  raw.FinalizeSmtCommitNodeWrites,
			FinalizeSmtCommitNodeDeletes: raw.FinalizeSmtCommitNodeDeletes,
			FinalizeSetFinalized:         finalizeSetFinalized,
			FinalizeAck:                  finalizeAck,
			HasProposalToProofReady:      hasProposalToProofReady,
			ProposalToProofReady:         proposalToProofReadyDur,
			HasProofReady:                hasProofReady,
			ProofMedian:                  medianDur,
			ProofP95:                     p95Dur,
			ProofP99:                     p99Dur,
			RedisTotal:                   raw.RedisTotal,
			RedisPending:                 raw.RedisPending,
		}
		summaries = append(summaries, summary)
	}

	if err := scanner.Err(); err != nil {
		return summaries, err
	}

	return summaries, nil
}

func parseAggregatorPrecollectorLogs(path string, start, end time.Time) ([]aggregatorPrecollectorSummary, error) {
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

	var summaries []aggregatorPrecollectorSummary
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		var raw aggregatorLogRaw
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		if raw.Msg != "Precollector prepared" && raw.Msg != "Precollector advanced" {
			continue
		}

		timestamp, err := time.Parse(time.RFC3339Nano, raw.Time)
		if err != nil {
			continue
		}
		if timestamp.Before(start) || timestamp.After(end) {
			continue
		}

		advanceFlush, _, err := parseOptionalLogDuration(raw.AdvanceFlush)
		if err != nil {
			continue
		}
		flushTotal, _, err := parseOptionalLogDuration(raw.FlushTotal)
		if err != nil {
			continue
		}
		flushMax, _, err := parseOptionalLogDuration(raw.FlushMax)
		if err != nil {
			continue
		}
		stageTotal, _, err := parseOptionalLogDuration(raw.StageTotal)
		if err != nil {
			continue
		}
		stageMax, _, err := parseOptionalLogDuration(raw.StageMax)
		if err != nil {
			continue
		}
		fork, _, err := parseOptionalLogDuration(raw.Fork)
		if err != nil {
			continue
		}
		total, _, err := parseOptionalLogDuration(raw.Total)
		if err != nil {
			continue
		}

		summaries = append(summaries, aggregatorPrecollectorSummary{
			Timestamp:        timestamp,
			Prepared:         raw.Msg == "Precollector prepared",
			Advance:          raw.Msg == "Precollector advanced",
			Commitments:      raw.Commitments,
			Leaves:           raw.Leaves,
			PendingAtAdvance: raw.PendingAtAdvance,
			TailMerged:       raw.TailMerged,
			TailRemaining:    raw.TailRemaining,
			AlreadyPrepared:  raw.AlreadyPrepared,
			AdvanceFlush:     advanceFlush,
			FlushCalls:       raw.FlushCalls,
			FlushAdded:       raw.FlushAdded,
			FlushTotal:       flushTotal,
			FlushMax:         flushMax,
			StageCalls:       raw.StageCalls,
			StageAdded:       raw.StageAdded,
			StageTotal:       stageTotal,
			StageMax:         stageMax,
			Fork:             fork,
			Total:            total,
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

type aggregatorPrecollectorSummary struct {
	Timestamp        time.Time
	Prepared         bool
	Advance          bool
	Commitments      int
	Leaves           int
	PendingAtAdvance int
	TailMerged       int
	TailRemaining    int
	AlreadyPrepared  bool
	AdvanceFlush     time.Duration
	FlushCalls       int
	FlushAdded       int
	FlushTotal       time.Duration
	FlushMax         time.Duration
	StageCalls       int
	StageAdded       int
	StageTotal       time.Duration
	StageMax         time.Duration
	Fork             time.Duration
	Total            time.Duration
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

func printFinalizationBreakdownSummary(label string, entries []aggregatorRoundSummary) {
	prefix := "Average"
	if label != "" {
		prefix = label + " average"
	}

	withBreakdown := make([]aggregatorRoundSummary, 0, len(entries))
	for _, entry := range entries {
		if entry.HasFinalizationBreakdown {
			withBreakdown = append(withBreakdown, entry)
		}
	}
	if len(withBreakdown) == 0 {
		return
	}

	var scanSum, convertSum, storeBlockSum, storeBlockDocSum, storeBlockRecordsSum, storeDataSum time.Duration
	var storeSmtSum, storeRecordsSum, lockWaitSum time.Duration
	var smtCommitSum, setFinalizedSum, ackSum time.Duration
	var smtCommitCollectSum, smtCommitTombstoneSum, smtCommitBatchBuildSum time.Duration
	var smtCommitRootHashSum, smtCommitEngineWriteSum, smtCommitCacheUpdateSum time.Duration
	var smtCommitNodeWritesSum, smtCommitNodeDeletesSum int
	for _, entry := range withBreakdown {
		scanSum += entry.FinalizeScan
		convertSum += entry.FinalizeConvert
		storeBlockSum += entry.FinalizeStoreBlock
		storeBlockDocSum += entry.FinalizeStoreBlockDoc
		storeBlockRecordsSum += entry.FinalizeStoreBlockRecords
		storeDataSum += entry.FinalizeStoreData
		storeSmtSum += entry.FinalizeStoreSmt
		storeRecordsSum += entry.FinalizeStoreRecords
		lockWaitSum += entry.FinalizeLockWait
		smtCommitSum += entry.FinalizeSmtCommit
		smtCommitCollectSum += entry.FinalizeSmtCommitCollect
		smtCommitTombstoneSum += entry.FinalizeSmtCommitTombstone
		smtCommitBatchBuildSum += entry.FinalizeSmtCommitBatchBuild
		smtCommitRootHashSum += entry.FinalizeSmtCommitRootHash
		smtCommitEngineWriteSum += entry.FinalizeSmtCommitEngineWrite
		smtCommitCacheUpdateSum += entry.FinalizeSmtCommitCacheUpdate
		smtCommitNodeWritesSum += entry.FinalizeSmtCommitNodeWrites
		smtCommitNodeDeletesSum += entry.FinalizeSmtCommitNodeDeletes
		setFinalizedSum += entry.FinalizeSetFinalized
		ackSum += entry.FinalizeAck
	}

	count := time.Duration(len(withBreakdown))
	intCount := len(withBreakdown)
	fmt.Printf("%s finalization breakdown: scan=%v convert=%v storeBlock=%v (blockDoc=%v blockRecords=%v) storeData=%v (smt=%v records=%v) lockWait=%v smtCommit=%v (collect=%v tombstone=%v batchBuild=%v rootHash=%v engineWrite=%v cacheUpdate=%v nodeWrites=%d nodeDeletes=%d) setFinalized=%v ack=%v (%d rounds)\n",
		prefix,
		(scanSum / count).Truncate(time.Millisecond),
		(convertSum / count).Truncate(time.Millisecond),
		(storeBlockSum / count).Truncate(time.Millisecond),
		(storeBlockDocSum / count).Truncate(time.Millisecond),
		(storeBlockRecordsSum / count).Truncate(time.Millisecond),
		(storeDataSum / count).Truncate(time.Millisecond),
		(storeSmtSum / count).Truncate(time.Millisecond),
		(storeRecordsSum / count).Truncate(time.Millisecond),
		(lockWaitSum / count).Truncate(time.Millisecond),
		(smtCommitSum / count).Truncate(time.Millisecond),
		(smtCommitCollectSum / count).Truncate(time.Millisecond),
		(smtCommitTombstoneSum / count).Truncate(time.Millisecond),
		(smtCommitBatchBuildSum / count).Truncate(time.Millisecond),
		(smtCommitRootHashSum / count).Truncate(time.Millisecond),
		(smtCommitEngineWriteSum / count).Truncate(time.Millisecond),
		(smtCommitCacheUpdateSum / count).Truncate(time.Millisecond),
		smtCommitNodeWritesSum/intCount,
		smtCommitNodeDeletesSum/intCount,
		(setFinalizedSum / count).Truncate(time.Millisecond),
		(ackSum / count).Truncate(time.Millisecond),
		len(withBreakdown))
}

func printPrecollectorBreakdownSummary(label string, entries []aggregatorPrecollectorSummary) {
	prefix := "Average"
	if label != "" {
		prefix = label + " average"
	}
	if len(entries) == 0 {
		return
	}

	var prepareCount, advanceCount int
	var prepareTotal, prepareFlushTotal, prepareStageTotal, prepareFork time.Duration
	var prepareCommitments, prepareLeaves, prepareFlushCalls, prepareFlushAdded, prepareStageCalls, prepareStageAdded int
	var advanceTotal, advanceFlush, advanceFlushTotal, advanceFlushMax, advanceStageTotal, advanceStageMax, advanceFork time.Duration
	var advanceCommitments, advanceLeaves, advancePending, advanceTailMerged, advanceTailRemaining int
	var advanceFlushCalls, advanceFlushAdded, advanceStageCalls, advanceStageAdded, alreadyPrepared int

	for _, entry := range entries {
		if entry.Prepared {
			prepareCount++
			prepareTotal += entry.Total
			prepareFlushTotal += entry.FlushTotal
			prepareStageTotal += entry.StageTotal
			prepareFork += entry.Fork
			prepareCommitments += entry.Commitments
			prepareLeaves += entry.Leaves
			prepareFlushCalls += entry.FlushCalls
			prepareFlushAdded += entry.FlushAdded
			prepareStageCalls += entry.StageCalls
			prepareStageAdded += entry.StageAdded
		}
		if entry.Advance {
			advanceCount++
			advanceTotal += entry.Total
			advanceFlush += entry.AdvanceFlush
			advanceFlushTotal += entry.FlushTotal
			if entry.FlushMax > advanceFlushMax {
				advanceFlushMax = entry.FlushMax
			}
			advanceStageTotal += entry.StageTotal
			if entry.StageMax > advanceStageMax {
				advanceStageMax = entry.StageMax
			}
			advanceFork += entry.Fork
			advanceCommitments += entry.Commitments
			advanceLeaves += entry.Leaves
			advancePending += entry.PendingAtAdvance
			advanceTailMerged += entry.TailMerged
			advanceTailRemaining += entry.TailRemaining
			advanceFlushCalls += entry.FlushCalls
			advanceFlushAdded += entry.FlushAdded
			advanceStageCalls += entry.StageCalls
			advanceStageAdded += entry.StageAdded
			if entry.AlreadyPrepared {
				alreadyPrepared++
			}
		}
	}

	if prepareCount > 0 {
		count := time.Duration(prepareCount)
		fmt.Printf("%s precollector prepare: total=%v snapshotAdd/materialize=%v durableStage=%v fork=%v commitments=%.0f leaves=%.0f flushCalls=%.1f flushAdded=%.0f stageCalls=%.1f stageAdded=%.0f (%d prepares)\n",
			prefix,
			(prepareTotal / count).Truncate(time.Millisecond),
			(prepareFlushTotal / count).Truncate(time.Millisecond),
			(prepareStageTotal / count).Truncate(time.Millisecond),
			(prepareFork / count).Truncate(time.Millisecond),
			float64(prepareCommitments)/float64(prepareCount),
			float64(prepareLeaves)/float64(prepareCount),
			float64(prepareFlushCalls)/float64(prepareCount),
			float64(prepareFlushAdded)/float64(prepareCount),
			float64(prepareStageCalls)/float64(prepareCount),
			float64(prepareStageAdded)/float64(prepareCount),
			prepareCount)
	}
	if advanceCount > 0 {
		count := time.Duration(advanceCount)
		fmt.Printf("%s precollector advance: total=%v advanceFlush=%v cumulativeSnapshotAdd/materialize=%v flushMax=%v cumulativeDurableStage=%v stageMax=%v fork=%v commitments=%.0f pendingAtAdvance=%.0f tailMerged=%.0f tailRemaining=%.0f alreadyPrepared=%d/%d flushCalls=%.1f flushAdded=%.0f stageCalls=%.1f stageAdded=%.0f (%d advances)\n",
			prefix,
			(advanceTotal / count).Truncate(time.Millisecond),
			(advanceFlush / count).Truncate(time.Millisecond),
			(advanceFlushTotal / count).Truncate(time.Millisecond),
			advanceFlushMax.Truncate(time.Millisecond),
			(advanceStageTotal / count).Truncate(time.Millisecond),
			advanceStageMax.Truncate(time.Millisecond),
			(advanceFork / count).Truncate(time.Millisecond),
			float64(advanceCommitments)/float64(advanceCount),
			float64(advancePending)/float64(advanceCount),
			float64(advanceTailMerged)/float64(advanceCount),
			float64(advanceTailRemaining)/float64(advanceCount),
			alreadyPrepared,
			advanceCount,
			float64(advanceFlushCalls)/float64(advanceCount),
			float64(advanceFlushAdded)/float64(advanceCount),
			float64(advanceStageCalls)/float64(advanceCount),
			float64(advanceStageAdded)/float64(advanceCount),
			advanceCount)
	}
}

func printAggregatorAverages(label string, entries []aggregatorRoundSummary) {
	prefix := "Average"
	if label != "" {
		prefix = label + " average"
	}

	if len(entries) == 0 {
		return
	}

	var roundSum, finalSum, procSum, bftSum time.Duration
	var proposalToProofReadySum time.Duration
	var proofMedSum, proofP95Sum, proofP99Sum time.Duration
	totalCommitments := 0
	proposalToProofReadyCount := 0
	proofCount := 0
	for _, entry := range entries {
		roundSum += entry.RoundTime
		finalSum += entry.Finalization
		procSum += entry.Processing
		bftSum += entry.BftWait
		totalCommitments += entry.Commitments
		if entry.HasProposalToProofReady {
			proposalToProofReadySum += entry.ProposalToProofReady
			proposalToProofReadyCount++
		}
		if entry.HasProofReady {
			proofMedSum += entry.ProofMedian
			proofP95Sum += entry.ProofP95
			proofP99Sum += entry.ProofP99
			proofCount++
		}
	}

	count := time.Duration(len(entries))
	avgFinal := finalSum / count
	avgProcessing := procSum / count
	avgBft := bftSum / count
	avgCommit := float64(totalCommitments) / float64(len(entries))

	finalPct := 0.0
	procPct := 0.0
	bftPct := 0.0
	if roundSum > 0 {
		finalPct = float64(finalSum) / float64(roundSum) * 100
		procPct = float64(procSum) / float64(roundSum) * 100
		bftPct = float64(bftSum) / float64(roundSum) * 100
	}

	fmt.Printf("%s round time: %v\n", prefix, (roundSum / count).Truncate(time.Millisecond))
	fmt.Printf("%s finalization time: %v (%.1f%% of round time)\n", prefix, avgFinal.Truncate(time.Millisecond), finalPct)
	fmt.Printf("%s commitments per round: %.0f\n", prefix, avgCommit)
	fmt.Printf("%s processing time: %v (%.1f%% of round time)\n", prefix, avgProcessing.Truncate(time.Millisecond), procPct)
	fmt.Printf("%s BFT wait: %v (%.1f%% of round time)\n", prefix, avgBft.Truncate(time.Millisecond), bftPct)
	if proposalToProofReadyCount > 0 {
		proposalToProofReadyCountDuration := time.Duration(proposalToProofReadyCount)
		fmt.Printf("%s proposal to proof-ready: %v (%d rounds)\n",
			prefix,
			(proposalToProofReadySum / proposalToProofReadyCountDuration).Truncate(time.Millisecond),
			proposalToProofReadyCount)
	} else {
		fmt.Printf("%s proposal to proof-ready: n/a (no proposal-to-proof-ready rounds in window)\n", prefix)
	}
	if proofCount > 0 {
		proofCountDuration := time.Duration(proofCount)
		fmt.Printf("%s proof readiness: median %v, p95 %v, p99 %v (%d rounds)\n",
			prefix,
			(proofMedSum / proofCountDuration).Truncate(time.Millisecond),
			(proofP95Sum / proofCountDuration).Truncate(time.Millisecond),
			(proofP99Sum / proofCountDuration).Truncate(time.Millisecond),
			proofCount)
	} else {
		fmt.Printf("%s proof readiness: n/a (no proof-ready rounds in window)\n", prefix)
	}
	printFinalizationBreakdownSummary(label, entries)
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

	fmt.Printf("\nAGGREGATOR SERVER STATS [%s] (%d rounds, averages exclude first/last when possible)\n", header, len(ordered))
	printAggregatorAverages("", usable)

	active := make([]aggregatorRoundSummary, 0, len(usable))
	for _, entry := range usable {
		if entry.Commitments > 0 {
			active = append(active, entry)
		}
	}
	if len(active) > 0 && len(active) != len(usable) {
		printAggregatorAverages("Active", active)
	}

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
	combinedPrecollector := make([]aggregatorPrecollectorSummary, 0)
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

		precollectorSummaries, err := parseAggregatorPrecollectorLogs(source.path, start, end)
		if err != nil {
			readErrors = append(readErrors, fmt.Sprintf("failed to read precollector logs from %s (%s): %v", source.path, source.label, err))
		}

		foundSources++
		combined = append(combined, summaries...)
		combinedPrecollector = append(combinedPrecollector, precollectorSummaries...)
		printAggregatorServerStatsSummary(fmt.Sprintf("%s (%s)", source.label, source.path), summaries)
		printPrecollectorBreakdownSummary(fmt.Sprintf("%s (%s)", source.label, source.path), precollectorSummaries)
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
		printPrecollectorBreakdownSummary("combined", combinedPrecollector)
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
	if verifyProofs {
		fmt.Printf("Proof scheduling: exact per-submission timer (PROOF_WORKERS ignored, value=%d)\n", proofWorkerCount)
		fmt.Printf("Proof initial delay: %v\n", proofInitialDelay)
		fmt.Printf("Proof retry delay: %v\n", proofRetryDelay)
		fmt.Printf("Server proof-readiness metric: %s_bucket (direct /metrics scrape)\n", proofReadyMetric)
		if !verifyProofCrypto {
			fmt.Printf("Proof crypto verification: disabled\n")
		}
	} else {
		fmt.Printf("Proof verification: disabled\n")
	}
	if requestWireFormat != "" {
		fmt.Printf("Certification request wire format: %s\n", requestWireFormat)
	}
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
		startingBlockNumber, err := waitForStartingBlock(sc, startupProbeWait)
		if err != nil {
			log.Fatalf("Failed to connect to aggregator at %s after %v: %v", sc.url, startupProbeWait, err)
		}
		sc.startingBlock = startingBlockNumber

		fmt.Printf("✓ Connected successfully to %s\n", sc.url)
		fmt.Printf("✓ Starting block number for %s: %d\n", sc.url, startingBlockNumber)
	}

	// proofQueue receives successful submissions. Each proof job owns its
	// submittedAt+PROOF_INITIAL_DELAY timer so first-attempt latency matches an
	// external client polling at that exact delay.
	var proofQueue chan proofJob
	if verifyProofs {
		proofQueue = make(chan proofJob, 10000)
	}

	// Start a bounded commitment generator so client-side crypto stays ahead
	// of submission without retaining the full test duration in memory.
	poolSize := int(float64(requestsPerSec) * testDuration.Seconds() * 1.1)
	workerCountPreGen := runtime.NumCPU()
	if workerCountPreGen < 1 {
		workerCountPreGen = 1
	}
	if workerCountPreGen > 32 {
		workerCountPreGen = 32
	}
	fmt.Printf("\nStarting bounded commitment generator: total=%d buffer=%d workers=%d\n\n",
		poolSize, commitmentBuffer, workerCountPreGen)
	generatorCtx, generatorCancel := context.WithCancel(context.Background())
	defer generatorCancel()
	commitmentRequests := startCommitmentGenerator(generatorCtx, poolSize, commitmentBuffer, workerCountPreGen, 100000, "test")

	// === WARMUP PHASE ===
	fmt.Printf("========================================\n")
	fmt.Printf("WARMUP PHASE\n")
	fmt.Printf("========================================\n")
	warmupDuration := 3 * time.Second
	warmupPoolSize := int(float64(requestsPerSec) * warmupDuration.Seconds() * 1.1)
	fmt.Printf("Starting warmup commitment generator: total=%d buffer=%d workers=%d\n",
		warmupPoolSize, warmupPoolSize, workerCountPreGen)
	warmupGeneratorCtx, warmupGeneratorCancel := context.WithCancel(context.Background())
	warmupRequests := startCommitmentGenerator(warmupGeneratorCtx, warmupPoolSize, warmupPoolSize, workerCountPreGen, 0, "warmup")
	defer warmupGeneratorCancel()
	fmt.Printf("Warming up servers...\n")

	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), warmupDuration)

	var warmupWg sync.WaitGroup
	var warmupSubmissionWg sync.WaitGroup
	var warmupMetrics Metrics
	warmupMetrics.submissionStartTime = time.Now()

	// Start warmup workers with separate warmup pool
	for i := 0; i < workerCount; i++ {
		warmupWg.Add(1)
		go func() {
			defer warmupWg.Done()
			commitmentWorker(warmupCtx, shardClients, &warmupMetrics, nil, warmupRequests, nil, &warmupSubmissionWg)
		}()
	}

	// Wait for warmup to complete
	<-warmupCtx.Done()
	warmupCancel()
	warmupGeneratorCancel()

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

	var wg sync.WaitGroup
	var submissionWg sync.WaitGroup // Track outstanding submission requests

	metricsScrapeCtx, metricsScrapeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	proofReadyBefore, proofReadyBeforeErr := scrapeProofReadinessHistogram(metricsScrapeCtx, shardClients, authHeader)
	metricsScrapeCancel()
	if proofReadyBeforeErr != nil {
		fmt.Printf("Server proof-readiness histogram before test unavailable: %v\n", proofReadyBeforeErr)
	}

	// Record when submission actually starts
	metrics.submissionStartTime = time.Now()

	// Start commitment workers (use submitCtx - stops after testDuration)
	// These workers ONLY handle submissions - no proof requests
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commitmentWorker(submitCtx, shardClients, metrics, proofQueue, commitmentRequests, rateCounters, &submissionWg)
		}()
	}

	// Start proof scheduling separately from submissions. The scheduler gives
	// each successful submission its own absolute first-proof timestamp.
	var proofDispatchWg sync.WaitGroup
	var proofJobsWg sync.WaitGroup
	if verifyProofs {
		proofDispatchWg.Add(1)
		go func() {
			defer proofDispatchWg.Done()
			scheduleProofJobs(proofCtx, proofQueue, shardClients, metrics, rateCounters, &proofJobsWg)
		}()
	}

	perfLogCtx := proofCtx
	plannedRequests := int64(float64(requestsPerSec) * testDuration.Seconds())
	if plannedRequests <= 0 {
		plannedRequests = int64(poolSize)
	}
	go logClientPerfRates(perfLogCtx, metrics, rateCounters, plannedRequests, shardClients)

	// Wait for all workers to complete (both submission and proof verification).
	// Proof jobs continue running for up to proofTimeout after submissions stop.
	fmt.Printf("\n----------------------------------------\n")
	if verifyProofs {
		fmt.Printf("Commitment submissions in progress; will verify proofs after...\n")
	} else {
		fmt.Printf("Commitment submissions in progress; proof verification disabled...\n")
	}

	wg.Wait() // Wait for submission workers to stop accepting new work

	fmt.Printf("\n----------------------------------------\n")
	fmt.Printf("Submission window closed; waiting for outstanding requests to complete...\n")
	submissionWg.Wait()
	metrics.submissionEndTime = time.Now()
	if verifyProofs {
		close(proofQueue)
	}
	fmt.Printf("All submissions completed.\n")

	if verifyProofs {
		proofDispatchWg.Wait() // Wait for scheduler to drain proofQueue
		proofJobsWg.Wait()     // Wait for delayed proof jobs to finish request/retry/verification
	}
	proofCancel()

	metricsScrapeCtx, metricsScrapeCancel = context.WithTimeout(context.Background(), 10*time.Second)
	proofReadyAfter, proofReadyAfterErr := scrapeProofReadinessHistogram(metricsScrapeCtx, shardClients, authHeader)
	metricsScrapeCancel()

	// Stop submission phase and get counts
	fmt.Printf("\n----------------------------------------\n")
	successful := atomic.LoadInt64(&metrics.successfulRequests)
	verified := atomic.LoadInt64(&metrics.proofVerified)
	proofFailedCount := atomic.LoadInt64(&metrics.proofFailed)
	if !verifyProofs {
		fmt.Printf("Proof verification skipped.\n")
	} else if verified+proofFailedCount < successful {
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

	if verifyProofs {
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

			firstStartMedian, firstStartP95, firstStartP99, schedulerMedian, schedulerP95, schedulerP99 := metrics.getProofStartTimingStats()
			if firstStartMedian > 0 || firstStartP95 > 0 || firstStartP99 > 0 {
				fmt.Printf("First proof request start lag: median %v, p95 %v, p99 %v\n",
					firstStartMedian.Truncate(time.Millisecond),
					firstStartP95.Truncate(time.Millisecond),
					firstStartP99.Truncate(time.Millisecond))
				fmt.Printf("Proof scheduler lag after target: median %v, p95 %v, p99 %v\n",
					schedulerMedian.Truncate(time.Millisecond),
					schedulerP95.Truncate(time.Millisecond),
					schedulerP99.Truncate(time.Millisecond))
			}

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
	}

	printServerProofReadinessHistogram(proofReadyBefore, proofReadyAfter, proofReadyBeforeErr, proofReadyAfterErr)

	fmt.Printf("========================================\n")

	printShardFinalReport(metrics, shardClients)

	// Print error summary
	metrics.printErrorSummary()

	reportAggregatorServerStats(testWindowStart, time.Now(), shardClients)
}
