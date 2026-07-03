// Command perf-blackbox is a pure black-box load/verification client for a
// deployed aggregator (or subscription gateway fronting several shards).
//
// It speaks exactly the wire protocol the state-transition SDK speaks:
//   - certification_request  params = hex(CBOR(CertificationRequest))  (tag 39030, v1)
//   - get_inclusion_proof.v2 params = {"stateId":"<32-byte hex>"}
//   - X-API-Key   header carries the shared API key
//   - X-State-ID  header carries the 32-byte state id hex (the gateway routes on it)
//
// It scrapes NO /metrics and reads NO server logs. Everything reported is
// observed from the client: submission results, and — after a delay — inclusion
// proofs fetched back and cryptographically verified, to confirm every accepted
// commitment actually landed in the SMT.
//
// Commitments are uniform: each carries a fresh key pair and random state, so
// the derived state ids are uniformly distributed and the gateway spreads them
// evenly across all shards. No shard rejection-sampling is done.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"golang.org/x/net/http2"

	"github.com/unicitynetwork/aggregator-go/internal/proofverify"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type config struct {
	url              string
	apiKey           string
	rate             int
	duration         time.Duration
	count            int
	proofDelay       time.Duration
	proofRetry       time.Duration
	proofTimeout     time.Duration
	proofs           bool
	verify           bool
	submitConcurrency int
	proofConcurrency int
	httpTimeout      time.Duration
	conns            int
	insecure         bool
	genWorkers       int
}

func parseConfig() config {
	var c config
	flag.StringVar(&c.url, "url", "https://gateway.testnet2.unicity.network/", "Aggregator / gateway JSON-RPC URL")
	flag.StringVar(&c.apiKey, "api-key", "", "X-API-Key value (falls back to env UNICITY_API_KEY)")
	flag.IntVar(&c.rate, "rate", 1000, "Target submission rate (requests/second)")
	flag.DurationVar(&c.duration, "duration", 10*time.Second, "Submission duration (ignored if -count > 0)")
	flag.IntVar(&c.count, "count", 0, "Total commitments to submit (0 = rate*duration)")
	flag.DurationVar(&c.proofDelay, "proof-delay", 1500*time.Millisecond, "Delay after a submission before the first proof fetch")
	flag.DurationVar(&c.proofRetry, "proof-retry", 500*time.Millisecond, "Delay between proof-fetch retries until included")
	flag.DurationVar(&c.proofTimeout, "proof-timeout", 30*time.Second, "Max time from submission to give up on inclusion")
	flag.BoolVar(&c.proofs, "proofs", true, "Fetch inclusion proofs after submitting (disable to measure accept rate only, e.g. for rate ramps)")
	flag.BoolVar(&c.verify, "verify", true, "Cryptographically verify each inclusion proof (SMT path)")
	flag.IntVar(&c.submitConcurrency, "submit-concurrency", 512, "Max in-flight submission requests")
	flag.IntVar(&c.proofConcurrency, "proof-concurrency", 512, "Max in-flight proof requests")
	flag.DurationVar(&c.httpTimeout, "http-timeout", 30*time.Second, "Per-request HTTP timeout")
	flag.IntVar(&c.conns, "conns", 8, "HTTP client pool size (each multiplexes over HTTP/2)")
	flag.BoolVar(&c.insecure, "insecure", false, "Skip TLS certificate verification")
	flag.IntVar(&c.genWorkers, "gen-workers", 0, "Commitment generation workers (0 = NumCPU)")
	flag.Parse()

	if c.apiKey == "" {
		c.apiKey = strings.TrimSpace(os.Getenv("UNICITY_API_KEY"))
	}
	if c.count <= 0 {
		c.count = int(float64(c.rate) * c.duration.Seconds())
	}
	if c.count <= 0 {
		c.count = c.rate
	}
	if c.genWorkers <= 0 {
		c.genWorkers = numCPU()
	}
	return c
}

// ---------------------------------------------------------------------------
// Metrics (client-side only)
// ---------------------------------------------------------------------------

type metrics struct {
	submitted   atomic.Int64
	submitOK    atomic.Int64
	submitFail  atomic.Int64
	stateExists atomic.Int64

	included    atomic.Int64 // proof came back with an inclusion (CertificationData != nil)
	verified    atomic.Int64 // inclusion proof passed local crypto verification
	verifyFail  atomic.Int64
	notIncluded atomic.Int64 // gave up before inclusion within proof-timeout
	proofPolls  atomic.Int64

	submitInFlight atomic.Int64
	proofInFlight  atomic.Int64

	mu           sync.Mutex
	submitLat    []time.Duration // submission request latency
	inclusionLat []time.Duration // submit -> first included proof
	errors       map[string]int64

	// per top-3-bit shard prefix (8 buckets) inclusion accounting
	shardSubmitOK [8]atomic.Int64
	shardIncluded [8]atomic.Int64
}

func newMetrics() *metrics {
	return &metrics{errors: make(map[string]int64)}
}

func (m *metrics) recordSubmitLatency(d time.Duration) {
	m.mu.Lock()
	m.submitLat = append(m.submitLat, d)
	m.mu.Unlock()
}

func (m *metrics) recordInclusionLatency(d time.Duration) {
	m.mu.Lock()
	m.inclusionLat = append(m.inclusionLat, d)
	m.mu.Unlock()
}

func (m *metrics) recordError(msg string) {
	m.mu.Lock()
	m.errors[msg]++
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// JSON-RPC client
// ---------------------------------------------------------------------------

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int64       `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      int64           `json:"id"`
}

type rpcClient struct {
	url        string
	apiKey     string
	httpClients []*http.Client
	idx        atomic.Int64
	nextID     atomic.Int64
}

func newRPCClient(cfg config) *rpcClient {
	clients := make([]*http.Client, 0, cfg.conns)
	for i := 0; i < cfg.conns; i++ {
		tr := &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 10000,
			MaxConnsPerHost:     10000,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.insecure},
		}
		// Enable HTTP/2 (h2) so thousands of concurrent requests multiplex over
		// a few TCP connections instead of exhausting sockets.
		if err := http2.ConfigureTransport(tr); err != nil {
			log.Printf("WARN: could not enable HTTP/2 transport: %v", err)
		}
		clients = append(clients, &http.Client{Timeout: cfg.httpTimeout, Transport: tr})
	}
	return &rpcClient{url: cfg.url, apiKey: cfg.apiKey, httpClients: clients}
}

func (c *rpcClient) client() *http.Client {
	if len(c.httpClients) == 1 {
		return c.httpClients[0]
	}
	i := int(c.idx.Add(1)-1) % len(c.httpClients)
	if i < 0 {
		i = 0
	}
	return c.httpClients[i]
}

// call issues one JSON-RPC request. stateIDHex, when non-empty, is sent as the
// X-State-ID routing header. The parsed result JSON is returned raw so callers
// decode it into the right typed response.
func (c *rpcClient) call(ctx context.Context, method string, params interface{}, stateIDHex string) (json.RawMessage, *rpcError, error) {
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      c.nextID.Add(1),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}
	if stateIDHex != "" {
		req.Header.Set("X-State-ID", stateIDHex)
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(ct, "application/json") {
		snippet := readSnippet(resp.Body, 256)
		return nil, nil, fmt.Errorf("non-JSON response: HTTP %d ct=%q body=%s", resp.StatusCode, ct, snippet)
	}

	var rpcResp rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, nil, fmt.Errorf("decode response (HTTP %d): %w", resp.StatusCode, err)
	}
	return rpcResp.Result, rpcResp.Error, nil
}

// ---------------------------------------------------------------------------
// Commitment generation (uniform, cryptographically valid)
// ---------------------------------------------------------------------------

func generateCommitment() (*api.CertificationRequest, error) {
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	ownerPredicate := api.NewPayToPublicKeyPredicate(priv.PubKey().SerializeCompressed())

	stateData := make([]byte, 32)
	if _, err := rand.Read(stateData); err != nil {
		return nil, err
	}
	sourceStateHash := signing.CreateDataHash(stateData)

	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	if err != nil {
		return nil, fmt.Errorf("create state id: %w", err)
	}

	txData := make([]byte, 32)
	if _, err := rand.Read(txData); err != nil {
		return nil, err
	}
	txHash := signing.CreateDataHash(txData)

	certData := &api.CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: txHash,
	}
	if err := signing.NewSigningService().SignCertData(certData, priv.Serialize()); err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}

	return &api.CertificationRequest{StateID: stateID, CertificationData: *certData}, nil
}

// startGenerator pre-generates commitments into a buffered channel so the
// signing/keygen work stays ahead of submission.
func startGenerator(ctx context.Context, total, workers, buffer int, m *metrics) <-chan *api.CertificationRequest {
	if buffer < 1 {
		buffer = 1
	}
	out := make(chan *api.CertificationRequest, buffer)
	jobs := make(chan struct{}, workers*2)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req, err := generateCommitment()
				if err != nil {
					m.recordError("generate: " + err.Error())
					continue
				}
				select {
				case out <- req:
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

// ---------------------------------------------------------------------------
// Submit + proof
// ---------------------------------------------------------------------------

type proofParams struct {
	StateID string `json:"stateId"`
}

func shardBucket(req *api.CertificationRequest) int {
	b := req.StateID.DataBytes()
	if len(b) == 0 {
		return 0
	}
	return int(b[0] >> 5) // top 3 bits -> one of 8 shard prefixes
}

func submitOne(ctx context.Context, c *rpcClient, m *metrics, cfg config, req *api.CertificationRequest, proofWg *sync.WaitGroup, proofSem chan struct{}) {
	stateIDHex := strings.ToLower(req.StateID.String())
	bucket := shardBucket(req)

	m.submitted.Add(1)
	m.submitInFlight.Add(1)
	start := time.Now()
	result, rerr, err := c.call(ctx, "certification_request", req, stateIDHex)
	m.recordSubmitLatency(time.Since(start))
	m.submitInFlight.Add(-1)

	if err != nil {
		m.submitFail.Add(1)
		m.recordError("submit transport: " + err.Error())
		return
	}
	if rerr != nil {
		m.submitFail.Add(1)
		m.recordError(fmt.Sprintf("submit rpc error %d: %s", rerr.Code, rerr.Message))
		return
	}

	var sr api.CertificationResponse
	if len(result) > 0 {
		if err := json.Unmarshal(result, &sr); err != nil {
			m.submitFail.Add(1)
			m.recordError("submit decode: " + err.Error())
			return
		}
	}

	switch sr.Status {
	case "SUCCESS", "STATE_ID_EXISTS":
		if sr.Status == "STATE_ID_EXISTS" {
			m.stateExists.Add(1)
		}
		m.submitOK.Add(1)
		m.shardSubmitOK[bucket].Add(1)
		if cfg.proofs {
			submittedAt := time.Now()
			proofWg.Add(1)
			go func() {
				defer proofWg.Done()
				chaseProof(ctx, c, m, cfg, req, stateIDHex, bucket, submittedAt, proofSem)
			}()
		}
	default:
		m.submitFail.Add(1)
		m.recordError("submit status: " + sr.Status)
	}
}

// chaseProof waits proof-delay, then polls get_inclusion_proof.v2 until the
// state id is included in the SMT (or proof-timeout elapses), optionally
// verifying the returned proof cryptographically.
func chaseProof(ctx context.Context, c *rpcClient, m *metrics, cfg config, req *api.CertificationRequest, stateIDHex string, bucket int, submittedAt time.Time, proofSem chan struct{}) {
	if !sleepCtx(ctx, time.Until(submittedAt.Add(cfg.proofDelay))) {
		return
	}
	deadline := submittedAt.Add(cfg.proofTimeout)

	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			m.notIncluded.Add(1)
			m.recordError("proof: not included before timeout")
			return
		}

		proofSem <- struct{}{}
		m.proofInFlight.Add(1)
		m.proofPolls.Add(1)
		result, rerr, err := c.call(ctx, "get_inclusion_proof.v2", proofParams{StateID: stateIDHex}, stateIDHex)
		m.proofInFlight.Add(-1)
		<-proofSem

		if err != nil {
			// transient network error; retry until deadline
			if !sleepCtx(ctx, cfg.proofRetry) {
				return
			}
			continue
		}
		if rerr != nil {
			// aggregator not ready to answer yet; retry until deadline
			if !sleepCtx(ctx, cfg.proofRetry) {
				return
			}
			continue
		}

		var pr api.GetInclusionProofResponseV2
		if err := json.Unmarshal(result, &pr); err != nil {
			m.recordError("proof decode: " + err.Error())
			if !sleepCtx(ctx, cfg.proofRetry) {
				return
			}
			continue
		}

		if pr.InclusionProof == nil || pr.InclusionProof.CertificationData == nil {
			// not in the tree yet — keep polling
			if !sleepCtx(ctx, cfg.proofRetry) {
				return
			}
			continue
		}

		// Included.
		m.included.Add(1)
		m.shardIncluded[bucket].Add(1)
		m.recordInclusionLatency(time.Since(submittedAt))

		if cfg.verify {
			if err := proofverify.VerifyInclusionProofLocal(pr.InclusionProof, req); err != nil {
				m.verifyFail.Add(1)
				m.recordError("proof verify: " + err.Error())
			} else {
				m.verified.Add(1)
			}
		}
		return
	}
}

// ---------------------------------------------------------------------------
// Progress + reporting
// ---------------------------------------------------------------------------

func progressLoop(ctx context.Context, m *metrics, start time.Time) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			elapsed := time.Since(start).Seconds()
			sub := m.submitted.Load()
			rps := float64(sub) / maxf(elapsed, 0.001)
			fmt.Printf("[%5.1fs] submitted=%d ok=%d fail=%d rps=%.0f | included=%d verified=%d notIncluded=%d | inflight submit=%d proof=%d polls=%d\n",
				elapsed, sub, m.submitOK.Load(), m.submitFail.Load(), rps,
				m.included.Load(), m.verified.Load(), m.notIncluded.Load(),
				m.submitInFlight.Load(), m.proofInFlight.Load(), m.proofPolls.Load())
		}
	}
}

func report(cfg config, m *metrics, submitWall time.Duration) {
	submitted := m.submitted.Load()
	submitOK := m.submitOK.Load()
	included := m.included.Load()
	verified := m.verified.Load()

	fmt.Printf("\n========================================\n")
	fmt.Printf("BLACK-BOX PERF RESULTS\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Target:            %s\n", cfg.url)
	fmt.Printf("Target rate:       %d req/s\n", cfg.rate)
	fmt.Printf("Submission window: %s\n", submitWall.Truncate(time.Millisecond))
	if submitWall > 0 {
		fmt.Printf("Achieved rate:     %.0f req/s\n", float64(submitted)/submitWall.Seconds())
	}
	fmt.Printf("\nSUBMISSION\n")
	fmt.Printf("  submitted:       %d\n", submitted)
	fmt.Printf("  accepted:        %d\n", submitOK)
	fmt.Printf("  failed:          %d\n", m.submitFail.Load())
	fmt.Printf("  state_id_exists: %d\n", m.stateExists.Load())
	if submitted > 0 {
		fmt.Printf("  accept rate:     %.2f%%\n", pct(submitOK, submitted))
	}
	printLatencies("  submit latency", m.submitLat)

	if cfg.proofs {
		fmt.Printf("\nINCLUSION (proofs fetched back, delay=%s)\n", cfg.proofDelay)
		fmt.Printf("  included in SMT: %d\n", included)
		if cfg.verify {
			fmt.Printf("  verified proof:  %d\n", verified)
			fmt.Printf("  verify failed:   %d\n", m.verifyFail.Load())
		}
		fmt.Printf("  NOT included:    %d\n", m.notIncluded.Load())
		fmt.Printf("  proof polls:     %d\n", m.proofPolls.Load())
		if submitOK > 0 {
			fmt.Printf("  inclusion rate:  %.2f%% (%d/%d accepted)\n", pct(included, submitOK), included, submitOK)
		}
		printLatencies("  submit->included", m.inclusionLat)
	}

	// Uniformity: how load spread across the 8 shard prefixes (top 3 state-id bits).
	fmt.Printf("\nSHARD DISTRIBUTION (by state-id top 3 bits -> 8 shards)\n")
	for i := 0; i < 8; i++ {
		ok := m.shardSubmitOK[i].Load()
		if cfg.proofs {
			inc := m.shardIncluded[i].Load()
			incPct := 0.0
			if ok > 0 {
				incPct = pct(inc, ok)
			}
			fmt.Printf("  prefix %03b: accepted=%-7d included=%-7d (%.1f%%)\n", i, ok, inc, incPct)
		} else {
			fmt.Printf("  prefix %03b: accepted=%-7d\n", i, ok)
		}
	}

	if len(m.errors) > 0 {
		fmt.Printf("\nERRORS\n")
		type kv struct {
			msg string
			n   int64
		}
		list := make([]kv, 0, len(m.errors))
		for k, v := range m.errors {
			list = append(list, kv{k, v})
		}
		sort.Slice(list, func(i, j int) bool { return list[i].n > list[j].n })
		for i, e := range list {
			if i >= 20 {
				fmt.Printf("  ... and %d more distinct errors\n", len(list)-20)
				break
			}
			fmt.Printf("  [%d] %s\n", e.n, e.msg)
		}
	}

	fmt.Printf("\n")
	if !cfg.proofs {
		fmt.Printf("submit-only: %d/%d accepted (%.2f%%), %d shed\n", submitOK, m.submitted.Load(), pct(submitOK, m.submitted.Load()), m.submitFail.Load())
	} else if submitOK > 0 && included == submitOK && (!cfg.verify || verified == included) {
		fmt.Printf("✅ ALL %d accepted commitments are in the SMT", submitOK)
		if cfg.verify {
			fmt.Printf(" (proofs verified)")
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("⚠️  %d/%d accepted commitments confirmed in SMT\n", included, submitOK)
	}
	fmt.Printf("========================================\n")
}

func printLatencies(label string, values []time.Duration) {
	if len(values) == 0 {
		return
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p := func(q float64) time.Duration {
		idx := int(q * float64(len(sorted)))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	fmt.Printf("%s: p50=%s p95=%s p99=%s min=%s max=%s\n",
		label,
		p(0.50).Truncate(time.Millisecond),
		p(0.95).Truncate(time.Millisecond),
		p(0.99).Truncate(time.Millisecond),
		sorted[0].Truncate(time.Millisecond),
		sorted[len(sorted)-1].Truncate(time.Millisecond))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	cfg := parseConfig()
	if cfg.apiKey == "" {
		log.Println("WARN: no API key set (-api-key or UNICITY_API_KEY); gateway will likely reject requests")
	}

	fmt.Printf("Black-box perf test\n")
	fmt.Printf("  url:          %s\n", cfg.url)
	fmt.Printf("  api key:      %s\n", maskKey(cfg.apiKey))
	fmt.Printf("  rate:         %d req/s\n", cfg.rate)
	fmt.Printf("  count:        %d commitments\n", cfg.count)
	fmt.Printf("  proof delay:  %s (retry every %s, give up after %s)\n", cfg.proofDelay, cfg.proofRetry, cfg.proofTimeout)
	fmt.Printf("  verify:       %v\n", cfg.verify)
	fmt.Printf("  concurrency:  submit=%d proof=%d conns=%d gen-workers=%d\n", cfg.submitConcurrency, cfg.proofConcurrency, cfg.conns, cfg.genWorkers)
	fmt.Printf("----------------------------------------\n")

	m := newMetrics()
	client := newRPCClient(cfg)

	// Overall context: submission window + enough tail for the slowest proof.
	runCtx, runCancel := context.WithTimeout(context.Background(),
		cfg.duration+cfg.proofTimeout+2*cfg.proofDelay+30*time.Second)
	defer runCancel()

	// Connectivity probe. The subscription gateway routes EVERY JSON-RPC call by
	// state id, so send get_block_height with a random X-State-ID to reach a
	// shard. This also validates the API key (a bad key returns HTTP 401 here).
	probeState := make([]byte, api.StateTreeKeyLengthBytes)
	if _, err := rand.Read(probeState); err != nil {
		log.Fatalf("probe: %v", err)
	}
	probeHex := api.HexBytes(probeState).String()
	probeCtx, probeCancel := context.WithTimeout(runCtx, 15*time.Second)
	if _, rerr, err := client.call(probeCtx, "get_block_height", map[string]interface{}{}, probeHex); err != nil {
		probeCancel()
		log.Fatalf("connectivity probe failed (get_block_height): %v", err)
	} else if rerr != nil {
		probeCancel()
		log.Fatalf("connectivity probe rpc error %d: %s", rerr.Code, rerr.Message)
	}
	probeCancel()
	fmt.Printf("✓ connected: %s\n\n", cfg.url)

	// Pre-generate commitments.
	genCtx, genCancel := context.WithCancel(runCtx)
	defer genCancel()
	commitments := startGenerator(genCtx, cfg.count, cfg.genWorkers, cfg.rate*2+1000, m)

	proofSem := make(chan struct{}, cfg.proofConcurrency)
	submitSem := make(chan struct{}, cfg.submitConcurrency)
	var submitWg, proofWg sync.WaitGroup

	start := time.Now()
	go progressLoop(runCtx, m, start)

	// Paced submission at the target rate.
	interval := time.Duration(0)
	if cfg.rate > 0 {
		interval = time.Second / time.Duration(cfg.rate)
	}
	for i := 0; i < cfg.count; i++ {
		if interval > 0 {
			target := start.Add(time.Duration(i) * interval)
			if d := time.Until(target); d > 0 {
				sleepCtx(runCtx, d)
			}
		}
		var req *api.CertificationRequest
		var ok bool
		select {
		case req, ok = <-commitments:
			if !ok || req == nil {
				m.recordError("generator exhausted early")
				break
			}
		case <-runCtx.Done():
			ok = false
		}
		if !ok || req == nil {
			break
		}

		submitSem <- struct{}{}
		submitWg.Add(1)
		go func(req *api.CertificationRequest) {
			defer submitWg.Done()
			defer func() { <-submitSem }()
			submitOne(runCtx, client, m, cfg, req, &proofWg, proofSem)
		}(req)
	}

	submitWg.Wait()
	submitWall := time.Since(start)
	if cfg.proofs {
		fmt.Printf("\n--- submissions complete (%s); waiting for proofs ---\n\n", submitWall.Truncate(time.Millisecond))
	} else {
		fmt.Printf("\n--- submissions complete (%s); proofs disabled ---\n\n", submitWall.Truncate(time.Millisecond))
	}

	proofWg.Wait()
	report(cfg, m, submitWall)
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func readSnippet(r interface{ Read([]byte) (int, error) }, n int) string {
	buf := make([]byte, n)
	read := 0
	for read < n {
		k, err := r.Read(buf[read:])
		read += k
		if err != nil {
			break
		}
	}
	return strings.TrimSpace(string(buf[:read]))
}

func maskKey(k string) string {
	if k == "" {
		return "(none)"
	}
	if len(k) <= 8 {
		return "****"
	}
	return k[:6] + "…" + k[len(k)-2:]
}

func pct(n, d int64) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d) * 100
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func numCPU() int {
	n := runtime.NumCPU()
	if n < 1 {
		return 1
	}
	if n > 32 {
		return 32
	}
	return n
}
