package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// JSON-RPC types and API requests
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

type GetBlockHeightResponse struct {
	BlockNumber string `json:"blockNumber"`
}

type GetInclusionProofRequest struct {
	RequestID string `json:"requestId"`
}

type GetInclusionProofResponse struct {
	InclusionProof *InclusionProof `json:"inclusionProof"`
}

type InclusionProof struct {
	Authenticator      *api.Authenticator `json:"authenticator"`
	MerkleTreePath     *MerkleTreePath    `json:"merkleTreePath"`
	TransactionHash    string             `json:"transactionHash"`
	UnicityCertificate string             `json:"unicityCertificate"`
}

type MerkleTreePath struct {
	Root  string               `json:"root"`
	Steps []api.MerkleTreeStep `json:"steps"`
}

type shardTarget struct {
	name      string
	url       string
	shardMask int
}

// Metrics aggregates counters across submissions and proof checks.
type Metrics struct {
	totalRequests       int64
	successfulRequests  int64
	failedRequests      int64
	requestIdExistsErr  int64
	startTime           time.Time
	submissionStartTime time.Time
	submissionEndTime   time.Time
	submittedRequestIDs sync.Map
	submissionTimes     sync.Map
	errorCounts         sync.Map
	activeConnections   atomic.Int64
	totalConnections    atomic.Int64
	failedConnections   atomic.Int64

	proofAttempts         int64
	proofSuccess          int64
	proofFailed           int64
	proofVerified         int64
	proofVerifyFailed     int64
	proofActiveRequests   int64
	proofRetries          int64
	proofLatencies        []time.Duration
	proofRequestDurations []time.Duration
	proofLatenciesMutex   sync.RWMutex
	proofRequestDurMutex  sync.RWMutex
	proofSuccessAttempts  [proofMaxRetries]atomic.Int64
	shardMetrics          []*ShardMetrics
}

type ShardMetrics struct {
	totalRequests      atomic.Int64
	successfulRequests atomic.Int64
	failedRequests     atomic.Int64
	requestIdExistsErr atomic.Int64
	proofAttempts      atomic.Int64
	proofSuccess       atomic.Int64
	proofFailed        atomic.Int64
	proofVerified      atomic.Int64
	proofVerifyFailed  atomic.Int64
	proofRetries       atomic.Int64
}

type ShardClient struct {
	name          string
	url           string
	shardMask     int
	client        *JSONRPCClient
	startingBlock int64
}

type proofJob struct {
	shardIdx  int
	requestID string
}

// RequestRateCounters tracks per-second client-side request activity.
type RequestRateCounters struct {
	submitStarted   atomic.Int64
	submitCompleted atomic.Int64
	proofStarted    atomic.Int64
	proofCompleted  atomic.Int64
	proofRetries    atomic.Int64
}

func (rr *RequestRateCounters) IncSubmitStarted()   { rr.submitStarted.Add(1) }
func (rr *RequestRateCounters) IncSubmitCompleted() { rr.submitCompleted.Add(1) }
func (rr *RequestRateCounters) IncProofStarted()    { rr.proofStarted.Add(1) }
func (rr *RequestRateCounters) IncProofCompleted()  { rr.proofCompleted.Add(1) }
func (rr *RequestRateCounters) IncProofRetries()    { rr.proofRetries.Add(1) }

type aggregatorLogRaw struct {
	Time             string `json:"time"`
	Msg              string `json:"msg"`
	Block            string `json:"block"`
	Commitments      int    `json:"commitments"`
	RoundTime        string `json:"roundTime"`
	Processing       string `json:"processing"`
	BftWait          string `json:"bftWait"`
	Finalization     string `json:"finalization"`
	ProofReadyMedian string `json:"proofReadyMedian"`
	ProofReadyP95    string `json:"proofReadyP95"`
	ProofReadyP99    string `json:"proofReadyP99"`
	RedisTotal       int    `json:"redisTotal"`
	RedisPending     int    `json:"redisPending"`
}

type aggregatorRoundSummary struct {
	Timestamp    time.Time
	Block        string
	Commitments  int
	RoundTime    time.Duration
	Processing   time.Duration
	BftWait      time.Duration
	Finalization time.Duration
	ProofMedian  time.Duration
	ProofP95     time.Duration
	ProofP99     time.Duration
	RedisTotal   int
	RedisPending int
}

func (m *Metrics) addProofLatency(latency time.Duration) {
	m.proofLatenciesMutex.Lock()
	defer m.proofLatenciesMutex.Unlock()
	m.proofLatencies = append(m.proofLatencies, latency)
}

func (m *Metrics) addProofRequestDuration(duration time.Duration) {
	m.proofRequestDurMutex.Lock()
	defer m.proofRequestDurMutex.Unlock()
	m.proofRequestDurations = append(m.proofRequestDurations, duration)
}

func (m *Metrics) recordProofSuccessAttempt(attempt int) {
	if attempt >= 0 && attempt < len(m.proofSuccessAttempts) {
		m.proofSuccessAttempts[attempt].Add(1)
	}
}

func (m *Metrics) proofSuccessAttemptBuckets() []int64 {
	buckets := make([]int64, len(m.proofSuccessAttempts))
	for i := range m.proofSuccessAttempts {
		buckets[i] = m.proofSuccessAttempts[i].Load()
	}
	return buckets
}

func (m *Metrics) initShardMetrics(count int) {
	if count <= 0 {
		m.shardMetrics = nil
		return
	}
	m.shardMetrics = make([]*ShardMetrics, count)
	for i := range m.shardMetrics {
		m.shardMetrics[i] = &ShardMetrics{}
	}
}

func (m *Metrics) shard(idx int) *ShardMetrics {
	if idx < 0 || idx >= len(m.shardMetrics) {
		return nil
	}
	return m.shardMetrics[idx]
}

func (m *Metrics) recordSubmissionTimestamp(id string) {
	m.submissionTimes.Store(id, time.Now())
}

func (m *Metrics) getSubmissionTimestamp(id string) (time.Time, bool) {
	if value, ok := m.submissionTimes.Load(id); ok {
		if ts, ok := value.(time.Time); ok {
			return ts, true
		}
	}
	return time.Time{}, false
}

func (m *Metrics) clearSubmissionTimestamp(id string) {
	m.submissionTimes.Delete(id)
}

func (m *Metrics) recordConnectionAttempt() {
	m.totalConnections.Add(1)
}

func (m *Metrics) incActiveConnection() {
	m.activeConnections.Add(1)
}

func (m *Metrics) decActiveConnection() {
	m.activeConnections.Add(-1)
}

func (m *Metrics) incFailedConnection() {
	m.failedConnections.Add(1)
}

func (m *Metrics) currentActiveConnections() int64 {
	return m.activeConnections.Load()
}

func (m *Metrics) totalConnectionAttempts() int64 {
	return m.totalConnections.Load()
}

func (m *Metrics) totalFailedConnections() int64 {
	return m.failedConnections.Load()
}

func (m *Metrics) recordError(errorMsg string) {
	val, _ := m.errorCounts.LoadOrStore(errorMsg, new(int64))
	counter := val.(*int64)
	atomic.AddInt64(counter, 1)
}

func (m *Metrics) printErrorSummary() {
	fmt.Println("\nERROR SUMMARY:")
	fmt.Println("========================================")

	hasErrors := false
	m.errorCounts.Range(func(key, value interface{}) bool {
		hasErrors = true
		errorMsg := key.(string)
		count := atomic.LoadInt64(value.(*int64))
		fmt.Printf("  [%d occurrences] %s\n", count, errorMsg)
		return true
	})

	if !hasErrors {
		fmt.Println("  No errors encountered")
	}
	fmt.Println("========================================")
}

func (m *Metrics) getProofLatencyStats() (median, p95, p99 time.Duration) {
	m.proofLatenciesMutex.RLock()
	defer m.proofLatenciesMutex.RUnlock()

	if len(m.proofLatencies) == 0 {
		return 0, 0, 0
	}

	sorted := make([]time.Duration, len(m.proofLatencies))
	copy(sorted, m.proofLatencies)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	median = sorted[len(sorted)/2]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	return
}

func (m *Metrics) getProofRequestStats() (avg, min, max, p50, p95, p99 time.Duration) {
	m.proofRequestDurMutex.RLock()
	defer m.proofRequestDurMutex.RUnlock()

	if len(m.proofRequestDurations) == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	sorted := make([]time.Duration, len(m.proofRequestDurations))
	copy(sorted, m.proofRequestDurations)

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	avg = total / time.Duration(len(sorted))

	min = sorted[0]
	max = sorted[len(sorted)-1]

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	return avg, min, max, p50, p95, p99
}

type JSONRPCClient struct {
	clients     []*http.Client
	clientIndex atomic.Int64
	url         string
	authHeader  string
	requestID   int64
	metrics     *Metrics
}

type trackingConn struct {
	net.Conn
	onClose func()
	closed  sync.Once
}

func (c *trackingConn) Close() error {
	err := c.Conn.Close()
	c.closed.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
	})
	return err
}

func NewJSONRPCClient(url string, authHeader string, metrics *Metrics) *JSONRPCClient {
	poolSize := httpClientPoolSize
	clients := make([]*http.Client, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		clients = append(clients, newHTTPClient(url, metrics))
	}

	return &JSONRPCClient{
		clients:    clients,
		url:        url,
		authHeader: authHeader,
		metrics:    metrics,
	}
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pemData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		return nil, fmt.Errorf("failed to append CA certs from %s", path)
	}
	return pool, nil
}

func newHTTPClient(url string, metrics *Metrics) *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: buildHTTPTransport(url, metrics),
	}
}

func buildHTTPTransport(url string, metrics *Metrics) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	var tlsConfig *tls.Config
	if strings.HasPrefix(strings.ToLower(url), "https://") {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
		if caPath := os.Getenv("AGGREGATOR_CA_CERT"); caPath != "" {
			if certPool, err := loadCertPool(caPath); err == nil {
				tlsConfig = &tls.Config{
					RootCAs:            certPool,
					InsecureSkipVerify: false,
				}
			} else {
				log.Printf("WARN: failed to load CA cert from %s (%v); falling back to insecure skip verify", caPath, err)
			}
		}
		if tlsConfig != nil && len(tlsConfig.NextProtos) == 0 {
			tlsConfig.NextProtos = []string{"h2", "http/1.1"}
		}
	}
	transport := &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 10000,
		MaxConnsPerHost:     10000,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig:     tlsConfig,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if metrics != nil {
				metrics.recordConnectionAttempt()
			}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				if metrics != nil {
					metrics.incFailedConnection()
				}
				return nil, err
			}
			if metrics != nil {
				metrics.incActiveConnection()
			}
			return &trackingConn{
				Conn: conn,
				onClose: func() {
					if metrics != nil {
						metrics.decActiveConnection()
					}
				},
			}, nil
		},
	}

	if tlsConfig != nil {
		if err := http2.ConfigureTransport(transport); err != nil {
			log.Printf("WARN: failed to enable HTTP/2 transport: %v", err)
		}
	}
	return transport
}

func (c *JSONRPCClient) nextHTTPClient() *http.Client {
	if len(c.clients) == 1 {
		return c.clients[0]
	}
	idx := int(c.clientIndex.Add(1) - 1)
	if idx < 0 {
		idx = 0
	}
	return c.clients[idx%len(c.clients)]
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
	/*if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}*/
	req.Header.Set("Authorization", "Bearer sk_4ae5084318574c42877d78cb0d53e5a6")

	client := c.nextHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(contentType), "application/json") {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected response content-type %q status=%d body=%s", contentType, resp.StatusCode, string(bodyBytes))
	}

	var response JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}
