//go:build e2e_bft

// Integrated measurement of proof availability and certification impact (see
// docs/uc-consolidation-measurements.md, "Integrated measurements").
//
// A real `ubft` root node, n in-process bft-shard aggregators with the production wiring (A4
// publication into the retained-view catalog, D1 batch proofs, the production batch handler),
// MongoDB and Redis in Docker, continuous writers and probes that time submission-to-proof.
// Phases compare the certification path with and without get_inclusion_proofs.v1 load:
//
//	INTEGRATED_SHARDS=4 INTEGRATED_PHASE_SECONDS=15 INTEGRATED_CYCLES=5 \
//	go test -tags "e2e_bft rocksdb" ./test/integration -run TestBFTIntegratedMeasurement -v -timeout 55m
//
// Everything is reported as counters (requests by typed outcome, finalized blocks, responses,
// catalog evictions and pins) and as latency relative to the no-load phase; absolute times are
// not meaningful on a shared host.
package integration

import (
	"bytes"
	"context"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ---- typed RPC ----

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Reason    string `json:"reason"`
		Detail    string `json:"detail"`
		Retryable bool   `json:"retryable"`
	} `json:"data"`
}

// rpcRaw is rpcCall with the structured error preserved. err is a transport failure.
func rpcRaw(url, method string, params any) (json.RawMessage, *rpcError, error) {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcError       `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, err
	}
	return out.Result, out.Error, nil
}

// outcome names the typed result of a batch request: ok, or reason/detail of the error.
func outcome(rerr *rpcError, err error) string {
	switch {
	case err != nil:
		return "transport-error"
	case rerr == nil:
		return "ok"
	case rerr.Data.Reason != "":
		return rerr.Data.Reason + "/" + rerr.Data.Detail
	default:
		return "error/" + strconv.Itoa(rerr.Code)
	}
}

// typedRefusal reports whether a refusal is one of the specified typed answers of the batch method
// (plan section 2.4) with retryable set as specified. Anything else (an internal error, an untyped
// error, state_not_included for a root the test knows is part of the view) fails the test.
func typedRefusal(rerr *rpcError) bool {
	if rerr == nil || !rerr.Data.Retryable {
		return false
	}
	switch rerr.Data.Reason + "/" + rerr.Data.Detail {
	case "proof_unavailable/view_not_retained", "proof_unavailable/no_finalized_view", "proof_unavailable/partition_view_unavailable",
		"busy/active_requests", "busy/pinned_views", "busy/retention_budget", "busy/deadline":
		return true
	}
	return false
}

// ---- statistics ----

func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[int(float64(len(s)-1)*q)]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.0f", float64(d.Microseconds())/1000) }

// ---- recorder ----

type observedRoot struct {
	root   string
	height int64
	at     time.Time
}

type certified struct {
	req *api.CertificationRequest
	at  time.Time
}

type recorder struct {
	mu          sync.Mutex
	probeLat    []time.Duration
	batchLat    map[string][]time.Duration // by mode, successful requests
	outcomes    map[string]map[string]int  // mode -> outcome -> count
	verified    int                        // proofs verified with the full verifier (every proof of every batch)
	verifyBad   int
	untyped     []string // refusals that are not one of the specified typed, retryable answers
	served      int      // proofs served in OK batches
	servedBy    map[string]int
	servedMulti int            // proofs served in batches of more than one ID (the union-path builder serves these)
	ageTotal    map[string]int // exact-root attempts by age bucket
	ageOK       map[string]int
}

func newRecorder() *recorder {
	return &recorder{servedBy: map[string]int{}, batchLat: map[string][]time.Duration{}, outcomes: map[string]map[string]int{}, ageTotal: map[string]int{}, ageOK: map[string]int{}}
}

func (r *recorder) outcome(mode, o string, lat time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes[mode] == nil {
		r.outcomes[mode] = map[string]int{}
	}
	r.outcomes[mode][o]++
	if o == "ok" {
		r.batchLat[mode] = append(r.batchLat[mode], lat)
	}
}

func ageBucket(blocks int64) string {
	switch {
	case blocks <= 0:
		return "0"
	case blocks <= 2:
		return "1-2"
	case blocks <= 4:
		return "3-4"
	case blocks <= 8:
		return "5-8"
	case blocks <= 16:
		return "9-16"
	default:
		return "17+"
	}
}

var ageOrder = []string{"0", "1-2", "3-4", "5-8", "9-16", "17+"}

// ---- cluster ----

type clusterShard struct {
	*e2eShard
	url  atomic.Value // string: the serving aggregator (changes on failover)
	agg  aggHandle
	opts consolidationAggOpts
	keys *shardKeys
	pool struct {
		mu  sync.Mutex
		ids []certified
	}
	roots struct {
		mu sync.Mutex
		rs []observedRoot
	}
	height atomic.Int64
	subs   atomic.Int64
	// guards the aggregator while it is being replaced
	stopFn func()
}

func (s *clusterShard) serving() string { return s.url.Load().(string) }

type cluster struct {
	t        *testing.T
	n        int
	root     *bftRootInfo
	shards   []*clusterShard
	mongoURI string
	redisH   string
	redisP   int
	rec      atomic.Pointer[recorder]
	stopBg   atomic.Bool
	bg       sync.WaitGroup
	hashes   [][]byte
}

func newCluster(t *testing.T, ubft, mongoURI, redisHost string, redisPort, n, portBase int, smt *config.SMTConfig, tag string) *cluster {
	bits := map[int]int{1: 0, 2: 1, 4: 2, 8: 3}[n]
	if n != 1 {
		require.NotZero(t, bits, "1, 2, 4 or 8 shards")
	}
	root := startHostRoot(t, ubft, portBase, portBase+1)
	ids := []bfttypes.ShardID{{}}
	for range bits {
		var next []bfttypes.ShardID
		for _, id := range ids {
			a, b := id.Split()
			next = append(next, a, b)
		}
		ids = next
	}
	c := &cluster{t: t, n: n, root: root, mongoURI: mongoURI, redisH: redisHost, redisP: redisPort}
	for i, id := range ids {
		keys := generateShardKeys(t)
		sh := &clusterShard{e2eShard: &e2eShard{id: id, index: i, conf: buildShardConf(t, id, keys), url: fmt.Sprintf("http://127.0.0.1:%d", portBase+10+i)}, keys: keys}
		sh.url.Store(sh.e2eShard.url)
		uploadShardConf(t, root.restURL, sh.conf)
		h, err := sh.conf.Hash(crypto.SHA256)
		require.NoError(t, err)
		c.hashes = append(c.hashes, h)
		shardSMT := smt
		if smt != nil && smt.Backend == config.SMTBackendRocksDB {
			cp := *smt
			cp.DiskPath = t.TempDir() // survives restarts of this shard (same opts)
			shardSMT = &cp
		}
		sh.opts = consolidationAggOpts{
			name: fmt.Sprintf("m%s-%d", tag, i), port: strconv.Itoa(portBase + 10 + i), bftListen: fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", portBase+40+i),
			mongoURI: mongoURI, redisHost: redisHost, redisPort: redisPort, stream: fmt.Sprintf("commitments:meas:%s:%d", tag, i),
			conf: sh.conf, keys: keys, root: root, shardIDLength: bits, smt: shardSMT, out: &sh.agg,
		}
		c.shards = append(c.shards, sh)
	}
	for _, sh := range c.shards {
		sh.stopFn = startConsolidationAggregator(t.Context(), t, sh.opts)
		stop := sh
		t.Cleanup(func() {
			if stop.stopFn != nil {
				stop.stopFn()
			}
		})
	}
	for _, sh := range c.shards {
		waitForBlock(t, sh.serving(), 1, 90*time.Second)
	}
	return c
}

func (c *cluster) newState(sh *clusterShard, tag string) *api.CertificationRequest {
	for k := 0; k < 5000; k++ {
		cr := testutil.CreateTestCertificationRequest(c.t, fmt.Sprintf("meas-%s-%d-%d", tag, sh.index, k))
		req := cr.ToAPI()
		key, _ := req.StateID.GetTreeKey()
		if sh.id.Comparator()(key) {
			return req
		}
	}
	return nil
}

func (c *cluster) submit(sh *clusterShard, req *api.CertificationRequest) bool {
	if req == nil {
		return false
	}
	raw, rerr, err := rpcRaw(sh.serving(), "certification_request", req) // non-fatal: the aggregator may be restarting
	if err != nil || rerr != nil {
		return false
	}
	var cr api.CertificationResponse
	if json.Unmarshal(raw, &cr) == nil && cr.Status == "SUCCESS" {
		sh.subs.Add(1)
		return true
	}
	return false
}

const probers = 3 // submission-to-proof probes per shard

// startBackground starts, per shard: a writer (steady load), a prober (timed submission to proof,
// fills the pool of certified states), a height sampler.
func (c *cluster) startBackground() {
	c.stopBg.Store(false)
	for _, sh := range c.shards {
		sh := sh
		c.bg.Add(2 + probers)
		go func() { // writer
			defer c.bg.Done()
			for seq := 0; !c.stopBg.Load(); seq++ {
				c.submit(sh, c.newState(sh, fmt.Sprintf("w%d-%d", seq, time.Now().UnixNano())))
				time.Sleep(150 * time.Millisecond)
			}
		}()
		for pr := 0; pr < probers; pr++ {
			go func() { // probe: submit, then poll for the single proof; latency = submit accepted -> proof visible
				defer c.bg.Done()
				for seq := 0; !c.stopBg.Load(); seq++ {
					req := c.newState(sh, fmt.Sprintf("p%d-%d", seq, time.Now().UnixNano()))
					if !c.submit(sh, req) {
						time.Sleep(200 * time.Millisecond)
						continue
					}
					t0 := time.Now()
					for !c.stopBg.Load() && time.Since(t0) < 60*time.Second {
						raw, rerr, err := rpcRaw(sh.serving(), "get_inclusion_proof.v2", &api.GetInclusionProofRequestV2{StateID: req.StateID})
						if err == nil && rerr == nil {
							var resp api.GetInclusionProofResponseV2
							if json.Unmarshal(raw, &resp) == nil && resp.InclusionProof != nil && len(resp.InclusionProof.CertificateBytes) > 0 {
								lat := time.Since(t0)
								if r := c.rec.Load(); r != nil {
									r.mu.Lock()
									r.probeLat = append(r.probeLat, lat)
									r.mu.Unlock()
								}
								sh.pool.mu.Lock()
								sh.pool.ids = append(sh.pool.ids, certified{req, time.Now()})
								sh.pool.mu.Unlock()
								break
							}
						}
						time.Sleep(50 * time.Millisecond)
					}
					time.Sleep(300 * time.Millisecond)
				}
			}()
		}
		go func() { // height
			defer c.bg.Done()
			for !c.stopBg.Load() {
				if h := blockHeightQuiet(sh.serving()); h > 0 {
					sh.height.Store(h)
				}
				time.Sleep(250 * time.Millisecond)
			}
		}()
	}
}

func (c *cluster) stopBackground() { c.stopBg.Store(true); c.bg.Wait() }

// ---- batch load ----

func (c *cluster) verifyProof(sh *clusterShard, req *api.CertificationRequest, p *api.GetInclusionProofResponseV2) error {
	vctx := &api.VerifierContext{TrustBase: c.root.trustBase, PartitionID: bftPartitionID, ExpectedShardID: sh.id, ShardConfHash: c.hashes[sh.index]}
	return p.InclusionProof.Verify(req, vctx)
}

// batchOnce issues one get_inclusion_proofs.v1 request and records its typed outcome.
// mode is "latest-1", "latest-16" or "exact-root".
func (c *cluster) batchOnce(sh *clusterShard, mode string, rng *rand.Rand, rec *recorder) (ok bool) {
	sh.pool.mu.Lock()
	pool := slices.Clone(sh.pool.ids)
	sh.pool.mu.Unlock()
	if len(pool) == 0 {
		time.Sleep(100 * time.Millisecond)
		return false
	}
	pick := func(k int, notAfter time.Time) []certified {
		var eligible []certified
		for _, p := range pool {
			if p.at.Before(notAfter) {
				eligible = append(eligible, p)
			}
		}
		if len(eligible) == 0 {
			eligible = pool[:1]
		}
		rng.Shuffle(len(eligible), func(i, j int) { eligible[i], eligible[j] = eligible[j], eligible[i] })
		if len(eligible) > k {
			eligible = eligible[:k]
		}
		return eligible
	}
	var items []certified
	params := map[string]any{}
	var ageKey string
	var ageH int64
	switch mode {
	case "latest-1":
		items = pick(1, time.Now())
		params["stateRoot"] = "latest"
	case "latest-16":
		items = pick(16, time.Now())
		params["stateRoot"] = "latest"
	case "exact-root":
		sh.roots.mu.Lock()
		if len(sh.roots.rs) == 0 {
			sh.roots.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			return false
		}
		o := sh.roots.rs[rng.Intn(len(sh.roots.rs))]
		sh.roots.mu.Unlock()
		items = pick(1, o.at.Add(-1500*time.Millisecond)) // certified well before the root was observed, so it is part of that root
		params["stateRoot"] = o.root
		ageH = sh.height.Load() - o.height
		ageKey = ageBucket(ageH)
	}
	ids := make([]string, len(items))
	for i, it := range items {
		ids[i] = hex.EncodeToString(it.req.StateID)
	}
	params["stateIds"] = ids

	t0 := time.Now()
	raw, rerr, err := rpcRaw(sh.serving(), "get_inclusion_proofs.v1", params)
	lat := time.Since(t0)
	o := outcome(rerr, err)
	rec.outcome(mode, o, lat)
	if o != "ok" && !typedRefusal(rerr) {
		rec.mu.Lock()
		rec.untyped = append(rec.untyped, fmt.Sprintf("%s: %s (err=%v)", mode, o, err))
		rec.mu.Unlock()
	}
	if mode == "exact-root" {
		rec.mu.Lock()
		rec.ageTotal[ageKey]++
		if o == "ok" {
			rec.ageOK[ageKey]++
		}
		rec.mu.Unlock()
	}
	if o != "ok" {
		return false
	}
	var res struct {
		Proofs []json.RawMessage `json:"proofs"`
	}
	if json.Unmarshal(raw, &res) != nil || len(res.Proofs) != len(items) {
		rec.mu.Lock()
		rec.verifyBad++
		rec.mu.Unlock()
		return false
	}
	proofs := make([]api.GetInclusionProofResponseV2, len(res.Proofs))
	bad := 0
	for i, raw := range res.Proofs { // every proof of the batch is verified against its own request
		if json.Unmarshal(raw, &proofs[i]) != nil || proofs[i].InclusionProof == nil || c.verifyProof(sh, items[i].req, &proofs[i]) != nil {
			bad++
		}
	}
	rec.mu.Lock()
	rec.verified += len(proofs)
	rec.served += len(proofs)
	rec.servedBy[mode] += len(proofs)
	if len(proofs) > 1 {
		rec.servedMulti += len(proofs)
	}
	rec.verifyBad += bad
	rec.mu.Unlock()
	first := proofs[0]
	if mode != "exact-root" && first.InclusionProof != nil {
		var uc bfttypes.UnicityCertificate
		if bfttypes.Cbor.Unmarshal(first.InclusionProof.UnicityCertificate, &uc) == nil {
			root := hex.EncodeToString(uc.GetStateHash())
			sh.roots.mu.Lock()
			if n := len(sh.roots.rs); n == 0 || sh.roots.rs[n-1].root != root {
				sh.roots.rs = append(sh.roots.rs, observedRoot{root: root, height: sh.height.Load(), at: time.Now()})
			}
			sh.roots.mu.Unlock()
		}
	}
	return true
}

// runLoad runs `workers` batch clients (round-robin over the shards) until ctx ends.
func (c *cluster) runLoad(ctx context.Context, workers int, rec *recorder) {
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			sh := c.shards[w%len(c.shards)]
			modes := []string{"latest-1", "latest-1", "latest-16", "exact-root"}
			for ctx.Err() == nil {
				if !c.batchOnce(sh, modes[rng.Intn(len(modes))], rng, rec) {
					time.Sleep(25 * time.Millisecond) // a client backs off after an unavailable or busy answer
				}
			}
		}()
	}
	wg.Wait()
}

// ---- phases ----

type snapshot struct {
	responses, nonprogress, handshakes, blocks, subs []float64
	round                                            uint64
	t                                                time.Time
	certSum                                          float64 // seconds, BFT proposal -> UC received (all shards)
	certCount                                        uint64
	diskLeaves                                       float64 // leaves proven by D1 union-path builds on retained disk views
}

func certDuration() (sum float64, count uint64) {
	var m dto.Metric
	if err := metrics.BFTCertificationDuration.Write(&m); err != nil || m.Histogram == nil {
		return 0, 0
	}
	return m.Histogram.GetSampleSum(), m.Histogram.GetSampleCount()
}

func (c *cluster) take() snapshot {
	sn := snapshot{round: rootRound(c.t, c.root), t: time.Now(), diskLeaves: promtest.ToFloat64(metrics.SMTDiskBatchProofLeaves)}
	sn.certSum, sn.certCount = certDuration()
	for _, sh := range c.shards {
		sn.responses = append(sn.responses, promtest.ToFloat64(metrics.BFTCertificationResponsesTotal.WithLabelValues(sh.id.String())))
		sn.nonprogress = append(sn.nonprogress, promtest.ToFloat64(metrics.BFTCertificationResponseNonProgressTotal.WithLabelValues(sh.id.String())))
		sn.handshakes = append(sn.handshakes, promtest.ToFloat64(metrics.BFTHandshakesTotal.WithLabelValues(sh.id.String())))
		sn.blocks = append(sn.blocks, float64(blockHeightQuiet(sh.serving())))
		sn.subs = append(sn.subs, float64(sh.subs.Load()))
	}
	return sn
}

type catalogPeak struct {
	views, retired, pins int
	retained             uint64
	evictions, kills     uint64
}

type phaseResult struct {
	name     string
	dur      time.Duration
	counts   phaseCount
	rec      *recorder
	probeP50 time.Duration
	probeP95 time.Duration
	probeMax time.Duration
	probeN   int
	peak     catalogPeak
	certMean float64 // mean BFT proposal -> UC received (ms): the certification latency of the shards
	diskLeaf float64 // leaves proven by D1 union-path builds during the phase
}

func (c *cluster) phase(name string, d time.Duration, workers int) phaseResult {
	rec := newRecorder()
	c.rec.Store(rec)
	s0 := c.take()
	gaps := make([]float64, c.n)
	var peak catalogPeak
	stopSampling := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		last := make([]int64, c.n)
		lastAt := make([]time.Time, c.n)
		for i := range c.shards {
			last[i], lastAt[i] = int64(s0.blocks[i]), time.Now()
		}
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopSampling:
				for i := range c.shards {
					gaps[i] = max(gaps[i], time.Since(lastAt[i]).Seconds())
				}
				return
			case <-tick.C:
				for i, sh := range c.shards {
					if h := sh.height.Load(); h > last[i] {
						gaps[i] = max(gaps[i], time.Since(lastAt[i]).Seconds())
						last[i], lastAt[i] = h, time.Now()
					}
					if cat := sh.agg.catalog(); cat != nil {
						st := cat.Stats()
						peak.views = max(peak.views, st.Views)
						peak.retired = max(peak.retired, st.RetiredPinned)
						peak.pins = max(peak.pins, st.ActivePins)
						peak.retained = max(peak.retained, st.RetainedBytes)
						peak.evictions = max(peak.evictions, st.Evictions)
						peak.kills = max(peak.kills, st.PressureKills)
					}
				}
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	if workers > 0 {
		c.runLoad(ctx, workers, rec)
	} else {
		<-ctx.Done()
	}
	cancel()
	close(stopSampling)
	sampler.Wait()
	s1 := c.take()
	c.rec.Store(nil)

	pc := phaseCount{Rounds: float64(s1.round - s0.round), MaxGap: gaps}
	for i := range c.shards {
		pc.Responses = append(pc.Responses, s1.responses[i]-s0.responses[i])
		pc.NonProgress = append(pc.NonProgress, s1.nonprogress[i]-s0.nonprogress[i])
		pc.Handshakes = append(pc.Handshakes, s1.handshakes[i]-s0.handshakes[i])
		pc.Blocks = append(pc.Blocks, s1.blocks[i]-s0.blocks[i])
		pc.Submitted = append(pc.Submitted, s1.subs[i]-s0.subs[i])
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	certMean := 0.0
	if dc := s1.certCount - s0.certCount; dc > 0 {
		certMean = 1000 * (s1.certSum - s0.certSum) / float64(dc)
	}
	return phaseResult{certMean: certMean, diskLeaf: s1.diskLeaves - s0.diskLeaves, name: name, dur: s1.t.Sub(s0.t), counts: pc, rec: rec, probeP50: percentile(rec.probeLat, 0.5), probeP95: percentile(rec.probeLat, 0.95),
		probeMax: percentile(rec.probeLat, 1), probeN: len(rec.probeLat), peak: peak}
}

func sum(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s
}

func (p phaseResult) outcomeTable() string {
	var sb strings.Builder
	sb.WriteString("| mode | outcome | requests | share of mode | latency p50 ms (ok) | p95 ms (ok) |\n|---|---|---|---|---|---|\n")
	for _, mode := range []string{"latest-1", "latest-16", "exact-root"} {
		outs := p.rec.outcomes[mode]
		total := 0
		for _, n := range outs {
			total += n
		}
		var keys []string
		for k := range outs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			p50, p95 := "", ""
			if k == "ok" {
				p50, p95 = ms(percentile(p.rec.batchLat[mode], 0.5)), ms(percentile(p.rec.batchLat[mode], 0.95))
			}
			fmt.Fprintf(&sb, "| %s | %s | %d | %.1f%% | %s | %s |\n", mode, k, outs[k], 100*float64(outs[k])/float64(total), p50, p95)
		}
	}
	return sb.String()
}

func (p phaseResult) ageTable() string {
	var sb strings.Builder
	sb.WriteString("| age of the requested root (blocks behind the tip) | exact-root requests | served | availability |\n|---|---|---|---|\n")
	for _, b := range ageOrder {
		if n := p.rec.ageTotal[b]; n > 0 {
			fmt.Fprintf(&sb, "| %s | %d | %d | %.0f%% |\n", b, n, p.rec.ageOK[b], 100*float64(p.rec.ageOK[b])/float64(n))
		}
	}
	return sb.String()
}

// metricOf extracts one comparison metric from a phase.
type cycleMetric struct {
	name string
	f    func(p phaseResult) float64
	fmt  string
}

func blocksPerRound(p phaseResult) float64 { return sum(p.counts.Blocks) / max(p.counts.Rounds, 1) }

var cycleMetrics = []cycleMetric{
	{"finalized blocks per root round (sum over shards)", blocksPerRound, "%.2f"},
	{"finalized blocks per second (sum over shards)", func(p phaseResult) float64 { return sum(p.counts.Blocks) / p.dur.Seconds() }, "%.2f"},
	{"accepted writes per second (sum over shards)", func(p phaseResult) float64 { return sum(p.counts.Submitted) / p.dur.Seconds() }, "%.2f"},
	{"certification latency: mean BFT proposal -> UC received (ms)", func(p phaseResult) float64 { return p.certMean }, "%.0f"},
	{"responses per finalized block", func(p phaseResult) float64 { return sum(p.counts.Responses) / max(sum(p.counts.Blocks), 1) }, "%.2f"},
	{"longest gap without a finalized block (s, max over shards)", func(p phaseResult) float64 { return slices.Max(p.counts.MaxGap) }, "%.1f"},
	{"submission-to-proof latency p50 (probes, ms)", func(p phaseResult) float64 { return float64(p.probeP50.Milliseconds()) }, "%.0f"},
	{"submission-to-proof latency p95 (probes, ms)", func(p phaseResult) float64 { return float64(p.probeP95.Milliseconds()) }, "%.0f"},
}

func median(v []float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	return s[len(s)/2]
}

func spread(v []float64, f string) string {
	return fmt.Sprintf(f+" ("+f+"-"+f+")", median(v), slices.Min(v), slices.Max(v))
}

// compareCycles reports interleaved baseline/loaded cycles: per metric the baseline and loaded
// distributions (median and range over the cycles), the per-cycle loaded/baseline ratios (median and
// range), and a verdict. The verdict is "inside the baseline range" when the loaded median lies within
// the baseline min-max, in which case nothing can be concluded beyond "no effect larger than the host
// variation was observed"; otherwise the difference is outside the baseline variation and is reported as such.
func compareCycles(bases, loads []phaseResult) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d interleaved cycles (baseline/loaded order alternates). Values: median (min-max over cycles).\n\n", len(bases))
	sb.WriteString("| metric | baseline | with batch load | loaded / baseline per cycle: median (min-max) | verdict |\n|---|---|---|---|---|\n")
	for _, m := range cycleMetrics {
		var bv, lv, ratios []float64
		for i := range bases {
			b, l := m.f(bases[i]), m.f(loads[i])
			bv, lv = append(bv, b), append(lv, l)
			if b > 0 {
				ratios = append(ratios, l/b)
			}
		}
		verdict := "n/a"
		if len(ratios) > 0 {
			lm := median(lv)
			if lm >= slices.Min(bv) && lm <= slices.Max(bv) {
				verdict = "loaded median inside the baseline range"
			} else if lm < slices.Min(bv) {
				verdict = "loaded median BELOW the baseline range"
			} else {
				verdict = "loaded median ABOVE the baseline range"
			}
		}
		r := "n/a"
		if len(ratios) > 0 {
			r = spread(ratios, "%.2fx")
		}
		fmt.Fprintf(&sb, "| %s | %s | %s | %s | %s |\n", m.name, spread(bv, m.fmt), spread(lv, m.fmt), r, verdict)
	}
	reqs := 0
	for _, l := range loads {
		for _, outs := range l.rec.outcomes {
			for _, n := range outs {
				reqs += n
			}
		}
	}
	var secs float64
	for _, l := range loads {
		secs += l.dur.Seconds()
	}
	fmt.Fprintf(&sb, "\nbatch requests issued over the %d loaded phases: %d (%.0f per second)\n", len(loads), reqs, float64(reqs)/secs)
	return sb.String()
}

// mergeLoaded combines the recorders of several loaded phases for the outcome and age tables.
func mergeLoaded(loads []phaseResult) phaseResult {
	out := loads[0]
	rec := newRecorder()
	for _, l := range loads {
		for mode, outs := range l.rec.outcomes {
			if rec.outcomes[mode] == nil {
				rec.outcomes[mode] = map[string]int{}
			}
			for o, n := range outs {
				rec.outcomes[mode][o] += n
			}
		}
		for mode, v := range l.rec.batchLat {
			rec.batchLat[mode] = append(rec.batchLat[mode], v...)
		}
		for k, v := range l.rec.ageTotal {
			rec.ageTotal[k] += v
		}
		for k, v := range l.rec.ageOK {
			rec.ageOK[k] += v
		}
		rec.verified += l.rec.verified
		rec.verifyBad += l.rec.verifyBad
		rec.served += l.rec.served
		rec.servedMulti += l.rec.servedMulti
		for k, v := range l.rec.servedBy {
			rec.servedBy[k] += v
		}
		rec.untyped = append(rec.untyped, l.rec.untyped...)
	}
	out.rec = rec
	return out
}

func (p phaseResult) String() string {
	return fmt.Sprintf("%s: %.0fs, %s | probes n=%d p50=%sms p95=%sms max=%sms | catalog peak views=%d retiredPinned=%d pins=%d retained=%dB evictions=%d pressureKills=%d",
		p.name, p.dur.Seconds(), p.counts, p.probeN, ms(p.probeP50), ms(p.probeP95), ms(p.probeMax), p.peak.views, p.peak.retired, p.peak.pins, p.peak.retained, p.peak.evictions, p.peak.kills)
}

// ---- the measurement ----

func TestBFTIntegratedMeasurement(t *testing.T) {
	n := 4
	if s := os.Getenv("INTEGRATED_SHARDS"); s != "" {
		v, err := strconv.Atoi(s)
		require.NoError(t, err)
		n = v
	}
	dur := 15 * time.Second
	if s := os.Getenv("INTEGRATED_PHASE_SECONDS"); s != "" {
		v, err := strconv.Atoi(s)
		require.NoError(t, err)
		dur = time.Duration(v) * time.Second
	}
	cycles := 5
	if s := os.Getenv("INTEGRATED_CYCLES"); s != "" {
		v, err := strconv.Atoi(s)
		require.NoError(t, err)
		cycles = v
	}
	only := os.Getenv("INTEGRATED_ONLY") // "", a variant name prefix ("default", "rocksdb"), "recovery" or "ha"
	ubft := buildBinary(t, os.Getenv("UBFT_BIN"), "github.com/unicitynetwork/bft-core/cli/ubft", "ubft")
	mongoURI := startMongoCleanly(t)
	redisHost, redisPort := startRedisCleanly(t)

	type variant struct {
		name string
		smt  *config.SMTConfig
		disk bool
	}
	variants := []variant{
		{"default", nil, false},                                      // 16 retained views, 8 concurrent batches, 5 s lease
		{"views2", &config.SMTConfig{RetainedViews: 2}, false},       // retention pressure: at most 2 retained views per shard
		{"pins1", &config.SMTConfig{MaxConcurrentBatches: 1}, false}, // admission pressure: one batch at a time per shard
	}
	if haveRocksDB { // the same configurations on the RocksDB backend, where D1's union-path builder serves the batches
		variants = append(variants,
			variant{"rocksdb-default", &config.SMTConfig{Backend: config.SMTBackendRocksDB}, true},
			variant{"rocksdb-views2", &config.SMTConfig{Backend: config.SMTBackendRocksDB, RetainedViews: 2}, true})
	}
	for vi, v := range variants {
		if only != "" && !strings.HasPrefix(v.name, only) && !(only == "recovery" && (v.name == "default" || v.name == "rocksdb-default")) {
			continue
		}
		t.Run("limits-"+v.name, func(t *testing.T) {
			c := newCluster(t, ubft, mongoURI, redisHost, redisPort, n, 9700+vi*100, v.smt, v.name)
			c.startBackground()
			defer c.stopBackground()
			// warm-up: certified states for the pool on every shard
			require.Eventually(t, func() bool {
				for _, sh := range c.shards {
					sh.pool.mu.Lock()
					l := len(sh.pool.ids)
					sh.pool.mu.Unlock()
					if l < 3 {
						return false
					}
				}
				return true
			}, 90*time.Second, time.Second)

			workers := 2 * n
			var bases, loads []phaseResult
			for cycle := 0; cycle < cycles; cycle++ {
				runBase := func() { bases = append(bases, c.phase(fmt.Sprintf("cycle %d baseline", cycle+1), dur, 0)) }
				runLoad := func() {
					loads = append(loads, c.phase(fmt.Sprintf("cycle %d with %d batch clients", cycle+1, workers), dur, workers))
				}
				if cycle%2 == 0 { // alternate the order to cancel slow drift of the host
					runBase()
					runLoad()
				} else {
					runLoad()
					runBase()
				}
				t.Logf("%s", bases[cycle])
				t.Logf("%s", loads[cycle])
				require.NoError(t, checkWriteProgress(bases[cycle].counts), "baseline cycle %d", cycle+1)
				require.NoError(t, checkWriteProgress(loads[cycle].counts), "loaded cycle %d", cycle+1)
			}
			with := mergeLoaded(loads)
			rec := with.rec
			require.Zero(t, rec.verifyBad, "every served proof verifies with the full verifier")
			require.Empty(t, rec.untyped, "every refusal is one of the specified typed, retryable answers")
			var diskLeaves float64
			for _, l := range loads {
				diskLeaves += l.diskLeaf
			}
			var baseDisk float64
			for _, b := range bases {
				baseDisk += b.diskLeaf
			}
			if v.disk {
				// D1: the union-path batch builder ran on retained RocksDB views, once per served proof
				require.Greater(t, diskLeaves, 0.0, "D1 batch proofs must be exercised on the disk backend")
				// batches of one ID take the single-proof path; every proof of a multi-ID batch is built by the union-path builder
				require.GreaterOrEqual(t, diskLeaves, float64(rec.servedMulti), "multi-ID batches use the union-path builder (served by mode: %v)", rec.servedBy)
			} else {
				require.Zero(t, diskLeaves, "the memory backend does not use the disk union-path builder")
			}
			require.Zero(t, baseDisk, "no batch proofs are built without load")
			t.Logf("RESULT limits=%s backend=%s shards=%d\n\n%s\n%s\n%s\nverified proofs: %d (failures %d); proofs served %d (in multi-ID batches: %d); D1 disk batch-proof leaves during the loaded phases: %.0f\n",
				v.name, map[bool]string{true: "rocksdb", false: "memory"}[v.disk], n, compareCycles(bases, loads), with.outcomeTable(), with.ageTable(), rec.verified, rec.verifyBad, rec.served, rec.servedMulti, diskLeaves)
			for mode, outs := range rec.outcomes {
				for o := range outs {
					require.NotEqual(t, "transport-error", o, "mode %s", mode)
				}
			}

			if only == "" || only == "recovery" || strings.HasPrefix(v.name, only) {
				if v.name == "default" || v.name == "rocksdb-default" {
					recoveryPhase(t, c, dur)
				}
			}
		})
	}
	if only == "" || only == "ha" {
		t.Run("ha-failover", func(t *testing.T) { haFailover(t, ubft, mongoURI, redisHost, redisPort, dur) })
	}
}

// recoveryPhase restarts shard 0's aggregator on the same database (cold restart): the catalog is
// rebuilt only for the current validated state, so history is unavailable and the latest becomes
// available once the first certification arrives.
func recoveryPhase(t *testing.T, c *cluster, dur time.Duration) {
	sh := c.shards[0]
	// observed roots before the restart (these are the "history")
	sh.roots.mu.Lock()
	before := slices.Clone(sh.roots.rs)
	sh.roots.mu.Unlock()
	require.NotEmpty(t, before)
	t0 := time.Now()
	sh.stopFn()
	sh.stopFn = nil
	tStopped := time.Since(t0)
	sh.stopFn = startConsolidationAggregator(t.Context(), t, sh.opts)
	tStarted := time.Since(t0)

	pool := func() []certified { sh.pool.mu.Lock(); defer sh.pool.mu.Unlock(); return slices.Clone(sh.pool.ids) }()
	ids := []string{hex.EncodeToString(pool[0].req.StateID)}
	var tServing, tLatest, tNewBlock time.Duration
	var hRestart int64
	firstOutcomes := map[string]int{}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && (tLatest == 0 || tNewBlock == 0) {
		h := blockHeightQuiet(sh.serving())
		if h > 0 && tServing == 0 {
			tServing, hRestart = time.Since(t0), h
		}
		if tServing != 0 && tLatest == 0 {
			_, rerr, err := rpcRaw(sh.serving(), "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": "latest"})
			o := outcome(rerr, err)
			firstOutcomes[o]++
			if o == "ok" {
				tLatest = time.Since(t0)
			}
		}
		if tServing != 0 && tNewBlock == 0 && h > hRestart {
			tNewBlock = time.Since(t0)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// old roots after the restart: only the current state is rebuilt, history is not
	okOld, total := 0, 0
	byOutcome := map[string]int{}
	for _, o := range before {
		_, rerr, err := rpcRaw(sh.serving(), "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": o.root})
		oc := outcome(rerr, err)
		byOutcome[oc]++
		total++
		if oc == "ok" {
			okOld++
		}
	}
	t.Logf("RECOVERY shard 0 (cold restart on the same database, times since the stop began): stop %s, process started %s, serving block height %d at %s, "+
		"first latest batch ok at %s (outcomes while waiting: %v), first new finalized block after the restart at %s; "+
		"roots observed before the restart that are served after it: %d of %d (%v); history is intentionally not rebuilt",
		tStopped.Round(time.Millisecond), tStarted.Round(time.Millisecond), hRestart, tServing.Round(time.Millisecond), tLatest.Round(time.Millisecond), firstOutcomes, tNewBlock.Round(time.Millisecond), okOld, total, byOutcome)
	require.NotZero(t, tLatest, "latest becomes available after recovery")
	require.NotZero(t, tNewBlock, "finalization resumes after recovery")
}

type neverLeader struct{}

func (neverLeader) IsLeader(context.Context) (bool, error) { return false, nil }

// haFailover: shard 0 has a leader and a follower on the same database. The follower replays finalized
// blocks (block syncer) and publishes the replayed state into its own catalog. The leader is stopped; the
// follower syncs and activates as the production leader-change listener does.
func haFailover(t *testing.T, ubft, mongoURI, redisHost string, redisPort int, dur time.Duration) {
	c := newCluster(t, ubft, mongoURI, redisHost, redisPort, 2, 9900, nil, "ha")
	sh := c.shards[0]
	leaderURL := sh.serving()

	// follower on shard 0's database, not activated
	fport := 9900 + 70
	fopts := sh.opts
	fopts.name = "mha-follower"
	fopts.dbName = dbNameFor(sh.opts)
	fopts.port = strconv.Itoa(fport)
	fopts.bftListen = fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 9900+80)
	fopts.noActivate = true
	if fopts.smt != nil && fopts.smt.Backend == config.SMTBackendRocksDB {
		cp := *fopts.smt
		cp.DiskPath = t.TempDir()
		fopts.smt = &cp
	}
	var fh aggHandle
	fopts.out = &fh
	fstop := startConsolidationAggregator(t.Context(), t, fopts)
	t.Cleanup(fstop)
	followerURL := fmt.Sprintf("http://127.0.0.1:%d", fport)
	syncer := ha.NewBlockSyncer(fh.log, neverLeader{}, fh.stor, fh.mgr.GetSMTBackend(), 0, 500*time.Millisecond, fh.tracker)
	if pv, ok := fh.mgr.(interface{ ProofViewPublisher() *proofpub.Publisher }); ok && pv.ProofViewPublisher() != nil {
		syncer.SetViewPublisher(pv.ProofViewPublisher())
	}
	syncer.Start(t.Context())

	c.startBackground()
	defer c.stopBackground()
	require.Eventually(t, func() bool {
		for _, s := range c.shards {
			s.pool.mu.Lock()
			l := len(s.pool.ids)
			s.pool.mu.Unlock()
			if l < 3 {
				return false
			}
		}
		return true
	}, 90*time.Second, time.Second)

	// let the leader serve batch load so roots are observed, while the follower syncs
	rec := newRecorder()
	c.rec.Store(rec)
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	c.runLoad(ctx, 2, rec)
	cancel()
	require.Eventually(t, func() bool { cat := fh.catalog(); return cat != nil && cat.Stats().Views > 0 }, 30*time.Second, 200*time.Millisecond)
	folStats := fh.catalog().Stats()
	sh.roots.mu.Lock()
	observed := slices.Clone(sh.roots.rs)
	sh.roots.mu.Unlock()
	pool := func() []certified { sh.pool.mu.Lock(); defer sh.pool.mu.Unlock(); return slices.Clone(sh.pool.ids) }()
	ids := []string{hex.EncodeToString(pool[0].req.StateID)}
	followerServes := 0
	for _, o := range observed {
		if _, rerr, err := rpcRaw(followerURL, "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": o.root}); outcome(rerr, err) == "ok" {
			followerServes++
		}
	}
	_, rerr, err := rpcRaw(followerURL, "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": "latest"})
	followerLatestBefore := outcome(rerr, err)
	t.Logf("HA before failover: leader observed %d roots; follower catalog views=%d retained=%dB; follower serves %d of %d of the leader's observed roots; follower latest: %s",
		len(observed), folStats.Views, folStats.RetainedBytes, followerServes, len(observed), followerLatestBefore)

	// ---- failover: stop the leader, promote the follower (sync, then activate) ----
	hBefore := blockHeightQuiet(leaderURL)
	t0 := time.Now()
	sh.stopFn()
	sh.stopFn = nil
	tLeaderStopped := time.Since(t0)
	syncer.Stop()
	tS := time.Now()
	require.NoError(t, syncer.SyncToLatestBlock(t.Context()))
	tSync := time.Since(tS)
	tA := time.Now()
	require.NoError(t, fh.mgr.Activate(t.Context()))
	tActivate := time.Since(tA)
	sh.url.Store(followerURL) // writers, probes and batch clients now use the new leader
	hPromoted := blockHeightQuiet(followerURL)

	var tLatest, tNewBlock time.Duration
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && (tLatest == 0 || tNewBlock == 0) {
		if tLatest == 0 {
			if _, rerr, err := rpcRaw(followerURL, "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": "latest"}); outcome(rerr, err) == "ok" {
				tLatest = time.Since(t0)
			}
		}
		if tNewBlock == 0 && blockHeightQuiet(followerURL) > hPromoted {
			tNewBlock = time.Since(t0)
		}
		time.Sleep(100 * time.Millisecond)
	}
	followerServesAfter := 0
	for _, o := range observed {
		if _, rerr, err := rpcRaw(followerURL, "get_inclusion_proofs.v1", map[string]any{"stateIds": ids, "stateRoot": o.root}); outcome(rerr, err) == "ok" {
			followerServesAfter++
		}
	}
	t.Logf("HA FAILOVER shard 0: leader stopped after %s, follower sync %s, activate %s; first latest batch ok at %s, first block finalized by the new leader at %s after the stop (height %d at the stop, %d when promoted); "+
		"leader-observed roots served by the new leader: %d of %d (follower retains replayed states, not every block)",
		tLeaderStopped.Round(time.Millisecond), tSync.Round(time.Millisecond), tActivate.Round(time.Millisecond), tLatest.Round(time.Millisecond), tNewBlock.Round(time.Millisecond),
		hBefore, hPromoted, followerServesAfter, len(observed))
	require.NotZero(t, tLatest)
	require.NotZero(t, tNewBlock)
	c.rec.Store(nil)
}
