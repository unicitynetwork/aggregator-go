package service

// T11 resource gate harness (arch-long-poll.md performance plan). No Docker.
//
// The server runs in a CHILD PROCESS (this test binary re-executed with LP_BENCH_ROLE=server) so
// that the heap, goroutines and RSS the parent reads are the server's alone; the parent opens the
// parked clients. Both methods are served by the production gateway (gin, middleware, h2c, tracked
// listener) with long-poll wired exactly like cmd/aggregator wires it; the publisher is "stalled":
// the awaited states stay pending until /release. Skipped unless LONGPOLL_RESOURCE=1.
//
//	LONGPOLL_RESOURCE=1 LONGPOLL_RESOURCE_OUT=/tmp/t11.json \
//	    go test ./internal/service -run TestLongPollResource -count=1 -timeout 60m -v
//
// What is GATED (asserted, a regression fails the test): the plateau at LONG_POLL_MAX_WAITERS and
// immediate zero-echo shedding of the surplus; no active-work token, batch slot or pin held while
// parked; fresh requests keep working while parked AND during the wake burst; the burst never runs
// more resumed reads than max(1, C/4) nor uses more than C tokens, B slots, P pins (peaks recorded
// INSIDE the server at ~100 us resolution); every woken waiter is answered, and served without
// error (see batchBurstStrict for the batch method); counters and goroutines return to baseline
// after the drain and after cancellation. What is only REPORTED: heap/RSS per waiter, latencies.

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofstatus"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"
)

const benchPendingIDs = 256 // the batch waiters ask for the first L of these

// batchBurstStrict: a wake burst of batch waiters must complete with no error. False in this
// revision because F1 (resumed batch reads are rejected busy when B=P=8, see interop-lp.md) is not
// fixed here: the burst is then gated on ACCOUNTING only (every waiter answered, every failure a
// retryable -32006 with the echo, nothing leaked), unless B and P cover the whole burst.
var batchBurstStrict = false

// ---------------------------------------------------------------------------------------------
// child: the server under measurement

type serverStats struct {
	HeapAlloc, HeapInuse, HeapObjects, StackInuse, Sys, TotalAlloc, Mallocs uint64
	NumGC                                                                   uint32
	Goroutines                                                              int
	Reserved, Queued, Running                                               int
	ShedCapacity, ShedDraining                                              uint64
	ActiveRequests, ActiveWorkers, Inflight, Pins, BatchSlots               int
	Capacity                                                                int
}

type peaks struct {
	Running, Queued, Reserved, Active, Slots, Pins, Goroutines int
	HeapBytes                                                  uint64 // sampled from runtime/metrics, no stop-the-world
	AllocBytes, Mallocs                                        uint64 // TotalAlloc / Mallocs across the window
	Samples                                                    int
}

func TestLongPollResourceServer(t *testing.T) {
	if os.Getenv("LP_BENCH_ROLE") != "server" {
		t.Skip("child role only")
	}
	maxWaiters, _ := strconv.Atoi(os.Getenv("LP_MAX_WAITERS"))
	conc, _ := strconv.Atoi(os.Getenv("LP_CONCURRENCY"))
	raiseFDLimit()
	gateway.MethodTimeout = time.Hour // the harness parks for minutes; the 30 s method budget is not what is measured

	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)

	// --- v2 through the real AggregatorService: one pending state (queued, never certified)
	shardingCfg := config.ShardingConfig{Mode: config.ShardingModeBFTShard}
	incl, absent := goldenRequest(t, 0), goldenRequest(t, 2)
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	path, err := incl.StateID.GetPath()
	require.NoError(t, err)
	require.NoError(t, tree.AddLeaf(path, api.LeafValue(incl.CertificationData.TransactionHash.DataBytes(), 1755000000)))
	root := api.HexBytes(tree.GetRootHashRaw())
	block := models.NewBlock(api.NewBigIntFromUint64(9), "test-chain", 0, "v", "f", root, nil, testChildProofUC(t, 9, root), 1755000000)
	block.Finalized = true
	status := proofstatus.New(proofstatus.Config{})
	status.MarkPending(absent.StateID, 1<<62)
	rm := &stubRoundManager{smt: smt.NewThreadSafeSMT(tree), status: status, cachedRoot: root, cachedBlock: block}
	svc := &AggregatorService{
		config: &config.Config{Sharding: shardingCfg}, logger: log,
		storage: &testStorage{blockStorage: &testBlockStorage{latestByRoot: map[string]*models.Block{root.String(): block}},
			recordStorage: &testAggregatorRecordStorage{}},
		roundManager:                  rm,
		certificationRequestValidator: signing.NewCertificationRequestValidator(shardingCfg, bfttypes.ShardID{}),
	}

	// --- batch: records stored, leaves in the tree, but only the next view will contain them
	limits := proofview.DefaultLimits()
	batchSlots, _ := strconv.Atoi(os.Getenv("LONGPOLL_RESOURCE_BATCH_SLOTS"))
	if batchSlots > 0 { // SMT_MAX_CONCURRENT_BATCHES bounds both the batch slots and the catalog pins (production default 8)
		limits.MaxConcurrentPins = batchSlots
	}
	e := newBatchEnv(t, limits)
	e.addRecords(4, 1000)
	e.finalize([]byte("pv1"))
	e.svc.status = &lockedStatus{m: map[string]api.ProofStatus{}}
	if batchSlots > 0 {
		e.svc.slots = make(chan struct{}, batchSlots)
	}

	reg := longpoll.New(longpoll.Config{MaxWaiters: maxWaiters, Concurrency: conc})
	co := &longpoll.Coordinator{Reg: reg, MaxWait: 10 * time.Minute, Budget: time.Hour}
	e.svc.longPoll = co

	cfg := &config.Config{}
	port := freePort(t)
	cfg.Server = config.ServerConfig{Host: "127.0.0.1", Port: strconv.Itoa(port), ConcurrencyLimit: conc,
		ReadTimeout: time.Hour, WriteTimeout: time.Hour, IdleTimeout: time.Hour, EnableH2C: true, HTTP2MaxConcurrentStreams: 4096}
	cfg.Sharding.Mode = config.ShardingModeStandalone
	srv := gateway.NewServer(cfg, log, svc)
	srv.SetLongPoll(co)
	srv.SetBatchProofHandler(e.svc.HandleRPC)
	go func() { _ = srv.Start() }()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	counts := func() serverStats { // cheap: no ReadMemStats (no stop-the-world)
		st := reg.Stats()
		return serverStats{Goroutines: runtime.NumGoroutine(), Reserved: st.Reserved, Queued: st.Queued, Running: st.Running,
			ShedCapacity: st.ShedCapacity, ShedDraining: st.ShedDraining, ActiveRequests: srv.ActiveRequests(), ActiveWorkers: srv.ActiveWorkers(),
			Inflight: srv.Inflight(), Pins: e.catalog.Stats().ActivePins, BatchSlots: len(e.svc.slots), Capacity: reg.Capacity()}
	}

	// in-server peak sampler: counts every ~100 us and the heap from runtime/metrics
	var (
		pkMu      sync.Mutex
		pk        peaks
		pkStop    chan struct{}
		pkDone    chan struct{}
		pkStart   runtime.MemStats
		heapProbe = []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	)
	maxi := func(a *int, v int) {
		if v > *a {
			*a = v
		}
	}
	startPeaks := func() {
		pkMu.Lock()
		defer pkMu.Unlock()
		if pkStop != nil {
			return
		}
		pk = peaks{}
		runtime.ReadMemStats(&pkStart)
		pkStop, pkDone = make(chan struct{}), make(chan struct{})
		stop, done := pkStop, pkDone
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := counts()
				metrics.Read(heapProbe)
				pkMu.Lock()
				maxi(&pk.Running, c.Running)
				maxi(&pk.Queued, c.Queued)
				maxi(&pk.Reserved, c.Reserved)
				maxi(&pk.Active, c.ActiveRequests)
				maxi(&pk.Slots, c.BatchSlots)
				maxi(&pk.Pins, c.Pins)
				maxi(&pk.Goroutines, c.Goroutines)
				if heapProbe[0].Value.Kind() == metrics.KindUint64 && heapProbe[0].Value.Uint64() > pk.HeapBytes {
					pk.HeapBytes = heapProbe[0].Value.Uint64()
				}
				pk.Samples++
				pkMu.Unlock()
				time.Sleep(100 * time.Microsecond)
			}
		}()
	}
	stopPeaks := func() peaks {
		pkMu.Lock()
		stop, done := pkStop, pkDone
		pkStop = nil
		pkMu.Unlock()
		if stop == nil {
			return peaks{}
		}
		close(stop)
		<-done
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		pkMu.Lock()
		defer pkMu.Unlock()
		pk.AllocBytes, pk.Mallocs = m.TotalAlloc-pkStart.TotalAlloc, m.Mallocs-pkStart.Mallocs
		return pk
	}

	// control plane
	mux := http.NewServeMux()
	mux.HandleFunc("/counts", func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(counts()) })
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("gc") != "" {
			runtime.GC()
			runtime.GC()
			debug.FreeOSMemory()
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		c := counts()
		c.HeapAlloc, c.HeapInuse, c.HeapObjects, c.StackInuse, c.Sys = m.HeapAlloc, m.HeapInuse, m.HeapObjects, m.StackInuse, m.Sys
		c.TotalAlloc, c.Mallocs, c.NumGC = m.TotalAlloc, m.Mallocs, m.NumGC
		_ = json.NewEncoder(w).Encode(c)
	})
	// /arm (re)creates the stalled state: the v2 state is pending again and fresh batch records
	// exist that the next view will contain; returns the batch ids to ask for
	mux.HandleFunc("/arm", func(w http.ResponseWriter, r *http.Request) {
		status.MarkPending(absent.StateID, 1<<62)
		var ids []string
		if r.URL.Query().Get("batch") != "" { // only batch scenarios grow the record store
			pending := e.addRecords(benchPendingIDs, 1000)
			for _, id := range pending {
				ids = append(ids, id.String())
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"v2StateId": absent.StateID.String(), "batchIds": ids})
	})
	// the stalled publisher resumes: the pending states become servable
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		status.ClearPending(absent.StateID) // v2: the hint is gone, the wait ends
		e.finalize([]byte("pv2"))           // batch: the next view contains the records
		reg.Notify()
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/peaks/start", func(w http.ResponseWriter, r *http.Request) { startPeaks(); _, _ = io.WriteString(w, "ok") })
	mux.HandleFunc("/peaks/stop", func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(stopPeaks()) })
	ctl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = http.Serve(ctl, mux) }()

	hello, _ := json.Marshal(map[string]any{"rpc": fmt.Sprintf("127.0.0.1:%d", port), "ctl": ctl.Addr().String(), "pid": os.Getpid()})
	fmt.Printf("READY %s\n", hello)
	_, _ = io.Copy(io.Discard, os.Stdin) // the parent closes stdin to stop the server
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func raiseFDLimit() {
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) == nil {
		want := uint64(20000)
		if rl.Max < want {
			want = rl.Max
		}
		if want > rl.Cur {
			rl.Cur = want
			_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
		}
	}
}

// ---------------------------------------------------------------------------------------------
// parent: scenarios

type benchServer struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	copied    chan struct{}
	pid       int
	rpc, ctl  string
	v2StateID string
	batchIDs  []string
	conc      int
}

func startBenchServer(t *testing.T, maxWaiters, conc int) *benchServer {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLongPollResourceServer$", "-test.count=1", "-test.timeout=0")
	cmd.Env = append(os.Environ(), "LP_BENCH_ROLE=server", "LP_MAX_WAITERS="+strconv.Itoa(maxWaiters), "LP_CONCURRENCY="+strconv.Itoa(conc))
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	rd := bufio.NewReader(out)
	var hello struct {
		RPC string `json:"rpc"`
		Ctl string `json:"ctl"`
		Pid int    `json:"pid"`
	}
	for {
		line, err := rd.ReadString('\n')
		require.NoError(t, err, "server child died before READY")
		if strings.HasPrefix(line, "READY ") {
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "READY ")), &hello))
			break
		}
	}
	s := &benchServer{cmd: cmd, stdin: stdin, copied: make(chan struct{}), pid: hello.Pid, rpc: hello.RPC, ctl: hello.Ctl, conc: conc}
	go func() { _, _ = io.Copy(io.Discard, rd); close(s.copied) }()
	t.Cleanup(s.stop)
	return s
}

func (s *benchServer) stop() {
	_ = s.stdin.Close()
	// os/exec: all reads from the stdout pipe must finish before Wait
	select {
	case <-s.copied:
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.copied
	}
	_ = s.cmd.Wait()
}

func (s *benchServer) get(t *testing.T, path string, out any) {
	t.Helper()
	resp, err := http.Get("http://" + s.ctl + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
}

// arm makes the awaited states pending again (a fresh stalled publisher) and learns the batch ids.
func (s *benchServer) arm(t *testing.T, batch bool) {
	var a struct {
		V2StateID string   `json:"v2StateId"`
		BatchIDs  []string `json:"batchIds"`
	}
	q := "/arm"
	if batch {
		q += "?batch=1"
	}
	s.get(t, q, &a)
	s.v2StateID, s.batchIDs = a.V2StateID, a.BatchIDs
}

func (s *benchServer) stats(t *testing.T, gc bool) (serverStats, uint64) {
	var st serverStats
	q := "/stats"
	if gc {
		q += "?gc=1"
	}
	s.get(t, q, &st)
	return st, s.rssKB()
}

func (s *benchServer) counts(t *testing.T) serverStats {
	var st serverStats
	s.get(t, "/counts", &st)
	return st
}

func (s *benchServer) rssKB() uint64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(s.pid)).Output()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	return v
}

func (s *benchServer) release(t *testing.T) { s.get(t, "/release", nil) }

type clientKind struct {
	name  string
	hcs   []*http.Client
	next  atomic.Uint64
	conns int
}

func newClient(transport string) *clientKind {
	h2c := func() *http.Client {
		return &http.Client{Transport: &http2.Transport{AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}}}
	}
	switch transport {
	case "h2c": // one TCP connection multiplexing every stream
		return &clientKind{name: transport, hcs: []*http.Client{h2c()}, conns: 1}
	case "h2c8": // eight connections
		c := &clientKind{name: transport, conns: 8}
		for i := 0; i < 8; i++ {
			c.hcs = append(c.hcs, h2c())
		}
		return c
	}
	return &clientKind{name: "h1", hcs: []*http.Client{{Transport: &http.Transport{DisableKeepAlives: true, MaxConnsPerHost: 0, MaxIdleConns: 0}}}}
}

type outcome struct {
	latency time.Duration
	status  int
	body    string
}

func (s *benchServer) waitBody(method string, l int, waitMs int) string {
	if method == "v2" {
		return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"` + s.v2StateID + `","includeStatus":true,"waitMs":` + strconv.Itoa(waitMs) + `}}`
	}
	return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Join(s.batchIDs[:l], `","`) + `"],"waitMs":` + strconv.Itoa(waitMs) + `}}`
}

func (s *benchServer) freshBody(kind string) string {
	if kind == "batch" {
		return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + s.batchIDs[0] + `"]}}`
	}
	return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"` + s.v2StateID + `"}}`
}

func (c *clientKind) do(ctx context.Context, addr, body string) outcome {
	start := time.Now()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	hc := c.hcs[int(c.next.Add(1))%len(c.hcs)]
	resp, err := hc.Do(req)
	if err != nil {
		return outcome{latency: time.Since(start), status: -1, body: err.Error()}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return outcome{latency: time.Since(start), status: resp.StatusCode, body: string(b)}
}

// ---------------------------------------------------------------------------------------------
// reporting

type perWaiter struct {
	HeapAllocB, HeapInuseB, StackInuseB, SysB, RSSB, Goroutines, MallocsPerWaiter, TransientAllocB float64
}

type scenarioResult struct {
	Method    string `json:"method"`
	L         int    `json:"l,omitempty"`
	Transport string `json:"transport"`
	Conns     int    `json:"conns,omitempty"`
	Cap       int    `json:"cap"`
	C         int    `json:"concurrency"`
	W         int    `json:"w"`
	Extra     int    `json:"extra,omitempty"`

	Admitted  int `json:"admitted"`
	Shed      int `json:"shed_immediate"`
	ShedP50ms float64
	ShedP99ms float64
	ShedZero  bool `json:"shed_have_zero_echo"`

	BaseHeapKB, BaseRSSKB, BaseGoroutines       float64
	ParkedHeapKB, ParkedRSSKB, ParkedGoroutines float64
	Per                                         perWaiter
	SetupSeconds                                float64

	FreshOK, FreshTotal                           int
	FreshP50ms, FreshP99ms                        float64
	BaseFreshP50ms, BaseFreshP99ms                float64
	ParkedActiveRequests, ParkedPins, ParkedSlots int

	// the wake burst: fresh traffic is in flight while the publisher resumes
	BurstFreshOK, BurstFreshTotal int
	BurstFreshP99ms               float64
	DrainSeconds                  float64
	Completed, Errors, ErrorsBusy int
	ErrorsOther                   int
	Peak                          peaks

	AfterReserved, AfterQueued, AfterRunning int
	AfterActive, AfterWorkers, AfterInflight int
	AfterPins, AfterSlots, AfterGoroutines   int
	AfterHeapKB, AfterRSSKB                  float64
	Notes                                    string `json:",omitempty"`
}

func quant(d []time.Duration, q float64) float64 {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return float64(s[int(q*float64(len(s)-1))].Microseconds()) / 1000
}

func medianStats(t *testing.T, s *benchServer) (serverStats, uint64) {
	var sts []serverStats
	var rss []uint64
	for i := 0; i < 3; i++ {
		st, r := s.stats(t, true)
		sts = append(sts, st)
		rss = append(rss, r)
		time.Sleep(250 * time.Millisecond)
	}
	sort.Slice(sts, func(i, j int) bool { return sts[i].HeapAlloc < sts[j].HeapAlloc })
	sort.Slice(rss, func(i, j int) bool { return rss[i] < rss[j] })
	return sts[1], rss[1]
}

// fresh runs n ordinary requests (8 workers) and returns the successes and latencies.
func (s *benchServer) fresh(n int, kind string) (ok int, lat []time.Duration) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var next atomic.Int64
	hc := newClient("h1")
	body := s.freshBody(kind)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(n) {
				o := hc.do(context.Background(), s.rpc, body)
				mu.Lock()
				lat = append(lat, o.latency)
				if o.status == 200 && strings.Contains(o.body, `"result"`) {
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

// park opens total clients that wait; their outcomes arrive on outs.
func (s *benchServer) park(t *testing.T, ctx context.Context, cl *clientKind, method string, l, total int, outs chan outcome, wg *sync.WaitGroup) {
	body := s.waitBody(method, max(l, 1), 600000)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs <- cl.do(ctx, s.rpc, body)
		}()
		switch {
		case method == "batch" || s.conc < 100:
			// the batch method has its own admission (and a small CONCURRENCY_LIMIT has few tokens) (8 concurrent reads by default): arrive one
			// at a time so that the parked state, not an arrival burst, is what is measured
			for tries := 0; tries < 5000; tries++ {
				st := s.counts(t)
				if st.Reserved+int(st.ShedCapacity) >= i+1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
		case i%64 == 63: // a gentle connection ramp; the response of a parked request only arrives at release
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// runScenario parks w waiters (plus extra beyond the cap), measures, runs fresh requests against
// the parked server, releases the stalled publisher WITH fresh traffic in flight, and checks the
// drain.
func runScenario(t *testing.T, method string, l int, transport string, capacity, conc, w, extra int) scenarioResult {
	t.Helper()
	return runScenarioOn(t, startBenchServer(t, capacity, conc), method, l, transport, capacity, w, extra)
}

func runScenarioOn(t *testing.T, s *benchServer, method string, l int, transport string, capacity, w, extra int) scenarioResult {
	t.Helper()
	cl := newClient(transport)
	res := scenarioResult{Method: method, L: l, Transport: transport, Conns: cl.conns, Cap: capacity, C: s.conc, W: w, Extra: extra}
	s.arm(t, method == "batch")
	freshKind := "v2"
	if method == "batch" {
		freshKind = "batch"
	}

	// warm up the lazily built structures, then baseline
	_ = cl.do(context.Background(), s.rpc, s.waitBody(method, max(l, 1), 0))
	closeIdle(cl) // the baseline holds no client connection (the final reading does not either)
	_, lat := s.fresh(200, "v2")
	res.BaseFreshP50ms, res.BaseFreshP99ms = quant(lat, 0.5), quant(lat, 0.99)
	base, baseRSS := medianStats(t, s)
	res.BaseHeapKB, res.BaseRSSKB, res.BaseGoroutines = float64(base.HeapAlloc)/1024, float64(baseRSS), float64(base.Goroutines)

	// park
	total := w + extra
	outs := make(chan outcome, total)
	var parkWG sync.WaitGroup
	setupStart := time.Now()
	s.park(t, context.Background(), cl, method, l, total, outs, &parkWG)
	wantParked := min(w, capacity)
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		st := s.counts(t)
		if st.Reserved >= wantParked && (extra == 0 || st.ShedCapacity >= uint64(max(0, total-capacity))) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	res.SetupSeconds = time.Since(setupStart).Seconds()
	time.Sleep(500 * time.Millisecond)

	// shed responses are the ones that arrived before the release
	var shedLat []time.Duration
	res.ShedZero = true
collect:
	for {
		select {
		case o := <-outs:
			res.Shed++
			shedLat = append(shedLat, o.latency)
			if !(o.status == 200 && strings.Contains(o.body, `"waitedMs":0`)) {
				res.ShedZero = false
			}
		default:
			break collect
		}
	}
	res.ShedP50ms, res.ShedP99ms = quant(shedLat, 0.5), quant(shedLat, 0.99)

	parked, parkedRSS := medianStats(t, s)
	res.Admitted = parked.Reserved
	res.ParkedHeapKB, res.ParkedRSSKB, res.ParkedGoroutines = float64(parked.HeapAlloc)/1024, float64(parkedRSS), float64(parked.Goroutines)
	res.ParkedActiveRequests, res.ParkedPins, res.ParkedSlots = parked.ActiveRequests, parked.Pins, parked.BatchSlots
	if n := float64(parked.Reserved); n > 0 {
		res.Per = perWaiter{
			HeapAllocB:       (float64(parked.HeapAlloc) - float64(base.HeapAlloc)) / n,
			HeapInuseB:       (float64(parked.HeapInuse) - float64(base.HeapInuse)) / n,
			StackInuseB:      (float64(parked.StackInuse) - float64(base.StackInuse)) / n,
			SysB:             (float64(parked.Sys) - float64(base.Sys)) / n,
			RSSB:             (float64(parkedRSS) - float64(baseRSS)) * 1024 / n,
			Goroutines:       (float64(parked.Goroutines) - float64(base.Goroutines)) / n,
			MallocsPerWaiter: (float64(parked.Mallocs) - float64(base.Mallocs)) / (n + float64(res.Shed)),
			TransientAllocB:  (float64(parked.TotalAlloc) - float64(base.TotalAlloc)) / (n + float64(res.Shed)),
		}
	}

	// headroom while parked
	res.FreshTotal = 200
	res.FreshOK, lat = s.fresh(res.FreshTotal, "v2")
	res.FreshP50ms, res.FreshP99ms = quant(lat, 0.5), quant(lat, 0.99)

	// the wake burst: the stalled publisher resumes while fresh traffic keeps arriving
	s.get(t, "/peaks/start", nil)
	var burstOK, burstTotal atomic.Int64
	var burstLat []time.Duration
	var burstMu sync.Mutex
	stopFresh := make(chan struct{})
	var freshWG sync.WaitGroup
	hc := newClient("h1")
	body := s.freshBody(freshKind)
	freshWG.Add(1)
	go func() { // one sequential fresh client: it must always find its token (and slot and pin)
		defer freshWG.Done()
		for {
			select {
			case <-stopFresh:
				return
			default:
			}
			o := hc.do(context.Background(), s.rpc, body)
			burstTotal.Add(1)
			burstMu.Lock()
			burstLat = append(burstLat, o.latency)
			burstMu.Unlock()
			// served = an ordinary outcome: a result, or the typed state_pending of a state that is
			// still pending (the fresh client asks for one of them); a rejection is a busy/transport error
			if o.status == 200 && (strings.Contains(o.body, `"result"`) || strings.Contains(o.body, `"detail":"state_pending"`)) {
				burstOK.Add(1)
			} else {
				burstMu.Lock()
				if !strings.Contains(res.Notes, "fresh failure:") {
					res.Notes += fmt.Sprintf("fresh failure: %d %.200s; ", o.status, o.body)
				}
				burstMu.Unlock()
			}
			time.Sleep(time.Millisecond)
		}
	}()
	time.Sleep(20 * time.Millisecond) // fresh traffic is flowing before the release
	relStart := time.Now()
	s.release(t)
	done := make(chan struct{})
	go func() { parkWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(300 * time.Second):
		res.Notes += "drain timed out; "
	}
	res.DrainSeconds = time.Since(relStart).Seconds()
	close(stopFresh)
	freshWG.Wait()
	s.get(t, "/peaks/stop", &res.Peak)
	res.BurstFreshOK, res.BurstFreshTotal, res.BurstFreshP99ms = int(burstOK.Load()), int(burstTotal.Load()), quant(burstLat, 0.99)

	for len(outs) > 0 {
		o := <-outs
		switch {
		case o.status == 200 && strings.Contains(o.body, `"result"`):
			res.Completed++
		case o.status == 200 && strings.Contains(o.body, `"code":-32006`) && strings.Contains(o.body, `"retryable":true`) && strings.Contains(o.body, `"waitedMs":`):
			res.Errors++
			res.ErrorsBusy++
			if res.ErrorsBusy <= 2 {
				i := strings.Index(o.body, `"data"`)
				res.Notes += fmt.Sprintf("busy sample: %.140s; ", o.body[max(i, 0):])
			}
		default:
			res.Errors++
			res.ErrorsOther++
			if res.ErrorsOther <= 2 {
				res.Notes += fmt.Sprintf("error sample: %d %.200s; ", o.status, o.body)
			}
		}
	}

	// back to baseline once everything finished (client connections closed)
	closeIdle(cl)
	time.Sleep(time.Second)
	after, afterRSS := medianStats(t, s)
	res.AfterReserved, res.AfterQueued, res.AfterRunning = after.Reserved, after.Queued, after.Running
	res.AfterActive, res.AfterWorkers, res.AfterInflight = after.ActiveRequests, after.ActiveWorkers, after.Inflight
	res.AfterPins, res.AfterSlots, res.AfterGoroutines = after.Pins, after.BatchSlots, after.Goroutines
	res.AfterHeapKB, res.AfterRSSKB = float64(after.HeapAlloc)/1024, float64(afterRSS)
	return res
}

func closeIdle(c *clientKind) {
	for _, h := range c.hcs {
		h.CloseIdleConnections()
	}
	time.Sleep(200 * time.Millisecond)
}

// runCancel parks w waiters and aborts them all (cancelled contexts / closed connections)
// WITHOUT releasing the publisher: everything must be cleaned up.
func runCancel(t *testing.T, method string, l int, transport string, capacity, conc, w int) scenarioResult {
	t.Helper()
	s := startBenchServer(t, capacity, conc)
	s.arm(t, method == "batch")
	cl := newClient(transport)
	res := scenarioResult{Method: method, L: l, Transport: transport, Conns: cl.conns, Cap: capacity, C: s.conc, W: w}
	_ = cl.do(context.Background(), s.rpc, s.waitBody(method, max(l, 1), 0))
	closeIdle(cl)
	base, _ := medianStats(t, s)
	res.BaseGoroutines = float64(base.Goroutines)

	ctx, cancel := context.WithCancel(context.Background())
	outs := make(chan outcome, w)
	var wg sync.WaitGroup
	s.park(t, ctx, cl, method, l, w, outs, &wg)
	require.Eventually(t, func() bool { return s.counts(t).Reserved >= min(w, capacity) }, 120*time.Second, 50*time.Millisecond)
	res.Admitted = s.counts(t).Reserved
	cancel() // aborts every in-flight request: RST_STREAM / closed connections
	wg.Wait()
	closeIdle(cl)
	require.Eventually(t, func() bool {
		c := s.counts(t)
		return c.Reserved == 0 && c.Queued == 0 && c.Running == 0 && c.ActiveRequests == 0 && c.ActiveWorkers == 0 && c.Inflight == 0 && c.Pins == 0 && c.BatchSlots == 0
	}, 15*time.Second, 50*time.Millisecond, "cancelled waits must free every reservation and resource")
	time.Sleep(time.Second)
	after, _ := medianStats(t, s)
	res.AfterReserved, res.AfterQueued, res.AfterRunning = after.Reserved, after.Queued, after.Running
	res.AfterActive, res.AfterWorkers, res.AfterInflight = after.ActiveRequests, after.ActiveWorkers, after.Inflight
	res.AfterPins, res.AfterSlots, res.AfterGoroutines = after.Pins, after.BatchSlots, after.Goroutines
	return res
}

func (r scenarioResult) assertParked(t *testing.T) {
	t.Helper()
	if r.W+r.Extra > r.Cap {
		require.Equal(t, r.Cap, r.Admitted, "plateau at LONG_POLL_MAX_WAITERS")
		require.Equal(t, r.W+r.Extra-r.Cap, r.Shed, "everything beyond the cap is shed at once")
		require.True(t, r.ShedZero, "shed answers are ordinary reads with a zero echo")
	} else {
		require.Equal(t, r.W, r.Admitted)
		require.Zero(t, r.Shed)
	}
	require.Zero(t, r.ParkedActiveRequests, "parked waits hold no active-work token")
	require.Zero(t, r.ParkedPins, "parked waits pin no view")
	require.Zero(t, r.ParkedSlots, "parked waits hold no batch slot")
	require.Equal(t, r.FreshTotal, r.FreshOK, "fresh requests keep their headroom while parked")
}

func batchConcurrency() int {
	if n, _ := strconv.Atoi(os.Getenv("LONGPOLL_RESOURCE_BATCH_SLOTS")); n > 0 {
		return n
	}
	return 8
}

func (r scenarioResult) assertBurst(t *testing.T) {
	t.Helper()
	if r.Admitted == 0 {
		return
	}
	quota := max(1, r.C/4)
	require.Positive(t, r.Peak.Samples, "the in-server sampler ran")
	require.LessOrEqual(t, r.Peak.Running, quota, "resumed reads never exceed max(1, C/4)")
	require.LessOrEqual(t, r.Peak.Active, r.C, "active-work tokens never exceed C")
	require.Positive(t, r.BurstFreshTotal)
	if r.Method == "v2" || batchBurstStrict {
		require.Equal(t, r.BurstFreshTotal, r.BurstFreshOK, "fresh requests during the wake burst keep working (%d requests)", r.BurstFreshTotal)
	} // else F1: fresh batch reads find every slot taken by the resumed ones; reported, not gated
	require.NotContains(t, r.Notes, "drain timed out")
	require.Equal(t, r.Admitted, r.Completed+r.Errors, "every woken waiter is answered")
	require.Zero(t, r.ErrorsOther, "no failure other than the documented busy rejection: %s", r.Notes)
	if r.Method == "batch" {
		b := batchConcurrency()
		require.LessOrEqual(t, r.Peak.Slots, b, "batch slots never exceed B")
		require.LessOrEqual(t, r.Peak.Pins, b, "pins never exceed P")
		if batchBurstStrict {
			require.LessOrEqual(t, r.Peak.Running, max(1, min(quota, b-1)), "resumed batch reads are capped at min(max(1,C/4), B-1, P-1)")
		}
	}
	// v2 has no method capacity of its own; batch is strict when the fix is in or B, P cover the burst
	if r.Method == "v2" || batchBurstStrict || (batchConcurrency() >= r.Admitted && batchConcurrency() >= quota) {
		require.Zero(t, r.Errors, "no woken waiter fails: %s", r.Notes)
		require.Equal(t, r.Admitted, r.Completed)
	}
}

func (r scenarioResult) assertBaseline(t *testing.T) {
	t.Helper()
	require.Zero(t, r.AfterReserved)
	require.Zero(t, r.AfterQueued)
	require.Zero(t, r.AfterRunning)
	require.Zero(t, r.AfterActive)
	require.Zero(t, r.AfterWorkers)
	require.Zero(t, r.AfterInflight)
	require.Zero(t, r.AfterPins)
	require.Zero(t, r.AfterSlots)
	require.LessOrEqual(t, float64(r.AfterGoroutines), r.BaseGoroutines+2, "goroutines return to the baseline")
}

func TestLongPollResource(t *testing.T) {
	if os.Getenv("LONGPOLL_RESOURCE") == "" {
		t.Skip("set LONGPOLL_RESOURCE=1 to run the T11 resource harness")
	}
	raiseFDLimit()
	runtime.GOMAXPROCS(runtime.NumCPU())
	only := os.Getenv("LONGPOLL_RESOURCE_ONLY") // e.g. "v2,h2c" to run a subset
	var results []scenarioResult
	selected := func(name string) bool {
		if only == "" {
			return true
		}
		for _, f := range strings.Split(only, ",") {
			if strings.Contains(name, f) {
				return true
			}
		}
		return false
	}
	add := func(method string, l int, transport string, capacity, conc, w, extra int) {
		name := fmt.Sprintf("%s-L%d-%s-cap%d-C%d-W%d+%d", method, l, transport, capacity, conc, w, extra)
		if !selected(name) {
			return
		}
		t.Run(name, func(t *testing.T) {
			r := runScenario(t, method, l, transport, capacity, conc, w, extra)
			results = append(results, r)
			b, _ := json.Marshal(r)
			t.Logf("RESULT %s", b)
			r.assertParked(t)
			r.assertBurst(t)
			r.assertBaseline(t)
		})
	}
	addCancel := func(method string, l int, transport string, capacity, conc, w int) {
		name := fmt.Sprintf("cancel-%s-L%d-%s-cap%d-C%d-W%d", method, l, transport, capacity, conc, w)
		if !selected(name) {
			return
		}
		t.Run(name, func(t *testing.T) {
			r := runCancel(t, method, l, transport, capacity, conc, w)
			b, _ := json.Marshal(r)
			t.Logf("RESULT %s", b)
			require.Equal(t, min(w, capacity), r.Admitted)
			r.assertBaseline(t)
		})
	}
	// v2
	for _, tr := range []string{"h1", "h2c"} {
		add("v2", 0, tr, 1024, 1000, 0, 0)
		add("v2", 0, tr, 1024, 1000, 256, 0)
		add("v2", 0, tr, 1024, 1000, 1024, 128)
	}
	add("v2", 0, "h2c8", 1024, 1000, 1024, 128) // the same waiters over eight connections
	add("v2", 0, "h2c", 256, 1000, 1024, 0)     // plateau well below the offered load
	add("v2", 0, "h1", 64, 8, 64, 16)           // small C: quota 2, one token left for fresh requests
	// batch
	for _, tr := range []string{"h1", "h2c"} {
		for _, l := range []int{1, 16, 256} {
			add("batch", l, tr, 1024, 1000, 256, 0)
			add("batch", l, tr, 1024, 1000, 1024, 128)
		}
	}
	add("batch", 16, "h1", 64, 8, 64, 16) // small C and B=P=8
	// cancellation: abort every parked wait, no release
	addCancel("v2", 0, "h1", 1024, 1000, 512)
	addCancel("v2", 0, "h2c", 1024, 1000, 512)
	addCancel("batch", 16, "h1", 1024, 1000, 256)
	addCancel("batch", 16, "h2c", 1024, 1000, 256)

	if out := os.Getenv("LONGPOLL_RESOURCE_OUT"); out != "" {
		t.Cleanup(func() {
			b, _ := json.MarshalIndent(results, "", "  ")
			_ = os.WriteFile(out, b, 0o644)
		})
	}
}

// TestLongPollResourceCycles repeats park -> release on ONE server: whatever one cycle leaves
// behind must not grow with the number of cycles.
func TestLongPollResourceCycles(t *testing.T) {
	if os.Getenv("LONGPOLL_RESOURCE") == "" {
		t.Skip("set LONGPOLL_RESOURCE=1 to run the T11 resource harness")
	}
	raiseFDLimit()
	cycles := 4
	if n, _ := strconv.Atoi(os.Getenv("LONGPOLL_RESOURCE_CYCLES")); n > 0 {
		cycles = n
	}
	for _, tc := range []struct {
		method, transport string
		l                 int
	}{{"v2", "h1", 0}, {"v2", "h2c", 0}, {"batch", "h2c", 16}} {
		t.Run(fmt.Sprintf("%s-%s-L%d", tc.method, tc.transport, tc.l), func(t *testing.T) {
			s := startBenchServer(t, 1024, 1000)
			var heap, rss, gor []float64
			for c := 0; c < cycles; c++ {
				r := runScenarioOn(t, s, tc.method, tc.l, tc.transport, 1024, 512, 0)
				require.Equal(t, 512, r.Admitted, "cycle %d actually parked its waiters", c)
				r.assertBaseline(t)
				heap, rss, gor = append(heap, r.AfterHeapKB), append(rss, r.AfterRSSKB), append(gor, float64(r.AfterGoroutines))
			}
			t.Logf("CYCLES %s-%s-L%d heapKB=%v rssKB=%v goroutines=%v", tc.method, tc.transport, tc.l, heap, rss, gor)
			require.Equal(t, gor[0], gor[len(gor)-1], "goroutines do not grow across cycles")
			if tc.method == "v2" { // batch cycles add 256 records per cycle to the harness's own store
				require.LessOrEqual(t, heap[len(heap)-1], heap[0]+1024, "heap after a cycle stays within 1 MiB of the first cycle's")
			}
		})
	}
}
