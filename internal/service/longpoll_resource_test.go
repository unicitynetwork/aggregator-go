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

// ---------------------------------------------------------------------------------------------
// child: the server under measurement

type serverStats struct {
	HeapAlloc, HeapInuse, HeapObjects, StackInuse, Sys, TotalAlloc, Mallocs uint64
	NumGC                                                                   uint32
	Goroutines                                                              int
	Reserved, Queued, Running                                               int
	ShedCapacity, ShedDraining                                              uint64
	ActiveRequests, ActiveWorkers, Inflight, Conns, Pins, BatchSlots        int
	Capacity                                                                int
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
	pending := e.addRecords(benchPendingIDs, 1000)
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

	// control plane
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("gc") != "" {
			runtime.GC()
			runtime.GC()
			debug.FreeOSMemory()
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		st := reg.Stats()
		_ = json.NewEncoder(w).Encode(serverStats{
			HeapAlloc: m.HeapAlloc, HeapInuse: m.HeapInuse, HeapObjects: m.HeapObjects, StackInuse: m.StackInuse, Sys: m.Sys,
			TotalAlloc: m.TotalAlloc, Mallocs: m.Mallocs, NumGC: m.NumGC, Goroutines: runtime.NumGoroutine(),
			Reserved: st.Reserved, Queued: st.Queued, Running: st.Running, ShedCapacity: st.ShedCapacity, ShedDraining: st.ShedDraining,
			ActiveRequests: srvActive(srv), ActiveWorkers: srvWorkers(srv), Inflight: srv.Inflight(), Pins: e.catalog.Stats().ActivePins,
			BatchSlots: len(e.svc.slots), Capacity: reg.Capacity(),
		})
	})
	// the stalled publisher resumes: the pending states become servable
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		status.ClearPending(absent.StateID) // v2: the hint is gone, the wait ends
		e.finalize([]byte("pv2"))           // batch: the next view contains the records
		reg.Notify()
		_, _ = io.WriteString(w, "ok")
	})
	ctl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = http.Serve(ctl, mux) }()

	ids := make([]string, len(pending))
	for i, id := range pending {
		ids[i] = id.String()
	}
	hello, _ := json.Marshal(map[string]any{"rpc": fmt.Sprintf("127.0.0.1:%d", port), "ctl": ctl.Addr().String(),
		"v2StateId": absent.StateID.String(), "batchIds": ids, "pid": os.Getpid()})
	fmt.Printf("READY %s\n", hello)
	_, _ = io.Copy(io.Discard, os.Stdin) // the parent closes stdin to stop the server
}

func srvActive(s *gateway.Server) int  { return s.ActiveRequests() }
func srvWorkers(s *gateway.Server) int { return s.ActiveWorkers() }

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
	pid       int
	rpc, ctl  string
	v2StateID string
	batchIDs  []string
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
		RPC       string   `json:"rpc"`
		Ctl       string   `json:"ctl"`
		V2StateID string   `json:"v2StateId"`
		BatchIDs  []string `json:"batchIds"`
		Pid       int      `json:"pid"`
	}
	for {
		line, err := rd.ReadString('\n')
		require.NoError(t, err, "server child died before READY")
		if strings.HasPrefix(line, "READY ") {
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "READY ")), &hello))
			break
		}
	}
	go func() { _, _ = io.Copy(io.Discard, rd) }()
	s := &benchServer{cmd: cmd, stdin: stdin, pid: hello.Pid, rpc: hello.RPC, ctl: hello.Ctl, v2StateID: hello.V2StateID, batchIDs: hello.BatchIDs}
	t.Cleanup(s.stop)
	return s
}

func (s *benchServer) stop() {
	_ = s.stdin.Close()
	done := make(chan struct{})
	go func() { _ = s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = s.cmd.Process.Kill()
	}
}

func (s *benchServer) stats(t *testing.T, gc bool) (serverStats, uint64) {
	q := ""
	if gc {
		q = "?gc=1"
	}
	resp, err := http.Get("http://" + s.ctl + "/stats" + q)
	require.NoError(t, err)
	defer resp.Body.Close()
	var st serverStats
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))
	return st, s.rssKB()
}

func (s *benchServer) rssKB() uint64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(s.pid)).Output()
	if err != nil {
		return 0
	}
	v, _ := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	return v
}

func (s *benchServer) release(t *testing.T) {
	resp, err := http.Post("http://"+s.ctl+"/release", "text/plain", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

type clientKind struct {
	name string
	hc   *http.Client
}

func newClient(transport string) clientKind {
	if transport == "h2c" {
		return clientKind{"h2c", &http.Client{Transport: &http2.Transport{AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}}}}
	}
	return clientKind{"h1", &http.Client{Transport: &http.Transport{DisableKeepAlives: true, MaxConnsPerHost: 0, MaxIdleConns: 0}}}
}

type outcome struct {
	latency time.Duration
	status  int
	shed    bool // answered at once with a zero echo
	body    string
}

func (s *benchServer) waitBody(method string, l int, wantWaitMs int) string {
	if method == "v2" {
		return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"` + s.v2StateID + `","includeStatus":true,"waitMs":` + strconv.Itoa(wantWaitMs) + `}}`
	}
	return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Join(s.batchIDs[:l], `","`) + `"],"waitMs":` + strconv.Itoa(wantWaitMs) + `}}`
}

func (s *benchServer) legacyBody() string {
	return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"` + s.v2StateID + `"}}`
}

func (c clientKind) do(addr, body string) outcome {
	start := time.Now()
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return outcome{latency: time.Since(start), status: -1, body: err.Error()}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	o := outcome{latency: time.Since(start), status: resp.StatusCode, body: string(b)}
	return o
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
	Cap       int    `json:"cap"`
	W         int    `json:"w"`
	Extra     int    `json:"extra,omitempty"`

	Admitted  int `json:"admitted"`
	Parked    int `json:"parked_reserved"`
	Shed      int `json:"shed_immediate"`
	ShedP50ms float64
	ShedP99ms float64
	ShedZero  bool `json:"shed_have_zero_echo"`

	BaseHeapKB, BaseRSSKB, BaseGoroutines       float64
	ParkedHeapKB, ParkedRSSKB, ParkedGoroutines float64
	Per                                         perWaiter
	PerIDHeapB                                  float64 `json:",omitempty"`
	SetupSeconds                                float64

	FreshOK, FreshTotal                           int
	FreshP50ms, FreshP99ms                        float64
	BaseFreshP50ms, BaseFreshP99ms                float64
	ParkedActiveRequests, ParkedPins, ParkedSlots int

	DrainSeconds                             float64
	AfterReserved, AfterQueued, AfterRunning int
	AfterActive, AfterWorkers, AfterInflight int
	AfterPins, AfterSlots, AfterGoroutines   int
	AfterHeapKB, AfterRSSKB                  float64
	PeakHeapKB, PeakRunning                  float64
	Completed, Errors                        int
	Notes                                    string `json:",omitempty"`
}

func quant(d []time.Duration, q float64) float64 {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(q * float64(len(s)-1))
	return float64(s[i].Microseconds()) / 1000
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

func freshLatencies(s *benchServer, n int) (ok int, lat []time.Duration) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var next atomic.Int64
	hc := newClient("h1")
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(n) {
				o := hc.do(s.rpc, s.legacyBody())
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

// runScenario parks w waiters (plus extra beyond the cap), measures, runs fresh requests against
// the parked server, releases the stalled publisher and checks the drain.
func runScenario(t *testing.T, method string, l int, transport string, capacity, w, extra int) scenarioResult {
	t.Helper()
	return runScenarioOn(t, startBenchServer(t, capacity, 1000), method, l, transport, capacity, w, extra)
}

// runScenarioOn is runScenario against an already running server (repeated cycles).
func runScenarioOn(t *testing.T, s *benchServer, method string, l int, transport string, capacity, w, extra int) scenarioResult {
	t.Helper()
	res := scenarioResult{Method: method, L: l, Transport: transport, Cap: capacity, W: w, Extra: extra}

	// warm up the lazily built structures (one immediate request of each kind), then baseline
	cl := newClient(transport)
	_ = cl.do(s.rpc, s.waitBody(method, max(l, 1), 0))
	ok, lat := freshLatencies(s, 200)
	_ = ok
	res.BaseFreshP50ms, res.BaseFreshP99ms = quant(lat, 0.5), quant(lat, 0.99)
	base, baseRSS := medianStats(t, s)
	res.BaseHeapKB, res.BaseRSSKB, res.BaseGoroutines = float64(base.HeapAlloc)/1024, float64(baseRSS), float64(base.Goroutines)

	// peak sampler
	var peakHeap, peakRunning atomic.Uint64
	stopSampler := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSampler:
				return
			case <-time.After(100 * time.Millisecond):
				resp, err := http.Get("http://" + s.ctl + "/stats")
				if err != nil {
					continue
				}
				var st serverStats
				_ = json.NewDecoder(resp.Body).Decode(&st)
				resp.Body.Close()
				for {
					p := peakHeap.Load()
					if st.HeapAlloc <= p || peakHeap.CompareAndSwap(p, st.HeapAlloc) {
						break
					}
				}
				for {
					p := peakRunning.Load()
					if uint64(st.Running) <= p || peakRunning.CompareAndSwap(p, uint64(st.Running)) {
						break
					}
				}
			}
		}
	}()

	// park
	body := s.waitBody(method, max(l, 1), 600000)
	total := w + extra
	outs := make(chan outcome, total)
	setupStart := time.Now()
	var parkWG sync.WaitGroup
	for i := 0; i < total; i++ {
		parkWG.Add(1)
		go func() {
			defer parkWG.Done()
			outs <- cl.do(s.rpc, body)
		}()
		switch {
		case method == "batch" && i%4 == 3:
			// the batch method has its own admission (8 concurrent reads by default); arrive no
			// faster than it reads so that the parked state, not the arrival burst, is measured
			for tries := 0; tries < 2000; tries++ {
				st, _ := s.stats(t, false)
				if st.Reserved+int(st.ShedCapacity) >= i+1-2 {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
		case i%64 == 63: // a gentle connection ramp; the response of a parked request only arrives at release
			time.Sleep(20 * time.Millisecond)
		}
	}
	// requests are in flight; wait until the server reached the expected plateau
	wantParked := w
	if wantParked > capacity {
		wantParked = capacity
	}
	var st serverStats
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		st, _ = s.stats(t, false)
		if st.Reserved >= wantParked && (extra == 0 || st.ShedCapacity >= uint64(max(0, total-capacity))) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	res.SetupSeconds = time.Since(setupStart).Seconds()
	time.Sleep(500 * time.Millisecond)

	// shed responses are the ones that arrive before the release
	var shedLat []time.Duration
	shedZero := true
collect:
	for {
		select {
		case o := <-outs:
			res.Shed++
			shedLat = append(shedLat, o.latency)
			if !(o.status == 200 && strings.Contains(o.body, `"waitedMs":0`)) {
				shedZero = false
			}
		default:
			break collect
		}
	}
	res.ShedZero = shedZero
	res.ShedP50ms, res.ShedP99ms = quant(shedLat, 0.5), quant(shedLat, 0.99)

	parked, parkedRSS := medianStats(t, s)
	res.Parked = parked.Reserved
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

	// headroom: ordinary requests while everything is parked
	res.FreshTotal = 200
	res.FreshOK, lat = freshLatencies(s, res.FreshTotal)
	res.FreshP50ms, res.FreshP99ms = quant(lat, 0.5), quant(lat, 0.99)

	// release the stalled publisher: a synchronized wake burst
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
	close(stopSampler)
	for len(outs) > 0 {
		o := <-outs
		if o.status == 200 && strings.Contains(o.body, `"result"`) {
			res.Completed++
		} else {
			res.Errors++
			if res.Errors <= 2 {
				res.Notes += fmt.Sprintf("error sample: %d %.400s; ", o.status, o.body)
			}
		}
	}
	res.PeakHeapKB, res.PeakRunning = float64(peakHeap.Load())/1024, float64(peakRunning.Load())

	// back to baseline once everything finished (connections of h1 are closed per request)
	time.Sleep(1 * time.Second)
	after, afterRSS := medianStats(t, s)
	res.AfterReserved, res.AfterQueued, res.AfterRunning = after.Reserved, after.Queued, after.Running
	res.AfterActive, res.AfterWorkers, res.AfterInflight = after.ActiveRequests, after.ActiveWorkers, after.Inflight
	res.AfterPins, res.AfterSlots, res.AfterGoroutines = after.Pins, after.BatchSlots, after.Goroutines
	res.AfterHeapKB, res.AfterRSSKB = float64(after.HeapAlloc)/1024, float64(afterRSS)
	cl.hc.CloseIdleConnections()
	return res
}

func TestLongPollResource(t *testing.T) {
	if os.Getenv("LONGPOLL_RESOURCE") == "" {
		t.Skip("set LONGPOLL_RESOURCE=1 to run the T11 resource harness")
	}
	raiseFDLimit()
	runtime.GOMAXPROCS(runtime.NumCPU())
	only := os.Getenv("LONGPOLL_RESOURCE_ONLY") // e.g. "v2,h2c" to run a subset
	var results []scenarioResult
	add := func(method string, l int, transport string, capacity, w, extra int) {
		name := fmt.Sprintf("%s-L%d-%s-cap%d-W%d+%d", method, l, transport, capacity, w, extra)
		if only != "" {
			ok := false
			for _, f := range strings.Split(only, ",") {
				if strings.Contains(name, f) {
					ok = true
				}
			}
			if !ok {
				return
			}
		}
		t.Run(name, func(t *testing.T) {
			r := runScenario(t, method, l, transport, capacity, w, extra)
			results = append(results, r)
			b, _ := json.Marshal(r)
			t.Logf("RESULT %s", b)
			// the gate: plateau, immediate zero-echo shedding, headroom, baseline after drain
			if w+extra > capacity {
				require.Equal(t, capacity, r.Admitted, "plateau at LONG_POLL_MAX_WAITERS")
				require.Equal(t, w+extra-capacity, r.Shed, "everything beyond the cap is shed at once")
				require.True(t, r.ShedZero, "shed answers are ordinary reads with a zero echo")
			} else {
				require.Equal(t, w, r.Admitted)
			}
			require.Zero(t, r.ParkedActiveRequests, "parked waits hold no active-work token")
			require.Zero(t, r.ParkedPins, "parked waits pin no view")
			require.Zero(t, r.ParkedSlots, "parked waits hold no batch slot")
			require.Equal(t, r.FreshTotal, r.FreshOK, "fresh requests keep their headroom")
			require.Zero(t, r.AfterReserved)
			require.Zero(t, r.AfterQueued)
			require.Zero(t, r.AfterRunning)
			require.Zero(t, r.AfterActive)
			require.Zero(t, r.AfterWorkers)
			require.Zero(t, r.AfterInflight)
			require.Zero(t, r.AfterPins)
			require.Zero(t, r.AfterSlots)
		})
	}
	// v2
	for _, tr := range []string{"h1", "h2c"} {
		add("v2", 0, tr, 1024, 0, 0)
		add("v2", 0, tr, 1024, 256, 0)
		add("v2", 0, tr, 1024, 1024, 128)
	}
	add("v2", 0, "h2c", 256, 1024, 0) // plateau well below the offered load
	// batch
	for _, tr := range []string{"h1", "h2c"} {
		for _, l := range []int{1, 16, 256} {
			add("batch", l, tr, 1024, 256, 0)
			add("batch", l, tr, 1024, 1024, 128)
		}
	}
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
				r := runScenarioOn(t, s, tc.method, tc.l, tc.transport, 1024, 1024, 0)
				require.Zero(t, r.AfterReserved)
				heap, rss, gor = append(heap, r.AfterHeapKB), append(rss, r.AfterRSSKB), append(gor, float64(r.AfterGoroutines))
			}
			t.Logf("CYCLES %s-%s-L%d heapKB=%v rssKB=%v goroutines=%v", tc.method, tc.transport, tc.l, heap, rss, gor)
		})
	}
}
