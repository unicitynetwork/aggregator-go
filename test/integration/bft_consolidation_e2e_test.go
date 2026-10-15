//go:build e2e_bft

// Live master-first consolidation against a real bft-core root node.
//
// A real `ubft` root node runs as a host process (built from the bft-core module
// version pinned in go.mod, so it includes the C1/C2 sidecar producer), N in-process
// bft-shard aggregators validate a complete 4- or 8-shard partition under it
// (MongoDB and Redis run in Docker), writers submit continuously, and the built
// cmd/consolidate-proofs binary consolidates proofs certified under different seals.
//
//	go test -tags=e2e_bft ./test/integration -run TestBFTConsolidationE2E -v -timeout 20m
//
// CONSOLIDATION_E2E_SHARDS selects the shard counts (default "4,8"); UBFT_BIN may
// point at a prebuilt ubft binary; the trusted topology is written to a file in the
// BFT_CONSOLIDATION_TOPOLOGY_FILE format and loaded from there.
package integration

import (
	"bytes"
	"context"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"
	cmdbft "github.com/unicitynetwork/bft-core/cli/ubft/cmd"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
)

func TestBFTConsolidationE2E(t *testing.T) {
	counts := os.Getenv("CONSOLIDATION_E2E_SHARDS")
	if counts == "" {
		counts = "4,8"
	}
	ubft := buildBinary(t, os.Getenv("UBFT_BIN"), "github.com/unicitynetwork/bft-core/cli/ubft", "ubft")
	cli := buildBinary(t, "", "github.com/unicitynetwork/aggregator-go/cmd/consolidate-proofs", "consolidate-proofs")
	mongoURI := startMongoCleanly(t)
	redisHost, redisPort := startRedisCleanly(t)

	for _, s := range strings.Split(counts, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		require.NoError(t, err)
		t.Run(fmt.Sprintf("%d shards", n), func(t *testing.T) {
			runConsolidationE2E(t, n, ubft, cli, mongoURI, redisHost, redisPort)
		})
	}
}

func buildBinary(t *testing.T, prebuilt, pkg, name string) string {
	t.Helper()
	if prebuilt != "" {
		return prebuilt
	}
	out := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", out, pkg)
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, string(b))
	return out
}

type host struct {
	dir     string
	root    *bftRootInfo
	cmd     *exec.Cmd
	rpcAddr string
}

// startHostRoot runs a real ubft root node as a local process.
func startHostRoot(t *testing.T, ubft string, p2pPort, rpcPort int) *bftRootInfo {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command(ubft, args...)
		b, err := cmd.CombinedOutput()
		require.NoError(t, err, "ubft %v: %s", args, b)
	}
	run("root-node", "init", "--home", filepath.Join(dir, "root"), "-g")
	run("trust-base", "generate", "--home", dir, "--network-id", "3", "--node-info", filepath.Join(dir, "root", "node-info.json"))
	run("trust-base", "sign", "--home", filepath.Join(dir, "root"), "--trust-base", filepath.Join(dir, "trust-base.json"))

	logf, err := os.Create(filepath.Join(dir, "root.log"))
	require.NoError(t, err)
	cmd := exec.Command(ubft, "root-node", "run", "--home", filepath.Join(dir, "root"),
		"--address", fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", p2pPort),
		"--trust-base", filepath.Join(dir, "trust-base.json"),
		"--rpc-server-address", fmt.Sprintf("127.0.0.1:%d", rpcPort))
	cmd.Stdout, cmd.Stderr = logf, logf
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			b, _ := os.ReadFile(filepath.Join(dir, "root.log"))
			t.Logf("=== root log (tail) ===\n%s", tail(b, 4000))
		}
	})

	raw, err := os.ReadFile(filepath.Join(dir, "trust-base.json"))
	require.NoError(t, err)
	var tb bfttypes.RootTrustBaseV1
	require.NoError(t, json.Unmarshal(raw, &tb))
	require.NotEmpty(t, tb.GetRootNodes())
	info := &bftRootInfo{
		restURL:       fmt.Sprintf("http://127.0.0.1:%d", rpcPort),
		p2pAddr:       fmt.Sprintf("127.0.0.1:%d", p2pPort),
		bootstrapAddr: fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", p2pPort, tb.GetRootNodes()[0].NodeID),
		trustBase:     &tb,
	}
	waitForBFTRPC(t, info.restURL, 60*time.Second)
	return info
}

func tail(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

type e2eShard struct {
	id    bfttypes.ShardID
	url   string
	conf  *bfttypes.PartitionDescriptionRecord
	stop  func()
	index int
}

func runConsolidationE2E(t *testing.T, n int, ubft, cli, mongoURI, redisHost string, redisPort int) {
	ctx := t.Context()
	bits := map[int]int{4: 2, 8: 3}[n]
	require.NotZero(t, bits, "4 or 8 shards")
	basePort := 9300 + n*20

	root := startHostRoot(t, ubft, basePort, basePort+1)

	// complete prefix-free scheme in canonical order
	ids := []bfttypes.ShardID{{}}
	for range bits {
		var next []bfttypes.ShardID
		for _, id := range ids {
			a, b := id.Split()
			next = append(next, a, b)
		}
		ids = next
	}
	shards := make([]*e2eShard, n)
	keys := make([]*shardKeys, n)
	for i, id := range ids {
		keys[i] = generateShardKeys(t)
		shards[i] = &e2eShard{id: id, index: i, conf: buildShardConf(t, id, keys[i]), url: fmt.Sprintf("http://127.0.0.1:%d", basePort+10+i)}
		uploadShardConf(t, root.restURL, shards[i].conf)
	}

	// trusted topology, in the BFT_CONSOLIDATION_TOPOLOGY_FILE format, loaded from a file
	var topo []config.ConsolidationShard
	for _, s := range shards {
		h, err := s.conf.Hash(crypto.SHA256)
		require.NoError(t, err)
		topo = append(topo, config.ConsolidationShard{ShardID: s.id, ConfigHash: hex.EncodeToString(h)})
	}
	topoFile := filepath.Join(t.TempDir(), "consolidation-topology.json")
	tb, err := json.Marshal(topo)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(topoFile, tb, 0o600))
	var loaded []config.ConsolidationShard
	raw, err := os.ReadFile(topoFile)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &loaded))
	require.Len(t, loaded, n)

	for i, s := range shards {
		s.stop = startConsolidationAggregator(ctx, t, consolidationAggOpts{
			name: fmt.Sprintf("c%d-%d", n, i), port: strconv.Itoa(basePort + 10 + i), bftListen: fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", basePort+40+i),
			mongoURI: mongoURI, redisHost: redisHost, redisPort: redisPort, stream: fmt.Sprintf("commitments:cons:%d:%d", n, i),
			conf: s.conf, keys: keys[i], root: root, topology: loaded, shardIDLength: bits,
		})
		t.Cleanup(s.stop)
	}
	for _, s := range shards {
		waitForBlock(t, s.url, 1, 90*time.Second)
	}

	// ---- continuous writers ----
	type certified struct {
		req   *api.CertificationRequest
		shard int
	}
	var mu sync.Mutex
	var early []certified
	var stopWriters atomic.Bool
	subs := make([]atomic.Int64, n) // accepted submissions per shard
	var recording atomic.Bool       // collect states submitted while consolidations run
	var duringMu sync.Mutex
	during := make([][]*api.CertificationRequest, n)
	var wg sync.WaitGroup
	submit := func(i int, tag string) *api.CertificationRequest {
		for k := 0; k < 5000; k++ {
			c := testutil.CreateTestCertificationRequest(t, fmt.Sprintf("cons-%d-%s-%d-%d", n, tag, i, k))
			req := c.ToAPI()
			key, _ := req.StateID.GetTreeKey()
			if shards[i].id.Comparator()(key) {
				if st := submitCertificationRequestStatus(t, shards[i].url, req); st == "SUCCESS" {
					subs[i].Add(1)
					if recording.Load() {
						duringMu.Lock()
						during[i] = append(during[i], req)
						duringMu.Unlock()
					}
					return req
				}
				return nil
			}
		}
		return nil
	}
	startWriters := func() {
		stopWriters.Store(false)
		for i := range shards {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for seq := 0; !stopWriters.Load(); seq++ {
					submit(i, fmt.Sprintf("w%d", seq))
					time.Sleep(150 * time.Millisecond)
				}
			}()
		}
	}
	stopAll := func() { stopWriters.Store(true); wg.Wait() }
	t.Cleanup(stopAll)

	// early states, certified in two separate waves (different seals)
	wave := func(tag string, perShard int) {
		for i := range shards {
			for k := 0; k < perShard; k++ {
				if r := submit(i, fmt.Sprintf("%s%d", tag, k)); r != nil {
					mu.Lock()
					early = append(early, certified{r, i})
					mu.Unlock()
				}
			}
		}
	}
	wave("early", 2)
	for _, c := range early {
		fetchInclusionProofV2(t, shards[c.shard].url, c.req.StateID, 90*time.Second)
	}
	time.Sleep(3 * time.Second)
	waveStart := len(early)
	wave("late", 1)
	_ = waveStart

	startWriters()
	defer stopAll()

	// original proofs
	type item struct {
		Req   *api.CertificationRequest
		Proof *api.GetInclusionProofResponseV2
		Shard int
	}
	var items []item
	for _, c := range early {
		p := fetchInclusionProofV2(t, shards[c.shard].url, c.req.StateID, 90*time.Second)
		items = append(items, item{c.req, p, c.shard})
	}

	// latest views with a partition view must be available on every shard
	require.Eventually(t, func() bool {
		for _, s := range shards {
			raw, err := rpcCall(s.url, "get_inclusion_proofs.v1", map[string]any{"stateIds": []string{hex.EncodeToString(items[0].Req.StateID)}, "stateRoot": "latest"})
			_ = raw
			if err != nil && !strings.Contains(err.Error(), "Proof unavailable") && !strings.Contains(err.Error(), "Invalid params") {
				return false
			}
		}
		return true
	}, 60*time.Second, time.Second)

	// ---- consolidation inputs for the CLI ----
	dir := t.TempDir()
	cfgFile := writeConsolidationConfig(t, dir, n, shards, loaded, root)
	var in struct {
		Items []map[string]string `json:"items"`
	}
	for _, it := range items {
		rb, err := bfttypes.Cbor.Marshal(it.Req)
		require.NoError(t, err)
		pb, err := bfttypes.Cbor.Marshal(it.Proof)
		require.NoError(t, err)
		in.Items = append(in.Items, map[string]string{"request": hex.EncodeToString(rb), "proof": hex.EncodeToString(pb)})
	}
	inFile := filepath.Join(dir, "items.json")
	b, _ := json.Marshal(in)
	require.NoError(t, os.WriteFile(inFile, b, 0o600))

	// ---- phase 1: writes only; phase 2: writes + repeated consolidations ----
	type snapshot struct {
		responses, blocks, subs []float64
		round                   uint64
	}
	take := func() snapshot {
		sn := snapshot{round: rootRound(t, root)}
		for i, sh := range shards {
			sn.responses = append(sn.responses, promtest.ToFloat64(metrics.BFTCertificationResponsesTotal.WithLabelValues(sh.id.String())))
			sn.blocks = append(sn.blocks, float64(blockHeight(t, sh.url)))
			sn.subs = append(sn.subs, float64(subs[i].Load()))
		}
		return sn
	}
	diff := func(a, b snapshot) phaseCount {
		pc := phaseCount{Rounds: float64(b.round - a.round)}
		for i := range shards {
			pc.Responses = append(pc.Responses, b.responses[i]-a.responses[i])
			pc.Blocks = append(pc.Blocks, b.blocks[i]-a.blocks[i])
			pc.Submitted = append(pc.Submitted, b.subs[i]-a.subs[i])
		}
		return pc
	}
	phase := func(d time.Duration, work func()) phaseCount {
		s0 := take()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if work != nil {
				work()
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		}
		return diff(s0, take())
	}
	if os.Getenv("CONSOLIDATION_E2E_NEGATIVE") == "stop-writers" {
		stopAll() // negative control: the progress checks below must fail
	}
	p1 := phase(12*time.Second, nil)
	t.Logf("phase 1 (writes only): %s", p1)

	var bundles []consolidationBundle
	var attempts, unavailable int
	recording.Store(true)
	p2 := phase(12*time.Second, func() {
		attempts++
		out := filepath.Join(dir, fmt.Sprintf("bundle-%d.json", attempts))
		code, stderr := runCLI(t, cli, cfgFile, inFile, out)
		switch code {
		case 0:
			bundles = append(bundles, readBundle(t, out))
		case 3:
			unavailable++
			t.Logf("attempt %d unavailable (explicit, retried as new attempt): %s", attempts, strings.TrimSpace(stderr))
		default:
			t.Fatalf("consolidate-proofs exit %d: %s", code, stderr)
		}
	})
	recording.Store(false)
	t.Logf("phase 2 (writes + %d consolidations): %s", attempts, p2)
	require.NotEmpty(t, bundles, "at least one consolidation must succeed under continuous writes (unavailable=%d)", unavailable)
	stopAll()

	// writes and finalization really continued on every shard during both phases
	require.NoError(t, checkWriteProgress(p1), "phase 1")
	require.NoError(t, checkWriteProgress(p2), "phase 2 (during consolidation)")
	// and the states submitted while consolidations ran get finalized with proofs
	for i, sh := range shards {
		duringMu.Lock()
		reqs := append([]*api.CertificationRequest(nil), during[i]...)
		duringMu.Unlock()
		require.NotEmpty(t, reqs, "shard %d accepted no write during consolidation", i)
		fetchInclusionProofV2(t, sh.url, reqs[len(reqs)-1].StateID, 90*time.Second)
	}

	// root-to-shard messages follow the finalization workload: consolidation adds none
	require.NoError(t, checkMessageRate(p1, p2), "root-to-shard message rate with consolidation traffic")

	// ---- verify every bundle with the reference verifier ----
	for bi, bundle := range bundles {
		require.Equal(t, 1, bundle.Counters.QuorumChecks)
		require.Equal(t, n, bundle.Counters.BodyChecks)
		require.Equal(t, len(items), bundle.Counters.LeafChecks)
		require.Equal(t, n, bundle.Stats.Requests)
		require.Equal(t, 2, bundle.Stats.Waves)
		require.Len(t, bundle.Proofs, len(items))

		var sealBytes []byte
		for i, bp := range bundle.Proofs {
			pb, err := hex.DecodeString(bp.Proof)
			require.NoError(t, err)
			var pr api.GetInclusionProofResponseV2
			require.NoError(t, bfttypes.Cbor.Unmarshal(pb, &pr))
			sh := shards[items[i].Shard]
			require.Equal(t, sh.id.String(), bp.Shard)
			h, _ := sh.conf.Hash(crypto.SHA256)
			vctx := &api.VerifierContext{TrustBase: root.trustBase, PartitionID: bftPartitionID, ExpectedShardID: sh.id, ShardConfHash: h}
			require.NoError(t, pr.InclusionProof.Verify(items[i].Req, vctx), "bundle %d proof %d", bi, i)
			// original data and reference time are preserved
			require.Equal(t, *items[i].Proof.InclusionProof.ReferenceTime, *pr.InclusionProof.ReferenceTime)
			// the SMT path legitimately differs (original root vs the master view's root); the
			// certified data is the same
			require.Equal(t, items[i].Req.CertificationData.TransactionHash, pr.InclusionProof.CertificationData.TransactionHash)

			var uc bfttypes.UnicityCertificate
			require.NoError(t, bfttypes.Cbor.Unmarshal(pr.InclusionProof.UnicityCertificate, &uc))
			require.NoError(t, uc.Verify(root.trustBase, crypto.SHA256, bftPartitionID, sh.id, h), "native UC verification")
			sb, err := uc.UnicitySeal.MarshalCBOR()
			require.NoError(t, err)
			if sealBytes == nil {
				sealBytes = sb
			}
			require.Equal(t, sealBytes, sb, "one master seal for all shards")
		}
		// the evidence really was certified under several seals
		seals := map[uint64]bool{}
		for _, it := range items {
			var uc bfttypes.UnicityCertificate
			require.NoError(t, bfttypes.Cbor.Unmarshal(it.Proof.InclusionProof.UnicityCertificate, &uc))
			seals[uc.UnicitySeal.RootChainRoundNumber] = true
		}
		require.Greater(t, len(seals), 1, "original proofs carry different seals")
		_ = consolidation.OutcomeStale
	}
	last := bundles[len(bundles)-1]
	t.Logf("RESULT shards=%d items=%d attempts=%d success=%d unavailable=%d masterRound=%d requests=%d waves=%d viewBytes=%d responseBytes=%d",
		n, len(items), attempts, len(bundles), unavailable, last.RootRound, last.Stats.Requests, last.Stats.Waves, last.Stats.PartitionViewBytes, last.Stats.ResponseBytes)
	t.Logf("sidecar results: ok=%.0f missing=%.0f", promtest.ToFloat64(metrics.BFTPartitionViewTotal.WithLabelValues("ok")), promtest.ToFloat64(metrics.BFTPartitionViewTotal.WithLabelValues("missing")))
}

type consolidationBundle struct {
	RootRound uint64 `json:"rootRound"`
	Stats     struct {
		Requests           int `json:"Requests"`
		Waves              int `json:"Waves"`
		PartitionViewBytes int `json:"PartitionViewBytes"`
		ResponseBytes      int `json:"ResponseBytes"`
	} `json:"stats"`
	Counters struct {
		QuorumChecks int `json:"QuorumChecks"`
		BodyChecks   int `json:"BodyChecks"`
		LeafChecks   int `json:"LeafChecks"`
	} `json:"counters"`
	Proofs []struct {
		Input int    `json:"input"`
		Shard string `json:"shard"`
		Proof string `json:"proof"`
	} `json:"proofs"`
}

func readBundle(t *testing.T, path string) consolidationBundle {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	var out consolidationBundle
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func runCLI(t *testing.T, cli, cfg, in, out string) (int, string) {
	t.Helper()
	cmd := exec.Command(cli, "-config", cfg, "-in", in, "-out", out, "-timeout", "20s")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return 0, ""
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), stderr.String()
	}
	t.Fatalf("running consolidate-proofs: %v", err)
	return -1, ""
}

func writeConsolidationConfig(t *testing.T, dir string, n int, shards []*e2eShard, topo []config.ConsolidationShard, root *bftRootInfo) string {
	t.Helper()
	tbJSON, err := json.Marshal(root.trustBase)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "trust-base.json"), tbJSON, 0o600))
	type shardCfg struct {
		ShardBits string `json:"shardBits"`
		URL       string `json:"url"`
		ConfHash  string `json:"confHash"`
	}
	cfg := struct {
		PartitionID    uint32     `json:"partitionId"`
		Shards         []shardCfg `json:"shards"`
		TrustBases     []string   `json:"trustBases"`
		TimeoutSeconds float64    `json:"timeoutSeconds"`
	}{PartitionID: uint32(bftPartitionID), TrustBases: []string{"trust-base.json"}, TimeoutSeconds: 20}
	byKey := map[string]string{}
	for _, t := range topo { // the trusted topology file is the single source of the config hashes
		byKey[t.ShardID.Key()] = t.ConfigHash
	}
	for _, s := range shards {
		cfg.Shards = append(cfg.Shards, shardCfg{ShardBits: s.id.String(), URL: s.url, ConfHash: byKey[s.id.Key()]})
	}
	b, _ := json.Marshal(cfg)
	p := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(p, b, 0o600))
	return p
}

func rootRound(t *testing.T, root *bftRootInfo) uint64 {
	t.Helper()
	resp, err := http.Get(root.restURL + "/api/v1/roundInfo")
	require.NoError(t, err)
	defer resp.Body.Close()
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	for _, k := range []string{"roundNumber", "round"} {
		switch v := body[k].(type) {
		case string:
			n, err := strconv.ParseUint(v, 10, 64)
			require.NoError(t, err)
			return n
		case float64:
			return uint64(v)
		}
	}
	t.Fatalf("no round number in %v", body)
	return 0
}

type consolidationAggOpts struct {
	name, port, bftListen, mongoURI, redisHost, stream string
	redisPort, shardIDLength                           int
	conf                                               *bfttypes.PartitionDescriptionRecord
	keys                                               *shardKeys
	root                                               *bftRootInfo
	topology                                           []config.ConsolidationShard
}

// startConsolidationAggregator is startBFTShardAggregator with the consolidation
// topology and the production batch API wiring (service.BatchHandlerFor).
func startConsolidationAggregator(ctx context.Context, t *testing.T, o consolidationAggOpts) func() {
	t.Helper()
	tb := o.root.trustBase.(*bfttypes.RootTrustBaseV1)
	keyConf := &cmdbft.KeyConf{
		SigKey:  cmdbft.Key{Algorithm: cmdbft.KeyAlgorithmSecp256k1, PrivateKey: o.keys.sigPriv},
		AuthKey: cmdbft.Key{Algorithm: cmdbft.KeyAlgorithmSecp256k1, PrivateKey: o.keys.authPriv},
	}
	cfg := &config.Config{
		Server: config.ServerConfig{Host: "127.0.0.1", Port: o.port, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second,
			IdleTimeout: 60 * time.Second, ConcurrencyLimit: 100, HTTP2MaxConcurrentStreams: 1024},
		Database: config.DatabaseConfig{URI: o.mongoURI, Database: "aggregator_" + strings.ReplaceAll(o.name, "-", "_"),
			ConnectTimeout: 30 * time.Second, ServerSelectionTimeout: 30 * time.Second, SocketTimeout: 90 * time.Second, MaxPoolSize: 10, MinPoolSize: 1},
		Redis:   config.RedisConfig{Host: o.redisHost, Port: o.redisPort},
		HA:      config.HAConfig{Enabled: false},
		Logging: config.LoggingConfig{Level: "warn", Format: "json"},
		BFT: config.BFTConfig{
			Enabled: true, KeyConf: keyConf, ShardConf: o.conf, TrustBases: []bfttypes.RootTrustBaseV1{*tb},
			ConsolidationShards: o.topology,
			Address:             o.bftListen, BootstrapAddresses: []string{o.root.bootstrapAddr},
			BootstrapConnectRetry: 3, BootstrapConnectRetryDelay: 2,
			HeartbeatInterval: time.Second, InactivityTimeout: 10 * time.Second,
		},
		Signing: config.SigningConfig{KeyConf: keyConf},
		Processing: config.ProcessingConfig{BatchLimit: 1000, MaxCommitmentsPerRound: 1000, CommitmentStreamBufferSize: 50000,
			CollectPhaseDuration: 200 * time.Millisecond, CollectMiniBatchSize: 500, SkipDuplicateCheck: true},
		Storage: config.StorageConfig{UseRedisForCommitments: true, RedisStreamName: o.stream, RedisFlushInterval: 100 * time.Millisecond,
			RedisMaxBatchSize: 1000, RedisCleanupInterval: time.Minute, RedisMaxStreamLength: 100000},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard, ShardIDLength: o.shardIDLength},
	}
	require.NoError(t, cfg.Validate())

	aggCtx, aggCancel := context.WithCancel(ctx)
	log, _ := logger.New(cfg.Logging.Level, cfg.Logging.Format, "", false)
	var queue interfaces.CommitmentQueue
	var stor interfaces.Storage
	var err error
	for attempt := 0; attempt < 4; attempt++ { // index creation can time out on the shared, loaded VM
		if queue, stor, err = storage.NewStorage(aggCtx, cfg, log); err == nil {
			break
		}
		t.Logf("storage for %s: attempt %d failed: %v", o.name, attempt+1, err)
		time.Sleep(2 * time.Second)
	}
	require.NoError(t, err)
	queue.Initialize(aggCtx)
	eventBus := events.NewEventBus(log)
	tree := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	mgr, err := round.NewManager(aggCtx, cfg, log, queue, stor, state.NewSyncStateTracker(), nil, eventBus, tree, stor.TrustBaseStorage())
	require.NoError(t, err)
	require.NoError(t, stor.TrustBaseStorage().Store(aggCtx, tb))
	require.NoError(t, mgr.Start(aggCtx))
	require.NoError(t, mgr.Activate(aggCtx))

	svc, err := service.NewService(aggCtx, cfg, log, mgr, queue, stor, nil)
	require.NoError(t, err)
	srv := gateway.NewServer(cfg, log, svc)
	srv.SetBatchProofHandler(service.BatchHandlerFor(cfg, svc, mgr, stor))
	go func() { _ = srv.Start() }()
	time.Sleep(200 * time.Millisecond)

	return func() {
		aggCancel()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(sctx)
		_ = mgr.Stop(sctx)
		queue.Close(sctx)
		_ = stor.Close(sctx)
	}
}

// The shared helpers terminate their containers with the (already cancelled) test
// context and leak them on a loaded VM; these always terminate with a fresh context.
func startMongoCleanly(t *testing.T) string {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ { // the shared colima VM can be too busy to finish a start in time
		c, err := mongoContainer.Run(context.Background(), "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
		if c != nil {
			t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		}
		if err != nil {
			lastErr = err
			t.Logf("mongo start attempt %d failed: %v", attempt+1, err)
			continue
		}
		uri, err := c.ConnectionString(context.Background())
		require.NoError(t, err)
		return uri + "&directConnection=true"
	}
	require.NoError(t, lastErr)
	return ""
}

func startRedisCleanly(t *testing.T) (string, int) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		c, err := redisContainer.Run(context.Background(), "redis:7-alpine")
		if c != nil {
			t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		}
		if err != nil {
			lastErr = err
			t.Logf("redis start attempt %d failed: %v", attempt+1, err)
			continue
		}
		host, err := c.Host(context.Background())
		require.NoError(t, err)
		mapped, err := c.MappedPort(context.Background(), "6379")
		require.NoError(t, err)
		port, err := strconv.Atoi(mapped.Port())
		require.NoError(t, err)
		return host, port
	}
	require.NoError(t, lastErr)
	return "", 0
}

func blockHeight(t *testing.T, url string) int64 {
	t.Helper()
	raw, err := rpcCall(url, "get_block_height", nil)
	require.NoError(t, err)
	var resp api.GetBlockHeightResponse
	require.NoError(t, json.Unmarshal(raw, &resp))
	return resp.BlockNumber.Int64()
}
