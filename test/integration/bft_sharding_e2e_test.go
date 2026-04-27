//go:build e2e_bft

// Package integration contains end-to-end smoke tests, including BFT-side
// sharding coverage.
//
// TestBFTShardingE2E brings up a real multi-shard BFT partition with two
// aggregators in `SHARDING_MODE=bft-shard`, so it is gated behind
// `//go:build e2e_bft`.
//
// Run with:
//   go test -tags=e2e_bft ./test/integration/... -run TestBFTShardingE2E -v

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	cmdbft "github.com/unicitynetwork/bft-core/cli/ubft/cmd"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"
	bfthex "github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const bftCoreImage = "ghcr.io/unicitynetwork/bft-core:ceceacd11b7a735de74ce17884a3a45e0db1748d"

const (
	bftNetworkID       bfttypes.NetworkID       = 3
	bftPartitionID     bfttypes.PartitionID     = 7
	bftPartitionTypeID bfttypes.PartitionTypeID = 7
	bftTypeIDLen       uint32                   = 8
	bftUnitIDLen       uint32                   = 256
	bftT2Timeout                                = 5 * time.Second
	bftEpoch           uint64                   = 0
	bftEpochStart      uint64                   = 10
)

// TestBFTShardingE2E exercises the full bft-shard path: two aggregators
// validating a 1-bit-split partition under a real bft-core root node.
// Submission routing, block certification, and certified proof verification
// via the public InclusionProofV2.Verify(req, vctx) are all asserted end-to-end.
func TestBFTShardingE2E(t *testing.T) {
	ctx := t.Context()

	mongoURI := startMongo(ctx, t)
	redisHost, redisPort := startRedis(ctx, t)

	root := startBFTRoot(ctx, t)
	t.Logf("BFT root: rest=%s p2p=%s bootstrap=%s", root.restURL, root.p2pAddr, root.bootstrapAddr)

	sid0, sid1 := bfttypes.ShardID{}.Split()
	shard0Keys := generateShardKeys(t)
	shard1Keys := generateShardKeys(t)

	shard0Conf := buildShardConf(t, sid0, shard0Keys)
	shard1Conf := buildShardConf(t, sid1, shard1Keys)

	uploadShardConf(t, root.restURL, shard0Conf)
	uploadShardConf(t, root.restURL, shard1Conf)

	// Each in-process aggregator needs its own libp2p listen port.
	agg0 := startBFTShardAggregator(ctx, t, bftAggregatorOpts{
		name:            "shard0",
		port:            "9100",
		bftListenAddr:   "/ip4/127.0.0.1/tcp/9110",
		mongoURI:        mongoURI,
		redisHost:       redisHost,
		redisPort:       redisPort,
		redisStreamName: "commitments:bft-shard:0",
		shardConf:       shard0Conf,
		keys:            shard0Keys,
		trustBase:       root.trustBase,
		bootstrapAddr:   root.bootstrapAddr,
	})
	t.Cleanup(agg0.stop)

	agg1 := startBFTShardAggregator(ctx, t, bftAggregatorOpts{
		name:            "shard1",
		port:            "9101",
		bftListenAddr:   "/ip4/127.0.0.1/tcp/9111",
		mongoURI:        mongoURI,
		redisHost:       redisHost,
		redisPort:       redisPort,
		redisStreamName: "commitments:bft-shard:1",
		shardConf:       shard1Conf,
		keys:            shard1Keys,
		trustBase:       root.trustBase,
		bootstrapAddr:   root.bootstrapAddr,
	})
	t.Cleanup(agg1.stop)

	t.Log("Waiting for first block on each shard (real BFT — takes 10-30s)...")
	waitForBlock(t, "http://localhost:9100", 1, 60*time.Second)
	waitForBlock(t, "http://localhost:9101", 1, 60*time.Second)

	t.Run("cross_shard_rejection", func(t *testing.T) {
		// A stateID with MSB=1 belongs to shard 1; shard 0 must reject it.
		crossReq := createCommitmentWithMSB(t, 1)
		status := submitCertificationRequestStatus(t, "http://localhost:9100", crossReq)
		require.Equal(t, "INVALID_SHARD", status)

		crossReq2 := createCommitmentWithMSB(t, 0)
		status = submitCertificationRequestStatus(t, "http://localhost:9101", crossReq2)
		require.Equal(t, "INVALID_SHARD", status)
	})

	happyShard0 := createCommitmentWithMSB(t, 0)
	require.Equal(t, "SUCCESS", submitCertificationRequestStatus(t, "http://localhost:9100", happyShard0))

	happyShard1 := createCommitmentWithMSB(t, 1)
	require.Equal(t, "SUCCESS", submitCertificationRequestStatus(t, "http://localhost:9101", happyShard1))

	proof0 := fetchInclusionProofV2(t, "http://localhost:9100", happyShard0.StateID, 60*time.Second)
	proof1 := fetchInclusionProofV2(t, "http://localhost:9101", happyShard1.StateID, 60*time.Second)

	t.Run("verifier_happy_path", func(t *testing.T) {
		require.NoError(t, proof0.InclusionProof.Verify(happyShard0, &api.VerifierContext{
			TrustBase:       root.trustBase,
			PartitionID:     bftPartitionID,
			ExpectedShardID: sid0,
		}))

		require.NoError(t, proof1.InclusionProof.Verify(happyShard1, &api.VerifierContext{
			TrustBase:       root.trustBase,
			PartitionID:     bftPartitionID,
			ExpectedShardID: sid1,
		}))
	})

	t.Run("verifier_rejects_wrong_shard", func(t *testing.T) {
		err := proof0.InclusionProof.Verify(happyShard0, &api.VerifierContext{
			TrustBase:       root.trustBase,
			PartitionID:     bftPartitionID,
			ExpectedShardID: sid1,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid shard ID")
	})

	t.Run("verifier_rejects_wrong_partition", func(t *testing.T) {
		err := proof0.InclusionProof.Verify(happyShard0, &api.VerifierContext{
			TrustBase:       root.trustBase,
			PartitionID:     bftPartitionID + 1,
			ExpectedShardID: sid0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unicity certificate verification failed")
	})
}

// -----------------------------------------------------------------------------
// Container helpers.
// -----------------------------------------------------------------------------

func startMongo(ctx context.Context, t *testing.T) string {
	t.Helper()
	c, err := mongoContainer.Run(ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	uri, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	return uri + "&directConnection=true"
}

func startRedis(ctx context.Context, t *testing.T) (string, int) {
	t.Helper()
	c, err := redisContainer.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	mapped, err := c.MappedPort(ctx, "6379")
	require.NoError(t, err)
	port, err := strconv.Atoi(mapped.Port())
	require.NoError(t, err)
	return host, port
}

type bftRootInfo struct {
	restURL       string
	p2pAddr       string // host:port for P2P
	bootstrapAddr string // multiaddr the aggregators dial
	trustBase     bfttypes.RootTrustBase
}

// startBFTRoot starts a bft-core root node container, waits for RPC
// readiness, and returns the dial info plus the generated trust base.
func startBFTRoot(ctx context.Context, t *testing.T) *bftRootInfo {
	t.Helper()

	// Keep genesis under /tmp because the image runs as nonroot in this
	// testcontainers setup.
	cmdScript := `
        mkdir -p /tmp/genesis/root
        if [ -f /tmp/genesis/root/node-info.json ] && [ -f /tmp/genesis/trust-base.json ] && [ -f /tmp/genesis/root/trust-base-signed.json ]; then
          echo "Genesis files already exist, skipping initialization."
        else
          echo "Creating root genesis..." &&
          ubft root-node init --home /tmp/genesis/root -g &&
          echo "Creating root trust base..." &&
          ubft trust-base generate --home /tmp/genesis --network-id 3 --node-info /tmp/genesis/root/node-info.json &&
          echo "Signing root trust base..." &&
          ubft trust-base sign --home /tmp/genesis/root --trust-base /tmp/genesis/trust-base.json
        fi
        echo "Starting root node..."
        exec ubft root-node run --home /tmp/genesis/root --address "/ip4/0.0.0.0/tcp/8000" --trust-base /tmp/genesis/trust-base.json --rpc-server-address "0.0.0.0:8002"
    `

	req := testcontainers.ContainerRequest{
		Image:         bftCoreImage,
		ImagePlatform: "linux/amd64",
		ExposedPorts:  []string{"8000/tcp", "8002/tcp"},
		Entrypoint:    []string{"/busybox/sh", "-c"},
		Cmd:           []string{cmdScript},
		// The log line is only a coarse signal; waitForBFTRPC does the real
		// readiness check before configuration upload.
		WaitingFor: wait.ForLog("Starting root node"),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start bft-core root container")
	t.Cleanup(func() {
		if t.Failed() {
			if rc, err := c.Logs(context.Background()); err == nil {
				buf := new(bytes.Buffer)
				_, _ = buf.ReadFrom(rc)
				_ = rc.Close()
				t.Logf("=== bft-core root container logs ===\n%s\n=== end ===", buf.String())
			}
		}
		_ = c.Terminate(ctx)
	})

	host, err := c.Host(ctx)
	require.NoError(t, err)
	p2pMapped, err := c.MappedPort(ctx, "8000")
	require.NoError(t, err)
	restMapped, err := c.MappedPort(ctx, "8002")
	require.NoError(t, err)

	info := &bftRootInfo{
		restURL: fmt.Sprintf("http://%s:%s", host, restMapped.Port()),
		p2pAddr: fmt.Sprintf("%s:%s", host, p2pMapped.Port()),
	}

	info.trustBase = copyTrustBaseFromContainer(ctx, t, c, "/tmp/genesis/trust-base.json", 30*time.Second)
	waitForBFTRPC(t, info.restURL, 30*time.Second)

	// /ip4/ multiaddrs need a numeric IP, so translate "localhost".
	nodes := info.trustBase.GetRootNodes()
	require.NotEmpty(t, nodes, "trust base has no root nodes")
	ipHost := host
	if ipHost == "localhost" {
		ipHost = "127.0.0.1"
	}
	info.bootstrapAddr = fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", ipHost, p2pMapped.Port(), nodes[0].NodeID)

	return info
}

// copyTrustBaseFromContainer reads the signed trust base JSON out of the
// running container, retrying until init has finished writing it.
func copyTrustBaseFromContainer(ctx context.Context, t *testing.T, c testcontainers.Container, path string, timeout time.Duration) bfttypes.RootTrustBase {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		rc, err := c.CopyFileFromContainer(ctx, path)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(rc); err != nil {
			_ = rc.Close()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		_ = rc.Close()
		var tb bfttypes.RootTrustBaseV1
		if err := json.Unmarshal(buf.Bytes(), &tb); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(tb.GetRootNodes()) > 0 && len(tb.Signatures) > 0 {
			return &tb
		}
		lastErr = fmt.Errorf("trust base present but unsigned/empty (nodes=%d sigs=%d)",
			len(tb.GetRootNodes()), len(tb.Signatures))
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out copying signed trust base from container: %v", lastErr)
	return nil
}

// waitForBFTRPC polls /api/v1/roundInfo until the RPC server responds.
func waitForBFTRPC(t *testing.T, restURL string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	probe := restURL + "/api/v1/roundInfo"
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for {
		resp, err := client.Get(probe)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 500 { // 200 / 4xx both mean the server is up
				return
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for BFT RPC at %s: %v", restURL, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// -----------------------------------------------------------------------------
// Shard config + keys.
// -----------------------------------------------------------------------------

type shardKeys struct {
	sigPriv  bfthex.Bytes // secp256k1 private key bytes used to sign certification requests
	sigPub   []byte       // public key derived from sigPriv, for NodeInfo.SigKey
	authPriv bfthex.Bytes // secp256k1 private key bytes used to derive the libp2p peer ID
	nodeID   string       // libp2p peer ID (string form), used in NodeInfo.NodeID
}

// generateShardKeys produces a fresh validator identity: a sig key for
// signing certification requests and an auth key whose libp2p peer ID becomes
// the validator's NodeID.
func generateShardKeys(t *testing.T) *shardKeys {
	t.Helper()
	sigSigner, err := abcrypto.NewInMemorySecp256K1Signer()
	require.NoError(t, err)
	sigPriv, err := sigSigner.MarshalPrivateKey()
	require.NoError(t, err)
	sigVerifier, err := sigSigner.Verifier()
	require.NoError(t, err)
	sigPub, err := sigVerifier.MarshalPublicKey()
	require.NoError(t, err)

	authSigner, err := abcrypto.NewInMemorySecp256K1Signer()
	require.NoError(t, err)
	authPriv, err := authSigner.MarshalPrivateKey()
	require.NoError(t, err)

	// Match bft-core KeyConf.NodeID() derivation.
	libp2pPriv, err := p2pcrypto.UnmarshalSecp256k1PrivateKey(authPriv)
	require.NoError(t, err)
	peerID, err := peer.IDFromPrivateKey(libp2pPriv)
	require.NoError(t, err)

	return &shardKeys{
		sigPriv:  sigPriv,
		sigPub:   sigPub,
		authPriv: authPriv,
		nodeID:   peerID.String(),
	}
}

// buildShardConf constructs a valid PartitionDescriptionRecord pinned to the
// constants at the top of this file.
func buildShardConf(t *testing.T, shardID bfttypes.ShardID, keys *shardKeys) *bfttypes.PartitionDescriptionRecord {
	t.Helper()
	conf := &bfttypes.PartitionDescriptionRecord{
		Version:         1,
		NetworkID:       bftNetworkID,
		PartitionID:     bftPartitionID,
		PartitionTypeID: bftPartitionTypeID,
		ShardID:         shardID,
		TypeIDLen:       bftTypeIDLen,
		UnitIDLen:       bftUnitIDLen,
		T2Timeout:       bftT2Timeout,
		Epoch:           bftEpoch,
		EpochStart:      bftEpochStart,
		Validators: []*bfttypes.NodeInfo{
			{
				NodeID: keys.nodeID,
				SigKey: keys.sigPub,
				Stake:  1,
			},
		},
	}
	require.NoError(t, conf.IsValid(), "generated shard conf must be valid")
	return conf
}

// uploadShardConf PUTs the shard conf to the root node's REST endpoint, with
// retries for transient errors just after the server starts accepting traffic.
func uploadShardConf(t *testing.T, restURL string, conf *bfttypes.PartitionDescriptionRecord) {
	t.Helper()
	body, err := json.Marshal(conf)
	require.NoError(t, err)

	endpoint := restURL + "/api/v1/configurations"
	client := &http.Client{Timeout: 5 * time.Second}

	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; time.Now().Before(deadline); attempt++ {
		req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		respBody, _ := readBody(resp)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		lastErr = fmt.Errorf("attempt %d: status %d body=%q", attempt, resp.StatusCode, respBody)
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("upload shard conf for shard %s failed: %v", conf.ShardID, lastErr)
}

func readBody(r *http.Response) (string, error) {
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// -----------------------------------------------------------------------------
// In-process aggregator bootstrap (real BFT enabled).
// -----------------------------------------------------------------------------

type bftAggregatorOpts struct {
	name            string
	port            string
	bftListenAddr   string // e.g. /ip4/127.0.0.1/tcp/9110 — must be unique per aggregator
	mongoURI        string
	redisHost       string
	redisPort       int
	redisStreamName string
	shardConf       *bfttypes.PartitionDescriptionRecord
	keys            *shardKeys
	trustBase       bfttypes.RootTrustBase
	bootstrapAddr   string
}

type bftAggregator struct {
	port string
	stop func()
}

// startBFTShardAggregator builds a *config.Config in memory and wires it into
// the real BFT client. Because this test does not call config.Load, every
// config field that production would normally derive during load must be set
// explicitly here.
func startBFTShardAggregator(ctx context.Context, t *testing.T, opts bftAggregatorOpts) *bftAggregator {
	t.Helper()

	tb, ok := opts.trustBase.(*bfttypes.RootTrustBaseV1)
	require.True(t, ok, "trust base must be *RootTrustBaseV1 for inclusion in cfg.BFT.TrustBases")

	keyConf := &cmdbft.KeyConf{
		SigKey: cmdbft.Key{
			Algorithm:  cmdbft.KeyAlgorithmSecp256k1,
			PrivateKey: opts.keys.sigPriv,
		},
		AuthKey: cmdbft.Key{
			Algorithm:  cmdbft.KeyAlgorithmSecp256k1,
			PrivateKey: opts.keys.authPriv,
		},
	}

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost", Port: opts.port,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			ConcurrencyLimit:          100,
			HTTP2MaxConcurrentStreams: 1024,
		},
		Database: config.DatabaseConfig{
			URI: opts.mongoURI, Database: "aggregator_bft_" + opts.name,
			ConnectTimeout: 10 * time.Second, ServerSelectionTimeout: 10 * time.Second, SocketTimeout: 30 * time.Second,
			MaxPoolSize: 20, MinPoolSize: 2,
		},
		Redis: config.RedisConfig{Host: opts.redisHost, Port: opts.redisPort},
		HA:    config.HAConfig{Enabled: false},
		Logging: config.LoggingConfig{
			Level: "warn", Format: "json",
		},
		BFT: config.BFTConfig{
			Enabled:                    true,
			KeyConf:                    keyConf,
			ShardConf:                  opts.shardConf,
			TrustBases:                 []bfttypes.RootTrustBaseV1{*tb},
			Address:                    opts.bftListenAddr,
			BootstrapAddresses:         []string{opts.bootstrapAddr},
			BootstrapConnectRetry:      3,
			BootstrapConnectRetryDelay: 2,
			HeartbeatInterval:          1 * time.Second,
			InactivityTimeout:          10 * time.Second,
		},
		Signing: config.SigningConfig{KeyConf: keyConf},
		Processing: config.ProcessingConfig{
			RoundDuration: 2 * time.Second, BatchLimit: 1000, MaxCommitmentsPerRound: 1000,
		},
		Storage: config.StorageConfig{
			UseRedisForCommitments: true,
			RedisStreamName:        opts.redisStreamName,
			RedisFlushInterval:     100 * time.Millisecond,
			RedisMaxBatchSize:      1000,
			RedisCleanupInterval:   1 * time.Minute,
			RedisMaxStreamLength:   100000,
		},
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeBFTShard,
			ShardIDLength: 1,
		},
	}
	require.NoError(t, cfg.Validate())

	aggCtx, aggCancel := context.WithCancel(ctx)
	log, _ := logger.New(cfg.Logging.Level, cfg.Logging.Format, "", false)
	queue, stor, err := storage.NewStorage(aggCtx, cfg, log)
	require.NoError(t, err)
	queue.Initialize(aggCtx)

	eventBus := events.NewEventBus(log)
	smtInstance := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	threadSafeSmt := smt.NewThreadSafeSMT(smtInstance)

	mgr, err := round.NewManager(
		aggCtx, cfg, log, queue, stor,
		state.NewSyncStateTracker(),
		nil, // luc: fresh start, no recovered UC
		eventBus, threadSafeSmt, stor.TrustBaseStorage(),
	)
	require.NoError(t, err)

	// Seed epoch 0 trust base for the real BFT client.
	require.NoError(t, stor.TrustBaseStorage().Store(aggCtx, tb))

	require.NoError(t, mgr.Start(aggCtx))
	require.NoError(t, mgr.Activate(aggCtx))

	svc, err := service.NewService(aggCtx, cfg, log, mgr, queue, stor, nil)
	require.NoError(t, err)
	srv := gateway.NewServer(cfg, log, svc)
	go func() { _ = srv.Start() }()

	// HTTP bind is async; waitForBlock below is the real readiness gate.
	time.Sleep(200 * time.Millisecond)

	return &bftAggregator{
		port: opts.port,
		stop: func() {
			aggCancel()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Stop(shutdownCtx)
			_ = mgr.Stop(shutdownCtx)
			queue.Close(shutdownCtx)
			_ = stor.Close(shutdownCtx)
		},
	}
}

// -----------------------------------------------------------------------------
// Commitment construction + RPC helpers.
// -----------------------------------------------------------------------------

// createCommitmentWithMSB builds a certification request whose stateID has the
// requested top bit via rejection sampling.
func createCommitmentWithMSB(t *testing.T, msb byte) *api.CertificationRequest {
	t.Helper()
	require.True(t, msb == 0 || msb == 1, "msb must be 0 or 1")
	for i := 0; i < 1000; i++ {
		c := testutil.CreateTestCertificationRequest(t, fmt.Sprintf("bft-e2e-msb%d-%d-%d", msb, i, time.Now().UnixNano()))
		req := c.ToAPI()
		keyBytes, err := req.StateID.GetTreeKey()
		require.NoError(t, err)
		if (keyBytes[0]>>7)&1 == msb {
			return req
		}
	}
	t.Fatalf("unable to construct commitment with MSB=%d", msb)
	return nil
}

func submitCertificationRequestStatus(t *testing.T, url string, req *api.CertificationRequest) string {
	t.Helper()
	resp := mustRPCCall(t, url, "certification_request", req)
	var cr api.CertificationResponse
	require.NoError(t, json.Unmarshal(resp, &cr))
	return cr.Status
}

func fetchInclusionProofV2(t *testing.T, url string, stateID api.StateID, timeout time.Duration) *api.GetInclusionProofResponseV2 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		raw, err := rpcCall(url, "get_inclusion_proof.v2", &api.GetInclusionProofRequestV2{StateID: stateID})
		if err == nil {
			var resp api.GetInclusionProofResponseV2
			if err := json.Unmarshal(raw, &resp); err == nil {
				if resp.InclusionProof != nil && len(resp.InclusionProof.CertificateBytes) > 0 {
					return &resp
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for v2 inclusion proof from %s for stateId %s", url, stateID)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func mustRPCCall(t *testing.T, url, method string, params interface{}) json.RawMessage {
	t.Helper()
	raw, err := rpcCall(url, method, params)
	require.NoError(t, err, "%s failed", method)
	return raw
}
