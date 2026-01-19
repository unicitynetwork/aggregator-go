package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"

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

// TestShardingE2E tests the full sharding flow: parent + 2 child shards
// submitting commitments and verifying inclusion proofs.
func TestShardingE2E(t *testing.T) {
	ctx := context.Background()

	// Start containers (shared MongoDB with different databases per aggregator)
	redis, err := redisContainer.Run(ctx, "redis:7")
	require.NoError(t, err)
	defer redis.Terminate(ctx)
	redisURI, _ := redis.ConnectionString(ctx)

	mongo, err := mongoContainer.Run(ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	require.NoError(t, err)
	defer mongo.Terminate(ctx)
	mongoURI, _ := mongo.ConnectionString(ctx)
	mongoURI += "&directConnection=true"

	// Start aggregators (each uses a different database)
	parentCleanup := startAggregator(t, ctx, "parent", "9000", mongoURI, redisURI, config.ShardingModeParent, 0)
	defer parentCleanup()
	waitForBlock(t, "http://localhost:9000", 1, 15*time.Second)

	shard2Cleanup := startAggregator(t, ctx, "shard2", "9001", mongoURI, redisURI, config.ShardingModeChild, 2)
	defer shard2Cleanup()

	shard3Cleanup := startAggregator(t, ctx, "shard3", "9002", mongoURI, redisURI, config.ShardingModeChild, 3)
	defer shard3Cleanup()

	waitForBlock(t, "http://localhost:9001", 1, 15*time.Second)
	waitForBlock(t, "http://localhost:9002", 1, 15*time.Second)

	// Submit commitments over multiple rounds
	var shard2ReqIDs, shard3ReqIDs []string

	// Round 1: submit 2 commitments to each shard
	for i := 0; i < 2; i++ {
		c, reqID := createCommitmentForShard(t, 2)
		submitCommitment(t, "http://localhost:9001", c)
		shard2ReqIDs = append(shard2ReqIDs, reqID)

		c, reqID = createCommitmentForShard(t, 3)
		submitCommitment(t, "http://localhost:9002", c)
		shard3ReqIDs = append(shard3ReqIDs, reqID)
	}

	waitForBlock(t, "http://localhost:9001", 2, 15*time.Second)
	waitForBlock(t, "http://localhost:9002", 2, 15*time.Second)

	// Round 2: submit 2 more commitments to each shard
	for i := 0; i < 2; i++ {
		c, reqID := createCommitmentForShard(t, 2)
		submitCommitment(t, "http://localhost:9001", c)
		shard2ReqIDs = append(shard2ReqIDs, reqID)

		c, reqID = createCommitmentForShard(t, 3)
		submitCommitment(t, "http://localhost:9002", c)
		shard3ReqIDs = append(shard3ReqIDs, reqID)
	}

	waitForBlock(t, "http://localhost:9001", 3, 15*time.Second)
	waitForBlock(t, "http://localhost:9002", 3, 15*time.Second)

	// Round 3: submit 1 more commitment to each shard
	c, reqID := createCommitmentForShard(t, 2)
	submitCommitment(t, "http://localhost:9001", c)
	shard2ReqIDs = append(shard2ReqIDs, reqID)

	c, reqID = createCommitmentForShard(t, 3)
	submitCommitment(t, "http://localhost:9002", c)
	shard3ReqIDs = append(shard3ReqIDs, reqID)

	waitForBlock(t, "http://localhost:9001", 4, 15*time.Second)
	waitForBlock(t, "http://localhost:9002", 4, 15*time.Second)

	// Verify all proofs
	for _, reqID := range shard2ReqIDs {
		waitForValidProof(t, "http://localhost:9001", reqID, 15*time.Second)
	}
	for _, reqID := range shard3ReqIDs {
		waitForValidProof(t, "http://localhost:9002", reqID, 15*time.Second)
	}
}

func startAggregator(t *testing.T, ctx context.Context, name, port, mongoURI, redisURI string, mode config.ShardingMode, shardID api.ShardID) func() {
	redisURL, _ := url.Parse(redisURI)
	redisHost := redisURL.Hostname()
	redisPort, _ := strconv.Atoi(redisURL.Port())

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost", Port: port,
			ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
			ConcurrencyLimit: 100,
		},
		Database: config.DatabaseConfig{
			URI: mongoURI, Database: fmt.Sprintf("aggregator_%s", name),
			ConnectTimeout: 10 * time.Second, ServerSelectionTimeout: 10 * time.Second, SocketTimeout: 30 * time.Second,
			MaxPoolSize: 10, MinPoolSize: 2,
		},
		Redis:   config.RedisConfig{Host: redisHost, Port: redisPort},
		HA:      config.HAConfig{Enabled: false},
		Logging: config.LoggingConfig{Level: "warn", Format: "json"},
		BFT:     config.BFTConfig{Enabled: false, StubDelay: 500 * time.Millisecond},
		Processing: config.ProcessingConfig{
			RoundDuration: 2 * time.Second, BatchLimit: 1000, MaxCommitmentsPerRound: 1000,
		},
		Storage: config.StorageConfig{
			UseRedisForCommitments: true,
			RedisStreamName:        fmt.Sprintf("commitments:%s", name),
			RedisFlushInterval:     100 * time.Millisecond,
			RedisMaxBatchSize:      1000,
			RedisCleanupInterval:   1 * time.Minute,
			RedisMaxStreamLength:   100000,
		},
		Sharding: config.ShardingConfig{Mode: mode, ShardIDLength: 1},
	}

	if mode == config.ShardingModeChild {
		cfg.Sharding.Child = config.ChildConfig{
			ParentRpcAddr: "http://localhost:9000", ShardID: shardID,
			ParentPollTimeout: 30 * time.Second, ParentPollInterval: 100 * time.Millisecond,
		}
	}

	aggCtx, aggCancel := context.WithCancel(ctx)
	log, _ := logger.New("warn", "json", "", false)
	queue, stor, _ := storage.NewStorage(aggCtx, cfg, log)
	queue.Initialize(aggCtx)

	eventBus := events.NewEventBus(log)

	// Create SMT instance based on sharding mode
	var smtInstance *smt.SparseMerkleTree
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeChild:
		smtInstance = smt.NewSparseMerkleTree(api.SHA256, 16+256)
	case config.ShardingModeParent:
		smtInstance = smt.NewParentSparseMerkleTree(api.SHA256, cfg.Sharding.ShardIDLength)
	}
	threadSafeSmt := smt.NewThreadSafeSMT(smtInstance)

	mgr, _ := round.NewManager(aggCtx, cfg, log, queue, stor, state.NewSyncStateTracker(), nil, eventBus, threadSafeSmt, stor.TrustBaseStorage())
	mgr.Start(aggCtx)
	mgr.Activate(aggCtx)

	svc, _ := service.NewService(aggCtx, cfg, log, mgr, queue, stor, nil, nil)
	srv := gateway.NewServer(cfg, log, svc)
	go srv.Start()
	time.Sleep(100 * time.Millisecond)

	return func() {
		aggCancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Stop(shutdownCtx)
		mgr.Stop(shutdownCtx)
		queue.Close(shutdownCtx)
		stor.Close(shutdownCtx)
	}
}

func rpcCall(url, method string, params interface{}) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		Result json.RawMessage           `json:"result"`
		Error  *struct{ Message string } `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", result.Error.Message)
	}
	return result.Result, nil
}

func submitCommitment(t *testing.T, url string, c *api.SubmitCommitmentRequest) {
	result, err := rpcCall(url, "submit_commitment", c)
	require.NoError(t, err)
	var resp api.SubmitCommitmentResponse
	json.Unmarshal(result, &resp)
	require.Equal(t, "SUCCESS", resp.Status)
}

func waitForBlock(t *testing.T, url string, minBlock int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		result, err := rpcCall(url, "get_block_height", nil)
		if err == nil {
			var resp api.GetBlockHeightResponse
			json.Unmarshal(result, &resp)
			if resp.BlockNumber != nil && resp.BlockNumber.Int64() >= minBlock {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for block %d at %s", minBlock, url)
}

func waitForValidProof(t *testing.T, url, reqID string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	reqIDObj := api.RequestID(reqID)
	path, _ := reqIDObj.GetPath()

	for time.Now().Before(deadline) {
		result, err := rpcCall(url, "get_inclusion_proof", map[string]string{"requestId": reqID})
		if err == nil {
			var resp api.GetInclusionProofResponse
			json.Unmarshal(result, &resp)
			if resp.InclusionProof != nil && resp.InclusionProof.MerkleTreePath != nil {
				verifyResult, _ := resp.InclusionProof.MerkleTreePath.Verify(path)
				if verifyResult != nil && verifyResult.Result {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for valid proof at %s", url)
}

func createCommitmentForShard(t *testing.T, shardID api.ShardID) (*api.SubmitCommitmentRequest, string) {
	mask := big.NewInt(1)
	expected := big.NewInt(int64(shardID & 1))

	for i := 0; i < 1000; i++ {
		c := testutil.CreateTestCommitment(t, fmt.Sprintf("shard%d_%d_%d", shardID, i, time.Now().UnixNano()))
		reqBytes, _ := c.RequestID.Bytes()
		if new(big.Int).And(new(big.Int).SetBytes(reqBytes), mask).Cmp(expected) == 0 {
			receipt := true
			return &api.SubmitCommitmentRequest{
				RequestID:       c.RequestID,
				TransactionHash: api.TransactionHash(c.TransactionHash),
				Authenticator: api.Authenticator{
					Algorithm: c.Authenticator.Algorithm,
					PublicKey: api.HexBytes(c.Authenticator.PublicKey),
					Signature: api.HexBytes(c.Authenticator.Signature),
					StateHash: api.StateHash(c.Authenticator.StateHash),
				},
				Receipt: &receipt,
			}, c.RequestID.String()
		}
	}
	t.Fatal("Failed to generate commitment for shard")
	return nil, ""
}
