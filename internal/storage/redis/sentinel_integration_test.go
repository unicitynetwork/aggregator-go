package redis

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	redislib "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

const (
	sentinelMasterAlias = "redis-master"
	sentinelMasterName  = "mymaster"
)

// sentinelTestEnv is a running redis master + redis-sentinel pair on a shared
// Docker network. Master is reachable from inside the network at
// sentinelMasterAlias:6379 and from the host at masterHostAddr; Sentinel is
// reachable from the host at sentinelHostAddr.
type sentinelTestEnv struct {
	masterHostAddr   string
	sentinelHostAddr string
	network          *testcontainers.DockerNetwork
	master           testcontainers.Container
	sentinel         testcontainers.Container
}

func (e *sentinelTestEnv) close(ctx context.Context) {
	if e.sentinel != nil {
		_ = e.sentinel.Terminate(ctx)
	}
	if e.master != nil {
		_ = e.master.Terminate(ctx)
	}
	if e.network != nil {
		_ = e.network.Remove(ctx)
	}
}

func startSentinelTestEnv(ctx context.Context, t *testing.T) *sentinelTestEnv {
	t.Helper()

	env := &sentinelTestEnv{}
	t.Cleanup(func() { env.close(ctx) })

	nw, err := tcnetwork.New(ctx)
	require.NoError(t, err, "create docker network")
	env.network = nw

	masterReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			Networks:     []string{nw.Name},
			NetworkAliases: map[string][]string{
				nw.Name: {sentinelMasterAlias},
			},
			Cmd: []string{
				"redis-server",
				"--save", "",
				"--appendonly", "no",
			},
			WaitingFor: wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	}
	master, err := testcontainers.GenericContainer(ctx, masterReq)
	require.NoError(t, err, "start redis master")
	env.master = master

	masterHost, err := master.Host(ctx)
	require.NoError(t, err)
	masterPort, err := master.MappedPort(ctx, "6379")
	require.NoError(t, err)
	env.masterHostAddr = fmt.Sprintf("%s:%s", masterHost, masterPort.Port())

	sentinelConf := fmt.Sprintf(
		"port 26379\n"+
			"sentinel resolve-hostnames yes\n"+
			"sentinel monitor %s %s 6379 1\n"+
			"sentinel down-after-milliseconds %s 5000\n"+
			"sentinel failover-timeout %s 10000\n"+
			"sentinel parallel-syncs %s 1\n",
		sentinelMasterName, sentinelMasterAlias,
		sentinelMasterName, sentinelMasterName, sentinelMasterName,
	)
	confPath := filepath.Join(t.TempDir(), "sentinel.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(sentinelConf), 0o644))

	sentinelReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"26379/tcp"},
			Networks:     []string{nw.Name},
			Cmd:          []string{"redis-sentinel", "/etc/redis/sentinel.conf"},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      confPath,
					ContainerFilePath: "/etc/redis/sentinel.conf",
					FileMode:          0o644,
				},
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("26379/tcp"),
				wait.ForLog("+monitor master"),
			).WithStartupTimeoutDefault(60 * time.Second),
		},
		Started: true,
	}
	sentinel, err := testcontainers.GenericContainer(ctx, sentinelReq)
	require.NoError(t, err, "start redis-sentinel")
	env.sentinel = sentinel

	sentinelHost, err := sentinel.Host(ctx)
	require.NoError(t, err)
	sentinelPort, err := sentinel.MappedPort(ctx, "26379")
	require.NoError(t, err)
	env.sentinelHostAddr = fmt.Sprintf("%s:%s", sentinelHost, sentinelPort.Port())

	return env
}

// dialerForSentinel translates master-port dials to the host-routable mapped
// address. Sentinel resolves the master alias to the container's internal IP
// at monitor time and returns that IP (e.g. 172.x.x.x:6379) to clients via
// SENTINEL get-master-addr-by-name; tests run on the host where that IP is
// not reachable. Any dial to port 6379 is redirected to the host-mapped
// master, while sentinel dials (port 26379) pass through unchanged.
func dialerForSentinel(env *sentinelTestEnv) func(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	return func(ctx context.Context, netw, addr string) (net.Conn, error) {
		if _, port, err := net.SplitHostPort(addr); err == nil && port == "6379" {
			addr = env.masterHostAddr
		}
		return d.DialContext(ctx, netw, addr)
	}
}

// TestSentinelCommitmentQueueIntegration brings up redis master + redis-sentinel
// via testcontainers and verifies that a CommitmentStorage backed by go-redis's
// FailoverClient (the same client construction the production factory uses for
// REDIS_SENTINEL_ADDRS) can store and read commitments end-to-end through
// Sentinel-discovered master resolution.
//
// The production factory (createRedisCommitmentQueue) is bypassed because it
// does not expose a Dialer hook: Sentinel returns the master's in-network
// alias address to clients, but the test process runs on the host where the
// alias is not resolvable. A test-only Dialer translates the alias to the
// host-mapped address. Factory validation is covered by factory_test.go.
func TestSentinelCommitmentQueueIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	env := startSentinelTestEnv(ctx, t)

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	client := redislib.NewFailoverClient(&redislib.FailoverOptions{
		MasterName:    sentinelMasterName,
		SentinelAddrs: []string{env.sentinelHostAddr},
		Dialer:        dialerForSentinel(env),
		DialTimeout:   5 * time.Second,
		ReadTimeout:   3 * time.Second,
		WriteTimeout:  3 * time.Second,
		PoolSize:      10,
		MaxRetries:    3,
	})
	t.Cleanup(func() { _ = client.Close() })

	require.NoError(t, client.Ping(ctx).Err(), "Sentinel-backed Ping failed")

	storage := NewCommitmentStorage(client, "commitments-sentinel-test", "test-server", DefaultBatchConfig(), log)
	require.NoError(t, storage.Initialize(ctx))
	t.Cleanup(func() {
		_ = storage.Cleanup(ctx)
		_ = storage.Close(ctx)
	})

	commitments := []*models.CertificationRequest{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}
	for _, c := range commitments {
		require.NoError(t, storage.Store(ctx, c))
	}

	streamCtx, streamCancel := context.WithTimeout(ctx, 5*time.Second)
	defer streamCancel()

	out := make(chan *models.CertificationRequest, len(commitments))
	go func() { _ = storage.StreamCertificationRequests(streamCtx, out) }()

	streamed := make([]*models.CertificationRequest, 0, len(commitments))
	deadline := time.After(4 * time.Second)
collect:
	for len(streamed) < len(commitments) {
		select {
		case c := <-out:
			streamed = append(streamed, c)
		case <-deadline:
			break collect
		}
	}
	require.Len(t, streamed, len(commitments),
		"expected to stream %d commitments through Sentinel-backed queue, got %d",
		len(commitments), len(streamed))

	acks := make([]interfaces.CertificationRequestAck, len(streamed))
	for i, c := range streamed {
		acks[i] = interfaces.CertificationRequestAck{StateID: c.StateID, StreamID: c.StreamID}
	}
	require.NoError(t, storage.MarkProcessed(ctx, acks))

	pending, err := storage.CountUnprocessed(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), pending,
		"expected queue to be drained after MarkProcessed via Sentinel-backed client")
}
