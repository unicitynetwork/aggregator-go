package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Every method the gateway registers must be a known metrics label; otherwise its
// requests are reported as "unknown".
func TestEveryRegisteredMethodIsAKnownMetricsLabel(t *testing.T) {
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	noop := func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) { return nil, nil }

	for _, mode := range []config.ShardingMode{
		config.ShardingModeStandalone, config.ShardingModeParent,
		config.ShardingModeChild, config.ShardingModeBFTShard,
	} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Server = config.ServerConfig{ConcurrencyLimit: 10}
			cfg.Sharding.Mode = mode
			srv := NewServer(cfg, log, nil)
			srv.SetBatchProofHandler(noop)
			srv.SetNonInclusionHandler(noop)

			methods := srv.rpcServer.Methods()
			require.NotEmpty(t, methods)
			for _, m := range methods {
				require.Equal(t, m, metrics.NormalizeMethod(m), "method %q registered on the gateway is missing from metrics knownMethods", m)
			}
		})
	}
}
