//go:build !rocksdb

package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestConfiguredSMTBackendAllowsRocksDBHABFTShardModeToReachBackendFactory(t *testing.T) {
	backend, err := newConfiguredSMTBackend(&config.Config{
		HA:       config.HAConfig{Enabled: true},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
		SMT: config.SMTConfig{
			Backend:  config.SMTBackendRocksDB,
			DiskPath: t.TempDir(),
		},
	}, smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	require.Nil(t, backend)
	require.ErrorContains(t, err, "requires a binary built with -tags rocksdb")
}
