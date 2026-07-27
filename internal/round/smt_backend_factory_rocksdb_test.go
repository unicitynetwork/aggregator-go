//go:build rocksdb

package round

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestConfiguredSMTBackendOpensPrefixMajorRocksDB(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
		SMT: config.SMTConfig{
			Backend:               config.SMTBackendRocksDB,
			DiskPath:              t.TempDir(),
			NodeKeyFormat:         "prefix-major",
			RocksDBCacheMB:        8,
			RocksDBBGJobs:         2,
			RocksDBSubcompactions: 1,
			RocksDBBloomBits:      10,
			RocksDBMemTableMB:     8,
			MaterializeWorkers:    2,
		},
	}
	newMemorySMT := func() *smt.ThreadSafeSMT {
		return smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	}

	backend, err := newConfiguredSMTBackend(cfg, newMemorySMT())
	require.NoError(t, err)
	require.IsType(t, &smtbackend.DiskBackend{}, backend)

	root, err := backend.RootHashRaw(context.Background())
	require.NoError(t, err)
	require.Equal(t, emptyStateRoot(), root)
	require.NoError(t, backend.Close())

	cfg.SMT.NodeKeyFormat = "depth-major"
	_, err = newConfiguredSMTBackend(cfg, newMemorySMT())
	require.Error(t, err)
	require.True(t, rocksstore.IsLayoutMismatch(err), "got %v", err)
}

func TestConfiguredSMTBackendOpensRocksDBHABFTShardMode(t *testing.T) {
	backend, err := newConfiguredSMTBackend(&config.Config{
		HA:       config.HAConfig{Enabled: true},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
		SMT: config.SMTConfig{
			Backend:               config.SMTBackendRocksDB,
			DiskPath:              t.TempDir(),
			RocksDBCacheMB:        8,
			RocksDBBGJobs:         2,
			RocksDBSubcompactions: 1,
			RocksDBBloomBits:      10,
			RocksDBMemTableMB:     8,
			MaterializeWorkers:    2,
		},
	}, smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()
	require.IsType(t, &smtbackend.DiskBackend{}, backend)
}
