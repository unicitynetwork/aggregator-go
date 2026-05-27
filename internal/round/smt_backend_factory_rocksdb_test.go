//go:build rocksdb

package round

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestConfiguredSMTBackendOpensRocksDB(t *testing.T) {
	backend, err := newConfiguredSMTBackend(&config.Config{
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

	root, err := backend.RootHashRaw(context.Background())
	require.NoError(t, err)
	require.Equal(t, emptyStateRoot(), root)
}
