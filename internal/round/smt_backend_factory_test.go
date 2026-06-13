package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestConfiguredSMTBackendDefaultsToMemory(t *testing.T) {
	backend, err := newConfiguredSMTBackend(&config.Config{}, smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()
	require.IsType(t, &smtbackend.MemoryBackend{}, backend)
}

func TestConfiguredSMTBackendRejectsRocksDBHAOutsideBFTShardMode(t *testing.T) {
	backend, err := newConfiguredSMTBackend(&config.Config{
		HA: config.HAConfig{Enabled: true},
		SMT: config.SMTConfig{
			Backend:  config.SMTBackendRocksDB,
			DiskPath: t.TempDir(),
		},
	}, smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	require.Nil(t, backend)
	require.ErrorContains(t, err, "SHARDING_MODE=bft-shard")
}
