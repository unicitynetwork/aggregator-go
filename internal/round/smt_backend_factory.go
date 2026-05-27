package round

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

func newConfiguredSMTBackend(cfg *config.Config, threadSafeSmt *smt.ThreadSafeSMT) (smtbackend.Backend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	switch cfg.SMT.Backend.OrDefault() {
	case config.SMTBackendMemory:
		return smtbackend.NewMemoryBackend(threadSafeSmt), nil
	case config.SMTBackendRocksDB:
		if cfg.HA.Enabled {
			return nil, fmt.Errorf("SMT_BACKEND=rocksdb is not supported with HA enabled yet; see docs/disk-backed-smt-ha-replication.md")
		}
		if cfg.Sharding.Mode == config.ShardingModeParent || cfg.Sharding.Mode == config.ShardingModeChild {
			return nil, fmt.Errorf("SMT_BACKEND=rocksdb is not supported with SHARDING_MODE=%s in this phase", cfg.Sharding.Mode)
		}
		return newConfiguredRocksDBSMTBackend(cfg)
	default:
		return nil, fmt.Errorf("invalid SMT_BACKEND: %s", cfg.SMT.Backend)
	}
}
