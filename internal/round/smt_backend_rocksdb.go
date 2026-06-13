//go:build rocksdb

package round

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
)

func newConfiguredRocksDBSMTBackend(cfg *config.Config) (smtbackend.Backend, error) {
	if cfg.SMT.DiskPath == "" {
		return nil, fmt.Errorf("SMT_DISK_PATH is required when SMT_BACKEND=rocksdb")
	}

	store, err := rocksstore.Open(cfg.SMT.DiskPath, rocksstore.Options{
		CacheSizeBytes:    int64(cfg.SMT.RocksDBCacheMB) * 1024 * 1024,
		MaxBackgroundJobs: cfg.SMT.RocksDBBGJobs,
		MaxSubcompactions: cfg.SMT.RocksDBSubcompactions,
		MemTableSizeBytes: uint64(cfg.SMT.RocksDBMemTableMB) * 1024 * 1024,
		BloomBitsPerKey:   cfg.SMT.RocksDBBloomBits,
		EnableStatistics:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("open RocksDB SMT backend: %w", err)
	}

	opts := persist.DefaultOptions()
	opts.MaterializeWorkers = cfg.SMT.MaterializeWorkers
	backend, err := smtbackend.NewDiskBackend(store, opts)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open disk SMT backend: %w", err)
	}
	return backend, nil
}
