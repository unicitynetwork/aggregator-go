package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func validTestConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:                      "3000",
			HTTP2MaxConcurrentStreams: 1,
		},
		Database: DatabaseConfig{
			URI:                            "mongodb://localhost:27017",
			Database:                       "aggregator",
			FinalizationInsertChunkWorkers: 1,
		},
		Logging: LoggingConfig{
			Level: "info",
		},
		Sharding: ShardingConfig{
			Mode:          ShardingModeStandalone,
			ShardIDLength: 4,
		},
		Processing: ProcessingConfig{
			CommitmentStreamBufferSize: 10000,
			CollectPhaseDuration:       200 * time.Millisecond,
		},
		BFT: BFTConfig{
			Enabled: false,
		},
	}
}

func TestConfigValidate_FinalizationInsertChunking(t *testing.T) {
	t.Run("disabled chunking is valid", func(t *testing.T) {
		cfg := validTestConfig()

		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() returned error: %v", err)
		}
	})

	t.Run("negative chunk size is invalid", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.Database.FinalizationInsertChunkSize = -1

		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() expected error, got nil")
		}
		if !strings.Contains(err.Error(), "MONGODB_FINALIZATION_INSERT_CHUNK_SIZE") {
			t.Fatalf("Validate() error = %q, want chunk size env name", err.Error())
		}
	})

	t.Run("enabled chunking requires positive workers", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.Database.FinalizationInsertChunkSize = 500
		cfg.Database.FinalizationInsertChunkWorkers = 0

		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() expected error, got nil")
		}
		if !strings.Contains(err.Error(), "MONGODB_FINALIZATION_INSERT_CHUNK_WORKERS") {
			t.Fatalf("Validate() error = %q, want worker env name", err.Error())
		}
	})
}

func TestConfigValidate_CommitmentStreamBufferSize(t *testing.T) {
	cfg := validTestConfig()
	cfg.Processing.CommitmentStreamBufferSize = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "COMMITMENT_STREAM_BUFFER_SIZE") {
		t.Fatalf("Validate() error = %q, want stream buffer env name", err.Error())
	}
}

func TestConfigValidate_CollectPhaseDuration(t *testing.T) {
	cfg := validTestConfig()
	cfg.Processing.CollectPhaseDuration = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "COLLECT_PHASE_DURATION") {
		t.Fatalf("Validate() error = %q, want collect phase env name", err.Error())
	}
}

func TestConfigValidateSMTBackend(t *testing.T) {
	t.Run("memory is default", func(t *testing.T) {
		cfg := validTestConfig()

		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() returned error: %v", err)
		}
	})

	t.Run("invalid backend", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.SMT.Backend = "bad"

		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "SMT_BACKEND") {
			t.Fatalf("Validate() error = %v, want SMT_BACKEND error", err)
		}
	})

	t.Run("rocksdb requires path", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.SMT.Backend = SMTBackendRocksDB

		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "SMT_DISK_PATH") {
			t.Fatalf("Validate() error = %v, want SMT_DISK_PATH error", err)
		}
	})

	t.Run("rocksdb rejects HA", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.HA.Enabled = true
		cfg.HA.ServerID = "server-1"
		cfg.SMT.Backend = SMTBackendRocksDB
		cfg.SMT.DiskPath = t.TempDir()

		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "HA enabled") {
			t.Fatalf("Validate() error = %v, want HA rejection", err)
		}
	})

	t.Run("rocksdb rejects child mode", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.SMT.Backend = SMTBackendRocksDB
		cfg.SMT.DiskPath = t.TempDir()
		cfg.Sharding.Mode = ShardingModeChild
		cfg.Sharding.Child.ParentRpcAddr = "http://localhost:3009"
		cfg.Sharding.Child.ShardID = 2
		cfg.Sharding.Child.ParentPollTimeout = time.Second
		cfg.Sharding.Child.ParentPollInterval = time.Second

		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "SHARDING_MODE=child") {
			t.Fatalf("Validate() error = %v, want child-mode rejection", err)
		}
	})

	t.Run("rocksdb rejects parent mode", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.SMT.Backend = SMTBackendRocksDB
		cfg.SMT.DiskPath = t.TempDir()
		cfg.Sharding.Mode = ShardingModeParent

		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "SHARDING_MODE=parent") {
			t.Fatalf("Validate() error = %v, want parent-mode rejection", err)
		}
	})

	t.Run("rocksdb accepts single-active bft shard", func(t *testing.T) {
		cfg := validTestConfig()
		cfg.SMT.Backend = SMTBackendRocksDB
		cfg.SMT.DiskPath = t.TempDir()
		cfg.Sharding.Mode = ShardingModeStandalone

		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() returned error: %v", err)
		}
	})
}

func TestSMTEnvParsing(t *testing.T) {
	t.Setenv("BFT_ENABLED", "false")
	t.Setenv("DISABLE_HIGH_AVAILABILITY", "true")
	t.Setenv("SMT_BACKEND", "rocksdb")
	t.Setenv("SMT_DISK_PATH", "/tmp/aggr-smt")
	t.Setenv("SMT_ROCKSDB_CACHE_MB", "2048")
	t.Setenv("SMT_ROCKSDB_BG_JOBS", "8")
	t.Setenv("SMT_ROCKSDB_SUBCOMPACTIONS", "4")
	t.Setenv("SMT_ROCKSDB_BLOOM_BITS", "10.5")
	t.Setenv("SMT_ROCKSDB_MEMTABLE_MB", "128")
	t.Setenv("SMT_MATERIALIZE_WORKERS", "32")
	t.Setenv("SMT_STARTUP_REPLAY_LIMIT_BLOCKS", "7")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.SMT.Backend != SMTBackendRocksDB {
		t.Fatalf("SMT.Backend = %q, want rocksdb", cfg.SMT.Backend)
	}
	if cfg.SMT.DiskPath != "/tmp/aggr-smt" ||
		cfg.SMT.RocksDBCacheMB != 2048 ||
		cfg.SMT.RocksDBBGJobs != 8 ||
		cfg.SMT.RocksDBSubcompactions != 4 ||
		cfg.SMT.RocksDBBloomBits != 10.5 ||
		cfg.SMT.RocksDBMemTableMB != 128 ||
		cfg.SMT.MaterializeWorkers != 32 ||
		cfg.SMT.StartupReplayLimitBlocks != 7 {
		t.Fatalf("unexpected SMT config: %+v", cfg.SMT)
	}
}

func TestRedisSentinelEnvParsing(t *testing.T) {
	t.Setenv("REDIS_SENTINEL_ADDRS", "sentinel-1:26379, sentinel-2:26379 ,sentinel-3:26379")
	t.Setenv("REDIS_MASTER_NAME", "mymaster")
	t.Setenv("REDIS_SENTINEL_PASSWORD", "sentpass")
	t.Setenv("REDIS_SENTINEL_USERNAME", "sentuser")
	t.Setenv("REDIS_PASSWORD", "datapass")

	t.Setenv("BFT_ENABLED", "false")
	t.Setenv("DISABLE_HIGH_AVAILABILITY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	wantAddrs := []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"}
	if !reflect.DeepEqual(cfg.Redis.SentinelAddrs, wantAddrs) {
		t.Errorf("SentinelAddrs = %v, want %v", cfg.Redis.SentinelAddrs, wantAddrs)
	}
	if cfg.Redis.MasterName != "mymaster" {
		t.Errorf("MasterName = %q, want %q", cfg.Redis.MasterName, "mymaster")
	}
	if cfg.Redis.SentinelPassword != "sentpass" {
		t.Errorf("SentinelPassword = %q, want %q", cfg.Redis.SentinelPassword, "sentpass")
	}
	if cfg.Redis.SentinelUsername != "sentuser" {
		t.Errorf("SentinelUsername = %q, want %q", cfg.Redis.SentinelUsername, "sentuser")
	}
	if cfg.Redis.Password != "datapass" {
		t.Errorf("Password = %q, want %q", cfg.Redis.Password, "datapass")
	}
}

func TestRedisSentinelDefaults(t *testing.T) {
	t.Setenv("BFT_ENABLED", "false")
	t.Setenv("DISABLE_HIGH_AVAILABILITY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(cfg.Redis.SentinelAddrs) != 0 {
		t.Errorf("SentinelAddrs default = %v, want empty", cfg.Redis.SentinelAddrs)
	}
	if cfg.Redis.MasterName != "" {
		t.Errorf("MasterName default = %q, want empty", cfg.Redis.MasterName)
	}
}

func TestGetEnvStringSliceOrDefault(t *testing.T) {
	t.Run("unset returns default", func(t *testing.T) {
		def := []string{"a", "b"}
		got := getEnvStringSliceOrDefault("UNICITY_TEST_UNSET_VAR", def)
		if !reflect.DeepEqual(got, def) {
			t.Errorf("got %v, want %v", got, def)
		}
	})

	t.Run("trims and splits", func(t *testing.T) {
		t.Setenv("UNICITY_TEST_SLICE", " a , b,c ,, d ")
		got := getEnvStringSliceOrDefault("UNICITY_TEST_SLICE", nil)
		want := []string{"a", "b", "c", "d"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("only commas returns default", func(t *testing.T) {
		t.Setenv("UNICITY_TEST_SLICE_EMPTY", " , , ")
		def := []string{"x"}
		got := getEnvStringSliceOrDefault("UNICITY_TEST_SLICE_EMPTY", def)
		if !reflect.DeepEqual(got, def) {
			t.Errorf("got %v, want %v", got, def)
		}
	})
}
