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
