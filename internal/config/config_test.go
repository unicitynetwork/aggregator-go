package config

import (
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
