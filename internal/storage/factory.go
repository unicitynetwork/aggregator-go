package storage

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/storage/redis"
)

// NewStorage creates a storage instance based on configuration
func NewStorage(cfg *config.Config) (interfaces.Storage, error) {
	if cfg.Storage.UseRedisForCommitments {
		// Create hybrid storage: Redis for commitments, MongoDB for everything else
		return NewHybridStorage(cfg)
	} else {
		// Create pure MongoDB storage
		return mongodb.NewStorage(*cfg)
	}
}

// NewHybridStorage creates a hybrid storage that uses Redis for commitments and MongoDB for other data
func NewHybridStorage(cfg *config.Config) (interfaces.Storage, error) {
	// Convert config to Redis-specific config
	redisConfig := &redis.RedisConfig{
		Host:         cfg.Redis.Host,
		Port:         cfg.Redis.Port,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		MaxRetries:   cfg.Redis.MaxRetries,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	}

	// Create batch config for Redis commitments
	batchConfig := &redis.BatchConfig{
		FlushInterval: cfg.Storage.RedisFlushInterval,
		MaxBatchSize:  cfg.Storage.RedisMaxBatchSize,
	}

	// Create hybrid storage
	serverID := cfg.HA.ServerID
	if serverID == "" {
		serverID = "aggregator-server"
	}

	hybridStorage, err := redis.NewHybridStorageWithBatchConfig(cfg, redisConfig, serverID, batchConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid storage: %w", err)
	}

	return hybridStorage, nil
}
