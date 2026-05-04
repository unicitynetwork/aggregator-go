package storage

import (
	"context"
	"fmt"

	redislib "github.com/redis/go-redis/v9"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/storage/redis"
)

// NewStorage creates commitment queue and storage based on configuration
func NewStorage(ctx context.Context, cfg *config.Config, log *logger.Logger) (interfaces.CommitmentQueue, interfaces.Storage, error) {
	// Always create MongoDB for persistence
	mongoStorage, err := mongodb.NewStorage(ctx, *cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create MongoDB storage: %w", err)
	}

	// Choose commitment queue implementation
	var commitmentQueue interfaces.CommitmentQueue
	if cfg.Storage.UseRedisForCommitments {
		commitmentQueue, err = createRedisCommitmentQueue(cfg, log)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create Redis commitment queue: %w", err)
		}
	} else {
		// Use MongoDB for commitments too
		commitmentQueue = mongoStorage.CommitmentQueue()
	}

	return commitmentQueue, mongoStorage, nil
}

// createRedisCommitmentQueue creates a Redis-based commitment queue
func createRedisCommitmentQueue(cfg *config.Config, log *logger.Logger) (interfaces.CommitmentQueue, error) {
	var redisClient *redislib.Client
	if len(cfg.Redis.SentinelAddrs) > 0 {
		if cfg.Redis.MasterName == "" {
			return nil, fmt.Errorf("REDIS_MASTER_NAME is required when REDIS_SENTINEL_ADDRS is set")
		}
		redisClient = redislib.NewFailoverClient(&redislib.FailoverOptions{
			MasterName:       cfg.Redis.MasterName,
			SentinelAddrs:    cfg.Redis.SentinelAddrs,
			SentinelUsername: cfg.Redis.SentinelUsername,
			SentinelPassword: cfg.Redis.SentinelPassword,
			Password:         cfg.Redis.Password,
			DB:               cfg.Redis.DB,
			DialTimeout:      cfg.Redis.DialTimeout,
			ReadTimeout:      cfg.Redis.ReadTimeout,
			WriteTimeout:     cfg.Redis.WriteTimeout,
			PoolSize:         cfg.Redis.PoolSize,
			MaxRetries:       cfg.Redis.MaxRetries,
			RouteByLatency:   cfg.Redis.RouteByLatency,
			RouteRandomly:    cfg.Redis.RouteRandomly,
			ReplicaOnly:      cfg.Redis.ReplicaOnly,
		})
		if log != nil {
			log.Info("redis commitment queue using sentinel mode",
				"master", cfg.Redis.MasterName,
				"sentinels", cfg.Redis.SentinelAddrs)
		}
	} else {
		if cfg.Redis.MasterName != "" {
			return nil, fmt.Errorf("REDIS_MASTER_NAME is set but REDIS_SENTINEL_ADDRS is empty; either set REDIS_SENTINEL_ADDRS or unset REDIS_MASTER_NAME")
		}
		redisClient = redislib.NewClient(&redislib.Options{
			Addr:         fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			DialTimeout:  cfg.Redis.DialTimeout,
			ReadTimeout:  cfg.Redis.ReadTimeout,
			WriteTimeout: cfg.Redis.WriteTimeout,
			PoolSize:     cfg.Redis.PoolSize,
			MaxRetries:   cfg.Redis.MaxRetries,
		})
		if log != nil {
			log.Info("redis commitment queue using direct mode",
				"addr", fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port))
		}
	}

	// Test connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Create batch config
	batchConfig := &redis.BatchConfig{
		FlushInterval:   cfg.Storage.RedisFlushInterval,
		MaxBatchSize:    cfg.Storage.RedisMaxBatchSize,
		CleanupInterval: cfg.Storage.RedisCleanupInterval,
		MaxStreamLength: cfg.Storage.RedisMaxStreamLength,
	}

	// Get server ID
	serverID := cfg.HA.ServerID
	if serverID == "" {
		serverID = "aggregator-server"
	}

	// Get stream name (defaults to "commitments" if not set)
	streamName := cfg.Storage.RedisStreamName
	if streamName == "" {
		streamName = "commitments"
	}

	// Create certification request storage
	commitmentStorage := redis.NewCommitmentStorage(redisClient, streamName, serverID, batchConfig, log)

	return commitmentStorage, nil
}
