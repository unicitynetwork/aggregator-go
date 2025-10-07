package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
)

// HybridStorage uses Redis for commitments and MongoDB for everything else
type HybridStorage struct {
	redisClient       *redis.Client
	mongoStorage      interfaces.Storage
	commitmentStorage *CommitmentStorage
}

// NewHybridStorage creates a new hybrid storage with Redis commitments + MongoDB for other data
func NewHybridStorage(cfg *config.Config, redisConfig *RedisConfig, serverID string) (*HybridStorage, error) {
	return NewHybridStorageWithBatchConfig(cfg, redisConfig, serverID, DefaultBatchConfig())
}

// NewHybridStorageWithBatchConfig creates a new hybrid storage with custom Redis batch configuration
func NewHybridStorageWithBatchConfig(cfg *config.Config, redisConfig *RedisConfig, serverID string, batchConfig *BatchConfig) (*HybridStorage, error) {
	// Create Redis client using existing Redis config
	redisClient := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", redisConfig.Host, redisConfig.Port),
		Password:     redisConfig.Password,
		DB:           redisConfig.DB,
		DialTimeout:  redisConfig.DialTimeout,
		ReadTimeout:  redisConfig.ReadTimeout,
		WriteTimeout: redisConfig.WriteTimeout,
		PoolSize:     redisConfig.PoolSize,
		MaxRetries:   redisConfig.MaxRetries,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Create MongoDB storage for non-commitment data
	mongoStorage, err := mongodb.NewStorage(*cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB storage: %w", err)
	}

	// Create Redis commitment storage with custom batch config
	commitmentStorage := NewCommitmentStorageWithBatchConfig(redisClient, serverID, batchConfig)

	return &HybridStorage{
		redisClient:       redisClient,
		mongoStorage:      mongoStorage,
		commitmentStorage: commitmentStorage,
	}, nil
}

// Initialize initializes Redis commitment storage (MongoDB doesn't need explicit init)
func (hs *HybridStorage) Initialize(ctx context.Context) error {
	if err := hs.commitmentStorage.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize Redis commitment storage: %w", err)
	}

	return nil
}

// Ping tests connectivity to both Redis and MongoDB
func (hs *HybridStorage) Ping(ctx context.Context) error {
	if err := hs.redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping failed: %w", err)
	}

	if pinger, ok := hs.mongoStorage.(interface{ Ping(context.Context) error }); ok {
		if err := pinger.Ping(ctx); err != nil {
			return fmt.Errorf("mongoDB ping failed: %w", err)
		}
	}

	return nil
}

// Close closes both Redis and MongoDB connections
func (hs *HybridStorage) Close(ctx context.Context) error {
	hs.commitmentStorage.Stop()

	if err := hs.redisClient.Close(); err != nil {
		return fmt.Errorf("failed to close Redis client: %w", err)
	}

	if closer, ok := hs.mongoStorage.(interface{ Close(context.Context) error }); ok {
		if err := closer.Close(ctx); err != nil {
			return fmt.Errorf("failed to close MongoDB storage: %w", err)
		}
	}

	return nil
}

// CommitmentStorage returns the Redis-based commitment storage
func (hs *HybridStorage) CommitmentStorage() interfaces.CommitmentStorage {
	return hs.commitmentStorage
}

// All other storage methods delegate to MongoDB

// BlockStorage returns the block storage interface
func (hs *HybridStorage) BlockStorage() interfaces.BlockStorage {
	return hs.mongoStorage.BlockStorage()
}

// AggregatorRecordStorage returns the aggregator record storage interface
func (hs *HybridStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage {
	return hs.mongoStorage.AggregatorRecordStorage()
}

// SmtStorage returns the SMT storage interface
func (hs *HybridStorage) SmtStorage() interfaces.SmtStorage {
	return hs.mongoStorage.SmtStorage()
}

// BlockRecordsStorage returns the block records storage interface
func (hs *HybridStorage) BlockRecordsStorage() interfaces.BlockRecordsStorage {
	return hs.mongoStorage.BlockRecordsStorage()
}

// LeadershipStorage returns the leadership storage interface
func (hs *HybridStorage) LeadershipStorage() interfaces.LeadershipStorage {
	return hs.mongoStorage.LeadershipStorage()
}

// WithTransaction executes a function within a transaction (delegates to MongoDB)
func (hs *HybridStorage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	// Delegate to MongoDB storage for transactions
	if transactor, ok := hs.mongoStorage.(interface {
		WithTransaction(context.Context, func(context.Context) error) error
	}); ok {
		return transactor.WithTransaction(ctx, fn)
	}

	// If MongoDB doesn't support transactions, just execute the function
	return fn(ctx)
}
