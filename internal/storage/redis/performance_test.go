package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// PerformanceResult holds timing results
type PerformanceResult struct {
	Operation       string
	Implementation  string
	CommitmentCount int
	Duration        time.Duration
	Rate            float64 // operations per second
}

func (pr PerformanceResult) String() string {
	return fmt.Sprintf("%s (%s): %d commitments in %v (%.2f ops/sec)",
		pr.Operation, pr.Implementation, pr.CommitmentCount, pr.Duration, pr.Rate)
}

func TestPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	commitmentCounts := []int{1000, 5000, 10000}

	for _, count := range commitmentCounts {
		t.Run(fmt.Sprintf("MarkProcessed_%d_commitments", count), func(t *testing.T) {
			// Test Redis performance
			redisResult := testRedisMarkProcessedPerformance(t, ctx, count)
			t.Logf("Redis: %s", redisResult)

			// Test MongoDB performance
			mongoResult := testMongoMarkProcessedPerformance(t, ctx, count)
			t.Logf("MongoDB: %s", mongoResult)

			// Calculate improvement
			improvement := mongoResult.Duration.Seconds() / redisResult.Duration.Seconds()
			t.Logf("Redis is %.2fx faster than MongoDB for %d commitments", improvement, count)

			// Redis should be significantly faster
			if redisResult.Duration > mongoResult.Duration {
				t.Logf("Warning: Redis was slower than MongoDB - this might indicate a setup issue")
			}
		})
	}
}

func TestStoragePerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	commitmentCounts := []int{1000, 5000, 10000, 20000}

	for _, count := range commitmentCounts {
		t.Run(fmt.Sprintf("Store_%d_commitments", count), func(t *testing.T) {
			// Test Redis storage performance
			redisResult := testRedisStorePerformance(t, ctx, count)
			t.Logf("Redis Store: %s", redisResult)

			// Test MongoDB storage performance
			mongoResult := testMongoStorePerformance(t, ctx, count)
			t.Logf("MongoDB Store: %s", mongoResult)

			// Calculate improvement
			improvement := mongoResult.Duration.Seconds() / redisResult.Duration.Seconds()
			t.Logf("Redis is %.2fx faster than MongoDB for storing %d commitments", improvement, count)

			// Redis should be significantly faster
			if redisResult.Duration > mongoResult.Duration {
				t.Logf("Warning: Redis was slower than MongoDB - this might indicate a setup issue")
			}
		})
	}
}

func TestRedisBatchingIntervalComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	commitmentCount := 5000 // Fixed batch size for interval testing
	flushIntervals := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}

	for _, interval := range flushIntervals {
		t.Run(fmt.Sprintf("FlushInterval_%dms", interval.Milliseconds()), func(t *testing.T) {
			result := testRedisStorePerformanceWithInterval(t, ctx, commitmentCount, interval)
			t.Logf("Redis Store (flush %dms): %s", interval.Milliseconds(), result)
		})
	}
}

func testRedisMarkProcessedPerformance(t *testing.T, ctx context.Context, commitmentCount int) PerformanceResult {
	// Setup Redis
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	// Store commitments using batch for setup (not timed)
	err := storage.StoreBatch(ctx, commitments)
	require.NoError(t, err)

	// Get commitments to put them in pending state
	retrieved, err := storage.GetUnprocessedBatch(ctx, commitmentCount)
	require.NoError(t, err)
	require.Len(t, retrieved, commitmentCount)

	// Extract request IDs
	var requestIDs []api.RequestID
	for _, c := range retrieved {
		requestIDs = append(requestIDs, c.RequestID)
	}

	// Time the MarkProcessed operation
	start := time.Now()
	err = storage.MarkProcessed(ctx, requestIDs)
	duration := time.Since(start)

	require.NoError(t, err)

	rate := float64(commitmentCount) / duration.Seconds()

	return PerformanceResult{
		Operation:       "MarkProcessed",
		Implementation:  "Redis",
		CommitmentCount: commitmentCount,
		Duration:        duration,
		Rate:            rate,
	}
}

func testMongoMarkProcessedPerformance(t *testing.T, ctx context.Context, commitmentCount int) PerformanceResult {
	// Setup MongoDB using testcontainers
	mongoContainer, cfg := setupMongoContainer(t, ctx)
	defer mongoContainer.Terminate(ctx)

	mongoStorage, err := mongodb.NewStorage(cfg)
	require.NoError(t, err)
	defer mongoStorage.Close(ctx)

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	// Store commitments (not timed - setup)
	commitmentStorage := mongoStorage.CommitmentQueue()
	for _, c := range commitments {
		err := commitmentStorage.Store(ctx, c)
		require.NoError(t, err)
	}

	// Extract request IDs
	var requestIDs []api.RequestID
	for _, c := range commitments {
		requestIDs = append(requestIDs, c.RequestID)
	}

	// Time the MarkProcessed operation
	start := time.Now()
	err = commitmentStorage.MarkProcessed(ctx, requestIDs)
	duration := time.Since(start)

	require.NoError(t, err)

	rate := float64(commitmentCount) / duration.Seconds()

	return PerformanceResult{
		Operation:       "MarkProcessed",
		Implementation:  "MongoDB",
		CommitmentCount: commitmentCount,
		Duration:        duration,
		Rate:            rate,
	}
}

func generateTestCommitments(count int) []*models.Commitment {
	commitments := make([]*models.Commitment, count)

	for i := 0; i < count; i++ {
		commitments[i] = createTestCommitment()
	}
	return commitments
}

func setupRedisContainer(t *testing.T, ctx context.Context) (testcontainers.Container, *redis.Client) {
	// Start Redis container with disk persistence enabled
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
		Cmd: []string{
			"redis-server",
			"--save", "1", "1", // Save to disk every 1 second if at least 1 key changed
			"--appendonly", "yes", // Enable AOF for additional durability
			"--appendfsync", "everysec", // Fsync AOF every second
		},
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Could not start Redis container: %v", err)
	}

	// Get container host and port
	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", host, mappedPort.Port()),
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MaxRetries:   3,
	})

	// Test connection
	err = client.Ping(ctx).Err()
	require.NoError(t, err, "Failed to connect to Redis container")

	// Clean database
	err = client.FlushDB(ctx).Err()
	require.NoError(t, err)

	return redisContainer, client
}

func setupMongoContainer(t *testing.T, ctx context.Context) (testcontainers.Container, config.Config) {
	// Start MongoDB container
	req := testcontainers.ContainerRequest{
		Image:        "mongo:7.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections"),
		Env: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "admin",
			"MONGO_INITDB_ROOT_PASSWORD": "password",
			"MONGO_INITDB_DATABASE":      "test_performance",
		},
	}

	mongoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Could not start MongoDB container: %v", err)
	}

	// Get container host and port
	host, err := mongoContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := mongoContainer.MappedPort(ctx, "27017")
	require.NoError(t, err)

	// Create config
	mongoURI := fmt.Sprintf("mongodb://admin:password@%s:%s/test_performance?authSource=admin", host, mappedPort.Port())
	cfg := config.Config{
		Database: config.DatabaseConfig{
			URI:                    mongoURI,
			Database:               "test_performance",
			ConnectTimeout:         10 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            10,
			MinPoolSize:            1,
			MaxConnIdleTime:        5 * time.Minute,
		},
	}

	return mongoContainer, cfg
}

func testRedisStorePerformance(t *testing.T, ctx context.Context, commitmentCount int) PerformanceResult {
	// Setup Redis
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	// Time individual Store operations using parallel goroutines (realistic scenario)
	start := time.Now()

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	errChan := make(chan error, commitmentCount)

	for _, c := range commitments {
		wg.Add(1)
		go func(commitment *models.Commitment) {
			defer wg.Done()
			err := storage.Store(ctx, commitment)
			if err != nil {
				errChan <- err
			}
		}(c)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	duration := time.Since(start)
	rate := float64(commitmentCount) / duration.Seconds()

	return PerformanceResult{
		Operation:       "Store",
		Implementation:  "Redis",
		CommitmentCount: commitmentCount,
		Duration:        duration,
		Rate:            rate,
	}
}

func testRedisStorePerformanceWithInterval(t *testing.T, ctx context.Context, commitmentCount int, flushInterval time.Duration) PerformanceResult {
	// Setup Redis using testcontainers (like setupTestRedis)
	redisContainer, client := setupRedisContainer(t, ctx)
	defer func() {
		client.Close()
		redisContainer.Terminate(ctx)
	}()

	// Create storage with custom flush interval
	batchConfig := &BatchConfig{
		FlushInterval: flushInterval,
		MaxBatchSize:  5000,
	}

	storage := NewCommitmentStorageWithBatchConfig(client, "perf-test-server", batchConfig)
	storage.Initialize(ctx)
	defer storage.Close(ctx)

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	// Time individual Store operations using parallel goroutines
	start := time.Now()

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	errChan := make(chan error, commitmentCount)

	for _, c := range commitments {
		wg.Add(1)
		go func(commitment *models.Commitment) {
			defer wg.Done()
			err := storage.Store(ctx, commitment)
			if err != nil {
				errChan <- err
			}
		}(c)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	duration := time.Since(start)
	rate := float64(commitmentCount) / duration.Seconds()

	return PerformanceResult{
		Operation:       fmt.Sprintf("Store (flush %dms)", flushInterval.Milliseconds()),
		Implementation:  "Redis",
		CommitmentCount: commitmentCount,
		Duration:        duration,
		Rate:            rate,
	}
}

func testMongoStorePerformance(t *testing.T, ctx context.Context, commitmentCount int) PerformanceResult {
	// Setup MongoDB using testcontainers
	mongoContainer, cfg := setupMongoContainer(t, ctx)
	defer mongoContainer.Terminate(ctx)

	mongoStorage, err := mongodb.NewStorage(cfg)
	require.NoError(t, err)
	defer mongoStorage.Close(ctx)

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	// Time individual Store operations using parallel goroutines (same as Redis test)
	commitmentStorage := mongoStorage.CommitmentQueue()
	start := time.Now()

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	errChan := make(chan error, commitmentCount)

	for _, c := range commitments {
		wg.Add(1)
		go func(commitment *models.Commitment) {
			defer wg.Done()
			err := commitmentStorage.Store(ctx, commitment)
			if err != nil {
				errChan <- err
			}
		}(c)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	duration := time.Since(start)
	rate := float64(commitmentCount) / duration.Seconds()

	return PerformanceResult{
		Operation:       "Store",
		Implementation:  "MongoDB",
		CommitmentCount: commitmentCount,
		Duration:        duration,
		Rate:            rate,
	}
}

func BenchmarkRedisMarkProcessed(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	storage, cleanup := setupTestRedisBench(b)
	defer cleanup()

	ctx := context.Background()
	commitmentCount := 1000

	// Generate test data
	commitments := generateTestCommitments(commitmentCount)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup for this iteration
		for _, c := range commitments {
			storage.Store(ctx, c)
		}

		// Get commitments to put them in pending state
		retrieved, _ := storage.GetUnprocessedBatch(ctx, commitmentCount)
		var requestIDs []api.RequestID
		for _, c := range retrieved {
			requestIDs = append(requestIDs, c.RequestID)
		}

		b.StartTimer()

		// Time the operation
		storage.MarkProcessed(ctx, requestIDs)

		b.StopTimer()

		// Clean up for next iteration
		storage.client.FlushDB(ctx)
		storage.Initialize(ctx)
	}
}

func setupTestRedisBench(b *testing.B) (*CommitmentStorage, func()) {
	testConfig := &RedisConfig{
		Host:         "localhost",
		Port:         6379,
		Password:     "",
		DB:           15, // Use test database
		PoolSize:     10,
		MinIdleConns: 1,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		Password:     testConfig.Password,
		DB:           testConfig.DB,
		DialTimeout:  testConfig.DialTimeout,
		ReadTimeout:  testConfig.ReadTimeout,
		WriteTimeout: testConfig.WriteTimeout,
		PoolSize:     testConfig.PoolSize,
		MaxRetries:   testConfig.MaxRetries,
	})

	// Test connection
	ctx := context.Background()
	err := client.Ping(ctx).Err()
	if err != nil {
		b.Skipf("Redis not available: %v", err)
	}

	// Clean test database
	client.FlushDB(ctx)

	storage := NewCommitmentStorage(client, "bench-server")
	storage.Initialize(ctx)

	cleanup := func() {
		storage.Close(ctx)
		client.FlushDB(ctx)
		client.Close()
	}

	return storage, cleanup
}
