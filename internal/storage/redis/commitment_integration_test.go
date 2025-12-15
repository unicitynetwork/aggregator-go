package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RedisTestSuite defines the test suite with shared Redis container
type RedisTestSuite struct {
	suite.Suite
	ctx       context.Context
	client    *redis.Client
	container testcontainers.Container
	storage   *CommitmentStorage
}

// SetupSuite runs once before all tests - creates shared Redis container
func (suite *RedisTestSuite) SetupSuite() {
	suite.ctx = context.Background()

	container, _ := CreateTestRedisContainer(suite.ctx, suite.T())
	suite.container = container
	suite.client = CreateRedisClient(suite.ctx, container, suite.T())

	// Clean database once at the start
	err := suite.client.FlushDB(suite.ctx).Err()
	require.NoError(suite.T(), err)
}

// TearDownSuite runs once after all tests - terminates container
func (suite *RedisTestSuite) TearDownSuite() {
	if suite.client != nil {
		suite.client.Close()
	}
	if suite.container != nil {
		suite.container.Terminate(suite.ctx)
	}
}

// SetupTest runs before each test - creates fresh storage
func (suite *RedisTestSuite) SetupTest() {
	// Create new storage for this test
	log, _ := logger.New("info", "text", "stdout", false)
	suite.storage = NewCommitmentStorage(suite.client, "test-server", DefaultBatchConfig(), log)
	require.NoError(suite.T(), suite.storage.Initialize(suite.ctx))
}

// TearDownTest runs after each test - cleans up storage and Redis state
func (suite *RedisTestSuite) TearDownTest() {
	if suite.storage != nil {
		suite.storage.Close(suite.ctx)
		// Use Cleanup to remove stream and consumer group
		suite.storage.Cleanup(suite.ctx)
	}
}

// TestRedisTestSuite runs the test suite
func TestRedisTestSuite(t *testing.T) {
	suite.Run(t, new(RedisTestSuite))
}

// Test 1: Basic Pipeline (Store -> Stream -> MarkProcessed)
func (suite *RedisTestSuite) TestCommitmentPipeline_BasicFlow() {
	ctx := suite.ctx

	// Store commitments
	commitments := []*models.CertificationRequest{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := suite.storage.Store(ctx, c)
		require.NoError(suite.T(), err)
	}

	// Wait for async batch flush
	time.Sleep(150 * time.Millisecond)

	// Stream commitments
	commitmentChan := make(chan *models.CertificationRequest, 10)
	streamCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go func() {
		_ = suite.storage.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	// Collect streamed commitments
	var streamed []*models.CertificationRequest
	timeout := time.After(1 * time.Second)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			streamed = append(streamed, c)
			if len(streamed) == len(commitments) {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	require.Equal(suite.T(), len(commitments), len(streamed), "Should stream all stored commitments")

	// Mark as processed
	acks := make([]interfaces.CertificationRequestAck, len(streamed))
	for i, c := range streamed {
		acks[i] = interfaces.CertificationRequestAck{RequestID: c.StateID, StreamID: c.StreamID}
	}

	err := suite.storage.MarkProcessed(ctx, acks)
	require.NoError(suite.T(), err)

	// Verify no pending
	pending, err := suite.storage.CountUnprocessed(ctx)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), pending)
}

// Test 2: FIFO Ordering - Verify commitments streamed in exact order stored
func (suite *RedisTestSuite) TestCommitmentPipeline_FIFOOrdering() {

	ctx := suite.ctx

	// Store commitments in specific order using StoreBatch and track their StateIDs
	numCommitments := 100
	expectedOrder := make([]api.StateID, numCommitments)
	commitments := make([]*models.CertificationRequest, numCommitments)

	for i := 0; i < numCommitments; i++ {
		commitment := createTestCommitment()
		expectedOrder[i] = commitment.StateID
		commitments[i] = commitment
	}

	err := suite.storage.StoreBatch(ctx, commitments)
	require.NoError(suite.T(), err)

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Stream commitments and verify order
	commitmentChan := make(chan *models.CertificationRequest, 200)
	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	go func() {
		_ = suite.storage.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	// Collect streamed commitments
	actualOrder := make([]api.StateID, 0, numCommitments)
	timeout := time.After(2 * time.Second)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			actualOrder = append(actualOrder, c.StateID)
			if len(actualOrder) == numCommitments {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	require.Equal(suite.T(), numCommitments, len(actualOrder), "Should stream all commitments")

	// Verify FIFO order
	for i := 0; i < numCommitments; i++ {
		assert.Equal(suite.T(), expectedOrder[i], actualOrder[i],
			"CertificationData at position %d should match (expected: %s, got: %s)",
			i, expectedOrder[i], actualOrder[i])
	}
}

// Test 3: High Throughput - Sustain 5000 stores/sec for 10 seconds
func (suite *RedisTestSuite) TestCommitmentPipeline_HighThroughput() {
	if testing.Short() {
		suite.T().Skip("Skipping high throughput test in short mode")
	}

	ctx := suite.ctx

	// Test parameters
	targetRPS := 5000
	duration := 10 * time.Second // 10 seconds is enough to verify sustained throughput
	expectedTotal := targetRPS * int(duration.Seconds())

	suite.T().Logf("Testing %v at %d RPS (expected: %d commitments)", duration, targetRPS, expectedTotal)

	// Start streaming in background
	commitmentChan := make(chan *models.CertificationRequest, 10000)
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	go func() {
		_ = suite.storage.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	// Start consumer
	var processedCount atomic.Int64
	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		batch := make([]*models.CertificationRequest, 0, 1000)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case c := <-commitmentChan:
				batch = append(batch, c)
				if len(batch) >= 1000 {
					if err := suite.storage.MarkProcessed(ctx, toAckEntries(batch)); err == nil {
						processedCount.Add(int64(len(batch)))
					}
					batch = batch[:0]
				}

			case <-ticker.C:
				if len(batch) > 0 {
					if err := suite.storage.MarkProcessed(ctx, toAckEntries(batch)); err == nil {
						processedCount.Add(int64(len(batch)))
					}
					batch = batch[:0]
				}

			case <-streamCtx.Done():
				if len(batch) > 0 {
					_ = suite.storage.MarkProcessed(ctx, toAckEntries(batch))
					processedCount.Add(int64(len(batch)))
				}
				return
			}
		}
	}()

	// Start producers - use concurrent Store() calls to achieve high throughput
	var submittedCount atomic.Int64
	var submitErrors atomic.Int32
	var storeWg sync.WaitGroup
	startTime := time.Now()
	endTime := startTime.Add(duration)

	numProducers := 50 // More producers for higher concurrency
	var producerWg sync.WaitGroup

	for p := 0; p < numProducers; p++ {
		producerWg.Go(func() {
			ticker := time.NewTicker(time.Second / time.Duration(targetRPS/numProducers))
			defer ticker.Stop()

			commitmentNum := 0
			for time.Now().Before(endTime) {
				<-ticker.C

				commitment := createTestCommitment()
				commitmentNum++

				// Store in background goroutine to avoid blocking
				storeWg.Go(func() {
					if err := suite.storage.Store(ctx, commitment); err != nil {
						submitErrors.Add(1)
					} else {
						submittedCount.Add(1)
					}
				})
			}
		})
	}

	// Wait for all producers to finish submitting
	producerWg.Wait()

	// Wait for all Store() calls to complete
	storeWg.Wait()
	actualDuration := time.Since(startTime)

	suite.T().Logf("Submitted %d commitments in %v (%.0f/sec)",
		submittedCount.Load(), actualDuration, float64(submittedCount.Load())/actualDuration.Seconds())

	// Wait for processing to catch up
	time.Sleep(2 * time.Second)

	// Stop consumer
	cancelStream()
	<-consumerDone

	suite.T().Logf("Processed %d commitments", processedCount.Load())

	// Verify throughput
	require.Equal(suite.T(), int32(0), submitErrors.Load(), "Should have no submit errors")
	actualRPS := float64(submittedCount.Load()) / actualDuration.Seconds()
	require.GreaterOrEqual(suite.T(), actualRPS, float64(targetRPS)*0.95,
		"Should sustain at least 95%% of target RPS (%.0f/sec)", actualRPS)

	// Verify processing
	successRate := float64(processedCount.Load()) / float64(submittedCount.Load())
	require.Greater(suite.T(), successRate, 0.95, "Should process at least 95%% of commitments")
}

// Test 4: Stream Trimming - Verify stream doesn't grow beyond maxStreamLength
func (suite *RedisTestSuite) TestCommitmentPipeline_StreamTrimming() {
	if testing.Short() {
		suite.T().Skip("Skipping stream trimming test in short mode")
	}

	ctx := suite.ctx

	// Store commitments using StoreBatch for efficiency
	numToStore := 1000

	commitments := make([]*models.CertificationRequest, numToStore)
	for i := 0; i < numToStore; i++ {
		commitments[i] = createTestCommitment()
	}

	err := suite.storage.StoreBatch(ctx, commitments)
	require.NoError(suite.T(), err)

	time.Sleep(100 * time.Millisecond)

	// Verify stream length
	count, err := suite.storage.Count(ctx)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(numToStore), count, "Stream should contain all stored commitments")

	// Manually trigger trim (in production this happens periodically)
	suite.storage.trimStream(ctx)

	// Verify count is still within maxStreamLength
	count, err = suite.storage.Count(ctx)
	require.NoError(suite.T(), err)
	assert.LessOrEqual(suite.T(), count, int64(maxStreamLength),
		"Stream length should be <= maxStreamLength after trim")

	suite.T().Logf("Stream length after trim: %d (max: %d)", count, maxStreamLength)
}

// Test 5: No Message Loss - All stored commitments eventually stream out
func (suite *RedisTestSuite) TestCommitmentPipeline_NoMessageLoss() {

	ctx := suite.ctx

	// Store commitments with unique IDs using StoreBatch
	numCommitments := 1000
	storedIDs := make(map[api.StateID]bool)

	commitments := make([]*models.CertificationRequest, numCommitments)
	for i := 0; i < numCommitments; i++ {
		commitment := createTestCommitment()
		commitments[i] = commitment
		storedIDs[commitment.StateID] = true
	}

	err := suite.storage.StoreBatch(ctx, commitments)
	require.NoError(suite.T(), err)

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Stream all commitments
	commitmentChan := make(chan *models.CertificationRequest, 2000)
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		_ = suite.storage.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	// Collect all streamed commitments
	streamedIDs := make(map[api.StateID]bool)
	timeout := time.After(3 * time.Second)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			streamedIDs[c.StateID] = true
			if len(streamedIDs) == numCommitments {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	// Verify all stored commitments were streamed
	require.Equal(suite.T(), len(storedIDs), len(streamedIDs),
		"Should stream exactly the same number of commitments as stored")

	// Verify no missing commitments
	for stateID := range storedIDs {
		assert.True(suite.T(), streamedIDs[stateID],
			"Stored certification request %s was not streamed", stateID)
	}

	// Verify no extra commitments
	for stateID := range streamedIDs {
		assert.True(suite.T(), storedIDs[stateID],
			"Streamed certification request %s was not stored", stateID)
	}
}

// Test 6: StreamCertificationRequests recovers after consumer group deletion by recreating the group.
func (suite *RedisTestSuite) TestCommitmentStream_ConsumerGroupRecovery() {
	ctx := suite.ctx

	// Store initial certification request so the stream goroutine has data to consume.
	initial := createTestCommitment()
	require.NoError(suite.T(), suite.storage.Store(ctx, initial))
	time.Sleep(150 * time.Millisecond) // allow async flush

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	commitmentChan := make(chan *models.CertificationRequest, 4)
	var streamErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = suite.storage.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	// Expect the first certification request to arrive.
	select {
	case first := <-commitmentChan:
		require.Equal(suite.T(), initial.StateID, first.StateID)
	case <-time.After(2 * time.Second):
		suite.T().Fatal("did not receive initial certification request before timeout")
	}

	// drop the consumer group while the stream is active.
	err := suite.client.XGroupDestroy(ctx, commitmentStream, consumerGroup).Err()
	require.NoError(suite.T(), err, "failed to destroy consumer group for test")

	// Publish another certification request that should trigger the recovery path.
	nextCommitment := createTestCommitment()
	require.NoError(suite.T(), suite.storage.Store(ctx, nextCommitment))
	time.Sleep(150 * time.Millisecond)

	// The streaming goroutine should recreate the group and deliver the new commitment.
	var received *models.CertificationRequest
	require.Eventually(suite.T(), func() bool {
		select {
		case c := <-commitmentChan:
			received = c
			return c.StateID == nextCommitment.StateID
		default:
			return false
		}
	}, 5*time.Second, 100*time.Millisecond, "stream did not recover after consumer group removal")

	// Validate the streamed commitment.
	require.NotNil(suite.T(), received)
	assert.Equal(suite.T(), nextCommitment.StateID, received.StateID)
	assert.NotEmpty(suite.T(), received.StreamID, "StreamID should be populated after recovery")

	// Confirm the consumer group was recreated.
	groups := suite.client.XInfoGroups(ctx, commitmentStream)
	require.NoError(suite.T(), groups.Err())
	assert.NotEmpty(suite.T(), groups.Val(), "consumer group should have been recreated")

	// Clean up goroutine.
	cancel()
	wg.Wait()
	if streamErr != nil && streamErr != context.Canceled {
		suite.T().Logf("StreamCertificationRequests exit error: %v", streamErr)
	}
}

// setupCustomStorage creates a test Redis storage with custom cleanup interval (suite helper)
func (suite *RedisTestSuite) setupCustomStorage(cleanupInterval time.Duration, maxStreamLen int64) *CommitmentStorage {
	ctx := suite.ctx

	// Create storage with custom cleanup interval and max stream length
	cfg := &BatchConfig{
		FlushInterval:   50 * time.Millisecond,
		MaxBatchSize:    2000,
		CleanupInterval: cleanupInterval,
		MaxStreamLength: maxStreamLen,
	}
	log, _ := logger.New("info", "text", "stdout", false)
	customStorage := NewCommitmentStorage(suite.client, "test-server-custom", cfg, log)

	err := customStorage.Initialize(ctx)
	require.NoError(suite.T(), err)

	return customStorage
}

// Test 6: Periodic Cleanup - Verify automatic stream trimming runs periodically
func (suite *RedisTestSuite) TestCommitmentPipeline_PeriodicCleanup() {
	if testing.Short() {
		suite.T().Skip("Skipping periodic cleanup test in short mode")
	}

	ctx := suite.ctx

	// Setup custom storage with special cleanup interval (10K max stream length for testing)
	testMaxStreamLen := int64(10000)
	customStorage := suite.setupCustomStorage(1*time.Second, testMaxStreamLen)
	defer func() {
		customStorage.Cleanup(ctx)
		customStorage.Close(ctx)
	}()

	// Store more than the max stream length to verify trimming works
	targetCount := 15000
	batchSize := 5000
	numBatches := (targetCount + batchSize - 1) / batchSize

	suite.T().Logf("Storing %d commitments (max stream length: %d)...", targetCount, testMaxStreamLen)

	for i := 0; i < numBatches; i++ {
		remaining := targetCount - (i * batchSize)
		currentBatchSize := batchSize
		if remaining < batchSize {
			currentBatchSize = remaining
		}

		batch := make([]*models.CertificationRequest, currentBatchSize)
		for j := 0; j < currentBatchSize; j++ {
			batch[j] = createTestCommitment()
		}

		err := customStorage.StoreBatch(ctx, batch)
		require.NoError(suite.T(), err)
	}

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// Check initial count (may be full or already trimmed depending on timing)
	countBefore, err := customStorage.Count(ctx)
	require.NoError(suite.T(), err)
	suite.T().Logf("Stream count after storage: %d (target: %d, max: %d)", countBefore, targetCount, testMaxStreamLen)

	// If stream exceeds max, wait for periodic cleanup (runs every 1 second)
	if countBefore > testMaxStreamLen {
		suite.T().Log("Waiting for periodic cleanup to trim stream...")
		time.Sleep(1500 * time.Millisecond)

		countAfter, err := customStorage.Count(ctx)
		require.NoError(suite.T(), err)
		suite.T().Logf("Stream count after cleanup: %d (max: %d)", countAfter, testMaxStreamLen)

		assert.LessOrEqual(suite.T(), countAfter, testMaxStreamLen,
			"Stream should be trimmed to maxStreamLength after periodic cleanup")
	}

	// Store more commitments to exceed limit again
	suite.T().Log("Storing 3K more commitments...")
	moreBatch := make([]*models.CertificationRequest, 3000)
	for j := 0; j < 3000; j++ {
		moreBatch[j] = createTestCommitment()
	}
	err = customStorage.StoreBatch(ctx, moreBatch)
	require.NoError(suite.T(), err)

	time.Sleep(200 * time.Millisecond)

	countBeforeSecond, err := customStorage.Count(ctx)
	require.NoError(suite.T(), err)
	suite.T().Logf("Stream count before second cleanup: %d", countBeforeSecond)

	// Wait for second periodic cleanup cycle
	suite.T().Log("Waiting for second cleanup cycle...")
	time.Sleep(1200 * time.Millisecond)

	countFinal, err := customStorage.Count(ctx)
	require.NoError(suite.T(), err)
	suite.T().Logf("Stream count after second cleanup: %d (max: %d)", countFinal, testMaxStreamLen)

	// Verify still trimmed to limit
	assert.LessOrEqual(suite.T(), countFinal, testMaxStreamLen,
		"Stream should remain within maxStreamLength after second cleanup")
}

// Test 7: Restart Recovery - Verify pending and new messages are both recovered
func (suite *RedisTestSuite) TestCommitmentPipeline_RestartRecovery() {
	if testing.Short() {
		suite.T().Skip("Skipping restart recovery test in short mode")
	}

	ctx := suite.ctx
	storage1 := suite.storage

	// Phase 1: Store 10 commitments and stream them (but don't ACK - simulate crash)
	numPending := 10
	commitments := make([]*models.CertificationRequest, numPending)
	for i := 0; i < numPending; i++ {
		commitments[i] = createTestCommitment()
	}
	require.NoError(suite.T(), storage1.StoreBatch(ctx, commitments))
	time.Sleep(100 * time.Millisecond)

	commitmentChan := make(chan *models.CertificationRequest, 20)
	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	defer cancelStream()

	go func() {
		_ = storage1.StreamCertificationRequests(streamCtx, commitmentChan)
	}()

	var received []*models.CertificationRequest
	timeout := time.After(500 * time.Millisecond)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			received = append(received, c)
			if len(received) == numPending {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}
	require.Len(suite.T(), received, numPending)

	// Simulate crash: close without ACK
	cancelStream()
	storage1.Close(ctx)

	// Phase 2: Restart and add 5 NEW commitments
	log, _ := logger.New("info", "text", "stdout", false)
	storage2 := NewCommitmentStorage(storage1.client, "server-restarted", DefaultBatchConfig(), log)
	require.NoError(suite.T(), storage2.Initialize(ctx))

	numNew := 5
	newCommitments := make([]*models.CertificationRequest, numNew)
	for i := 0; i < numNew; i++ {
		newCommitments[i] = createTestCommitment()
	}
	require.NoError(suite.T(), storage2.StoreBatch(ctx, newCommitments))
	time.Sleep(100 * time.Millisecond)

	// Phase 3: StreamCertificationRequests should recover pending + new in one call
	totalExpected := numPending + numNew
	commitmentChan2 := make(chan *models.CertificationRequest, 20)
	streamCtx2, cancelStream2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancelStream2()

	go func() {
		_ = storage2.StreamCertificationRequests(streamCtx2, commitmentChan2)
	}()

	var allReceived []*models.CertificationRequest
	timeout2 := time.After(1 * time.Second)
CollectLoop2:
	for {
		select {
		case c := <-commitmentChan2:
			allReceived = append(allReceived, c)
			if len(allReceived) == totalExpected {
				break CollectLoop2
			}
		case <-timeout2:
			break CollectLoop2
		}
	}

	assert.Equal(suite.T(), totalExpected, len(allReceived),
		"Should recover all pending and new messages")

	storage2.Close(ctx)
}
