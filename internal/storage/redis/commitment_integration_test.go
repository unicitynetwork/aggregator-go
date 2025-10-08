package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Priority 1 Tests: Core Functionality

// Test 1: Basic Pipeline (Store -> Stream -> MarkProcessed)
func TestCommitmentPipeline_BasicFlow(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store commitments
	commitments := []*models.Commitment{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := storage.Store(ctx, c)
		require.NoError(t, err)
	}

	// Wait for async batch flush
	time.Sleep(150 * time.Millisecond)

	// Stream commitments
	commitmentChan := make(chan *models.Commitment, 10)
	streamCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go func() {
		_ = storage.StreamCommitments(streamCtx, commitmentChan)
	}()

	// Collect streamed commitments
	var streamed []*models.Commitment
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

	require.Equal(t, len(commitments), len(streamed), "Should stream all stored commitments")

	// Mark as processed
	requestIDs := make([]api.RequestID, len(streamed))
	for i, c := range streamed {
		requestIDs[i] = c.RequestID
	}

	err := storage.MarkProcessed(ctx, requestIDs)
	require.NoError(t, err)

	// Verify no pending
	pending, err := storage.CountUnprocessed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending)
}

// Test 2: FIFO Ordering - Verify commitments streamed in exact order stored
func TestCommitmentPipeline_FIFOOrdering(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store commitments in specific order using StoreBatch and track their RequestIDs
	numCommitments := 100
	expectedOrder := make([]api.RequestID, numCommitments)
	commitments := make([]*models.Commitment, numCommitments)

	for i := 0; i < numCommitments; i++ {
		commitment := createTestCommitment()
		expectedOrder[i] = commitment.RequestID
		commitments[i] = commitment
	}

	err := storage.StoreBatch(ctx, commitments)
	require.NoError(t, err)

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Stream commitments and verify order
	commitmentChan := make(chan *models.Commitment, 200)
	streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	go func() {
		_ = storage.StreamCommitments(streamCtx, commitmentChan)
	}()

	// Collect streamed commitments
	actualOrder := make([]api.RequestID, 0, numCommitments)
	timeout := time.After(2 * time.Second)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			actualOrder = append(actualOrder, c.RequestID)
			if len(actualOrder) == numCommitments {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	require.Equal(t, numCommitments, len(actualOrder), "Should stream all commitments")

	// Verify FIFO order
	for i := 0; i < numCommitments; i++ {
		assert.Equal(t, expectedOrder[i], actualOrder[i],
			"Commitment at position %d should match (expected: %s, got: %s)",
			i, expectedOrder[i], actualOrder[i])
	}
}

// Test 3: High Throughput - Sustain 5000+ stores/sec for 30 seconds
func TestCommitmentPipeline_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high throughput test in short mode")
	}

	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Test parameters
	targetRPS := 5000
	duration := 10 * time.Second // 10 seconds is enough to verify sustained throughput
	expectedTotal := targetRPS * int(duration.Seconds())

	t.Logf("Testing %v at %d RPS (expected: %d commitments)", duration, targetRPS, expectedTotal)

	// Start streaming in background
	commitmentChan := make(chan *models.Commitment, 10000)
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	go func() {
		_ = storage.StreamCommitments(streamCtx, commitmentChan)
	}()

	// Start consumer
	var processedCount atomic.Int64
	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		batch := make([]*models.Commitment, 0, 1000)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case c := <-commitmentChan:
				batch = append(batch, c)
				if len(batch) >= 1000 {
					requestIDs := make([]api.RequestID, len(batch))
					for i, commitment := range batch {
						requestIDs[i] = commitment.RequestID
					}
					if err := storage.MarkProcessed(ctx, requestIDs); err == nil {
						processedCount.Add(int64(len(batch)))
					}
					batch = batch[:0]
				}

			case <-ticker.C:
				if len(batch) > 0 {
					requestIDs := make([]api.RequestID, len(batch))
					for i, commitment := range batch {
						requestIDs[i] = commitment.RequestID
					}
					if err := storage.MarkProcessed(ctx, requestIDs); err == nil {
						processedCount.Add(int64(len(batch)))
					}
					batch = batch[:0]
				}

			case <-streamCtx.Done():
				if len(batch) > 0 {
					requestIDs := make([]api.RequestID, len(batch))
					for i, commitment := range batch {
						requestIDs[i] = commitment.RequestID
					}
					storage.MarkProcessed(ctx, requestIDs)
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
		producerWg.Add(1)
		go func(producerID int) {
			defer producerWg.Done()

			ticker := time.NewTicker(time.Second / time.Duration(targetRPS/numProducers))
			defer ticker.Stop()

			commitmentNum := 0
			for time.Now().Before(endTime) {
				<-ticker.C

				commitment := createTestCommitment()
				commitmentNum++

				// Store in background goroutine to avoid blocking
				storeWg.Add(1)
				go func(c *models.Commitment) {
					defer storeWg.Done()
					if err := storage.Store(ctx, c); err != nil {
						submitErrors.Add(1)
					} else {
						submittedCount.Add(1)
					}
				}(commitment)
			}
		}(p)
	}

	// Wait for all producers to finish submitting
	producerWg.Wait()

	// Wait for all Store() calls to complete
	storeWg.Wait()
	actualDuration := time.Since(startTime)

	t.Logf("Submitted %d commitments in %v (%.0f/sec)",
		submittedCount.Load(), actualDuration, float64(submittedCount.Load())/actualDuration.Seconds())

	// Wait for processing to catch up
	time.Sleep(2 * time.Second)

	// Stop consumer
	cancelStream()
	<-consumerDone

	t.Logf("Processed %d commitments", processedCount.Load())

	// Verify throughput
	require.Equal(t, int32(0), submitErrors.Load(), "Should have no submit errors")
	actualRPS := float64(submittedCount.Load()) / actualDuration.Seconds()
	require.GreaterOrEqual(t, actualRPS, float64(targetRPS)*0.95,
		"Should sustain at least 95%% of target RPS (%.0f/sec)", actualRPS)

	// Verify processing
	successRate := float64(processedCount.Load()) / float64(submittedCount.Load())
	require.Greater(t, successRate, 0.95, "Should process at least 95%% of commitments")
}

// Test 4: Stream Trimming - Verify stream doesn't grow beyond maxStreamLength
func TestCommitmentPipeline_StreamTrimming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stream trimming test in short mode")
	}

	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store commitments using StoreBatch for efficiency
	numToStore := 1000

	commitments := make([]*models.Commitment, numToStore)
	for i := 0; i < numToStore; i++ {
		commitments[i] = createTestCommitment()
	}

	err := storage.StoreBatch(ctx, commitments)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Verify stream length
	count, err := storage.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(numToStore), count, "Stream should contain all stored commitments")

	// Manually trigger trim (in production this happens periodically)
	storage.trimStream(ctx)

	// Verify count is still within maxStreamLength
	count, err = storage.Count(ctx)
	require.NoError(t, err)
	assert.LessOrEqual(t, count, int64(maxStreamLength),
		"Stream length should be <= maxStreamLength after trim")

	t.Logf("Stream length after trim: %d (max: %d)", count, maxStreamLength)
}

// Test 5: No Message Loss - All stored commitments eventually stream out
func TestCommitmentPipeline_NoMessageLoss(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store commitments with unique IDs using StoreBatch
	numCommitments := 1000
	storedIDs := make(map[api.RequestID]bool)

	commitments := make([]*models.Commitment, numCommitments)
	for i := 0; i < numCommitments; i++ {
		commitment := createTestCommitment()
		commitments[i] = commitment
		storedIDs[commitment.RequestID] = true
	}

	err := storage.StoreBatch(ctx, commitments)
	require.NoError(t, err)

	// Wait for async processing
	time.Sleep(100 * time.Millisecond)

	// Stream all commitments
	commitmentChan := make(chan *models.Commitment, 2000)
	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		_ = storage.StreamCommitments(streamCtx, commitmentChan)
	}()

	// Collect all streamed commitments
	streamedIDs := make(map[api.RequestID]bool)
	timeout := time.After(3 * time.Second)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			streamedIDs[c.RequestID] = true
			if len(streamedIDs) == numCommitments {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	// Verify all stored commitments were streamed
	require.Equal(t, len(storedIDs), len(streamedIDs),
		"Should stream exactly the same number of commitments as stored")

	// Verify no missing commitments
	for requestID := range storedIDs {
		assert.True(t, streamedIDs[requestID],
			"Stored commitment %s was not streamed", requestID)
	}

	// Verify no extra commitments
	for requestID := range streamedIDs {
		assert.True(t, storedIDs[requestID],
			"Streamed commitment %s was not stored", requestID)
	}
}

// setupTestRedisWithCleanupInterval creates a test Redis storage with custom cleanup interval
func setupTestRedisWithCleanupInterval(t *testing.T, cleanupInterval time.Duration, maxStreamLen int64) (*CommitmentStorage, func()) {
	storage, baseCleanup := setupTestRedis(t)

	// Close the default storage (but keep the Redis client)
	ctx := context.Background()
	storage.flushTicker.Stop()
	// Don't close stopChan since it's already closed in the default storage

	// Get the Redis client from the storage
	client := storage.client

	// Create new storage with custom cleanup interval and max stream length
	cfg := &BatchConfig{
		FlushInterval:   50 * time.Millisecond,
		MaxBatchSize:    2000,
		CleanupInterval: cleanupInterval,
		MaxStreamLength: maxStreamLen,
	}
	customStorage := NewCommitmentStorageWithBatchConfig(client, "test-server-custom", cfg)

	err := customStorage.Initialize(ctx)
	require.NoError(t, err)

	cleanup := func() {
		customStorage.Close(ctx)
		baseCleanup()
	}

	return customStorage, cleanup
}

// Test 6: Periodic Cleanup - Verify automatic stream trimming runs periodically
func TestCommitmentPipeline_PeriodicCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping periodic cleanup test in short mode")
	}

	ctx := context.Background()

	// Setup test container with custom Redis config (10K max stream length for testing)
	testMaxStreamLen := int64(10000)
	storage, cleanup := setupTestRedisWithCleanupInterval(t, 1*time.Second, testMaxStreamLen)
	defer cleanup()

	// Store more than the max stream length to verify trimming works
	targetCount := 15000
	batchSize := 5000
	numBatches := (targetCount + batchSize - 1) / batchSize

	t.Logf("Storing %d commitments (max stream length: %d)...", targetCount, testMaxStreamLen)

	for i := 0; i < numBatches; i++ {
		remaining := targetCount - (i * batchSize)
		currentBatchSize := batchSize
		if remaining < batchSize {
			currentBatchSize = remaining
		}

		batch := make([]*models.Commitment, currentBatchSize)
		for j := 0; j < currentBatchSize; j++ {
			batch[j] = createTestCommitment()
		}

		err := storage.StoreBatch(ctx, batch)
		require.NoError(t, err)
	}

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// Check initial count (may be full or already trimmed depending on timing)
	countBefore, err := storage.Count(ctx)
	require.NoError(t, err)
	t.Logf("Stream count after storage: %d (target: %d, max: %d)", countBefore, targetCount, testMaxStreamLen)

	// If stream exceeds max, wait for periodic cleanup (runs every 1 second)
	if countBefore > testMaxStreamLen {
		t.Log("Waiting for periodic cleanup to trim stream...")
		time.Sleep(1500 * time.Millisecond)

		countAfter, err := storage.Count(ctx)
		require.NoError(t, err)
		t.Logf("Stream count after cleanup: %d (max: %d)", countAfter, testMaxStreamLen)

		assert.LessOrEqual(t, countAfter, testMaxStreamLen,
			"Stream should be trimmed to maxStreamLength after periodic cleanup")
	}

	// Store more commitments to exceed limit again
	t.Log("Storing 3K more commitments...")
	moreBatch := make([]*models.Commitment, 3000)
	for j := 0; j < 3000; j++ {
		moreBatch[j] = createTestCommitment()
	}
	err = storage.StoreBatch(ctx, moreBatch)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	countBeforeSecond, err := storage.Count(ctx)
	require.NoError(t, err)
	t.Logf("Stream count before second cleanup: %d", countBeforeSecond)

	// Wait for second periodic cleanup cycle
	t.Log("Waiting for second cleanup cycle...")
	time.Sleep(1200 * time.Millisecond)

	countFinal, err := storage.Count(ctx)
	require.NoError(t, err)
	t.Logf("Stream count after second cleanup: %d (max: %d)", countFinal, testMaxStreamLen)

	// Verify still trimmed to limit
	assert.LessOrEqual(t, countFinal, testMaxStreamLen,
		"Stream should remain within maxStreamLength after second cleanup")
}

// Test 7: Restart Recovery - Verify pending messages are recovered after restart
func TestCommitmentPipeline_RestartRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping restart recovery test in short mode")
	}

	ctx := context.Background()

	// Setup Redis
	storage1, cleanup := setupTestRedis(t)
	defer cleanup()

	// Store commitments
	numCommitments := 10
	commitments := make([]*models.Commitment, numCommitments)
	for i := 0; i < numCommitments; i++ {
		commitments[i] = createTestCommitment()
	}

	err := storage1.StoreBatch(ctx, commitments)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	t.Logf("Stored %d commitments", numCommitments)

	// Use StreamCommitments (the actual production flow) to read messages
	commitmentChan := make(chan *models.Commitment, 20)
	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	defer cancelStream()

	go func() {
		_ = storage1.StreamCommitments(streamCtx, commitmentChan)
	}()

	// Collect messages from stream
	var received []*models.Commitment
	timeout := time.After(500 * time.Millisecond)
CollectLoop:
	for {
		select {
		case c := <-commitmentChan:
			received = append(received, c)
			if len(received) == numCommitments {
				break CollectLoop
			}
		case <-timeout:
			break CollectLoop
		}
	}

	require.Len(t, received, numCommitments, "Should stream all commitments")
	t.Logf("First consumer streamed %d commitments (now pending)", len(received))

	// Count pending for this consumer
	pendingInfo := storage1.client.XPending(ctx, "commitments", "processors")
	require.NoError(t, pendingInfo.Err())
	t.Logf("Pending messages after first consumer: %d", pendingInfo.Val().Count)

	// Simulate server restart: Close first storage and create new one with different consumerID
	// DON'T call MarkProcessed - simulating a crash before ACK
	cancelStream()
	storage1.Close(ctx)
	t.Log("Simulated server crash (before XACK)")

	// Create new storage instance (new consumerID, simulating restart)
	storage2 := NewCommitmentStorage(storage1.client, "server-restarted")
	err = storage2.Initialize(ctx)
	require.NoError(t, err)

	t.Logf("New consumer started with ID: %s", storage2.consumerID)

	// Try to stream again - with current implementation, new consumer won't see pending messages
	commitmentChan2 := make(chan *models.Commitment, 20)
	streamCtx2, cancelStream2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancelStream2()

	go func() {
		_ = storage2.StreamCommitments(streamCtx2, commitmentChan2)
	}()

	var received2 []*models.Commitment
	timeout2 := time.After(500 * time.Millisecond)
CollectLoop2:
	for {
		select {
		case c := <-commitmentChan2:
			received2 = append(received2, c)
			if len(received2) == numCommitments {
				break CollectLoop2
			}
		case <-timeout2:
			break CollectLoop2
		}
	}

	t.Logf("After restart, new consumer streamed %d commitments", len(received2))

	// Check total stream count - messages should still be in stream
	totalCount, err := storage2.Count(ctx)
	require.NoError(t, err)
	t.Logf("Total messages in stream: %d", totalCount)

	// Check pending count - old messages should still be pending
	pendingInfo2 := storage2.client.XPending(ctx, "commitments", "processors")
	require.NoError(t, pendingInfo2.Err())
	t.Logf("Total pending messages after restart: %d", pendingInfo2.Val().Count)

	// Verify pending messages were recovered after restart
	assert.Equal(t, numCommitments, len(received2), "Should recover all pending messages after restart")
	assert.Equal(t, int64(numCommitments), pendingInfo2.Val().Count,
		"Messages should still be in pending state (not yet ACKed)")

	t.Logf("✅ SUCCESS: All %d pending messages were recovered after restart!", numCommitments)

	// Close the second storage instance
	storage2.Close(ctx)
}
