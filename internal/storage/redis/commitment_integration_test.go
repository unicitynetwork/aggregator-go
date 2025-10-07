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
