package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	commitmentStream     = "commitments"
	consumerGroup        = "processors"
	cleanupInterval      = 5 * time.Minute
	maxStreamLength      = 1000000                // Keep last 1M messages
	defaultFlushInterval = 100 * time.Millisecond // How often to flush pending items to Redis
	defaultBatchSize     = 5000                   // Max batch size before forcing flush
)

// BatchConfig configures the asynchronous batching behavior
type BatchConfig struct {
	FlushInterval   time.Duration // How often to flush pending items
	MaxBatchSize    int           // Max items before forcing flush
	CleanupInterval time.Duration // How often to trim the stream
	MaxStreamLength int64         // Maximum stream length before trimming
}

// DefaultBatchConfig returns default batching configuration
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		FlushInterval:   defaultFlushInterval,
		MaxBatchSize:    defaultBatchSize,
		CleanupInterval: cleanupInterval,
		MaxStreamLength: maxStreamLength,
	}
}

// pendingCommitment represents a commitment waiting to be batched
type pendingCommitment struct {
	commitment *models.Commitment
	resultChan chan error
}

// CommitmentStorage implements commitment storage using Redis streams with cursor support
type CommitmentStorage struct {
	client      *redis.Client
	serverID    string
	consumerID  string
	stopChan    chan struct{}
	closed      atomic.Bool
	batchConfig *BatchConfig
	logger      *logger.Logger

	// Restart recovery: on startup, exhaust all pending messages before reading new ones
	// This ensures messages stuck in "pending" state (from crashed consumer) are recovered
	// Once all pending are read, switch to new messages permanently
	pendingExhausted atomic.Bool

	// Batching channels
	pendingChan chan *pendingCommitment
	flushTicker *time.Ticker
}

// NewCommitmentStorage creates a new Redis-based commitment storage instance with custom batching config
func NewCommitmentStorage(client *redis.Client, serverID string, batchConfig *BatchConfig, log *logger.Logger) *CommitmentStorage {
	cs := &CommitmentStorage{
		client:      client,
		serverID:    serverID,
		consumerID:  "processor",
		stopChan:    make(chan struct{}),
		batchConfig: batchConfig,
		logger:      log,
		pendingChan: make(chan *pendingCommitment, batchConfig.MaxBatchSize*2), // Buffer for 2x max batch
		flushTicker: time.NewTicker(batchConfig.FlushInterval),
	}

	go cs.batchProcessor()

	return cs
}

// Initialize creates the consumer group and starts cleanup routine
func (cs *CommitmentStorage) Initialize(ctx context.Context) error {
	err := cs.client.XGroupCreateMkStream(ctx, commitmentStream, consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Only start periodic cleanup if interval is configured
	if cs.batchConfig.CleanupInterval > 0 {
		go cs.periodicCleanup(ctx)
	}

	return nil
}

// Close gracefully stops the storage and cleanup routines
func (cs *CommitmentStorage) Close(ctx context.Context) error {
	// Prevent double-close
	if !cs.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	cs.flushTicker.Stop()
	close(cs.stopChan)
	return nil
}

// batchProcessor runs in the background and processes pending commitments in batches
func (cs *CommitmentStorage) batchProcessor() {
	var pendingBatch []*pendingCommitment
	var flushTimer <-chan time.Time

	for {
		select {
		case <-cs.stopChan:
			// Flush any remaining items before stopping
			if len(pendingBatch) > 0 {
				cs.flushBatch(pendingBatch)
			}
			return

		case pending := <-cs.pendingChan:
			// If this is the first item in a new batch, start the flush timer
			if len(pendingBatch) == 0 {
				flushTimer = time.After(cs.batchConfig.FlushInterval)
			}

			pendingBatch = append(pendingBatch, pending)

			// Force flush if batch reaches max size
			if len(pendingBatch) >= cs.batchConfig.MaxBatchSize {
				flushTimer = nil // Clear timer
				cs.flushBatch(pendingBatch)
				pendingBatch = nil
			}

		case <-cs.flushTicker.C:
			// Periodic flush (backup mechanism)
			if len(pendingBatch) > 0 {
				flushTimer = nil // Clear timer
				cs.flushBatch(pendingBatch)
				pendingBatch = nil
			}

		case <-flushTimer:
			// Flush when the batch timer expires
			if len(pendingBatch) > 0 {
				flushTimer = nil
				cs.flushBatch(pendingBatch)
				pendingBatch = nil
			}
		}
	}
}

// flushBatch sends a batch of commitments to Redis using pipeline
func (cs *CommitmentStorage) flushBatch(batch []*pendingCommitment) {
	if len(batch) == 0 {
		return
	}

	commitments := make([]*models.Commitment, len(batch))
	for i, pending := range batch {
		commitments[i] = pending.commitment
	}

	// Use StoreBatch to send all at once
	// Use background context since this is called from a goroutine that manages its own lifecycle
	// Individual Store() calls can still be cancelled via their own context
	err := cs.storeBatchSync(context.Background(), commitments)

	// Send results back to all waiting goroutines
	for _, pending := range batch {
		pending.resultChan <- err
		close(pending.resultChan)
	}
}

// storeBatchSync is the synchronous version of StoreBatch (internal use)
func (cs *CommitmentStorage) storeBatchSync(ctx context.Context, commitments []*models.Commitment) error {
	if len(commitments) == 0 {
		return nil
	}

	// Serialize all commitments first
	serializedCommitments := make([]string, len(commitments))
	for i, commitment := range commitments {
		commitmentJSON, err := json.Marshal(commitment)
		if err != nil {
			return fmt.Errorf("failed to serialize commitment %d: %w", i, err)
		}
		serializedCommitments[i] = string(commitmentJSON)
	}

	// Use pipeline to batch all operations
	pipe := cs.client.Pipeline()

	// Add all to stream using pipeline
	for i, commitment := range commitments {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: commitmentStream,
			Values: map[string]interface{}{
				"requestId": string(commitment.RequestID),
				"data":      serializedCommitments[i],
			},
		})
	}

	// Execute all operations at once
	results, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch operations: %w", err)
	}

	// Check if any operations failed
	for i, result := range results {
		if result.Err() != nil {
			return fmt.Errorf("batch operation %d failed: %w", i, result.Err())
		}
	}

	return nil
}

// Store stores a new commitment using asynchronous batching
func (cs *CommitmentStorage) Store(ctx context.Context, commitment *models.Commitment) error {
	// Create pending commitment with result channel
	pending := &pendingCommitment{
		commitment: commitment,
		resultChan: make(chan error, 1),
	}

	// Send to batch processor
	select {
	case cs.pendingChan <- pending:
		// Successfully queued, wait for result
		select {
		case err := <-pending.resultChan:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StoreBatch stores multiple commitments using Redis pipelining for high throughput
func (cs *CommitmentStorage) StoreBatch(ctx context.Context, commitments []*models.Commitment) error {
	if len(commitments) == 0 {
		return nil
	}

	// Serialize all commitments first
	serializedCommitments := make([]string, len(commitments))
	for i, commitment := range commitments {
		commitmentJSON, err := json.Marshal(commitment)
		if err != nil {
			return fmt.Errorf("failed to serialize commitment %d: %w", i, err)
		}
		serializedCommitments[i] = string(commitmentJSON)
	}

	// Use pipeline to batch all operations
	pipe := cs.client.Pipeline()

	// Add all to stream using pipeline
	for i, commitment := range commitments {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: commitmentStream,
			Values: map[string]interface{}{
				"requestId": string(commitment.RequestID),
				"data":      serializedCommitments[i],
			},
		})
	}

	// Execute all operations at once
	results, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch operations: %w", err)
	}

	// Check if any operations failed
	for i, result := range results {
		if result.Err() != nil {
			return fmt.Errorf("batch operation %d failed: %w", i, result.Err())
		}
	}

	return nil
}

// GetByRequestID is not implemented for Redis
func (cs *CommitmentStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error) {
	return nil, fmt.Errorf("GetByRequestID not implemented for Redis")
}

// GetUnprocessedBatch is not implemented for Redis - use StreamCommitments instead
func (cs *CommitmentStorage) GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error) {
	return nil, fmt.Errorf("GetUnprocessedBatch not implemented for Redis - use StreamCommitments")
}

// GetUnprocessedBatchWithCursor is not implemented for Redis - use StreamCommitments instead
func (cs *CommitmentStorage) GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.Commitment, string, error) {
	return nil, "", fmt.Errorf("GetUnprocessedBatchWithCursor not implemented for Redis - use StreamCommitments")
}

// MarkProcessed marks commitments as processed by acknowledging only the first N pending messages
// where N = len(requestIDs).
//
// IMPORTANT: This method assumes commitments are processed in FIFO order, which is guaranteed by:
// 1. Redis streams deliver messages in order
// 2. Consumer groups maintain order per consumer
// 3. The round manager processes from channel in order
func (cs *CommitmentStorage) MarkProcessed(ctx context.Context, requestIDs []api.RequestID) error {
	if len(requestIDs) == 0 {
		return nil
	}

	// Get pending entries for this consumer - acknowledge only the first len(requestIDs) entries
	pending := cs.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   commitmentStream,
		Group:    consumerGroup,
		Consumer: cs.consumerID,
		Start:    "-",
		End:      "+",
		Count:    int64(len(requestIDs)), // Only get the exact number we need
	})

	if pending.Err() != nil {
		if pending.Err() == redis.Nil {
			return nil // No pending entries
		}
		return fmt.Errorf("failed to get pending entries: %w", pending.Err())
	}

	pendingEntries := pending.Val()
	if len(pendingEntries) == 0 {
		return nil // No pending entries to acknowledge
	}

	// Validate we have enough pending entries (defensive check)
	if len(requestIDs) > len(pendingEntries) {
		return fmt.Errorf("ordering assumption violated: expected at least %d pending entries but only found %d",
			len(requestIDs), len(pendingEntries))
	}

	// Acknowledge exactly the first len(requestIDs) entries
	ackCount := len(requestIDs)

	entryIDs := make([]string, ackCount)
	for i := 0; i < ackCount; i++ {
		entryIDs[i] = pendingEntries[i].ID
	}

	// Use single XAck call for maximum performance
	err := cs.client.XAck(ctx, commitmentStream, consumerGroup, entryIDs...).Err()
	if err != nil {
		return fmt.Errorf("failed to acknowledge entries: %w", err)
	}

	return nil
}

// Delete removes processed commitments (not typically needed with streams)
func (cs *CommitmentStorage) Delete(ctx context.Context, requestIDs []api.RequestID) error {
	// In Redis streams, we typically don't delete individual messages
	// The periodic cleanup handles old messages
	return nil
}

// Count returns the total number of commitments in the stream
func (cs *CommitmentStorage) Count(ctx context.Context) (int64, error) {
	info := cs.client.XInfoStream(ctx, commitmentStream)
	if info.Err() != nil {
		if info.Err() == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get stream info: %w", info.Err())
	}
	return info.Val().Length, nil
}

// CountUnprocessed returns the number of unprocessed commitments
func (cs *CommitmentStorage) CountUnprocessed(ctx context.Context) (int64, error) {
	// Get pending count for the entire consumer group - this represents unprocessed items
	pendingInfo := cs.client.XPending(ctx, commitmentStream, consumerGroup)
	if pendingInfo.Err() != nil {
		if pendingInfo.Err() == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get pending count: %w", pendingInfo.Err())
	}

	pending := pendingInfo.Val()
	return pending.Count, nil
}

// parseCommitment parses a Redis stream message into a Commitment
func (cs *CommitmentStorage) parseCommitment(message redis.XMessage) (*models.Commitment, error) {
	dataStr, exists := message.Values["data"]
	if !exists {
		return nil, fmt.Errorf("data field not found in message")
	}

	var commitment models.Commitment
	if err := json.Unmarshal([]byte(dataStr.(string)), &commitment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal commitment: %w", err)
	}

	return &commitment, nil
}

// periodicCleanup runs periodic maintenance on the stream
func (cs *CommitmentStorage) periodicCleanup(ctx context.Context) {
	ticker := time.NewTicker(cs.batchConfig.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs.trimStream(ctx)
		case <-cs.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

// trimStream keeps the stream at a reasonable size
func (cs *CommitmentStorage) trimStream(ctx context.Context) {
	// Get current count before trimming
	countBefore, _ := cs.Count(ctx)

	// Trim to keep only the last N messages
	trimmed := cs.client.XTrimMaxLen(ctx, commitmentStream, cs.batchConfig.MaxStreamLength).Val()

	if trimmed > 0 {
		countAfter := countBefore - trimmed
		cs.logger.WithContext(ctx).Info("Redis stream trimmed",
			"stream", commitmentStream,
			"trimmed", trimmed,
			"before", countBefore,
			"after", countAfter,
			"maxLength", cs.batchConfig.MaxStreamLength)
	}
}

// StreamCommitments continuously streams commitments using blocking Redis reads
// This streams commitments directly to the provided channel as they arrive
func (cs *CommitmentStorage) StreamCommitments(ctx context.Context, commitmentChan chan<- *models.Commitment) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cs.stopChan:
			return nil
		default:
			var streams *redis.XStreamSliceCmd

			// On startup: exhaust all pending messages first
			// Once exhausted, switch to reading new messages
			if !cs.pendingExhausted.Load() {
				// Check for pending messages
				pendingStreams := cs.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    consumerGroup,
					Consumer: cs.consumerID,
					Streams:  []string{commitmentStream, "0"}, // "0" = read pending for this consumer
					Count:    100,
					Block:    0, // Don't block
				})

				if pendingStreams.Err() != nil && pendingStreams.Err() != redis.Nil {
					return fmt.Errorf("failed to check pending messages: %w", pendingStreams.Err())
				}

				// If we got pending, keep reading them
				if pendingStreams.Err() == nil && len(pendingStreams.Val()) > 0 && len(pendingStreams.Val()[0].Messages) > 0 {
					streams = pendingStreams
				} else {
					// No more pending! Switch to new messages mode
					cs.pendingExhausted.Store(true)
					continue // Next iteration will read new messages
				}
			} else {
				// Normal operation: read new messages only
				streams = cs.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    consumerGroup,
					Consumer: cs.consumerID,
					Streams:  []string{commitmentStream, ">"}, // ">" = new messages only
					Count:    100,
					Block:    100 * time.Millisecond,
				})
			}

			if streams.Err() != nil {
				if streams.Err() == redis.Nil {
					// No new messages, continue (shouldn't happen with block=0, but just in case)
					continue
				}
				if streams.Err() == context.Canceled || streams.Err() == context.DeadlineExceeded {
					return streams.Err() // Context cancelled, exit gracefully
				}
				return fmt.Errorf("failed to read from stream: %w", streams.Err())
			}

			// Process messages and stream them to the channel
			for _, stream := range streams.Val() {
				for _, message := range stream.Messages {
					commitment, err := cs.parseCommitment(message)
					if err != nil {
						// Skip malformed messages and continue processing
						continue
					}
					if commitment == nil {
						continue
					}

					// Stream commitment directly to channel
					select {
					case commitmentChan <- commitment:
					case <-ctx.Done():
						return ctx.Err()
					case <-cs.stopChan:
						return nil
					}
				}
			}
		}
	}
}
