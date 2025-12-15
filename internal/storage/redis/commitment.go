package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	defaultStreamName    = "commitments"
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
	streamName  string
	serverID    string
	consumerID  string
	stopChan    chan struct{}
	closed      atomic.Bool
	batchConfig *BatchConfig
	logger      *logger.Logger
	wg          sync.WaitGroup // Tracks background goroutines for graceful shutdown

	// Restart recovery: on startup, exhaust all pending messages before reading new ones
	// This ensures messages stuck in "pending" state (from crashed consumer) are recovered
	// Once all pending are read, switch to new messages permanently
	pendingExhausted atomic.Bool

	// Batching channels
	pendingChan chan *pendingCommitment
	flushTicker *time.Ticker
}

// NewCommitmentStorage creates a new Redis-based commitment storage instance with custom batching config
func NewCommitmentStorage(client *redis.Client, streamName string, serverID string, batchConfig *BatchConfig, log *logger.Logger) *CommitmentStorage {
	if streamName == "" {
		streamName = defaultStreamName
	}

	cs := &CommitmentStorage{
		client:      client,
		streamName:  streamName,
		serverID:    serverID,
		consumerID:  "processor",
		stopChan:    make(chan struct{}),
		batchConfig: batchConfig,
		logger:      log,
		pendingChan: make(chan *pendingCommitment, batchConfig.MaxBatchSize*2), // Buffer for 2x max batch
		flushTicker: time.NewTicker(batchConfig.FlushInterval),
	}

	return cs
}

// Initialize creates the consumer group and starts background goroutines
func (cs *CommitmentStorage) Initialize(ctx context.Context) error {
	if err := cs.ensureConsumerGroup(ctx); err != nil {
		return err
	}

	// Start batch processor
	cs.wg.Go(cs.batchProcessor)

	// Start periodic cleanup if interval is configured
	if cs.batchConfig.CleanupInterval > 0 {
		cs.wg.Go(func() {
			cs.periodicCleanup(ctx)
		})
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

	// Wait for all background goroutines to finish
	cs.wg.Wait()

	return nil
}

func (cs *CommitmentStorage) ensureConsumerGroup(ctx context.Context) error {
	err := cs.client.XGroupCreateMkStream(ctx, cs.streamName, consumerGroup, "0").Err()
	if err != nil {
		// Ignore "BUSYGROUP" error - group already exists
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	cs.logger.WithContext(ctx).Warn("Redis consumer group recreated",
		"stream", cs.streamName,
		"group", consumerGroup)
	return nil
}

func isNoGroupError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "NOGROUP")
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
			Stream: cs.streamName,
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
			Stream: cs.streamName,
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

// GetByRequestID is not implemented for Redis - use GetAllPending and filter instead
func (cs *CommitmentStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error) {
	return nil, fmt.Errorf("GetByRequestID not implemented for Redis")
}

// GetAllPending retrieves all pending (unacknowledged) commitments from the stream.
// Used for crash recovery to get commitments that weren't processed.
func (cs *CommitmentStorage) GetAllPending(ctx context.Context) ([]*models.Commitment, error) {
	var commitments []*models.Commitment
	const batchSize = 1000
	startID := "-"

	for {
		// Get pending message IDs from the consumer group
		pendingEntries, err := cs.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: cs.streamName,
			Group:  consumerGroup,
			Start:  startID,
			End:    "+",
			Count:  batchSize,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || isNoGroupError(err) {
				return commitments, nil
			}
			return nil, fmt.Errorf("failed to get pending entries: %w", err)
		}

		if len(pendingEntries) == 0 {
			break
		}

		// Collect message IDs
		messageIDs := make([]string, len(pendingEntries))
		for i, entry := range pendingEntries {
			messageIDs[i] = entry.ID
		}

		// Fetch the actual messages by their IDs
		for _, msgID := range messageIDs {
			messages, err := cs.client.XRange(ctx, cs.streamName, msgID, msgID).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to fetch message %s: %w", msgID, err)
			}
			if len(messages) == 0 {
				continue // Message was trimmed from stream
			}

			commitment, err := cs.parseCommitment(messages[0])
			if err != nil {
				return nil, fmt.Errorf("failed to parse commitment: %w", err)
			}
			commitment.StreamID = messages[0].ID
			commitments = append(commitments, commitment)
		}

		// Move to next batch
		if len(pendingEntries) < batchSize {
			break
		}
		startID = "(" + pendingEntries[len(pendingEntries)-1].ID
	}

	return commitments, nil
}

// GetByRequestIDs retrieves commitments matching the given request IDs.
// Streams through data in batches to avoid loading everything into memory.
func (cs *CommitmentStorage) GetByRequestIDs(ctx context.Context, requestIDs []api.RequestID) (map[string]*models.Commitment, error) {
	if len(requestIDs) == 0 {
		return make(map[string]*models.Commitment), nil
	}

	// Build lookup set
	needed := make(map[string]bool, len(requestIDs))
	for _, reqID := range requestIDs {
		needed[string(reqID)] = true
	}

	result := make(map[string]*models.Commitment, len(requestIDs))
	lastID := "0"
	const batchSize = 10000

	for {
		messages, err := cs.client.XRangeN(ctx, cs.streamName, lastID, "+", batchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to read Redis stream: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			lastID = "(" + msg.ID

			commitment, err := cs.parseCommitment(msg)
			if err != nil {
				continue // Skip malformed messages
			}

			reqIDStr := string(commitment.RequestID)
			if needed[reqIDStr] {
				commitment.StreamID = msg.ID
				result[reqIDStr] = commitment

				// Early exit if we found all
				if len(result) == len(needed) {
					return result, nil
				}
			}
		}

		if len(messages) < batchSize {
			break
		}
	}

	return result, nil
}

// GetUnprocessedBatch is not implemented for Redis - use StreamCommitments instead
func (cs *CommitmentStorage) GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error) {
	return nil, fmt.Errorf("GetUnprocessedBatch not implemented for Redis - use StreamCommitments")
}

// GetUnprocessedBatchWithCursor is not implemented for Redis - use StreamCommitments instead
func (cs *CommitmentStorage) GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.Commitment, string, error) {
	return nil, "", fmt.Errorf("GetUnprocessedBatchWithCursor not implemented for Redis - use StreamCommitments")
}

// MarkProcessed acknowledges commitments using their Redis stream IDs.
func (cs *CommitmentStorage) MarkProcessed(ctx context.Context, entries []interfaces.CommitmentAck) error {
	if len(entries) == 0 {
		return nil
	}

	streamIDs := make([]string, len(entries))
	for i, entry := range entries {
		if entry.StreamID == "" {
			return fmt.Errorf("missing stream ID for requestID %s", entry.RequestID.String())
		}
		streamIDs[i] = entry.StreamID
	}

	const maxAckBatch = 1000
	for start := 0; start < len(streamIDs); start += maxAckBatch {
		end := start + maxAckBatch
		if end > len(streamIDs) {
			end = len(streamIDs)
		}

		if err := cs.client.XAck(ctx, cs.streamName, consumerGroup, streamIDs[start:end]...).Err(); err != nil {
			return fmt.Errorf("failed to acknowledge entries by stream ID: %w", err)
		}
	}

	return nil
}

// Delete removes processed commitments (not typically needed with streams)
func (cs *CommitmentStorage) Delete(ctx context.Context, requestIDs []api.RequestID) error {
	// In Redis streams, we typically don't delete individual messages
	// The periodic cleanup handles old messages
	return nil
}

// Cleanup removes all data for this storage instance (useful for testing)
func (cs *CommitmentStorage) Cleanup(ctx context.Context) error {
	// Delete the stream entirely
	err := cs.client.Del(ctx, cs.streamName).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	// Delete the consumer group (will be recreated on next Initialize)
	err = cs.client.XGroupDestroy(ctx, cs.streamName, consumerGroup).Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		// Ignore "NOGROUP" error - group doesn't exist
		if !strings.Contains(err.Error(), "NOGROUP") {
			return fmt.Errorf("failed to destroy consumer group: %w", err)
		}
	}

	return nil
}

// Count returns the total number of commitments in the stream
func (cs *CommitmentStorage) Count(ctx context.Context) (int64, error) {
	info := cs.client.XInfoStream(ctx, cs.streamName)
	if err := info.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get stream info: %w", err)
	}
	return info.Val().Length, nil
}

// CountUnprocessed returns the number of unprocessed commitments
func (cs *CommitmentStorage) CountUnprocessed(ctx context.Context) (int64, error) {
	// Get pending count for the entire consumer group - this represents unprocessed items
	pendingInfo := cs.client.XPending(ctx, cs.streamName, consumerGroup)
	if err := pendingInfo.Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		if isNoGroupError(err) {
			if ensureErr := cs.ensureConsumerGroup(ctx); ensureErr != nil {
				return 0, fmt.Errorf("failed to ensure consumer group: %w", ensureErr)
			}
			pendingInfo = cs.client.XPending(ctx, cs.streamName, consumerGroup)
			if err := pendingInfo.Err(); err != nil {
				if errors.Is(err, redis.Nil) {
					return 0, nil
				}
				return 0, fmt.Errorf("failed to get pending count after recreating group: %w", err)
			}
		} else {
			return 0, fmt.Errorf("failed to get pending count: %w", err)
		}
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
	trimmed := cs.client.XTrimMaxLen(ctx, cs.streamName, cs.batchConfig.MaxStreamLength).Val()

	if trimmed > 0 {
		countAfter := countBefore - trimmed
		cs.logger.WithContext(ctx).Info("Redis stream trimmed",
			"stream", cs.streamName,
			"trimmed", trimmed,
			"before", countBefore,
			"after", countAfter,
			"maxLength", cs.batchConfig.MaxStreamLength)
	}
}

// StreamCommitments continuously streams commitments using blocking Redis reads
// This streams commitments directly to the provided channel as they arrive
func (cs *CommitmentStorage) StreamCommitments(ctx context.Context, commitmentChan chan<- *models.Commitment) error {
	windowStart := time.Now()
	countThisWindow := 0

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
					Streams:  []string{cs.streamName, "0"}, // "0" = read pending for this consumer
					Count:    1000,
					Block:    0, // Don't block
				})

				if err := pendingStreams.Err(); err != nil && !errors.Is(err, redis.Nil) {
					if isNoGroupError(err) {
						if ensureErr := cs.ensureConsumerGroup(ctx); ensureErr != nil {
							return fmt.Errorf("failed to ensure consumer group: %w", ensureErr)
						}
						continue
					}
					return fmt.Errorf("failed to check pending messages: %w", err)
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
					Streams:  []string{cs.streamName, ">"}, // ">" = new messages only
					Count:    1000,
					Block:    100 * time.Millisecond,
				})
			}

			if err := streams.Err(); err != nil {
				if errors.Is(err, redis.Nil) {
					// No new messages, continue (shouldn't happen with block=0, but just in case)
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err // Context cancelled, exit gracefully
				}
				if isNoGroupError(err) {
					if ensureErr := cs.ensureConsumerGroup(ctx); ensureErr != nil {
						return fmt.Errorf("failed to ensure consumer group: %w", ensureErr)
					}
					continue
				}
				return fmt.Errorf("failed to read from stream: %w", err)
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

					// Attach stream ID for precise acknowledgement support
					commitment.StreamID = message.ID

					// Stream commitment directly to channel
					select {
					case commitmentChan <- commitment:
						countThisWindow++
						elapsed := time.Since(windowStart)
						if elapsed >= time.Second {
							rate := float64(countThisWindow) / elapsed.Seconds()
							cs.logger.Info("PERF: Redis stream throughput",
								"serverID", cs.serverID,
								"ratePerSec", fmt.Sprintf("%.0f", rate),
								"windowMs", elapsed.Milliseconds())
							windowStart = time.Now()
							countThisWindow = 0
						}
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
