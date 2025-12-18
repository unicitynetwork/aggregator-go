package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const commitmentCollection = "commitments"

// CommitmentStorage implements commitment storage for MongoDB
type CommitmentStorage struct {
	collection *mongo.Collection
}

// NewCommitmentStorage creates a new commitment storage instance
func NewCommitmentStorage(db *mongo.Database) *CommitmentStorage {
	return &CommitmentStorage{
		collection: db.Collection(commitmentCollection),
	}
}

// Store stores a new commitment
func (cs *CommitmentStorage) Store(ctx context.Context, commitment *models.Commitment) error {
	_, err := cs.collection.InsertOne(ctx, commitment.ToBSON())
	if err != nil {
		return fmt.Errorf("failed to store commitment: %w", err)
	}
	return nil
}

// GetByRequestID retrieves a commitment by request ID
func (cs *CommitmentStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error) {
	var commitmentBSON models.CommitmentBSON
	err := cs.collection.FindOne(ctx, bson.M{"requestId": requestID}).Decode(&commitmentBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get commitment by request ID: %w", err)
	}

	commitment, err := commitmentBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert commitment from BSON: %w", err)
	}

	// Handle backward compatibility: default to 1 if AggregateRequestCount is 0
	if commitment.AggregateRequestCount == 0 {
		commitment.AggregateRequestCount = 1
	}
	return commitment, nil
}

// GetUnprocessedBatch retrieves a batch of unprocessed commitments
func (cs *CommitmentStorage) GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error) {
	commitments, _, err := cs.GetUnprocessedBatchWithCursor(ctx, "", limit)
	return commitments, err
}

// GetUnprocessedBatchWithCursor retrieves a batch of unprocessed commitments with cursor-based pagination
func (cs *CommitmentStorage) GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.Commitment, string, error) {
	filter := bson.M{"processedAt": bson.M{"$exists": false}}

	// If we have a cursor, only get commitments after that ID
	if lastID != "" {
		objID, err := primitive.ObjectIDFromHex(lastID)
		if err == nil {
			filter["_id"] = bson.M{"$gt": objID}
		}
	}

	opts := options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.M{"_id": 1}) // Sort by ID for stable cursor

	cursor, err := cs.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, "", fmt.Errorf("failed to find unprocessed commitments: %w", err)
	}
	defer cursor.Close(ctx)

	var commitments []*models.Commitment
	var newCursor string

	for cursor.Next(ctx) {
		var commitmentBSON models.CommitmentBSON
		if err := cursor.Decode(&commitmentBSON); err != nil {
			return nil, "", fmt.Errorf("failed to decode commitment: %w", err)
		}
		commitment, err := commitmentBSON.FromBSON()
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert commitment from BSON: %w", err)
		}

		// Handle backward compatibility: default to 1 if AggregateRequestCount is 0
		if commitment.AggregateRequestCount == 0 {
			commitment.AggregateRequestCount = 1
		}
		commitments = append(commitments, commitment)
		// Update cursor to the last fetched ID
		if commitment.ID != primitive.NilObjectID {
			newCursor = commitment.ID.Hex()
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, "", fmt.Errorf("cursor error: %w", err)
	}

	return commitments, newCursor, nil
}

// MarkProcessed marks commitments as processed
func (cs *CommitmentStorage) MarkProcessed(ctx context.Context, entries []interfaces.CommitmentAck) error {
	if len(entries) == 0 {
		return nil
	}

	requestIDs := make([]api.RequestID, len(entries))
	for i, entry := range entries {
		requestIDs[i] = entry.RequestID
	}

	filter := bson.M{"requestId": bson.M{"$in": requestIDs}}
	update := bson.M{"$set": bson.M{"processedAt": time.Now()}}

	_, err := cs.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to mark commitments as processed: %w", err)
	}

	return nil
}

// Delete removes processed commitments
func (cs *CommitmentStorage) Delete(ctx context.Context, requestIDs []api.RequestID) error {
	if len(requestIDs) == 0 {
		return nil
	}

	filter := bson.M{"requestId": bson.M{"$in": requestIDs}}
	_, err := cs.collection.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete commitments: %w", err)
	}

	return nil
}

// Count returns the total number of commitments
func (cs *CommitmentStorage) Count(ctx context.Context) (int64, error) {
	count, err := cs.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count commitments: %w", err)
	}
	return count, nil
}

// CountUnprocessed returns the number of unprocessed commitments
func (cs *CommitmentStorage) CountUnprocessed(ctx context.Context) (int64, error) {
	filter := bson.M{"processedAt": bson.M{"$exists": false}}
	count, err := cs.collection.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to count unprocessed commitments: %w", err)
	}
	return count, nil
}

// GetAllPending retrieves all unprocessed commitments (for crash recovery)
func (cs *CommitmentStorage) GetAllPending(ctx context.Context) ([]*models.Commitment, error) {
	filter := bson.M{"processedAt": bson.M{"$exists": false}}
	return cs.findCommitments(ctx, filter)
}

// GetByRequestIDs retrieves commitments matching the given request IDs.
func (cs *CommitmentStorage) GetByRequestIDs(ctx context.Context, requestIDs []api.RequestID) (map[string]*models.Commitment, error) {
	if len(requestIDs) == 0 {
		return make(map[string]*models.Commitment), nil
	}

	// Convert to strings for query
	reqIDStrings := make([]string, len(requestIDs))
	for i, reqID := range requestIDs {
		reqIDStrings[i] = string(reqID)
	}

	filter := bson.M{"requestId": bson.M{"$in": reqIDStrings}}
	commitments, err := cs.findCommitments(ctx, filter)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*models.Commitment, len(commitments))
	for _, c := range commitments {
		result[string(c.RequestID)] = c
	}
	return result, nil
}

func (cs *CommitmentStorage) findCommitments(ctx context.Context, filter bson.M) ([]*models.Commitment, error) {
	cursor, err := cs.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get commitments: %w", err)
	}
	defer cursor.Close(ctx)

	var commitments []*models.Commitment
	for cursor.Next(ctx) {
		var commitmentBSON models.CommitmentBSON
		if err := cursor.Decode(&commitmentBSON); err != nil {
			return nil, fmt.Errorf("failed to decode commitment: %w", err)
		}
		commitment, err := commitmentBSON.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert from BSON: %w", err)
		}
		commitments = append(commitments, commitment)
	}
	return commitments, cursor.Err()
}

// Initialize initializes the commitment storage (no-op for MongoDB)
func (cs *CommitmentStorage) Initialize(ctx context.Context) error {
	return nil
}

// StreamCommitments is not supported for MongoDB - use GetUnprocessedBatchWithCursor instead
func (cs *CommitmentStorage) StreamCommitments(ctx context.Context, commitmentChan chan<- *models.Commitment) error {
	return fmt.Errorf("StreamCommitments not supported for MongoDB - use GetUnprocessedBatchWithCursor for polling")
}

// Close closes the commitment storage (no-op for MongoDB as connection is managed by Storage)
func (cs *CommitmentStorage) Close(ctx context.Context) error {
	return nil
}

// CreateIndexes creates necessary indexes for the commitment collection
func (cs *CommitmentStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "requestId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "createdAt", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "processedAt", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "processedAt", Value: 1},
				{Key: "createdAt", Value: 1},
			},
		},
	}

	_, err := cs.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create commitment indexes: %w", err)
	}

	return nil
}
