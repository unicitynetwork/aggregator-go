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

// CommitmentStorage implements certification request storage for MongoDB
type CommitmentStorage struct {
	collection *mongo.Collection
}

// NewCommitmentStorage creates a new certification request storage instance
func NewCommitmentStorage(db *mongo.Database) *CommitmentStorage {
	return &CommitmentStorage{
		collection: db.Collection(commitmentCollection),
	}
}

// Store stores a new commitment
func (cs *CommitmentStorage) Store(ctx context.Context, commitment *models.CertificationRequest) error {
	_, err := cs.collection.InsertOne(ctx, commitment.ToBSON())
	if err != nil {
		return fmt.Errorf("failed to store commitment: %w", err)
	}
	return nil
}

// GetByStateID retrieves a certification request by state ID
func (cs *CommitmentStorage) GetByStateID(ctx context.Context, stateID api.StateID) (*models.CertificationRequest, error) {
	var commitmentBSON models.CertificationRequestBSON
	err := cs.collection.FindOne(ctx, bson.M{"requestId": stateID}).Decode(&commitmentBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get certification request by state ID: %w", err)
	}

	commitment, err := commitmentBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert certification request from BSON: %w", err)
	}

	// Handle backward compatibility: default to 1 if AggregateRequestCount is 0
	if commitment.AggregateRequestCount == 0 {
		commitment.AggregateRequestCount = 1
	}
	return commitment, nil
}

// GetUnprocessedBatch retrieves a batch of unprocessed commitments
func (cs *CommitmentStorage) GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.CertificationRequest, error) {
	commitments, _, err := cs.GetUnprocessedBatchWithCursor(ctx, "", limit)
	return commitments, err
}

// GetUnprocessedBatchWithCursor retrieves a batch of unprocessed commitments with cursor-based pagination
func (cs *CommitmentStorage) GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.CertificationRequest, string, error) {
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

	var commitments []*models.CertificationRequest
	var newCursor string

	for cursor.Next(ctx) {
		var commitmentBSON models.CertificationRequestBSON
		if err := cursor.Decode(&commitmentBSON); err != nil {
			return nil, "", fmt.Errorf("failed to decode commitment: %w", err)
		}
		commitment, err := commitmentBSON.FromBSON()
		if err != nil {
			return nil, "", fmt.Errorf("failed to convert certification request from BSON: %w", err)
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
func (cs *CommitmentStorage) MarkProcessed(ctx context.Context, entries []interfaces.CertificationRequestAck) error {
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
func (cs *CommitmentStorage) Delete(ctx context.Context, stateIDs []api.StateID) error {
	if len(stateIDs) == 0 {
		return nil
	}

	filter := bson.M{"requestId": bson.M{"$in": stateIDs}}
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

// Initialize initializes the certification request storage (no-op for MongoDB)
func (cs *CommitmentStorage) Initialize(ctx context.Context) error {
	return nil
}

// StreamCertificationRequests is not supported for MongoDB - use GetUnprocessedBatchWithCursor instead
func (cs *CommitmentStorage) StreamCertificationRequests(ctx context.Context, commitmentChan chan<- *models.CertificationRequest) error {
	return fmt.Errorf("StreamCertificationRequests not supported for MongoDB - use GetUnprocessedBatchWithCursor for polling")
}

// Close closes the certification request storage (no-op for MongoDB as connection is managed by Storage)
func (cs *CommitmentStorage) Close(ctx context.Context) error {
	return nil
}

// CreateIndexes creates necessary indexes for the certification request collection
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
		return fmt.Errorf("failed to create certification request indexes: %w", err)
	}

	return nil
}
