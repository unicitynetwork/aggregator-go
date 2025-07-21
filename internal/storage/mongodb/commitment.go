package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
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
	_, err := cs.collection.InsertOne(ctx, commitment)
	if err != nil {
		return fmt.Errorf("failed to store commitment: %w", err)
	}
	return nil
}

// GetByRequestID retrieves a commitment by request ID
func (cs *CommitmentStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error) {
	var commitment models.Commitment
	err := cs.collection.FindOne(ctx, bson.M{"requestId": requestID}).Decode(&commitment)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get commitment by request ID: %w", err)
	}
	// Handle backward compatibility: default to 1 if AggregateRequestCount is 0
	if commitment.AggregateRequestCount == 0 {
		commitment.AggregateRequestCount = 1
	}
	return &commitment, nil
}

// GetUnprocessedBatch retrieves a batch of unprocessed commitments
func (cs *CommitmentStorage) GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error) {
	filter := bson.M{"processedAt": bson.M{"$exists": false}}
	opts := options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.M{"createdAt": 1}) // Oldest first

	cursor, err := cs.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find unprocessed commitments: %w", err)
	}
	defer cursor.Close(ctx)

	var commitments []*models.Commitment
	for cursor.Next(ctx) {
		var commitment models.Commitment
		if err := cursor.Decode(&commitment); err != nil {
			return nil, fmt.Errorf("failed to decode commitment: %w", err)
		}
		// Handle backward compatibility: default to 1 if AggregateRequestCount is 0
		if commitment.AggregateRequestCount == 0 {
			commitment.AggregateRequestCount = 1
		}
		commitments = append(commitments, &commitment)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return commitments, nil
}

// MarkProcessed marks commitments as processed
func (cs *CommitmentStorage) MarkProcessed(ctx context.Context, requestIDs []api.RequestID) error {
	if len(requestIDs) == 0 {
		return nil
	}

	filter := bson.M{"requestId": bson.M{"$in": requestIDs}}
	update := bson.M{"$set": bson.M{"processedAt": api.Now()}}

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
