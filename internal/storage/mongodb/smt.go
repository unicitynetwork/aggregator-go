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

const smtCollection = "smt_nodes"

// SmtStorage implements SMT storage for MongoDB
type SmtStorage struct {
	collection *mongo.Collection
}

// NewSmtStorage creates a new SMT storage instance
func NewSmtStorage(db *mongo.Database) *SmtStorage {
	return &SmtStorage{
		collection: db.Collection(smtCollection),
	}
}

// Store stores a new SMT node using upsert to handle duplicates gracefully
func (ss *SmtStorage) Store(ctx context.Context, node *models.SmtNode) error {
	filter := bson.M{"key": node.Key}
	update := bson.M{
		"$setOnInsert": bson.M{
			"key":       node.Key,
			"value":     node.Value,
			"createdAt": node.CreatedAt,
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := ss.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to store SMT node: %w", err)
	}
	return nil
}

// StoreBatch stores multiple SMT nodes using insert operations, skipping duplicates
func (ss *SmtStorage) StoreBatch(ctx context.Context, nodes []*models.SmtNode) error {
	if len(nodes) == 0 {
		return nil
	}

	documents := make([]interface{}, len(nodes))
	for i, node := range nodes {
		documents[i] = bson.M{
			"key":       node.Key,
			"value":     node.Value,
			"createdAt": node.CreatedAt,
		}
	}

	opts := options.InsertMany().SetOrdered(false)
	_, err := ss.collection.InsertMany(ctx, documents, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("failed to store SMT nodes batch: %w", err)
	}
	return nil
}

// GetByKey retrieves an SMT node by key
func (ss *SmtStorage) GetByKey(ctx context.Context, key api.HexBytes) (*models.SmtNode, error) {
	var node models.SmtNode
	err := ss.collection.FindOne(ctx, bson.M{"key": key}).Decode(&node)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get SMT node by key: %w", err)
	}
	return &node, nil
}

// Delete removes an SMT node
func (ss *SmtStorage) Delete(ctx context.Context, key api.HexBytes) error {
	_, err := ss.collection.DeleteOne(ctx, bson.M{"key": key})
	if err != nil {
		return fmt.Errorf("failed to delete SMT node: %w", err)
	}
	return nil
}

// DeleteBatch removes multiple SMT nodes
func (ss *SmtStorage) DeleteBatch(ctx context.Context, keys []api.HexBytes) error {
	if len(keys) == 0 {
		return nil
	}

	filter := bson.M{"key": bson.M{"$in": keys}}
	_, err := ss.collection.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete SMT nodes batch: %w", err)
	}

	return nil
}

// Count returns the total number of nodes
func (ss *SmtStorage) Count(ctx context.Context) (int64, error) {
	count, err := ss.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count SMT nodes: %w", err)
	}
	return count, nil
}

// GetAll retrieves all SMT nodes (use with caution)
func (ss *SmtStorage) GetAll(ctx context.Context) ([]*models.SmtNode, error) {
	cursor, err := ss.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to find all SMT nodes: %w", err)
	}
	defer cursor.Close(ctx)

	var nodes []*models.SmtNode
	for cursor.Next(ctx) {
		var node models.SmtNode
		if err := cursor.Decode(&node); err != nil {
			return nil, fmt.Errorf("failed to decode SMT node: %w", err)
		}
		nodes = append(nodes, &node)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return nodes, nil
}

// GetChunked retrieves SMT nodes in chunks for efficient loading
func (ss *SmtStorage) GetChunked(ctx context.Context, offset, limit int) ([]*models.SmtNode, error) {
	opts := options.Find().
		SetSkip(int64(offset)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "_id", Value: 1}}) // Deterministic ordering by insertion order

	cursor, err := ss.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find SMT nodes chunk: %w", err)
	}
	defer cursor.Close(ctx)

	var nodes []*models.SmtNode
	for cursor.Next(ctx) {
		var node models.SmtNode
		if err := cursor.Decode(&node); err != nil {
			return nil, fmt.Errorf("failed to decode SMT node: %w", err)
		}
		nodes = append(nodes, &node)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return nodes, nil
}

// CreateIndexes creates necessary indexes for the SMT collection
func (ss *SmtStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "key", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "createdAt", Value: -1}},
		},
	}

	_, err := ss.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create SMT indexes: %w", err)
	}

	return nil
}
