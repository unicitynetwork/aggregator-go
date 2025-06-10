package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

const leadershipCollection = "leadership"

// LeadershipStorage implements leadership storage for MongoDB
type LeadershipStorage struct {
	collection *mongo.Collection
}

// NewLeadershipStorage creates a new leadership storage instance
func NewLeadershipStorage(db *mongo.Database) *LeadershipStorage {
	return &LeadershipStorage{
		collection: db.Collection(leadershipCollection),
	}
}

// AcquireLock attempts to acquire the leadership lock
func (ls *LeadershipStorage) AcquireLock(ctx context.Context, serverID string, ttlSeconds int) (bool, error) {
	lock := models.NewLeadershipLock(serverID, ttlSeconds)

	// Try to insert the lock document
	_, err := ls.collection.InsertOne(ctx, lock)
	if err != nil {
		// If document already exists, try to update if expired
		if mongo.IsDuplicateKeyError(err) {
			return ls.tryUpdateExpiredLock(ctx, serverID, ttlSeconds)
		}
		return false, fmt.Errorf("failed to acquire leadership lock: %w", err)
	}

	return true, nil
}

// tryUpdateExpiredLock attempts to update an expired lock
func (ls *LeadershipStorage) tryUpdateExpiredLock(ctx context.Context, serverID string, ttlSeconds int) (bool, error) {
	now := time.Now()

	// Filter for expired locks
	filter := bson.M{
		"_id":       "leadership",
		"expiresAt": bson.M{"$lt": api.NewTimestamp(now)},
	}

	lock := models.NewLeadershipLock(serverID, ttlSeconds)
	update := bson.M{"$set": lock}

	result, err := ls.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, fmt.Errorf("failed to update expired lock: %w", err)
	}

	return result.ModifiedCount > 0, nil
}

// RenewLock renews the leadership lock
func (ls *LeadershipStorage) RenewLock(ctx context.Context, serverID string, ttlSeconds int) error {
	now := time.Now()
	expiresAt := now.Add(time.Second * time.Duration(ttlSeconds))

	filter := bson.M{
		"_id":      "leadership",
		"serverId": serverID,
	}

	update := bson.M{
		"$set": bson.M{
			"expiresAt": api.NewTimestamp(expiresAt),
			"updatedAt": api.NewTimestamp(now),
		},
	}

	result, err := ls.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to renew leadership lock: %w", err)
	}

	if result.ModifiedCount == 0 {
		return fmt.Errorf("failed to renew lock: server is not the current leader")
	}

	return nil
}

// ReleaseLock releases the leadership lock
func (ls *LeadershipStorage) ReleaseLock(ctx context.Context, serverID string) error {
	filter := bson.M{
		"_id":      "leadership",
		"serverId": serverID,
	}

	_, err := ls.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to release leadership lock: %w", err)
	}

	return nil
}

// GetCurrentLeader retrieves the current leader information
func (ls *LeadershipStorage) GetCurrentLeader(ctx context.Context) (*models.LeadershipLock, error) {
	var lock models.LeadershipLock
	err := ls.collection.FindOne(ctx, bson.M{"_id": "leadership"}).Decode(&lock)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get current leader: %w", err)
	}

	// Check if lock is expired
	if lock.IsExpired() {
		// Clean up expired lock
		_, _ = ls.collection.DeleteOne(ctx, bson.M{"_id": "leadership"})
		return nil, nil
	}

	return &lock, nil
}

// IsLeader checks if the given server is the current leader
func (ls *LeadershipStorage) IsLeader(ctx context.Context, serverID string) (bool, error) {
	leader, err := ls.GetCurrentLeader(ctx)
	if err != nil {
		return false, err
	}

	if leader == nil {
		return false, nil
	}

	return leader.ServerID == serverID, nil
}

// CreateIndexes creates necessary indexes for the leadership collection
func (ls *LeadershipStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "serverId", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "expiresAt", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "updatedAt", Value: -1}},
		},
	}

	_, err := ls.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}

	return nil
}
