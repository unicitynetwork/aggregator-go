package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

const leadershipCollection = "leadership"

type LeadershipStorage struct {
	collection *mongo.Collection
	ttlSeconds int
}

func NewLeadershipStorage(db *mongo.Database, ttlSeconds int) *LeadershipStorage {
	collection := db.Collection(leadershipCollection)
	return &LeadershipStorage{
		collection: collection,
		ttlSeconds: ttlSeconds,
	}
}

// TryAcquireLock attempts to acquire the leadership lock,
// returns true if the lock was successfully acquired,
// returns false if the lock is already granted for this server.
func (ls *LeadershipStorage) TryAcquireLock(ctx context.Context, lockID, serverID string) (bool, error) {
	now := time.Now()
	expiredTime := now.Add(-time.Duration(ls.ttlSeconds) * time.Second)

	filter := bson.M{
		"lockId":        lockID,
		"lastHeartbeat": bson.M{"$lt": expiredTime},
	}
	update := bson.M{
		"$set": bson.M{
			"serverId":      serverID,
			"lastHeartbeat": now,
		},
	}
	opts := options.Update().SetUpsert(true)

	result, err := ls.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("error acquiring lock: %w", err)
	}

	// we became leader if we either updated an expired lock or inserted a new one
	return result.ModifiedCount > 0 || result.UpsertedCount > 0, nil
}

// UpdateHeartbeat updates the heartbeat timestamp to maintain leadership,
// returns true if the lock document was successfully updated, false otherwise.
func (ls *LeadershipStorage) UpdateHeartbeat(ctx context.Context, lockID, serverID string) (bool, error) {
	now := time.Now()

	filter := bson.M{"lockId": lockID, "serverId": serverID}
	update := bson.M{"$set": bson.M{"lastHeartbeat": now}}

	result := ls.collection.FindOneAndUpdate(ctx, filter, update)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, fmt.Errorf("error updating heartbeat: %w", result.Err())
	}
	return true, nil
}

// ReleaseLock releases the leadership lock, returns true if the lock was released, false otherwise.
func (ls *LeadershipStorage) ReleaseLock(ctx context.Context, lockID, serverID string) (bool, error) {
	res, err := ls.collection.DeleteOne(ctx, bson.M{"lockId": lockID, "serverId": serverID})
	if err != nil {
		return false, fmt.Errorf("error releasing lock: %w", err)
	}
	return res.DeletedCount > 0, nil
}

// IsLeader checks if the given server is the current leader.
func (ls *LeadershipStorage) IsLeader(ctx context.Context, lockID string, serverID string) (bool, error) {
	var lock models.LeadershipLock
	if err := ls.collection.FindOne(ctx, bson.M{"lockId": lockID}).Decode(&lock); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, fmt.Errorf("failed to find leadership lock with id %s: %w", lockID, err)
	}
	return lock.IsActive(serverID, ls.ttlSeconds), nil
}

// CreateIndexes creates necessary indexes for the leadership collection
func (ls *LeadershipStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "lockId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "serverId", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "lastHeartbeat", Value: 1}},
		},
	}

	_, err := ls.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}
	return nil
}
