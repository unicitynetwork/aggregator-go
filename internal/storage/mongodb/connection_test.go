package mongodb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func setupTransactionTestDB(t *testing.T) (*mongo.Client, *mongo.Database, func()) {
	ctx := context.Background()

	container, err := mongoContainer.Run(ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	if err != nil {
		t.Skipf("Skipping test - cannot start MongoDB container: %v", err)
		return nil, nil, nil
	}

	mongoURI, err := container.ConnectionString(ctx)
	require.NoError(t, err)
	mongoURI += "&directConnection=true"

	clientOpts := options.Client().ApplyURI(mongoURI).SetConnectTimeout(10 * time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	require.NoError(t, err)
	require.NoError(t, client.Ping(ctx, nil))

	db := client.Database("test_connection")

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
		_ = container.Terminate(ctx)
	}

	return client, db, cleanup
}

// TestWithTransaction_Success tests that a successful transaction commits
func TestWithTransaction_Success(t *testing.T) {
	client, db, cleanup := setupTransactionTestDB(t)
	if cleanup == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	coll := db.Collection("test_success")

	// Create a minimal Storage wrapper for testing
	storage := &Storage{client: client, database: db}

	// Execute transaction
	err := storage.WithTransaction(ctx, func(txCtx context.Context) error {
		_, err := coll.InsertOne(txCtx, bson.M{"key": "value1"})
		if err != nil {
			return err
		}
		_, err = coll.InsertOne(txCtx, bson.M{"key": "value2"})
		return err
	})

	require.NoError(t, err, "Transaction should succeed")

	// Verify both documents were committed
	count, err := coll.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "Both documents should be committed")
}

// TestWithTransaction_NonTransientError_FailsImmediately tests that non-transient errors fail without retry
func TestWithTransaction_NonTransientError_FailsImmediately(t *testing.T) {
	client, db, cleanup := setupTransactionTestDB(t)
	if cleanup == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	coll := db.Collection("test_non_transient")

	// Create unique index
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	// Insert a document that will cause duplicate
	_, err = coll.InsertOne(ctx, bson.M{"key": "duplicate"})
	require.NoError(t, err)

	storage := &Storage{client: client, database: db}

	var attempts int32
	err = storage.WithTransaction(ctx, func(txCtx context.Context) error {
		atomic.AddInt32(&attempts, 1)
		// Try to insert duplicate - this is NOT a transient error
		_, err := coll.InsertOne(txCtx, bson.M{"key": "duplicate"})
		return err
	})

	require.Error(t, err, "Transaction should fail due to duplicate key")
	require.Contains(t, err.Error(), "duplicate key")
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts), "Should only attempt once for non-transient error")
}

// TestWithTransaction_CallbackError_RollsBack tests that callback errors cause rollback
func TestWithTransaction_CallbackError_RollsBack(t *testing.T) {
	client, db, cleanup := setupTransactionTestDB(t)
	if cleanup == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	coll := db.Collection("test_rollback")

	storage := &Storage{client: client, database: db}

	// Execute transaction that inserts then fails
	err := storage.WithTransaction(ctx, func(txCtx context.Context) error {
		_, err := coll.InsertOne(txCtx, bson.M{"key": "should_rollback"})
		if err != nil {
			return err
		}
		// Return an error to trigger rollback
		return errors.New("intentional error")
	})

	require.Error(t, err, "Transaction should fail")
	require.Contains(t, err.Error(), "intentional error")

	// Verify the document was NOT committed (rolled back)
	count, err := coll.CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "Document should be rolled back")
}

// TestWithTransaction_TransientError_Retries tests that transient errors trigger retries
func TestWithTransaction_TransientError_Retries(t *testing.T) {
	client, db, cleanup := setupTransactionTestDB(t)
	if cleanup == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	coll := db.Collection("test_transient")

	// Insert a document that both transactions will try to modify
	_, err := coll.InsertOne(ctx, bson.M{"_id": "conflict", "value": 0})
	require.NoError(t, err)

	storage := &Storage{client: client, database: db}

	// Channels for coordination
	firstAttemptStarted := make(chan struct{})
	firstAttemptFailed := make(chan error, 1)
	blockingTxCommitted := make(chan struct{})

	// Start a blocking transaction that holds a write lock
	blockingSession, err := client.StartSession()
	require.NoError(t, err)
	defer blockingSession.EndSession(ctx)

	err = blockingSession.StartTransaction()
	require.NoError(t, err)

	// Modify the document in the blocking transaction (holds the lock)
	err = mongo.WithSession(ctx, blockingSession, func(sessCtx mongo.SessionContext) error {
		_, err := coll.UpdateOne(sessCtx, bson.M{"_id": "conflict"}, bson.M{"$inc": bson.M{"value": 1}})
		return err
	})
	require.NoError(t, err)

	// Now try our WithTransaction - it should get a write conflict on first attempt
	var attempts int32
	done := make(chan struct{})
	var txErr error

	go func() {
		defer close(done)
		txErr = storage.WithTransaction(ctx, func(txCtx context.Context) error {
			attemptNum := atomic.AddInt32(&attempts, 1)

			if attemptNum == 1 {
				close(firstAttemptStarted)
				// Try to update - this should fail with write conflict
				_, err := coll.UpdateOne(txCtx, bson.M{"_id": "conflict"}, bson.M{"$inc": bson.M{"value": 10}})
				if err != nil {
					firstAttemptFailed <- err
				}
				return err
			}

			// On retry, wait for blocking transaction to be committed
			<-blockingTxCommitted
			_, err := coll.UpdateOne(txCtx, bson.M{"_id": "conflict"}, bson.M{"$inc": bson.M{"value": 10}})
			return err
		})
	}()

	// Wait for first attempt to start
	select {
	case <-firstAttemptStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("First attempt didn't start")
	}

	// Wait for first attempt to fail (should be quick)
	select {
	case firstErr := <-firstAttemptFailed:
		t.Logf("First attempt failed with: %v", firstErr)
		require.Contains(t, firstErr.Error(), "WriteConflict", "Should get write conflict error")
	case <-time.After(5 * time.Second):
		t.Fatal("First attempt didn't fail with write conflict")
	}

	// Now commit the blocking transaction and signal retry can proceed
	err = blockingSession.CommitTransaction(ctx)
	require.NoError(t, err)
	close(blockingTxCommitted)

	// Wait for transaction to complete
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Transaction timed out")
	}

	finalAttempts := atomic.LoadInt32(&attempts)
	t.Logf("Transaction completed after %d attempts", finalAttempts)
	require.Greater(t, finalAttempts, int32(1), "Should have retried at least once")
	require.NoError(t, txErr, "Transaction should eventually succeed")

	// Verify final state
	var doc bson.M
	err = coll.FindOne(ctx, bson.M{"_id": "conflict"}).Decode(&doc)
	require.NoError(t, err)
	// value should be 0 + 1 (from blocking tx) + 10 (from our tx) = 11
	require.Equal(t, int32(11), doc["value"].(int32))
}

// TestWithTransaction_UpsertsDontAbort tests that our upsert approach works in transactions
func TestWithTransaction_UpsertsDontAbort(t *testing.T) {
	client, db, cleanup := setupTransactionTestDB(t)
	if cleanup == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()
	coll := db.Collection("test_upserts")

	// Create unique index
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	// Pre-insert a document
	_, err = coll.InsertOne(ctx, bson.M{"key": "existing", "value": "original"})
	require.NoError(t, err)

	storage := &Storage{client: client, database: db}

	// Transaction with upserts - should succeed even with "duplicate"
	err = storage.WithTransaction(ctx, func(txCtx context.Context) error {
		// Insert new document
		_, err := coll.InsertOne(txCtx, bson.M{"key": "new_doc", "value": "new"})
		if err != nil {
			return err
		}

		// Upsert that matches existing document - should NOT abort transaction
		writeModels := []mongo.WriteModel{
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"key": "existing"}).
				SetUpdate(bson.M{"$setOnInsert": bson.M{"key": "existing", "value": "wont_change"}}).
				SetUpsert(true),
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"key": "another_new"}).
				SetUpdate(bson.M{"$setOnInsert": bson.M{"key": "another_new", "value": "upserted"}}).
				SetUpsert(true),
		}
		_, err = coll.BulkWrite(txCtx, writeModels, options.BulkWrite().SetOrdered(false))
		return err
	})

	require.NoError(t, err, "Transaction with upserts should succeed")

	// Verify results
	var doc bson.M

	// new_doc should exist
	err = coll.FindOne(ctx, bson.M{"key": "new_doc"}).Decode(&doc)
	require.NoError(t, err)
	require.Equal(t, "new", doc["value"])

	// existing should keep original value
	err = coll.FindOne(ctx, bson.M{"key": "existing"}).Decode(&doc)
	require.NoError(t, err)
	require.Equal(t, "original", doc["value"])

	// another_new should be upserted
	err = coll.FindOne(ctx, bson.M{"key": "another_new"}).Decode(&doc)
	require.NoError(t, err)
	require.Equal(t, "upserted", doc["value"])
}
