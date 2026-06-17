package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDBName  = "test_aggregator_db"
	testTimeout = 30 * time.Second
)

func setupTestDB(t *testing.T) *mongo.Database {
	t.Helper()
	ctx := t.Context()

	mongoContainer, err := mongodb.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container: %v", err)
	}

	mongoURI, err := mongoContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}

	db := client.Database(testDBName)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		if err := db.Drop(ctx); err != nil {
			t.Logf("Failed to drop test database: %v", err)
		}
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect MongoDB client: %v", err)
		}
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	})

	return db
}
