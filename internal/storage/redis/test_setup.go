package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestRedis(t *testing.T) (*CommitmentStorage, func()) {
	ctx := context.Background()

	// Start Redis container
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
		Cmd: []string{
			"redis-server",
			"--save", "1", "1",
			"--appendonly", "yes",
			"--appendfsync", "everysec",
		},
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Could not start Redis container: %v", err)
	}

	// Get container host and port
	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", host, mappedPort.Port()),
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MaxRetries:   3,
	})

	// Test connection
	err = client.Ping(ctx).Err()
	require.NoError(t, err, "Failed to connect to Redis container")

	// Clean database
	err = client.FlushDB(ctx).Err()
	require.NoError(t, err)

	storage := NewCommitmentStorage(client, "test-server")
	require.NoError(t, storage.Initialize(ctx))

	cleanup := func() {
		storage.Close(ctx)
		client.Close()
		redisContainer.Terminate(ctx)
	}

	return storage, cleanup
}
