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

// CreateTestRedisContainer starts a Redis container for testing
// Returns the container and a cleanup function
func CreateTestRedisContainer(ctx context.Context, t *testing.T) (testcontainers.Container, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
		Cmd: []string{
			"redis-server",
			"--save", "", // Disable RDB persistence for tests
			"--appendonly", "no", // Disable AOF persistence for tests
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start Redis container")

	cleanup := func() {
		if container != nil {
			container.Terminate(ctx)
		}
	}

	return container, cleanup
}

// CreateRedisClient creates a Redis client connected to the given container
func CreateRedisClient(ctx context.Context, container testcontainers.Container, t *testing.T) *redis.Client {
	// Get container host and port
	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "6379")
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

	return client
}
