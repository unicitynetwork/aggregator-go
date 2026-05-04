package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/unicitynetwork/aggregator-go/internal/config"
)

// TestCreateRedisCommitmentQueue_SentinelRequiresMasterName verifies that the
// factory rejects a configuration which sets REDIS_SENTINEL_ADDRS but leaves
// REDIS_MASTER_NAME empty. The validation must trigger before any network I/O,
// so this can run as a pure unit test (no Redis/MongoDB required).
func TestCreateRedisCommitmentQueue_SentinelRequiresMasterName(t *testing.T) {
	cfg := &config.Config{
		Redis: config.RedisConfig{
			SentinelAddrs: []string{"sentinel-1:26379", "sentinel-2:26379"},
			MasterName:    "", // intentionally empty
		},
	}

	q, err := createRedisCommitmentQueue(context.Background(), cfg, nil)
	if err == nil {
		t.Fatalf("expected error when SentinelAddrs is set but MasterName is empty, got queue=%v", q)
	}
	if q != nil {
		t.Errorf("expected nil queue on validation error, got %v", q)
	}
	if !strings.Contains(err.Error(), "REDIS_MASTER_NAME") {
		t.Errorf("expected error to mention REDIS_MASTER_NAME, got: %v", err)
	}
	if !strings.Contains(err.Error(), "REDIS_SENTINEL_ADDRS") {
		t.Errorf("expected error to mention REDIS_SENTINEL_ADDRS, got: %v", err)
	}
}

// TestCreateRedisCommitmentQueue_MasterNameWithoutSentinelAddrs guards the
// reverse misconfiguration: REDIS_MASTER_NAME set but REDIS_SENTINEL_ADDRS
// empty. Without this check the factory would silently fall back to
// single-endpoint mode and ignore the master name — a real footgun for
// operators who intended to enable Sentinel.
func TestCreateRedisCommitmentQueue_MasterNameWithoutSentinelAddrs(t *testing.T) {
	cfg := &config.Config{
		Redis: config.RedisConfig{
			Host:          "localhost",
			Port:          6379,
			SentinelAddrs: nil, // intentionally empty
			MasterName:    "mymaster",
		},
	}

	q, err := createRedisCommitmentQueue(context.Background(), cfg, nil)
	if err == nil {
		t.Fatalf("expected error when MasterName is set but SentinelAddrs is empty, got queue=%v", q)
	}
	if q != nil {
		t.Errorf("expected nil queue on validation error, got %v", q)
	}
	if !strings.Contains(err.Error(), "REDIS_MASTER_NAME") || !strings.Contains(err.Error(), "REDIS_SENTINEL_ADDRS") {
		t.Errorf("expected error to mention both REDIS_MASTER_NAME and REDIS_SENTINEL_ADDRS, got: %v", err)
	}
}
