package models

import (
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ShardRootUpdate represents an incoming shard root submission from a child aggregator.
type ShardRootUpdate struct {
	ShardID   int
	RootHash  api.HexBytes
	Timestamp time.Time
}

// NewShardRootUpdate creates a new shard root update
func NewShardRootUpdate(shardID int, rootHash api.HexBytes) *ShardRootUpdate {
	return &ShardRootUpdate{
		ShardID:   shardID,
		RootHash:  rootHash,
		Timestamp: time.Now(),
	}
}

// Validate validates the shard root update
func (sru *ShardRootUpdate) Validate() error {
	if sru.ShardID < 0 {
		return fmt.Errorf("shard ID cannot be negative")
	}

	if len(sru.RootHash) == 0 {
		return fmt.Errorf("root hash cannot be empty")
	}

	return nil
}

// String returns a string representation of the shard root update
func (sru *ShardRootUpdate) String() string {
	return fmt.Sprintf("ShardRootUpdate{ShardID: %d, RootHash: %s, Timestamp: %v}",
		sru.ShardID, sru.RootHash.String(), sru.Timestamp)
}
