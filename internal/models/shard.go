package models

import (
	"fmt"
	"math/big"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ShardRootUpdate represents an incoming shard root submission from a child aggregator.
type ShardRootUpdate struct {
	ShardID   api.ShardID
	RootHash  api.HexBytes // Raw root hash from child SMT
	Timestamp time.Time
}

// NewShardRootUpdate creates a new shard root update
func NewShardRootUpdate(shardID api.ShardID, rootHash api.HexBytes) *ShardRootUpdate {
	return &ShardRootUpdate{
		ShardID:   shardID,
		RootHash:  rootHash,
		Timestamp: time.Now(),
	}
}

// GetPath returns the shard ID as a big.Int for SMT operations
func (sru *ShardRootUpdate) GetPath() *big.Int {
	return new(big.Int).SetInt64(int64(sru.ShardID))
}

// Validate validates the shard root update
func (sru *ShardRootUpdate) Validate() error {
	if sru.ShardID <= 1 {
		return fmt.Errorf("shard ID must be positive and have at least 2 bits")
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
