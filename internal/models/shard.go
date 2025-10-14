package models

import (
	"fmt"
	"math/big"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ShardRootUpdate represents an incoming shard root submission from a child aggregator.
type ShardRootUpdate struct {
	ShardID   api.HexBytes // Shard identifier with 0x01 prefix (e.g., "0104" for shard 4)
	RootHash  api.HexBytes // Raw root hash from child SMT
	Timestamp time.Time
}

// NewShardRootUpdate creates a new shard root update
func NewShardRootUpdate(shardID api.HexBytes, rootHash api.HexBytes) *ShardRootUpdate {
	return &ShardRootUpdate{
		ShardID:   shardID,
		RootHash:  rootHash,
		Timestamp: time.Now(),
	}
}

// GetPath returns the shard ID as a big.Int for SMT operations
func (sru *ShardRootUpdate) GetPath() *big.Int {
	return new(big.Int).SetBytes(sru.ShardID)
}

// Validate validates the shard root update
func (sru *ShardRootUpdate) Validate() error {
	if len(sru.ShardID) == 0 {
		return fmt.Errorf("shard ID cannot be empty")
	}

	if sru.ShardID[0] != 0x01 {
		return fmt.Errorf("shard ID must have 0x01 prefix")
	}

	if len(sru.RootHash) == 0 {
		return fmt.Errorf("root hash cannot be empty")
	}

	return nil
}

// String returns a string representation of the shard root update
func (sru *ShardRootUpdate) String() string {
	return fmt.Sprintf("ShardRootUpdate{ShardID: %s, RootHash: %s, Timestamp: %v}",
		sru.ShardID.String(), sru.RootHash.String(), sru.Timestamp)
}
