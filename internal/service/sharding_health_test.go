package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
)

// shardFromHex builds a ShardID from its trailing-1 end-marker hex form.
// 0x80 → empty, 0x40 → "0", 0xC0 → "1", 0xB0 → "101".
func shardFromHex(t *testing.T, h string) bfttypes.ShardID {
	t.Helper()
	var id bfttypes.ShardID
	require.NoError(t, id.UnmarshalText([]byte(h)))
	return id
}

func TestBuildShardingHealth_Standalone(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeStandalone,
			ShardIDLength: 4,
		},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "standalone", s.Mode)
	require.Equal(t, 4, s.ShardIDLen)
	require.Equal(t, 0, s.ShardID)
	require.Empty(t, s.BFTShardID)
}

func TestBuildShardingHealth_Child(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeChild,
			ShardIDLength: 8,
			Child:         config.ChildConfig{ShardID: 0b101},
		},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "child", s.Mode)
	require.Equal(t, 8, s.ShardIDLen)
	require.Equal(t, 0b101, s.ShardID)
	require.Empty(t, s.BFTShardID)
}

func TestBuildShardingHealth_Parent(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeParent,
			ShardIDLength: 8,
		},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "parent", s.Mode)
	require.Equal(t, 8, s.ShardIDLen)
	require.Equal(t, 0, s.ShardID)
	require.Empty(t, s.BFTShardID)
}

func TestBuildShardingHealth_BFTShard_PopulatesBitString(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeBFTShard,
			ShardIDLength: 4,
			Child:         config.ChildConfig{ShardID: 7},
		},
		BFT: config.BFTConfig{
			ShardConf: &bfttypes.PartitionDescriptionRecord{
				ShardID: shardFromHex(t, "0xB0"), // "101"
			},
		},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "bft-shard", s.Mode)
	require.Equal(t, "101", s.BFTShardID)
	// Integer shard fields must not leak through in bft-shard mode.
	require.Equal(t, 0, s.ShardIDLen)
	require.Equal(t, 0, s.ShardID)
}

func TestBuildShardingHealth_BFTShard_NilShardConf(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeBFTShard,
		},
		BFT: config.BFTConfig{ShardConf: nil},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "bft-shard", s.Mode)
	require.Empty(t, s.BFTShardID)
	require.Equal(t, 0, s.ShardIDLen)
	require.Equal(t, 0, s.ShardID)
}

func TestBuildShardingHealth_BFTShard_EmptyShardID(t *testing.T) {
	cfg := &config.Config{
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeBFTShard,
		},
		BFT: config.BFTConfig{
			ShardConf: &bfttypes.PartitionDescriptionRecord{
				ShardID: bfttypes.ShardID{},
			},
		},
	}
	s := buildShardingHealth(cfg)
	require.Equal(t, "bft-shard", s.Mode)
	require.Equal(t, "", s.BFTShardID)
}
