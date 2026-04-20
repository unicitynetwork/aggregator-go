package bft

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// shardFromHex builds a ShardID from its trailing-1 end-marker hex form.
func shardFromHex(t *testing.T, h string) types.ShardID {
	t.Helper()
	var id types.ShardID
	require.NoError(t, id.UnmarshalText([]byte(h)))
	return id
}

func TestValidateUCShardBinding_Accepts_MatchingShard(t *testing.T) {
	uc := &types.UnicityCertificate{
		ShardTreeCertificate: types.ShardTreeCertificate{Shard: types.ShardID{}},
	}
	require.NoError(t, validateUCShardBinding(uc, types.ShardID{}))

	expected := shardFromHex(t, "0x40")
	uc = &types.UnicityCertificate{
		ShardTreeCertificate: types.ShardTreeCertificate{Shard: expected},
	}
	require.NoError(t, validateUCShardBinding(uc, expected))
}

func TestValidateUCShardBinding_Rejects_DifferentShard(t *testing.T) {
	shard0 := shardFromHex(t, "0x40")
	shard1 := shardFromHex(t, "0xC0")

	uc := &types.UnicityCertificate{
		ShardTreeCertificate: types.ShardTreeCertificate{Shard: shard1},
	}
	err := validateUCShardBinding(uc, shard0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "UC.ShardTreeCertificate.Shard")
	require.Contains(t, err.Error(), "does not match configured shard")
}

// Equality is length-aware: prefix matches with different lengths are rejected.
func TestValidateUCShardBinding_Rejects_DifferentLength(t *testing.T) {
	short := shardFromHex(t, "0xC0") // length 1, "1"
	long := shardFromHex(t, "0xE0")  // length 2, "11"

	uc := &types.UnicityCertificate{
		ShardTreeCertificate: types.ShardTreeCertificate{Shard: long},
	}
	require.Error(t, validateUCShardBinding(uc, short))
}

func TestValidateUCShardBinding_NilUC(t *testing.T) {
	err := validateUCShardBinding(nil, types.ShardID{})
	require.Error(t, err)
	require.Equal(t, "nil unicity certificate", err.Error())
}
