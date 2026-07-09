package api

import (
	"encoding/hex"
	"fmt"
	"math/bits"
)

// MatchesShardPrefix checks whether the big-endian key bits of keyBytes match
// the shard prefix defined by shardBitmask. The bitmask encodes a
// sentinel-prefixed shard ID (e.g. 0b100 = shard 0 in a 2-bit tree): bit d of
// the mask is the routing decision at depth d, a path/shard-ID convention that
// is independent of the key's byte layout. The key side is read big-endian
// (bit d = MSB-first bit d), so under the yellowpaper big-endian layout a shard
// selects a contiguous lexicographic key range. keyBytes must be at least
// ceil(shardDepth/8) bytes long.
func MatchesShardPrefix(keyBytes []byte, shardBitmask int) (bool, error) {
	shardDepth := bits.Len(uint(shardBitmask)) - 1
	if shardDepth < 0 {
		return false, fmt.Errorf("invalid shard bitmask: %d", shardBitmask)
	}
	if len(keyBytes) < (shardDepth+7)/8 {
		return false, fmt.Errorf("key too short for shard depth %d: got %d bytes", shardDepth, len(keyBytes))
	}

	for d := 0; d < shardDepth; d++ {
		expected := byte((uint(shardBitmask) >> uint(d)) & 1)
		actual := KeyBitBE(keyBytes, d)
		if actual != expected {
			return false, nil
		}
	}
	return true, nil
}

// MatchesShardPrefixFromHex decodes a hex-encoded 32-byte state key and
// applies MatchesShardPrefix.
func MatchesShardPrefixFromHex(keyHex string, shardBitmask int) (bool, error) {
	keyBytes, err := hex.DecodeString(keyHex)
	if err != nil {
		return false, fmt.Errorf("failed to decode state key: %w", err)
	}

	if len(keyBytes) != StateTreeKeyLengthBytes {
		return false, fmt.Errorf(
			"state key must be exactly %d bytes, got %d",
			StateTreeKeyLengthBytes, len(keyBytes),
		)
	}

	return MatchesShardPrefix(keyBytes, shardBitmask)
}
