package main

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestShardIDFromBitString pins the MSB-first bit string to ShardID hex
// encoding mapping.
func TestShardIDFromBitString(t *testing.T) {
	cases := []struct {
		bits    string
		wantHex string
	}{
		{"", "80"},
		{"0", "40"},
		{"1", "c0"},
		{"00", "20"},
		{"01", "60"},
		{"10", "a0"},
		{"11", "e0"},
		{"101", "b0"},
	}
	for _, tc := range cases {
		t.Run("bits="+tc.bits, func(t *testing.T) {
			sid, err := shardIDFromBitString(tc.bits)
			require.NoError(t, err)
			raw, err := sid.MarshalText()
			require.NoError(t, err)
			got := strings.TrimPrefix(strings.ToLower(string(raw)), "0x")
			require.Equal(t, tc.wantHex, got, "bit string %q encoded wrong", tc.bits)
		})
	}
}

// TestMatchesShardBits: the BFT-mode routing predicate admits keys whose
// prefix matches the shard and rejects the rest.
func TestMatchesShardBits(t *testing.T) {
	key0 := make([]byte, 32)
	key1 := make([]byte, 32)
	key1[0] = 0x80

	match, err := matchesShardBits(hex.EncodeToString(key0), "0")
	require.NoError(t, err)
	require.True(t, match)

	match, err = matchesShardBits(hex.EncodeToString(key0), "1")
	require.NoError(t, err)
	require.False(t, match)

	match, err = matchesShardBits(hex.EncodeToString(key1), "1")
	require.NoError(t, err)
	require.True(t, match)

	match, err = matchesShardBits(hex.EncodeToString(key1), "0")
	require.NoError(t, err)
	require.False(t, match)

	// Empty bits means single-shard partition; accept everything.
	match, err = matchesShardBits(hex.EncodeToString(key1), "")
	require.NoError(t, err)
	require.True(t, match)

	keyMatch := make([]byte, 32)
	keyMatch[0] = 0b10100000
	match, err = matchesShardBits(hex.EncodeToString(keyMatch), "101")
	require.NoError(t, err)
	require.True(t, match)

	keyMismatch := make([]byte, 32)
	keyMismatch[0] = 0b10010000
	match, err = matchesShardBits(hex.EncodeToString(keyMismatch), "101")
	require.NoError(t, err)
	require.False(t, match)
}
