package signing

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// shardIDFromHex constructs a ShardID from its trailing-1 end-marker hex form.
func shardIDFromHex(t *testing.T, h string) types.ShardID {
	t.Helper()
	var id types.ShardID
	require.NoError(t, id.UnmarshalText([]byte(h)))
	return id
}

// stateIDWithMSB returns a 32-byte state ID whose first byte holds topBits in
// its top topBitCount bits (remaining bits zeroed).
func stateIDWithMSB(topBits byte, topBitCount int) api.StateID {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	mask := byte(0xFF << (8 - topBitCount))
	key[0] = (topBits << (8 - topBitCount)) & mask
	return api.StateID(key)
}

func newBFTShardValidator(shardID types.ShardID) *CertificationRequestValidator {
	return NewCertificationRequestValidator(
		config.ShardingConfig{Mode: config.ShardingModeBFTShard},
		shardID,
	)
}

func TestValidateShardID_BFTShard_SingleShardPartition(t *testing.T) {
	// Length-0 shard ID (0x80) = single-shard partition; every state ID is admitted.
	v := newBFTShardValidator(shardIDFromHex(t, "0x80"))

	for _, top := range []byte{0x00, 0x7F, 0x80, 0xFF} {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		key[0] = top
		require.NoError(t, v.ValidateShardID(api.StateID(key)), "top byte %02x", top)
	}
}

func TestValidateShardID_BFTShard_OneBitSplit(t *testing.T) {
	shard0 := newBFTShardValidator(shardIDFromHex(t, "0x40")) // bits "0"
	shard1 := newBFTShardValidator(shardIDFromHex(t, "0xC0")) // bits "1"

	tests := []struct {
		name        string
		msb         byte
		acceptedBy0 bool
	}{
		{"key MSB=0", 0, true},
		{"key MSB=1", 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sid := stateIDWithMSB(tc.msb, 1)

			err := shard0.ValidateShardID(sid)
			if tc.acceptedBy0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "does not match configured BFT shard prefix")
			}

			err = shard1.ValidateShardID(sid)
			if !tc.acceptedBy0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestValidateShardID_BFTShard_SubNibblePrefix(t *testing.T) {
	// 3-bit shard "101" (0xB0). Only keys whose top 3 bits are 101 are admitted.
	v := newBFTShardValidator(shardIDFromHex(t, "0xB0"))

	for prefix := byte(0); prefix < 8; prefix++ {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		key[0] = prefix << 5
		err := v.ValidateShardID(api.StateID(key))
		if prefix == 0b101 {
			require.NoErrorf(t, err, "3-bit prefix %03b", prefix)
		} else {
			require.Errorf(t, err, "3-bit prefix %03b", prefix)
		}
	}
}

func TestValidateShardID_BFTShard_WrongStateIDLength(t *testing.T) {
	v := newBFTShardValidator(shardIDFromHex(t, "0x40"))

	short := make([]byte, api.StateTreeKeyLengthBytes-1)
	err := v.ValidateShardID(api.StateID(short))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state ID")
}

// Only bft-shard mode consults bftShardID; other modes ignore it.
func TestValidateShardID_ModeDispatch_OnlyBFTShardConsultsBFTShardID(t *testing.T) {
	modes := []config.ShardingMode{
		config.ShardingModeStandalone,
		config.ShardingModeParent,
	}
	for _, m := range modes {
		t.Run(m.String(), func(t *testing.T) {
			v := NewCertificationRequestValidator(
				config.ShardingConfig{Mode: m},
				shardIDFromHex(t, "0x40"),
			)
			key := make([]byte, api.StateTreeKeyLengthBytes)
			key[0] = 0xFF // would be rejected by shard "0" in bft-shard mode
			require.NoError(t, v.ValidateShardID(api.StateID(key)))
		})
	}
}

// Pins the hex ↔ bit-string mapping used throughout these tests.
func TestShardIDHexEncoding_Documentation(t *testing.T) {
	cases := []struct {
		hex  string
		bits string
	}{
		{"0x80", ""},
		{"0x40", "0"},
		{"0xC0", "1"},
		{"0xB0", "101"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s=%q", tc.hex, tc.bits), func(t *testing.T) {
			id := shardIDFromHex(t, tc.hex)
			require.Equal(t, uint(len(tc.bits)), id.Length())
			require.Equal(t, tc.bits, id.String())
			out, err := id.MarshalText()
			require.NoError(t, err)
			require.Equal(t, strings.ToLower(tc.hex), "0x"+hex.EncodeToString(textHexFromMarshal(out)))
		})
	}
}

func textHexFromMarshal(b []byte) []byte {
	s := strings.TrimPrefix(string(b), "0x")
	out, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return out
}
