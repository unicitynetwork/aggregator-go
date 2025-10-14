package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmitShardRootRequest_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name         string
		json         string
		wantShardID  []byte
		wantRootHash []byte
		wantErr      bool
	}{
		{
			name:         "valid JSON without 0x prefix",
			json:         `{"shardId":"0104","rootHash":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"}`,
			wantShardID:  []byte{0x01, 0x04},
			wantRootHash: []byte{0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34},
			wantErr:      false,
		},
		{
			name:         "valid JSON with 0x prefix (should be stripped by HexBytes)",
			json:         `{"shardId":"0x0104","rootHash":"0xabcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"}`,
			wantShardID:  []byte{0x01, 0x04},
			wantRootHash: []byte{0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34, 0xab, 0xcd, 0x12, 0x34},
			wantErr:      false,
		},
		{
			name:    "invalid hex in shardId",
			json:    `{"shardId":"ZZZZ","rootHash":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req SubmitShardRootRequest
			err := json.Unmarshal([]byte(tt.json), &req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantShardID, []byte(req.ShardID))
				assert.Equal(t, tt.wantRootHash, []byte(req.RootHash))
			}
		})
	}
}

func TestGetShardProofRequest_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		json        string
		wantShardID []byte
		wantErr     bool
	}{
		{
			name:        "valid JSON without 0x prefix",
			json:        `{"shardId":"0102"}`,
			wantShardID: []byte{0x01, 0x02},
			wantErr:     false,
		},
		{
			name:        "valid JSON with 0x prefix",
			json:        `{"shardId":"0x0103"}`,
			wantShardID: []byte{0x01, 0x03},
			wantErr:     false,
		},
		{
			name:    "invalid hex",
			json:    `{"shardId":"GGGG"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req GetShardProofRequest
			err := json.Unmarshal([]byte(tt.json), &req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantShardID, []byte(req.ShardID))
			}
		})
	}
}
