package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetShardProofRequest_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		json        string
		wantShardID int
		wantErr     bool
	}{
		{
			name:        "valid JSON without 0x prefix",
			json:        `{"shardId":2}`,
			wantShardID: 2,
			wantErr:     false,
		},
		{
			name:        "valid JSON with 0x prefix",
			json:        `{"shardId":3}`,
			wantShardID: 3,
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
				assert.Equal(t, tt.wantShardID, req.ShardID)
			}
		})
	}
}
