package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateIDMarshalJSON(t *testing.T) {
	stateID := RequireNewImprintV2("0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00")

	data, err := json.Marshal(stateID)
	require.NoError(t, err, "Failed to marshal StateID")

	var unmarshaledID StateID
	err = json.Unmarshal(data, &unmarshaledID)
	require.NoError(t, err, "Failed to unmarshal StateID")

	require.Equal(t, stateID, unmarshaledID, "StateID mismatch")
}

func TestHexBytesMarshalJSON(t *testing.T) {
	hexBytes := HexBytes{0x01, 0x02, 0x03, 0x04}

	data, err := json.Marshal(hexBytes)
	require.NoError(t, err, "Failed to marshal HexBytes")

	var unmarshaledHex HexBytes
	err = json.Unmarshal(data, &unmarshaledHex)
	require.NoError(t, err, "Failed to unmarshal HexBytes")

	require.Equal(t, len(hexBytes), len(unmarshaledHex), "HexBytes length mismatch")

	for i, b := range hexBytes {
		require.Equal(t, b, unmarshaledHex[i], "HexBytes byte mismatch at index %d", i)
	}
}

func TestImprintHexStringMarshalJSON(t *testing.T) {
	imprint := RequireNewImprintV2("0000cd60")

	data, err := json.Marshal(imprint)
	require.NoError(t, err, "Failed to marshal ImprintHexString")

	var unmarshaledImprint ImprintV2
	err = json.Unmarshal(data, &unmarshaledImprint)
	require.NoError(t, err, "Failed to unmarshal ImprintHexString")

	require.Equal(t, imprint, unmarshaledImprint, "ImprintHexString mismatch")
}

func TestTimeNanoMarshalJSON(t *testing.T) {
	now := time.Now()

	data, err := json.Marshal(now)
	require.NoError(t, err, "Failed to marshal time.Time")

	var unmarshaledTime time.Time
	err = json.Unmarshal(data, &unmarshaledTime)
	require.NoError(t, err, "Failed to unmarshal time.Time")

	require.True(t, now.Equal(unmarshaledTime) || now.Sub(unmarshaledTime) < time.Millisecond, "time.Time mismatch")
}
