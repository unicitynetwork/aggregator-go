package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestIDMarshalJSON(t *testing.T) {
	requestID := RequestID("0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00")

	data, err := json.Marshal(requestID)
	require.NoError(t, err, "Failed to marshal RequestID")

	var unmarshaledID RequestID
	err = json.Unmarshal(data, &unmarshaledID)
	require.NoError(t, err, "Failed to unmarshal RequestID")

	require.Equal(t, requestID, unmarshaledID, "RequestID mismatch")
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

func TestSubmitCommitmentRequestJSON(t *testing.T) {
	req := &SubmitCommitmentRequest{
		RequestID:       "0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00",
		TransactionHash: "00008a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d",
		Authenticator: Authenticator{
			Algorithm: "secp256k1",
			PublicKey: HexBytes{0x03, 0x20, 0x44, 0xf2},
			Signature: HexBytes{0x41, 0x67, 0x51, 0xe8},
			StateHash: ImprintHexString("0000cd60"),
		},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err, "Failed to marshal SubmitCommitmentRequest")

	var unmarshaledReq SubmitCommitmentRequest
	err = json.Unmarshal(data, &unmarshaledReq)
	require.NoError(t, err, "Failed to unmarshal SubmitCommitmentRequest")

	require.Equal(t, req.RequestID, unmarshaledReq.RequestID, "RequestID mismatch")

	require.Equal(t, req.TransactionHash, unmarshaledReq.TransactionHash, "TransactionHash mismatch")

	require.Equal(t, req.Authenticator.Algorithm, unmarshaledReq.Authenticator.Algorithm, "Algorithm mismatch")
}
