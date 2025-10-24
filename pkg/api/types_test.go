package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
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

func TestRequestIDMarshalBSON(t *testing.T) {
	requestID := RequestID("0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00")

	// Wrap in a struct for BSON compatibility
	type wrapper struct {
		ID RequestID `bson:"id"`
	}
	w := wrapper{ID: requestID}

	data, err := bson.Marshal(w)
	require.NoError(t, err, "Failed to marshal RequestID to BSON")

	var unmarshaled wrapper
	err = bson.Unmarshal(data, &unmarshaled)
	require.NoError(t, err, "Failed to unmarshal RequestID from BSON")

	require.Equal(t, requestID, unmarshaled.ID, "RequestID mismatch")
}

func TestHexBytesMarshalBSON(t *testing.T) {
	hexBytes := HexBytes{0x01, 0x02, 0x03, 0x04}

	// Wrap HexBytes in a struct for BSON compatibility
	type wrapper struct {
		Data HexBytes `bson:"data"`
	}
	w := wrapper{Data: hexBytes}

	data, err := bson.Marshal(w)
	require.NoError(t, err, "Failed to marshal HexBytes to BSON")

	var unmarshaled wrapper
	err = bson.Unmarshal(data, &unmarshaled)
	require.NoError(t, err, "Failed to unmarshal HexBytes from BSON")

	require.Equal(t, len(hexBytes), len(unmarshaled.Data), "HexBytes length mismatch")

	for i, b := range hexBytes {
		require.Equal(t, b, unmarshaled.Data[i], "HexBytes byte mismatch at index %d", i)
	}
}

func TestSubmitCommitmentRequestBSON(t *testing.T) {
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

	data, err := bson.Marshal(req)
	require.NoError(t, err, "Failed to marshal SubmitCommitmentRequest to BSON")

	var unmarshaledReq SubmitCommitmentRequest
	err = bson.Unmarshal(data, &unmarshaledReq)
	require.NoError(t, err, "Failed to unmarshal SubmitCommitmentRequest from BSON")

	require.Equal(t, req.RequestID, unmarshaledReq.RequestID, "RequestID mismatch")

	require.Equal(t, req.TransactionHash, unmarshaledReq.TransactionHash, "TransactionHash mismatch")

	require.Equal(t, req.Authenticator.Algorithm, unmarshaledReq.Authenticator.Algorithm, "Algorithm mismatch")
}

func TestImprintHexStringMarshalJSON(t *testing.T) {
	imprint := ImprintHexString("0000cd60")

	data, err := json.Marshal(imprint)
	require.NoError(t, err, "Failed to marshal ImprintHexString")

	var unmarshaledImprint ImprintHexString
	err = json.Unmarshal(data, &unmarshaledImprint)
	require.NoError(t, err, "Failed to unmarshal ImprintHexString")

	require.Equal(t, imprint, unmarshaledImprint, "ImprintHexString mismatch")
}

func TestImprintHexStringMarshalBSON(t *testing.T) {
	imprint := ImprintHexString("0000cd60")

	// Wrap ImprintHexString in a struct for BSON compatibility
	type wrapper struct {
		Imprint ImprintHexString `bson:"imprint"`
	}
	w := wrapper{Imprint: imprint}

	data, err := bson.Marshal(w)
	require.NoError(t, err, "Failed to marshal ImprintHexString to BSON")

	var unmarshaled wrapper
	err = bson.Unmarshal(data, &unmarshaled)
	require.NoError(t, err, "Failed to unmarshal ImprintHexString from BSON")

	require.Equal(t, imprint, unmarshaled.Imprint, "ImprintHexString mismatch")
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

func TestTimeNanoMarshalBSON(t *testing.T) {
	now := time.Now()

	// Wrap time.Time in a struct for BSON compatibility
	type wrapper struct {
		Time time.Time `bson:"time"`
	}
	w := wrapper{Time: now}

	data, err := bson.Marshal(w)
	require.NoError(t, err, "Failed to marshal time.Time to BSON")

	var unmarshaled wrapper
	err = bson.Unmarshal(data, &unmarshaled)
	require.NoError(t, err, "Failed to unmarshal time.Time from BSON")

	require.True(t, now.Equal(unmarshaled.Time) || now.Sub(unmarshaled.Time) < time.Millisecond, "time.Time mismatch")
}
