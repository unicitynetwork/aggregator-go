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

func TestTimestamp_MarshalBSONValue(t *testing.T) {
	tests := []struct {
		name      string
		timestamp *Timestamp
		wantErr   bool
	}{
		{
			name:      "marshal valid timestamp",
			timestamp: NewTimestamp(time.Unix(1640995200, 0)), // 2022-01-01 00:00:00 UTC
			wantErr:   false,
		},
		{
			name:      "marshal current time",
			timestamp: Now(),
			wantErr:   false,
		},
		{
			name:      "marshal nil timestamp",
			timestamp: nil,
			wantErr:   false,
		},
		{
			name:      "marshal zero timestamp",
			timestamp: NewTimestamp(time.Time{}),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsonType, data, err := tt.timestamp.MarshalBSONValue()
			if (err != nil) != tt.wantErr {
				t.Errorf("Timestamp.MarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if tt.timestamp == nil {
					if bsonType != bson.TypeNull {
						t.Errorf("Expected TypeNull for nil timestamp, got %v", bsonType)
					}
				} else {
					if bsonType != bson.TypeInt64 {
						t.Errorf("Expected TypeInt64 for valid timestamp, got %v", bsonType)
					}
					if len(data) == 0 {
						t.Error("Timestamp.MarshalBSONValue() returned empty data for valid timestamp")
					}
				}
			}
		})
	}
}

func TestTimestamp_UnmarshalBSONValue(t *testing.T) {
	// Test valid timestamp
	testTime := time.Unix(1640995200, 0) // 2022-01-01 00:00:00 UTC
	expectedMillis := testTime.UnixMilli()

	// Marshal the expected value as int64
	bsonType, bsonData, err := bson.MarshalValue(expectedMillis)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	var ts Timestamp
	err = ts.UnmarshalBSONValue(bsonType, bsonData)
	if err != nil {
		t.Errorf("Timestamp.UnmarshalBSONValue() error = %v", err)
		return
	}

	if ts.UnixMilli() != expectedMillis {
		t.Errorf("Timestamp.UnmarshalBSONValue() = %d, want %d", ts.UnixMilli(), expectedMillis)
	}

	// Test null value
	var nullTs Timestamp
	err = nullTs.UnmarshalBSONValue(bson.TypeNull, nil)
	if err != nil {
		t.Errorf("Timestamp.UnmarshalBSONValue() error for null = %v", err)
	}

	if !nullTs.Time.IsZero() {
		t.Error("Expected zero time for null BSON value")
	}
}

func TestTimestamp_BSONRoundTrip(t *testing.T) {
	// Test round-trip marshaling and unmarshaling
	originalTime := time.Unix(1640995200, 123000000) // 2022-01-01 00:00:00.123 UTC
	original := NewTimestamp(originalTime)

	// Marshal to BSON value
	bsonType, bsonData, err := original.MarshalBSONValue()
	if err != nil {
		t.Fatalf("MarshalBSONValue() failed: %v", err)
	}

	// Unmarshal from BSON value
	var unmarshaled Timestamp
	err = unmarshaled.UnmarshalBSONValue(bsonType, bsonData)
	if err != nil {
		t.Fatalf("UnmarshalBSONValue() failed: %v", err)
	}

	// Compare milliseconds (BSON stores as milliseconds)
	if original.UnixMilli() != unmarshaled.UnixMilli() {
		t.Errorf("Round-trip failed: original %d ms, unmarshaled %d ms",
			original.UnixMilli(), unmarshaled.UnixMilli())
	}
}

func TestTimestamp_BSONWithStruct(t *testing.T) {
	// Test marshaling/unmarshaling as part of a struct
	type TestStruct struct {
		Name      string     `bson:"name"`
		CreatedAt *Timestamp `bson:"createdAt"`
	}

	original := TestStruct{
		Name:      "test",
		CreatedAt: NewTimestamp(time.Unix(1640995200, 0)),
	}

	// Marshal struct to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal struct: %v", err)
	}

	// Unmarshal struct from BSON
	var unmarshaled TestStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal struct: %v", err)
	}

	// Verify data integrity
	if original.Name != unmarshaled.Name {
		t.Errorf("Name mismatch: got %s, want %s", unmarshaled.Name, original.Name)
	}

	if original.CreatedAt.UnixMilli() != unmarshaled.CreatedAt.UnixMilli() {
		t.Errorf("Timestamp mismatch: got %d, want %d",
			unmarshaled.CreatedAt.UnixMilli(), original.CreatedAt.UnixMilli())
	}
}

func TestTimestamp_BSONNilHandling(t *testing.T) {
	// Test handling of nil timestamp in struct
	type TestStruct struct {
		Name      string     `bson:"name"`
		CreatedAt *Timestamp `bson:"createdAt,omitempty"`
	}

	original := TestStruct{
		Name:      "test",
		CreatedAt: nil,
	}

	// Marshal struct to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal struct with nil timestamp: %v", err)
	}

	// Unmarshal struct from BSON
	var unmarshaled TestStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal struct with nil timestamp: %v", err)
	}

	// Verify nil is preserved
	if unmarshaled.CreatedAt != nil {
		t.Error("Expected nil timestamp to remain nil after round-trip")
	}
}

func TestTimestamp_BSONCompatibility(t *testing.T) {
	// Test that Timestamp works correctly when embedded in complex structures
	type ComplexStruct struct {
		ID         string      `bson:"_id"`
		Timestamps []Timestamp `bson:"timestamps"`
		Created    *Timestamp  `bson:"created"`
		Updated    *Timestamp  `bson:"updated,omitempty"`
	}

	now := time.Now()
	original := ComplexStruct{
		ID: "test-id",
		Timestamps: []Timestamp{
			*NewTimestamp(now.Add(-time.Hour)),
			*NewTimestamp(now.Add(-time.Minute)),
		},
		Created: NewTimestamp(now),
		Updated: nil,
	}

	// Marshal to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal complex struct: %v", err)
	}

	// Unmarshal from BSON
	var unmarshaled ComplexStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal complex struct: %v", err)
	}

	// Verify all fields
	if original.ID != unmarshaled.ID {
		t.Errorf("ID mismatch: got %s, want %s", unmarshaled.ID, original.ID)
	}

	if len(original.Timestamps) != len(unmarshaled.Timestamps) {
		t.Errorf("Timestamps slice length mismatch: got %d, want %d",
			len(unmarshaled.Timestamps), len(original.Timestamps))
	}

	for i, ts := range original.Timestamps {
		if ts.UnixMilli() != unmarshaled.Timestamps[i].UnixMilli() {
			t.Errorf("Timestamp[%d] mismatch: got %d, want %d",
				i, unmarshaled.Timestamps[i].UnixMilli(), ts.UnixMilli())
		}
	}

	if original.Created.UnixMilli() != unmarshaled.Created.UnixMilli() {
		t.Errorf("Created timestamp mismatch: got %d, want %d",
			unmarshaled.Created.UnixMilli(), original.Created.UnixMilli())
	}

	if unmarshaled.Updated != nil {
		t.Error("Expected Updated to remain nil")
	}
}
