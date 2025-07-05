package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func TestRequestID_CreateAndSerialize(t *testing.T) {
	// Test data that should produce the exact same result as TypeScript
	// From RequestIdTest.ts:
	// RequestId.create(new Uint8Array(20), DataHash.fromImprint(new Uint8Array(34)))
	// Expected JSON: '0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40'
	// Expected CBOR: '58220000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40'

	t.Run("should create RequestID with exact TypeScript compatibility", func(t *testing.T) {
		// Create 20-byte public key (all zeros)
		publicKey := make([]byte, 20)

		// Create 34-byte state hash (DataHash.fromImprint with all zeros)
		stateHashBytes := make([]byte, 34)
		stateHash, err := NewImprintHexString(fmt.Sprintf("%x", stateHashBytes))
		require.NoError(t, err)

		// Create RequestID
		requestID, err := CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Test JSON serialization matches TypeScript
		expectedJSON := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		jsonBytes, err := json.Marshal(requestID)
		require.NoError(t, err)

		var jsonStr string
		err = json.Unmarshal(jsonBytes, &jsonStr)
		require.NoError(t, err)
		assert.Equal(t, expectedJSON, jsonStr)

		// Test that we can deserialize back
		var deserializedRequestID RequestID
		err = json.Unmarshal(jsonBytes, &deserializedRequestID)
		require.NoError(t, err)
		assert.Equal(t, requestID, deserializedRequestID)

		// Test string representation
		assert.Equal(t, expectedJSON, requestID.String())
		assert.Len(t, requestID.String(), 68) // Must be 68 characters (4 algorithm + 64 hash)
	})

	t.Run("should validate hex format", func(t *testing.T) {
		// Valid 68-character hex string
		validHex := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		requestID, err := NewImprintHexString(validHex)
		require.NoError(t, err)
		assert.Equal(t, validHex, requestID.String())

		// Invalid length
		_, err = NewImprintHexString("inv")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "imprint must be at least 3 bytes")

		// Invalid hex characters (correct length but invalid hex)
		invalidHex := "xxxx0000000000000000000000000000000000000000000000000000000000000000"
		_, err = NewImprintHexString(invalidHex)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid hex")
	})

	t.Run("should convert to bytes correctly", func(t *testing.T) {
		requestIDStr := "0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"
		requestID, err := NewImprintHexString(requestIDStr)
		require.NoError(t, err)

		bytes, err := requestID.Imprint()
		require.NoError(t, err)
		assert.Len(t, bytes, 34) // 68 hex chars = 34 bytes

		// Convert back to hex and verify
		hexStr := hex.EncodeToString(bytes)
		assert.Equal(t, requestIDStr, hexStr)
	})
}

func TestImprintHexString_MarshalBSONValue(t *testing.T) {
	tests := []struct {
		name      string
		hexString ImprintHexString
		wantErr   bool
	}{
		{
			name:      "marshal valid request id",
			hexString: ImprintHexString("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			wantErr:   false,
		},
		{
			name:      "marshal short hex string",
			hexString: ImprintHexString("0000abcd"),
			wantErr:   false,
		},
		{
			name:      "marshal long hex string",
			hexString: ImprintHexString("0000" + "abcdef1234567890" + "abcdef1234567890" + "abcdef1234567890" + "abcdef1234567890"),
			wantErr:   false,
		},
		{
			name:      "marshal empty string",
			hexString: ImprintHexString(""),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsonType, data, err := tt.hexString.MarshalBSONValue()
			if (err != nil) != tt.wantErr {
				t.Errorf("ImprintHexString.MarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if bsonType != bson.TypeString {
					t.Errorf("Expected TypeString for ImprintHexString, got %v", bsonType)
				}
				if len(data) == 0 {
					t.Error("ImprintHexString.MarshalBSONValue() returned empty data")
				}
			}
		})
	}
}

func TestImprintHexString_UnmarshalBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		original ImprintHexString
		wantErr  bool
	}{
		{
			name:     "unmarshal valid request id",
			original: ImprintHexString("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			wantErr:  false,
		},
		{
			name:     "unmarshal short hex string",
			original: ImprintHexString("0000abcd"),
			wantErr:  false,
		},
		{
			name:     "unmarshal with algorithm prefix",
			original: ImprintHexString("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal the original to get BSON data
			bsonType, data, err := tt.original.MarshalBSONValue()
			if err != nil {
				t.Fatalf("Failed to marshal original: %v", err)
			}

			// Unmarshal the BSON data
			var unmarshaled ImprintHexString
			err = unmarshaled.UnmarshalBSONValue(bsonType, data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ImprintHexString.UnmarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && string(tt.original) != string(unmarshaled) {
				t.Errorf("ImprintHexString.UnmarshalBSONValue() = %s, want %s", string(unmarshaled), string(tt.original))
			}
		})
	}
}

func TestImprintHexString_UnmarshalBSONValue_InvalidData(t *testing.T) {
	tests := []struct {
		name     string
		bsonType bsontype.Type
		data     []byte
		wantErr  bool
	}{
		{
			name:     "invalid hex string",
			bsonType: bson.TypeString,
			data:     []byte("xyz"),
			wantErr:  true,
		},
		{
			name:     "too short string",
			bsonType: bson.TypeString,
			data:     []byte("ab"),
			wantErr:  true,
		},
		{
			name:     "invalid bson type",
			bsonType: bson.TypeInt32,
			data:     []byte{0x01, 0x00, 0x00, 0x00},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h ImprintHexString
			err := h.UnmarshalBSONValue(tt.bsonType, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ImprintHexString.UnmarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestImprintHexString_BSONRoundTrip(t *testing.T) {
	testCases := []struct {
		name     string
		original ImprintHexString
	}{
		{
			name:     "typical request id",
			original: ImprintHexString("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
		},
		{
			name:     "algorithm prefix 1234",
			original: ImprintHexString("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
		},
		{
			name:     "algorithm prefix ffff",
			original: ImprintHexString("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
		},
		{
			name:     "short hex string",
			original: ImprintHexString("0000abcd"),
		},
		{
			name:     "long hex string",
			original: ImprintHexString("0000abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test struct-based round-trip
			type TestStruct struct {
				ID    string           `bson:"_id"`
				Value ImprintHexString `bson:"value"`
			}

			original := TestStruct{
				ID:    tc.name,
				Value: tc.original,
			}

			// Marshal to BSON
			bsonData, err := bson.Marshal(original)
			if err != nil {
				t.Fatalf("Marshal() failed: %v", err)
			}

			// Unmarshal from BSON
			var unmarshaled TestStruct
			err = bson.Unmarshal(bsonData, &unmarshaled)
			if err != nil {
				t.Fatalf("Unmarshal() failed: %v", err)
			}

			// Compare string representations
			if string(tc.original) != string(unmarshaled.Value) {
				t.Errorf("Round-trip failed: original %s, unmarshaled %s",
					string(tc.original), string(unmarshaled.Value))
			}

			// Test that we can still call methods on the unmarshaled value
			originalBytes, err := tc.original.Imprint()
			if err == nil { // Only test if original is valid
				unmarshaledBytes, err := unmarshaled.Value.Imprint()
				if err != nil {
					t.Errorf("Unmarshaled value failed Imprint(): %v", err)
				} else if !assert.Equal(t, originalBytes, unmarshaledBytes) {
					t.Errorf("Imprint() bytes don't match after round-trip")
				}
			}
		})
	}
}

func TestImprintHexString_BSONWithStruct(t *testing.T) {
	// Test marshaling/unmarshaling as part of a struct
	type TestStruct struct {
		RequestID RequestID        `bson:"request_id"`
		StateHash ImprintHexString `bson:"state_hash"`
		Name      string           `bson:"name"`
	}

	original := TestStruct{
		RequestID: RequestID("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
		StateHash: ImprintHexString("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
		Name:      "test-struct",
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

	if string(original.RequestID) != string(unmarshaled.RequestID) {
		t.Errorf("RequestID mismatch: got %s, want %s",
			string(unmarshaled.RequestID), string(original.RequestID))
	}

	if string(original.StateHash) != string(unmarshaled.StateHash) {
		t.Errorf("StateHash mismatch: got %s, want %s",
			string(unmarshaled.StateHash), string(original.StateHash))
	}
}

func TestImprintHexString_BSONNilHandling(t *testing.T) {
	// Test handling of nil ImprintHexString pointer in struct
	type TestStruct struct {
		Name      string            `bson:"name"`
		RequestID *ImprintHexString `bson:"request_id,omitempty"`
	}

	original := TestStruct{
		Name:      "test",
		RequestID: nil,
	}

	// Marshal struct to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal struct with nil ImprintHexString: %v", err)
	}

	// Unmarshal struct from BSON
	var unmarshaled TestStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal struct with nil ImprintHexString: %v", err)
	}

	// Verify nil is preserved
	if unmarshaled.RequestID != nil {
		t.Error("Expected nil ImprintHexString to remain nil after round-trip")
	}
}

func TestImprintHexString_BSONCompatibility(t *testing.T) {
	// Test that ImprintHexString works correctly when embedded in complex structures
	type ComplexStruct struct {
		ID         string             `bson:"_id"`
		RequestIDs []ImprintHexString `bson:"request_ids"`
		MainID     ImprintHexString   `bson:"main_id"`
		OptionalID *ImprintHexString  `bson:"optional_id,omitempty"`
		Metadata   map[string]string  `bson:"metadata"`
	}

	optionalID := ImprintHexString("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00")
	original := ComplexStruct{
		ID: "test-complex",
		RequestIDs: []ImprintHexString{
			ImprintHexString("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			ImprintHexString("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
			ImprintHexString("ffff000000000000000000000000000000000000000000000000000000000000"),
		},
		MainID:     ImprintHexString("0000abc000000000000000000000000000000000000000000000000000000000"),
		OptionalID: &optionalID,
		Metadata: map[string]string{
			"version": "1.0",
			"type":    "test",
		},
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

	if len(original.RequestIDs) != len(unmarshaled.RequestIDs) {
		t.Errorf("RequestIDs slice length mismatch: got %d, want %d",
			len(unmarshaled.RequestIDs), len(original.RequestIDs))
	}

	for i, id := range original.RequestIDs {
		if string(id) != string(unmarshaled.RequestIDs[i]) {
			t.Errorf("RequestIDs[%d] mismatch: got %s, want %s",
				i, string(unmarshaled.RequestIDs[i]), string(id))
		}
	}

	if string(original.MainID) != string(unmarshaled.MainID) {
		t.Errorf("MainID mismatch: got %s, want %s",
			string(unmarshaled.MainID), string(original.MainID))
	}

	if original.OptionalID == nil && unmarshaled.OptionalID != nil {
		t.Error("Expected nil OptionalID to remain nil")
	} else if original.OptionalID != nil && unmarshaled.OptionalID == nil {
		t.Error("Expected non-nil OptionalID to remain non-nil")
	} else if original.OptionalID != nil && unmarshaled.OptionalID != nil {
		if string(*original.OptionalID) != string(*unmarshaled.OptionalID) {
			t.Errorf("OptionalID mismatch: got %s, want %s",
				string(*unmarshaled.OptionalID), string(*original.OptionalID))
		}
	}

	// Verify metadata
	for key, value := range original.Metadata {
		if unmarshaled.Metadata[key] != value {
			t.Errorf("Metadata[%s] mismatch: got %s, want %s",
				key, unmarshaled.Metadata[key], value)
		}
	}
}

func TestRequestID_BSONFunctionality(t *testing.T) {
	// Test that RequestID (which is an alias for ImprintHexString) works with BSON
	t.Run("RequestID as alias works with BSON", func(t *testing.T) {
		// Create a RequestID using the standard creation function
		publicKey := make([]byte, 20)
		stateHashBytes := make([]byte, 34)
		stateHash, err := NewImprintHexString(fmt.Sprintf("%x", stateHashBytes))
		require.NoError(t, err)

		requestID, err := CreateRequestID(publicKey, stateHash)
		require.NoError(t, err)

		// Test struct-based BSON marshaling
		type TestDoc struct {
			ID        string    `bson:"_id"`
			RequestID RequestID `bson:"request_id"`
		}

		doc := TestDoc{
			ID:        "test-doc",
			RequestID: requestID,
		}

		// Marshal to BSON
		bsonData, err := bson.Marshal(doc)
		require.NoError(t, err)

		// Unmarshal from BSON
		var unmarshaled TestDoc
		err = bson.Unmarshal(bsonData, &unmarshaled)
		require.NoError(t, err)

		// Verify RequestID is preserved
		assert.Equal(t, string(requestID), string(unmarshaled.RequestID))
		assert.Equal(t, doc.ID, unmarshaled.ID)

		// Verify the RequestID still functions correctly
		originalBytes, err := requestID.Imprint()
		require.NoError(t, err)

		unmarshaledBytes, err := unmarshaled.RequestID.Imprint()
		require.NoError(t, err)

		assert.Equal(t, originalBytes, unmarshaledBytes)
	})
}

func TestImprintHexString_BSONComprehensive(t *testing.T) {
	// Test comprehensive ImprintHexString BSON marshaling/unmarshaling scenarios
	testCases := []struct {
		name        string
		value       ImprintHexString
		description string
	}{
		{
			name:        "standard request id",
			value:       ImprintHexString("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			description: "Standard RequestID format with 0000 algorithm prefix",
		},
		{
			name:        "different algorithm",
			value:       ImprintHexString("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
			description: "Different algorithm prefix (1234)",
		},
		{
			name:        "max algorithm",
			value:       ImprintHexString("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
			description: "Maximum algorithm prefix (ffff)",
		},
		{
			name:        "minimal valid",
			value:       ImprintHexString("0000abcd"),
			description: "Minimal valid hex string (3+ bytes)",
		},
		{
			name:        "extended length",
			value:       ImprintHexString("0000abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
			description: "Extended length hex string",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test direct value marshaling
			bsonType, data, err := tc.value.MarshalBSONValue()
			if err != nil {
				t.Fatalf("MarshalBSONValue failed: %v", err)
			}

			// Verify the BSON type is string
			if bsonType != bson.TypeString {
				t.Errorf("Expected BSON type string, got %v", bsonType)
			}

			// Test direct value unmarshaling
			var unmarshaled ImprintHexString
			err = unmarshaled.UnmarshalBSONValue(bsonType, data)
			if err != nil {
				t.Fatalf("UnmarshalBSONValue failed: %v", err)
			}

			// Verify values match
			if string(tc.value) != string(unmarshaled) {
				t.Errorf("Direct marshaling mismatch: expected %s, got %s",
					string(tc.value), string(unmarshaled))
			}

			// Test struct-based marshaling
			type TestDoc struct {
				ID    string           `bson:"_id"`
				Value ImprintHexString `bson:"value"`
			}

			doc := TestDoc{
				ID:    tc.name,
				Value: tc.value,
			}

			bsonData, err := bson.Marshal(doc)
			if err != nil {
				t.Fatalf("Struct marshal failed: %v", err)
			}

			var unmarshaledDoc TestDoc
			err = bson.Unmarshal(bsonData, &unmarshaledDoc)
			if err != nil {
				t.Fatalf("Struct unmarshal failed: %v", err)
			}

			// Verify struct marshaling preserves values
			if doc.ID != unmarshaledDoc.ID {
				t.Errorf("ID mismatch: expected %s, got %s", doc.ID, unmarshaledDoc.ID)
			}

			if string(doc.Value) != string(unmarshaledDoc.Value) {
				t.Errorf("Struct marshaling mismatch: expected %s, got %s",
					string(doc.Value), string(unmarshaledDoc.Value))
			}

			// Test that methods still work on unmarshaled value
			if len(string(tc.value)) >= 4 { // Only test if long enough for algorithm
				originalBytes, err := tc.value.Imprint()
				if err == nil { // Only test if original is valid
					unmarshaledBytes, err := unmarshaledDoc.Value.Imprint()
					if err != nil {
						t.Errorf("Unmarshaled value failed Imprint(): %v", err)
					} else if !assert.Equal(t, originalBytes, unmarshaledBytes) {
						t.Errorf("Imprint() bytes don't match after round-trip")
					}
				}
			}

			t.Logf("✓ %s: %s -> BSON -> %s", tc.description, string(tc.value), string(unmarshaledDoc.Value))
		})
	}
}
