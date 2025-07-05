package api

import (
	"math/big"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func TestBigInt_MarshalBSONValue(t *testing.T) {
	tests := []struct {
		name    string
		bigInt  *BigInt
		wantErr bool
	}{
		{
			name:    "marshal valid positive bigint",
			bigInt:  NewBigInt(big.NewInt(12345)),
			wantErr: false,
		},
		{
			name:    "marshal zero bigint",
			bigInt:  NewBigInt(big.NewInt(0)),
			wantErr: false,
		},
		{
			name:    "marshal negative bigint",
			bigInt:  NewBigInt(big.NewInt(-98765)),
			wantErr: false,
		},
		{
			name:    "marshal large bigint",
			bigInt:  mustNewBigIntFromString("123456789012345678901234567890"),
			wantErr: false,
		},
		{
			name:    "marshal nil internal bigint",
			bigInt:  &BigInt{Int: nil},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bsonType, data, err := tt.bigInt.MarshalBSONValue()
			if (err != nil) != tt.wantErr {
				t.Errorf("BigInt.MarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if bsonType != bson.TypeString {
					t.Errorf("Expected TypeString for BigInt, got %v", bsonType)
				}
				if len(data) == 0 {
					t.Error("BigInt.MarshalBSONValue() returned empty data")
				}
			}
		})
	}
}

func TestBigInt_UnmarshalBSONValue(t *testing.T) {
	tests := []struct {
		name     string
		original *BigInt
		wantErr  bool
	}{
		{
			name:     "unmarshal valid positive bigint",
			original: NewBigInt(big.NewInt(12345)),
			wantErr:  false,
		},
		{
			name:     "unmarshal zero bigint",
			original: NewBigInt(big.NewInt(0)),
			wantErr:  false,
		},
		{
			name:     "unmarshal negative bigint",
			original: NewBigInt(big.NewInt(-98765)),
			wantErr:  false,
		},
		{
			name:     "unmarshal large bigint",
			original: mustNewBigIntFromString("123456789012345678901234567890"),
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
			var unmarshaled BigInt
			err = unmarshaled.UnmarshalBSONValue(bsonType, data)
			if (err != nil) != tt.wantErr {
				t.Errorf("BigInt.UnmarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.original.String() != unmarshaled.String() {
				t.Errorf("BigInt.UnmarshalBSONValue() = %s, want %s", unmarshaled.String(), tt.original.String())
			}
		})
	}
}

func TestBigInt_UnmarshalBSONValue_InvalidData(t *testing.T) {
	tests := []struct {
		name     string
		bsonType bsontype.Type
		data     []byte
		wantErr  bool
	}{
		{
			name:     "invalid string data",
			bsonType: bson.TypeString,
			data:     []byte("not-a-number"),
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
			var bi BigInt
			err := bi.UnmarshalBSONValue(tt.bsonType, tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("BigInt.UnmarshalBSONValue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBigInt_BSONRoundTrip(t *testing.T) {
	testCases := []struct {
		name     string
		original *BigInt
	}{
		{
			name:     "positive number",
			original: NewBigInt(big.NewInt(12345)),
		},
		{
			name:     "zero",
			original: NewBigInt(big.NewInt(0)),
		},
		{
			name:     "negative number",
			original: NewBigInt(big.NewInt(-98765)),
		},
		{
			name:     "large number",
			original: mustNewBigIntFromString("123456789012345678901234567890"),
		},
		{
			name:     "very large number",
			original: mustNewBigIntFromString("999999999999999999999999999999999999999999999999999999999999999999"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test struct-based round-trip
			type TestStruct struct {
				Value *BigInt `bson:"value"`
			}

			original := TestStruct{Value: tc.original}

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
			if tc.original.String() != unmarshaled.Value.String() {
				t.Errorf("Round-trip failed: original %s, unmarshaled %s",
					tc.original.String(), unmarshaled.Value.String())
			}

			// Compare using Cmp if both have valid Int values
			if tc.original.Int != nil && unmarshaled.Value.Int != nil {
				if tc.original.Int.Cmp(unmarshaled.Value.Int) != 0 {
					t.Errorf("Round-trip comparison failed: original %s, unmarshaled %s",
						tc.original.String(), unmarshaled.Value.String())
				}
			}
		})
	}
}

func TestBigInt_BSONWithStruct(t *testing.T) {
	// Test marshaling/unmarshaling as part of a struct
	type TestStruct struct {
		Name   string  `bson:"name"`
		Amount *BigInt `bson:"amount"`
		Count  *BigInt `bson:"count"`
	}

	original := TestStruct{
		Name:   "test",
		Amount: NewBigInt(big.NewInt(12345)),
		Count:  mustNewBigIntFromString("999999999999999999999999999999"),
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

	if original.Amount.String() != unmarshaled.Amount.String() {
		t.Errorf("Amount mismatch: got %s, want %s",
			unmarshaled.Amount.String(), original.Amount.String())
	}

	if original.Count.String() != unmarshaled.Count.String() {
		t.Errorf("Count mismatch: got %s, want %s",
			unmarshaled.Count.String(), original.Count.String())
	}
}

func TestBigInt_BSONNilHandling(t *testing.T) {
	// Test handling of nil BigInt in struct
	type TestStruct struct {
		Name   string  `bson:"name"`
		Amount *BigInt `bson:"amount,omitempty"`
	}

	original := TestStruct{
		Name:   "test",
		Amount: nil,
	}

	// Marshal struct to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal struct with nil BigInt: %v", err)
	}

	// Unmarshal struct from BSON
	var unmarshaled TestStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal struct with nil BigInt: %v", err)
	}

	// Verify nil is preserved
	if unmarshaled.Amount != nil {
		t.Error("Expected nil BigInt to remain nil after round-trip")
	}
}

func TestBigInt_BSONNilInternalInt(t *testing.T) {
	// Test BigInt with nil internal Int
	bigInt := &BigInt{Int: nil}

	// Test struct-based marshaling
	type TestStruct struct {
		Value *BigInt `bson:"value"`
	}

	original := TestStruct{Value: bigInt}

	// Marshal to BSON
	bsonData, err := bson.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() failed for nil internal Int: %v", err)
	}

	// Unmarshal from BSON
	var unmarshaled TestStruct
	err = bson.Unmarshal(bsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Unmarshal() failed: %v", err)
	}

	// Should unmarshal to "0"
	if unmarshaled.Value.String() != "0" {
		t.Errorf("Expected '0' for nil internal Int, got %s", unmarshaled.Value.String())
	}
}

func TestBigInt_BSONCompatibility(t *testing.T) {
	// Test that BigInt works correctly when embedded in complex structures
	type ComplexStruct struct {
		ID      string    `bson:"_id"`
		Numbers []*BigInt `bson:"numbers"`
		Total   *BigInt   `bson:"total"`
		Nil     *BigInt   `bson:"nil,omitempty"`
	}

	original := ComplexStruct{
		ID: "test-id",
		Numbers: []*BigInt{
			NewBigInt(big.NewInt(100)),
			NewBigInt(big.NewInt(200)),
			mustNewBigIntFromString("999999999999999999999999999999"),
		},
		Total: NewBigInt(big.NewInt(300)),
		Nil:   nil,
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

	if len(original.Numbers) != len(unmarshaled.Numbers) {
		t.Errorf("Numbers slice length mismatch: got %d, want %d",
			len(unmarshaled.Numbers), len(original.Numbers))
	}

	for i, num := range original.Numbers {
		if num.String() != unmarshaled.Numbers[i].String() {
			t.Errorf("Numbers[%d] mismatch: got %s, want %s",
				i, unmarshaled.Numbers[i].String(), num.String())
		}
	}

	if original.Total.String() != unmarshaled.Total.String() {
		t.Errorf("Total mismatch: got %s, want %s",
			unmarshaled.Total.String(), original.Total.String())
	}

	if unmarshaled.Nil != nil {
		t.Error("Expected Nil to remain nil")
	}
}

func TestBigInt_BSONComprehensive(t *testing.T) {
	// Test comprehensive BigInt BSON marshaling/unmarshaling scenarios
	testCases := []struct {
		name        string
		value       *BigInt
		description string
	}{
		{
			name:        "small positive",
			value:       NewBigInt(big.NewInt(42)),
			description: "Small positive integer",
		},
		{
			name:        "large positive",
			value:       mustNewBigIntFromString("12345678901234567890123456789012345678901234567890"),
			description: "Large positive integer beyond int64 range",
		},
		{
			name:        "negative",
			value:       NewBigInt(big.NewInt(-999999999999999999)),
			description: "Negative integer",
		},
		{
			name:        "zero",
			value:       NewBigInt(big.NewInt(0)),
			description: "Zero value",
		},
		{
			name:        "nil internal int",
			value:       &BigInt{Int: nil},
			description: "BigInt with nil internal Int pointer",
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
			var unmarshaled BigInt
			err = unmarshaled.UnmarshalBSONValue(bsonType, data)
			if err != nil {
				t.Fatalf("UnmarshalBSONValue failed: %v", err)
			}

			// Verify values match
			if tc.value.String() != unmarshaled.String() {
				t.Errorf("Direct marshaling mismatch: expected %s, got %s",
					tc.value.String(), unmarshaled.String())
			}

			// Test struct-based marshaling
			type TestDoc struct {
				ID    string  `bson:"_id"`
				Value *BigInt `bson:"value"`
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

			if doc.Value.String() != unmarshaledDoc.Value.String() {
				t.Errorf("Struct marshaling mismatch: expected %s, got %s",
					doc.Value.String(), unmarshaledDoc.Value.String())
			}

			t.Logf("✓ %s: %s -> BSON -> %s", tc.description, tc.value.String(), unmarshaledDoc.Value.String())
		})
	}
}

// Helper function for test cases
func mustNewBigIntFromString(s string) *BigInt {
	bi, err := NewBigIntFromString(s)
	if err != nil {
		panic(err)
	}
	return bi
}
