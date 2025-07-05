package api

import (
	"encoding/json"
	"fmt"
	"math/big"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// BigInt wraps big.Int for JSON serialization
type BigInt struct {
	*big.Int
}

// NewBigInt creates a new BigInt
func NewBigInt(x *big.Int) *BigInt {
	if x == nil {
		return &BigInt{big.NewInt(0)}
	}
	return &BigInt{x}
}

// NewBigIntFromString creates a BigInt from string
func NewBigIntFromString(s string) (*BigInt, error) {
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid big int string: %s", s)
	}
	return &BigInt{i}, nil
}

// MarshalJSON implements json.Marshaler
func (b *BigInt) MarshalJSON() ([]byte, error) {
	if b.Int == nil {
		return json.Marshal("0")
	}
	return json.Marshal(b.Int.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (b *BigInt) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		i, ok := new(big.Int).SetString(s, 10)
		if !ok {
			return fmt.Errorf("invalid big int string: %s", s)
		}
		b.Int = i
		return nil
	}

	// Try to unmarshal as number
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		if n != float64(int64(n)) {
			return fmt.Errorf("big int cannot be a decimal: %f", n)
		}
		b.Int = big.NewInt(int64(n))
		return nil
	}

	return fmt.Errorf("invalid big int: must be string or number")
}

// String returns the string representation for BSON compatibility
func (b *BigInt) String() string {
	if b.Int == nil {
		return "0"
	}
	return b.Int.String()
}

// MarshalBSONValue implements bson.ValueMarshaler
func (b *BigInt) MarshalBSONValue() (bsontype.Type, []byte, error) {
	if b.Int == nil {
		return bson.MarshalValue("0")
	}
	return bson.MarshalValue(b.Int.String())
}

// UnmarshalBSONValue implements bson.ValueUnmarshaler
func (b *BigInt) UnmarshalBSONValue(bsonType bsontype.Type, data []byte) error {
	var s string
	err := bson.UnmarshalValue(bsonType, data, &s)
	if err != nil {
		return err
	}

	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return fmt.Errorf("invalid big int string: %s", s)
	}
	b.Int = i
	return nil
}
