package models

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"
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
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return fmt.Errorf("invalid big int: %s", s)
	}
	b.Int = i
	return nil
}

// HexBytes represents byte array that serializes to/from hex string
type HexBytes []byte

// NewHexBytes creates HexBytes from byte slice
func NewHexBytes(data []byte) HexBytes {
	return HexBytes(data)
}

// NewHexBytesFromString creates HexBytes from hex string
func NewHexBytesFromString(s string) (HexBytes, error) {
	// Remove 0x prefix if present
	if len(s) >= 2 && s[:2] == "0x" {
		s = s[2:]
	}
	
	data, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid hex string: %w", err)
	}
	return HexBytes(data), nil
}

// MarshalJSON implements json.Marshaler
func (h HexBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(h))
}

// UnmarshalJSON implements json.Unmarshaler
func (h *HexBytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	
	decoded, err := NewHexBytesFromString(s)
	if err != nil {
		return err
	}
	*h = decoded
	return nil
}

// String returns hex representation
func (h HexBytes) String() string {
	return hex.EncodeToString(h)
}

// Timestamp wraps time.Time for consistent JSON serialization
type Timestamp struct {
	time.Time
}

// NewTimestamp creates a new Timestamp
func NewTimestamp(t time.Time) *Timestamp {
	return &Timestamp{Time: t}
}

// Now creates a Timestamp for current time
func Now() *Timestamp {
	return &Timestamp{Time: time.Now()}
}

// MarshalJSON implements json.Marshaler
func (t *Timestamp) MarshalJSON() ([]byte, error) {
	// Use Unix timestamp in milliseconds as string
	millis := t.Time.UnixMilli()
	return json.Marshal(strconv.FormatInt(millis, 10))
}

// UnmarshalJSON implements json.Unmarshaler
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	
	millis, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}
	
	t.Time = time.UnixMilli(millis)
	return nil
}

// RequestID represents a 64-character hex request ID
type RequestID string

// NewRequestID creates a RequestID with validation
func NewRequestID(id string) (RequestID, error) {
	if len(id) != 64 {
		return "", fmt.Errorf("request ID must be 64 characters, got %d", len(id))
	}
	
	// Validate hex
	if _, err := hex.DecodeString(id); err != nil {
		return "", fmt.Errorf("request ID must be valid hex: %w", err)
	}
	
	return RequestID(id), nil
}

// String returns the string representation
func (r RequestID) String() string {
	return string(r)
}

// Bytes returns the byte representation
func (r RequestID) Bytes() ([]byte, error) {
	return hex.DecodeString(string(r))
}

// MarshalJSON implements json.Marshaler
func (r RequestID) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(r))
}

// UnmarshalJSON implements json.Unmarshaler
func (r *RequestID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	
	id, err := NewRequestID(s)
	if err != nil {
		return err
	}
	*r = id
	return nil
}

// TransactionHash represents a 64-character hex transaction hash
type TransactionHash string

// NewTransactionHash creates a TransactionHash with validation
func NewTransactionHash(hash string) (TransactionHash, error) {
	if len(hash) != 64 {
		return "", fmt.Errorf("transaction hash must be 64 characters, got %d", len(hash))
	}
	
	// Validate hex
	if _, err := hex.DecodeString(hash); err != nil {
		return "", fmt.Errorf("transaction hash must be valid hex: %w", err)
	}
	
	return TransactionHash(hash), nil
}

// String returns the string representation
func (t TransactionHash) String() string {
	return string(t)
}

// Bytes returns the byte representation
func (t TransactionHash) Bytes() ([]byte, error) {
	return hex.DecodeString(string(t))
}

// MarshalJSON implements json.Marshaler
func (t TransactionHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(t))
}

// UnmarshalJSON implements json.Unmarshaler
func (t *TransactionHash) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	
	hash, err := NewTransactionHash(s)
	if err != nil {
		return err
	}
	*t = hash
	return nil
}

// StateHash represents a 64-character hex state hash
type StateHash string

// NewStateHash creates a StateHash with validation
func NewStateHash(hash string) (StateHash, error) {
	if len(hash) != 64 {
		return "", fmt.Errorf("state hash must be 64 characters, got %d", len(hash))
	}
	
	// Validate hex
	if _, err := hex.DecodeString(hash); err != nil {
		return "", fmt.Errorf("state hash must be valid hex: %w", err)
	}
	
	return StateHash(hash), nil
}

// String returns the string representation
func (s StateHash) String() string {
	return string(s)
}

// Bytes returns the byte representation
func (s StateHash) Bytes() ([]byte, error) {
	return hex.DecodeString(string(s))
}

// MarshalJSON implements json.Marshaler
func (s StateHash) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(s))
}

// UnmarshalJSON implements json.Unmarshaler
func (s *StateHash) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	
	hash, err := NewStateHash(str)
	if err != nil {
		return err
	}
	*s = hash
	return nil
}