package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// HexBytes represents byte array that serializes to/from hex string
type HexBytes []byte

// NewHexBytes creates HexBytes from byte slice
func NewHexBytes(data []byte) HexBytes {
	return data
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
	return data, nil
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
