package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type RequestID = ImprintHexString

// ImprintHexString represents a hex string (4 chars (two bytes) algorithm + n-byte chars hash)
type ImprintHexString string

func (r RequestID) GetPath() (*big.Int, error) {
	// Converts RequestID hex string to a big.Int for use as an SMT path.
	// Prefixes with "0x01" to preserve leading zero bits in the original hex string,
	// ensuring consistent path representation in the Sparse Merkle Tree.
	path, ok := new(big.Int).SetString("0x01"+string(r), 0)
	if !ok {
		return nil, fmt.Errorf("failed to convert requestID %s to path", r)
	}
	return path, nil
}

func NewImprintHexStringFromBytes(imprint []byte) (ImprintHexString, error) {
	if len(imprint) <= 3 {
		return "", fmt.Errorf("imprint must be at least 3 bytes, got %d", len(imprint))
	}
	return ImprintHexString(fmt.Sprintf("%x", imprint)), nil
}

func NewImprintHexString(s string) (ImprintHexString, error) {
	if len(s) <= 4 {
		return "", fmt.Errorf("imprint must be at least 3 bytes, got %d", len(s)/2)
	}

	// Validate hex
	if _, err := hex.DecodeString(s); err != nil {
		return "", fmt.Errorf("'%s' is not a valid hex: %w", s, err)
	}

	return ImprintHexString(s), nil
}

func (r ImprintHexString) String() string {
	return string(r)
}

func (r ImprintHexString) Algorithm() ([]byte, error) {
	decoded, err := hex.DecodeString(string(r))
	if err != nil {
		return nil, err
	}
	return decoded[0:2], nil
}

func (r ImprintHexString) Imprint() ([]byte, error) {
	return hex.DecodeString(string(r))
}

func (r ImprintHexString) Bytes() ([]byte, error) {
	return r.Imprint()
}

func (r ImprintHexString) DataBytes() ([]byte, error) {
	decoded, err := hex.DecodeString(string(r))
	if err != nil {
		return nil, err
	}
	return decoded[2:], nil
}

// MarshalJSON implements json.Marshaler
func (r ImprintHexString) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(r))
}

// UnmarshalJSON implements json.Unmarshaler
func (r *ImprintHexString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	id, err := NewImprintHexString(s)
	if err != nil {
		return err
	}
	*r = id
	return nil
}

// CreateRequestID creates a RequestID from public key and state hash
func CreateRequestID(publicKey []byte, stateHash ImprintHexString) (RequestID, error) {
	stateHashBytes, err := stateHash.Imprint()
	if err != nil {
		return "", fmt.Errorf("failed to convert state hash to bytes: %w", err)
	}
	return CreateRequestIDFromBytes(publicKey, stateHashBytes)
}

func CreateRequestIDFromBytes(publicKey []byte, stateHashBytes []byte) (RequestID, error) {
	// Create the data to hash: publicKey + stateHash
	data := make([]byte, 0, len(publicKey)+len(stateHashBytes))
	data = append(data, publicKey...)
	data = append(data, stateHashBytes...)

	return NewImprintHexString(fmt.Sprintf("0000%x", sha256.Sum256(data)))
}

func ValidateRequestID(requestID RequestID, publicKey []byte, stateHashBytes []byte) (bool, error) {
	expectedRequestID, err := CreateRequestIDFromBytes(publicKey, stateHashBytes)
	if err != nil {
		return false, err
	}

	return requestID == expectedRequestID, nil
}

// MarshalBSONValue implements bson.ValueMarshaler for ImprintHexString
func (r ImprintHexString) MarshalBSONValue() (bsontype.Type, []byte, error) {
	return bson.MarshalValue(string(r))
}

// UnmarshalBSONValue implements bson.ValueUnmarshaler for ImprintHexString
func (r *ImprintHexString) UnmarshalBSONValue(bsonType bsontype.Type, data []byte) error {
	var s string
	err := bson.UnmarshalValue(bsonType, data, &s)
	if err != nil {
		return err
	}

	id, err := NewImprintHexString(s)
	if err != nil {
		return err
	}
	*r = id
	return nil
}
