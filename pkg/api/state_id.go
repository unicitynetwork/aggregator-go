package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
)

type StateID = ImprintHexString

// ImprintHexString represents a hex string (4 chars (two bytes) algorithm + n-byte chars hash)
type ImprintHexString string

func (r StateID) GetPath() (*big.Int, error) {
	// Converts StateID hex string to a big.Int for use as an SMT path.
	// Prefixes with "0x01" to preserve leading zero bits in the original hex string,
	// ensuring consistent path representation in the Sparse Merkle Tree.
	path, ok := new(big.Int).SetString("0x01"+string(r), 0)
	if !ok {
		return nil, fmt.Errorf("failed to convert stateID %s to path", r)
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

// CreateStateID creates a StateID from source state hash and public key
func CreateStateID(sourceStateHash SourceStateHash, publicKey HexBytes) (StateID, error) {
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	if err != nil {
		return "", fmt.Errorf("failed to convert source state hash imprint to bytes: %w", err)
	}
	return CreateStateIDFromImprint(sourceStateHashImprint, publicKey)
}

// CreateStateIDFromImprint creates a StateID from source state hash imprint and public key bytes
func CreateStateIDFromImprint(sourceStateHashImprint []byte, publicKey []byte) (StateID, error) {
	dataHash := StateIDDataHash(sourceStateHashImprint, publicKey)
	return NewImprintHexString(dataHash.ToHex())
}

func StateIDDataHash(sourceStateHashImprint []byte, publicKey []byte) *DataHash {
	return NewDataHasher(SHA256).
		AddData(CborArray(2)).
		AddCborBytes(sourceStateHashImprint).
		AddCborBytes(publicKey).
		GetHash()
}

func ValidateStateID(stateID StateID, sourceStateHashImprint []byte, publicKey []byte) (bool, error) {
	expectedStateID, err := CreateStateIDFromImprint(sourceStateHashImprint, publicKey)
	if err != nil {
		return false, err
	}
	return stateID == expectedStateID, nil
}
