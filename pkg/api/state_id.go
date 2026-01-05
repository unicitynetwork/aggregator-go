package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/bft-go-base/types"
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

func (r ImprintHexString) MarshalCBOR() ([]byte, error) {
	imprintBytes, err := r.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ImprintHexString: %w", err)
	}
	return types.Cbor.Marshal(imprintBytes)
}

func (r *ImprintHexString) UnmarshalCBOR(cborBytes []byte) error {
	var imprintBytes []byte
	if err := types.Cbor.Unmarshal(cborBytes, &imprintBytes); err != nil {
		return fmt.Errorf("failed to unmarshal ImprintHexString: %w", err)
	}
	res, err := NewImprintHexStringFromBytes(imprintBytes)
	if err != nil {
		return fmt.Errorf("failed to create ImprintHexString from bytes: %w", err)
	}
	*r = res
	return nil
}

// CreateStateID creates a StateID from source state hash and owner predicate
func CreateStateID(ownerPredicate Predicate, sourceStateHash SourceStateHash) (StateID, error) {
	sourceStateHashImprint, err := sourceStateHash.Imprint()
	if err != nil {
		return "", fmt.Errorf("failed to convert source state hash imprint to bytes: %w", err)
	}
	return CreateStateIDFromImprint(ownerPredicate, sourceStateHashImprint)
}

// CreateStateIDFromImprint creates a StateID from source state hash imprint and owner predicate bytes
func CreateStateIDFromImprint(ownerPredicate Predicate, sourceStateHashImprint []byte) (StateID, error) {
	dataHash, err := StateIDDataHash(ownerPredicate, sourceStateHashImprint)
	if err != nil {
		return "", err
	}
	return NewImprintHexString(dataHash.ToHex())
}

func StateIDDataHash(ownerPredicate Predicate, sourceStateHashImprint []byte) (*DataHash, error) {
	predicateBytes, err := types.Cbor.Marshal(ownerPredicate)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal owner predicate: %w", err)
	}

	return NewDataHasher(SHA256).
		AddData(CborArray(2)).
		AddData(predicateBytes).
		AddCborBytes(sourceStateHashImprint).
		GetHash(), nil
}

func ValidateStateID(stateID StateID, sourceStateHashImprint []byte, ownerPredicate Predicate) (bool, error) {
	expectedStateID, err := CreateStateIDFromImprint(ownerPredicate, sourceStateHashImprint)
	if err != nil {
		return false, err
	}
	return stateID == expectedStateID, nil
}
