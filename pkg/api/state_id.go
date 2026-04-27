package api

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/bft-go-base/types"
)

// ImprintV2 stores hash-like identifiers used by the public API.
type ImprintV2 HexBytes

type StateID = ImprintV2
type SourceStateHash = ImprintV2
type TransactionHash = ImprintV2

const (
	// StateTreeKeyLengthBits is the v2 SMT key size.
	// The key is the raw 32-byte hash value (no per-key algorithm prefix).
	StateTreeKeyLengthBits  = 256
	StateTreeKeyLengthBytes = StateTreeKeyLengthBits / 8
)

func NewImprintV2(s string) (ImprintV2, error) {
	b, err := NewHexBytesFromString(s)
	if err != nil {
		return nil, err
	}
	return []byte(b), nil
}

// RequireNewImprintV2 is a helper for tests that panics on error
func RequireNewImprintV2(s string) StateID {
	id, err := NewImprintV2(s)
	if err != nil {
		panic(err)
	}
	return id
}

func (r ImprintV2) String() string {
	return (HexBytes)(r).String()
}

func (r ImprintV2) GetPath() (*big.Int, error) {
	key, err := r.GetTreeKey()
	if err != nil {
		return nil, err
	}
	return FixedBytesToPath(key, StateTreeKeyLengthBits)
}

// GetTreeKey returns the canonical SMT key bytes (32 bytes, no algorithm prefix).
func (r ImprintV2) GetTreeKey() ([]byte, error) {
	key := r.DataBytes()
	if len(key) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid imprint length for SMT key: expected %d bytes key data, got %d", StateTreeKeyLengthBytes, len(key))
	}
	return append([]byte(nil), key...), nil
}

// PathToFixedBytes converts a sentinel-prefixed SMT path into fixed-width key bytes.
// Byte order follows the v2 SMT bit layout:
// key bit d is bit (d%8) of key[d/8] (LSB-first across bytes).
func PathToFixedBytes(path *big.Int, keyLengthBits int) ([]byte, error) {
	if keyLengthBits <= 0 {
		return nil, fmt.Errorf("invalid key length: %d", keyLengthBits)
	}
	if path == nil || path.Sign() <= 0 {
		return nil, fmt.Errorf("invalid path: must be positive")
	}
	if path.BitLen()-1 != keyLengthBits {
		return nil, fmt.Errorf("invalid path length: expected %d bits, got %d", keyLengthBits, path.BitLen()-1)
	}

	keyLengthBytes := (keyLengthBits + 7) / 8
	keyInt := new(big.Int).Set(path)
	// Clear sentinel bit (the highest bit at index keyLengthBits).
	keyInt.SetBit(keyInt, keyLengthBits, 0)

	beKey := keyInt.Bytes()
	if len(beKey) > keyLengthBytes {
		return nil, fmt.Errorf("path bytes too long: expected at most %d bytes, got %d", keyLengthBytes, len(beKey))
	}

	bePadded := make([]byte, keyLengthBytes)
	copy(bePadded[keyLengthBytes-len(beKey):], beKey)

	out := make([]byte, keyLengthBytes)
	for i := range out {
		// Convert from big-endian integer bytes to LSB-first SMT key byte order.
		out[i] = bePadded[keyLengthBytes-1-i]
	}
	return out, nil
}

// FixedBytesToPath converts fixed-width SMT key bytes into sentinel-prefixed path form.
func FixedBytesToPath(key []byte, keyLengthBits int) (*big.Int, error) {
	if keyLengthBits <= 0 {
		return nil, fmt.Errorf("invalid key length: %d", keyLengthBits)
	}
	keyLengthBytes := (keyLengthBits + 7) / 8
	if len(key) != keyLengthBytes {
		return nil, fmt.Errorf("invalid key length in bytes: expected %d, got %d", keyLengthBytes, len(key))
	}

	// For non-byte-aligned keys, ensure unused high bits are zero in the last
	// (highest-index) byte under LSB-first key-byte ordering.
	if rem := keyLengthBits % 8; rem != 0 {
		mask := byte(0xFF << rem)
		if key[keyLengthBytes-1]&mask != 0 {
			return nil, fmt.Errorf("invalid key: unused high bits must be zero")
		}
	}

	// Convert from LSB-first SMT key byte order to big-endian integer bytes.
	be := make([]byte, keyLengthBytes)
	for i := range be {
		be[i] = key[keyLengthBytes-1-i]
	}

	path := new(big.Int).SetBytes(be)
	path.SetBit(path, keyLengthBits, 1)
	return path, nil
}

func (r ImprintV2) Imprint() []byte {
	return r
}

func (r ImprintV2) Bytes() []byte {
	return r.Imprint()
}

func (r ImprintV2) DataBytes() []byte {
	return append([]byte(nil), r...)
}

func (r ImprintV2) MarshalJSON() ([]byte, error) {
	return HexBytes(r).MarshalJSON()
}

func (r *ImprintV2) UnmarshalJSON(data []byte) error {
	return (*HexBytes)(r).UnmarshalJSON(data)
}

func (r ImprintV2) MarshalCBOR() ([]byte, error) {
	return types.Cbor.Marshal([]byte(r))
}

func (r *ImprintV2) UnmarshalCBOR(cborBytes []byte) error {
	var b []byte
	if err := types.Cbor.Unmarshal(cborBytes, &b); err != nil {
		return err
	}
	*r = b
	return nil
}

// CreateStateID creates a StateID from source state hash and owner predicate
func CreateStateID(ownerPredicate Predicate, sourceStateHash SourceStateHash) (StateID, error) {
	dataHash, err := StateIDDataHash(ownerPredicate, sourceStateHash)
	if err != nil {
		return nil, err
	}
	return dataHash.RawHash, nil
}

func StateIDDataHash(ownerPredicate Predicate, sourceStateHash []byte) (*DataHash, error) {
	predicateBytes, err := types.Cbor.Marshal(ownerPredicate)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal owner predicate: %w", err)
	}

	return NewDataHasher(SHA256).
		AddData(CborArray(2)).
		AddData(predicateBytes).
		AddCborBytes(sourceStateHash).
		GetHash(), nil
}

func ValidateStateID(stateID StateID, sourceStateHash SourceStateHash, ownerPredicate Predicate) (bool, error) {
	expectedStateID, err := CreateStateID(ownerPredicate, sourceStateHash)
	if err != nil {
		return false, err
	}
	return bytes.Equal(stateID, expectedStateID), nil
}
