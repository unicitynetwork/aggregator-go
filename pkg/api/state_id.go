package api

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/bft-go-base/types"
)

// ImprintV2 is the unified type for both V1 (imprints with algorithm prefix) and V2 (raw bytes)
type ImprintV2 HexBytes

type StateID = ImprintV2
type SourceStateHash = ImprintV2
type TransactionHash = ImprintV2
type RequestID = ImprintV2

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

func (r ImprintV2) IsV1() bool {
	// V1 imprints are 34 bytes (2 prefix + 32 hash)
	// V2 are 32 bytes.
	return len(r) == 34
}

func (r ImprintV2) GetPath() (*big.Int, error) {
	// pad v2 imprints with two zero bytes to maintain consistency in smt
	var path []byte
	if len(r) == 32 {
		path = make([]byte, 34)
		copy(path[2:], r)
	} else {
		path = r[:]
	}

	// Converts StateID hex string to a big.Int for use as an SMT path.
	// Prefixes with "0x01" to preserve leading zero bits in the original hex string,
	// ensuring consistent path representation in the Sparse Merkle Tree.
	b := append([]byte{0x01}, path...)
	return new(big.Int).SetBytes(b), nil
}

func (r ImprintV2) Algorithm() []byte {
	if r.IsV1() {
		return r[0:2]
	}
	return []byte{0, 0}
}

func (r ImprintV2) Imprint() []byte {
	return r
}

func (r ImprintV2) Bytes() []byte {
	return r.Imprint()
}

func (r ImprintV2) DataBytes() []byte {
	if r.IsV1() {
		return r[2:]
	}
	return r
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
