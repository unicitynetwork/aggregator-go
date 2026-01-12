package api

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/bft-go-base/types"
)

type (
	merkleTreePathCBOR struct {
		_     struct{} `cbor:",toarray"`
		Root  []byte   // hex string
		Steps []merkleTreeStepCBOR
	}

	merkleTreeStepCBOR struct {
		_    struct{} `cbor:",toarray"`
		Path []byte   // base-10 BigInt
		Data []byte   // hex string
	}
)

func (m MerkleTreePath) MarshalCBOR() ([]byte, error) {
	pathCBOR, err := m.toCBOR()
	if err != nil {
		return nil, fmt.Errorf("failed to convert merkle tree path to cbor struct: %w", err)
	}
	return types.Cbor.Marshal(pathCBOR)
}

func (m *MerkleTreePath) UnmarshalCBOR(data []byte) error {
	var pathCBOR merkleTreePathCBOR
	if err := types.Cbor.Unmarshal(data, &pathCBOR); err != nil {
		return err
	}

	m.Root = hex.EncodeToString(pathCBOR.Root)

	m.Steps = make([]MerkleTreeStep, len(pathCBOR.Steps))
	for i, step := range pathCBOR.Steps {
		m.Steps[i] = step.fromCBOR()
	}

	return nil
}

func (m MerkleTreePath) toCBOR() (*merkleTreePathCBOR, error) {
	rootBytes, err := hex.DecodeString(m.Root)
	if err != nil {
		return nil, fmt.Errorf("invalid root hex string: %w", err)
	}

	steps := make([]merkleTreeStepCBOR, len(m.Steps))
	for i, step := range m.Steps {
		steps[i], err = step.toCBOR()
		if err != nil {
			return nil, fmt.Errorf("step %d: %w", i, err)
		}
	}

	return &merkleTreePathCBOR{
		Root:  rootBytes,
		Steps: steps,
	}, nil
}

func (s MerkleTreeStep) toCBOR() (merkleTreeStepCBOR, error) {
	n, ok := new(big.Int).SetString(s.Path, 10)
	if !ok {
		return merkleTreeStepCBOR{}, fmt.Errorf("failed to convert path string to bigint base10: %s", s.Path)
	}

	var data []byte
	if s.Data != nil {
		var err error
		data, err = hex.DecodeString(*s.Data)
		if err != nil {
			return merkleTreeStepCBOR{}, fmt.Errorf("invalid data hex string: %w", err)
		}
	}

	return merkleTreeStepCBOR{
		Path: n.Bytes(),
		Data: data,
	}, nil
}

func (s merkleTreeStepCBOR) fromCBOR() MerkleTreeStep {
	n := new(big.Int).SetBytes(s.Path)

	var data *string
	if len(s.Data) > 0 {
		str := hex.EncodeToString(s.Data)
		data = &str
	}

	return MerkleTreeStep{
		Path: n.String(),
		Data: data,
	}
}
