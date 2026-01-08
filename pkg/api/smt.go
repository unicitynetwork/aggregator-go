package api

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
)

type (
	// MerkleTreeStep represents a single step in a Merkle tree path
	MerkleTreeStep struct {
		Path string  `json:"path"`
		Data *string `json:"data"`
	}

	// MerkleTreePath represents the path to verify inclusion in a Merkle tree
	MerkleTreePath struct {
		Root  string           `json:"root"`
		Steps []MerkleTreeStep `json:"steps"`
	}
)

type PathVerificationResult struct {
	PathValid    bool
	PathIncluded bool
	Result       bool
}

func (m *MerkleTreePath) Verify(stateID *big.Int) (*PathVerificationResult, error) {
	// Extract the algorithm identifier from the root hash imprint
	if len(m.Root) < 4 {
		return nil, fmt.Errorf("invalid root hash format")
	}
	algorithm, err := strconv.ParseUint(m.Root[:4], 16, 16)
	if err != nil {
		return nil, err
	}
	hasher := NewDataHasher(HashAlgorithm(algorithm))
	if hasher == nil {
		return nil, fmt.Errorf("unknown algorithm identifier %s in root hash", m.Root[:4])
	}

	// The "running totals" as we go through the hashing steps
	var currentPath *big.Int
	var currentData *[]byte

	for i, step := range m.Steps {
		stepPath, ok := new(big.Int).SetString(step.Path, 10)
		if !ok || stepPath.Sign() < 0 {
			return nil, fmt.Errorf("invalid step path '%s'", step.Path)
		}

		var stepData *[]byte
		if step.Data != nil {
			data, err := hex.DecodeString(*step.Data)
			if err != nil {
				return nil, fmt.Errorf("invalid step data '%s': %w", *step.Data, err)
			}
			stepData = &data
		}

		if i == 0 {
			if stepPath.BitLen() >= 2 {
				// First step, normal case: data is the value in the leaf, apply the leaf hashing rule
				hasher.Reset().AddData(CborArray(2))
				hasher.AddCborBytes(BigintEncode(stepPath))
				if stepData == nil {
					hasher.AddCborNull()
				} else {
					hasher.AddCborBytes(*stepData)
				}
				currentData = &hasher.GetHash().RawHash
			} else {
				// First step, special case: data is the "our branch" hash value for the next step
				// Note that in this case stepPath is a "naked" direction bit
				currentData = stepData
			}
			currentPath = stepPath
		} else {
			// All subsequent steps: apply the non-leaf hashing rule
			var left, right *[]byte
			if currentPath.Bit(0) == 0 {
				// Our branch on the left, sibling on the right
				left = currentData
				right = stepData
			} else {
				// Sibling on the left, our branch on the right
				left = stepData
				right = currentData
			}

			hasher.Reset().AddData(CborArray(3))
			hasher.AddCborBytes(BigintEncode(stepPath))
			if left == nil {
				hasher.AddCborNull()
			} else {
				hasher.AddCborBytes(*left)
			}
			if right == nil {
				hasher.AddCborNull()
			} else {
				hasher.AddCborBytes(*right)
			}
			currentData = &hasher.GetHash().RawHash

			// Initialization for when currentPath is a "naked" direction bit
			if currentPath.BitLen() < 2 {
				currentPath = big.NewInt(1)
			}
			// Append step path bits to current path
			pathLen := stepPath.BitLen() - 1
			mask := new(big.Int).SetBit(stepPath, pathLen, 0)
			currentPath.Lsh(currentPath, uint(pathLen))
			currentPath.Or(currentPath, mask)
		}
	}

	pathValid := currentData != nil && m.Root == NewDataHash(hasher.algorithm, *currentData).ToHex()
	pathIncluded := currentPath != nil && stateID.Cmp(currentPath) == 0

	return &PathVerificationResult{
		PathValid:    pathValid,
		PathIncluded: pathIncluded,
		Result:       pathValid && pathIncluded,
	}, nil
}
