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

	stepPaths := make([]*big.Int, len(m.Steps))
	stepData := make([]*[]byte, len(m.Steps))
	for i, step := range m.Steps {
		parsedPath, ok := new(big.Int).SetString(step.Path, 10)
		if !ok || parsedPath.Sign() < 0 {
			return nil, fmt.Errorf("invalid step path '%s'", step.Path)
		}

		var parsedData *[]byte
		if step.Data != nil {
			data, err := hex.DecodeString(*step.Data)
			if err != nil {
				return nil, fmt.Errorf("invalid step data '%s': %w", *step.Data, err)
			}
			parsedData = &data
		}
		stepPaths[i] = parsedPath
		stepData[i] = parsedData
	}

	// Resolve the leaf path represented by the proof (may differ from stateID
	// for exclusion proofs). We need it to hash leaves as H(0x00 || key || value).
	var leafPath *big.Int
	if len(stepPaths) > 0 {
		leafPath = new(big.Int).Set(stepPaths[0])
		for i := 1; i < len(stepPaths); i++ {
			if leafPath.BitLen() < 2 {
				leafPath = big.NewInt(1)
			}
			pathLen := stepPaths[i].BitLen() - 1
			if pathLen < 0 {
				return nil, fmt.Errorf("invalid path '%s' on step %d", m.Steps[i].Path, i+1)
			}
			mask := new(big.Int).SetBit(new(big.Int).Set(stepPaths[i]), pathLen, 0)
			leafPath.Lsh(leafPath, uint(pathLen))
			leafPath.Or(leafPath, mask)
		}
	}

	var leafKey []byte
	if len(stepPaths) > 0 && stepPaths[0].BitLen() >= 2 {
		var err error
		leafKey, err = PathToFixedBytes(leafPath, leafPath.BitLen()-1)
		if err != nil {
			return nil, fmt.Errorf("invalid leaf path encoding in proof: %w", err)
		}
	}

	fullKeyBits := stateID.BitLen() - 1
	for i := range stepPaths {
		stepPath := stepPaths[i]
		stepBytes := stepData[i]

		if i == 0 {
			if stepPath.BitLen() >= 2 {
				// First step, normal case: data is the value in the leaf, apply
				// the current SMT leaf hashing rule H(0x00 || key || value).
				hasher.Reset().AddData([]byte{0x00}).AddData(leafKey)
				if stepBytes != nil {
					hasher.AddData(*stepBytes)
				}
				currentData = &hasher.GetHash().RawHash
			} else {
				// First step, special case: data is the "our branch" hash value for the next step
				// Note that in this case stepPath is a "naked" direction bit
				currentData = stepBytes
			}
			currentPath = stepPath
		} else {
			// All subsequent steps: apply the non-leaf hashing rule
			var left, right *[]byte
			if currentPath.Bit(0) == 0 {
				// Our branch on the left, sibling on the right
				left = currentData
				right = stepBytes
			} else {
				// Sibling on the left, our branch on the right
				left = stepBytes
				right = currentData
			}

			// Under the current SMT hashing rules, a unary node hash is the child hash.
			if left == nil && right != nil {
				currentData = right
			} else if right == nil && left != nil {
				currentData = left
			} else {
				var depth int
				if currentPath.BitLen() >= 2 {
					depth = fullKeyBits - (currentPath.BitLen() - 1)
				} else {
					// Some MerkleTreePath encodings use only a raw direction
					// bit ("0" or "1") for the current branch. In that case,
					// derive depth from the next step path as a fallback.
					depth = stepPath.BitLen() - 1
				}
				if depth < 0 || depth > 255 {
					return nil, fmt.Errorf("invalid node depth %d on step %d", depth, i+1)
				}

				hasher.Reset().
					AddData([]byte{0x01, byte(depth)})
				if left != nil {
					hasher.AddData(*left)
				}
				if right != nil {
					hasher.AddData(*right)
				}
				currentData = &hasher.GetHash().RawHash
			}

			// Initialization for when currentPath is a "naked" direction bit
			if currentPath.BitLen() < 2 {
				currentPath = big.NewInt(1)
			}
			// Append step path bits to current path
			pathLen := stepPath.BitLen() - 1
			if pathLen < 0 {
				return nil, fmt.Errorf("invalid path '%s' on step %d", m.Steps[i].Path, i+1)
			}
			mask := new(big.Int).SetBit(new(big.Int).Set(stepPath), pathLen, 0)
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
