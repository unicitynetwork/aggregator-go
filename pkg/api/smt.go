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
		Branch  []string `json:"branch"`
		Path    string   `json:"path"`
		Sibling []string `json:"sibling,omitempty"`
	}

	// MerkleTreePath represents the path to verify inclusion in a Merkle tree
	MerkleTreePath struct {
		Root  string           `json:"root"`
		Steps []MerkleTreeStep `json:"steps"`
	}
)

//  public async verify(requestId: bigint): Promise<PathVerificationResult> {
//    let currentPath = 1n;
//    let currentHash: DataHash | null = null;
//
//    for (let i = 0; i < this.steps.length; i++) {
//      const step = this.steps[i];
//      let hash: Uint8Array;
//      if (step.branch === null) {
//        hash = new Uint8Array(1);
//      } else {
//        const bytes = i === 0 ? step.branch.value : currentHash?.data;
//        const digest = await new DataHasher(HashAlgorithm.SHA256)
//          .update(BigintConverter.encode(step.path))
//          .update(bytes ?? new Uint8Array(1))
//          .digest();
//        hash = digest.data;
//
//        const length = BigInt(step.path.toString(2).length - 1);
//        currentPath = (currentPath << length) | (step.path & ((1n << length) - 1n));
//      }
//
//      const siblingHash = step.sibling?.data ?? new Uint8Array(1);
//      const isRight = step.path & 1n;
//      currentHash = await new DataHasher(HashAlgorithm.SHA256)
//        .update(isRight ? siblingHash : hash)
//        .update(isRight ? hash : siblingHash)
//        .digest();
//    }
//
//    return new PathVerificationResult(!!currentHash && this.root.equals(currentHash), requestId === currentPath);
//  }

type PathVerificationResult struct {
	PathValid    bool
	PathIncluded bool
	Result       bool
}

func (m *MerkleTreePath) Verify(requestId *big.Int) (*PathVerificationResult, error) {
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

	currentPath := big.NewInt(1)
	var currentHash []byte

	for i, step := range m.Steps {
		var hash []byte
		path, ok := new(big.Int).SetString(step.Path, 10)
		if !ok {
			return nil, fmt.Errorf("invalid path '%s'", step.Path)
		}

		if step.Branch == nil {
			// TypeScript: if (step.branch === null) { hash = new Uint8Array(1); }
			hash = []byte{0}
			// Don't update currentPath for null branches
		} else {
			// Branch is not nil (could be empty or have value)
			// TypeScript: const bytes = i === 0 ? step.branch.value : currentHash?.data;
			var bytes []byte

			if i == 0 && len(step.Branch) > 0 && step.Branch[0] != "" {
				// First step with branch value
				var err error
				bytes, err = hex.DecodeString(step.Branch[0])
				if err != nil {
					return nil, fmt.Errorf("failed to decode branch hex '%s': %w", step.Branch[0], err)
				}
			} else if currentHash != nil {
				// Use currentHash for subsequent steps or when no branch value
				bytes = currentHash
			} else {
				// Default to empty byte
				bytes = []byte{0}
			}

			// Always calculate hash for non-null branches
			pathBytes := BigintEncode(path)
			hash = hasher.Reset().AddData(pathBytes).AddData(bytes).GetHash().RawHash

			// Update currentPath for all non-null branches (even empty ones)
			// TypeScript: const length = BigInt(step.path.toString(2).length - 1);
			// Note: path.Text(2) returns "0" for 0, which has length 1
			binaryStr := path.Text(2)
			length := len(binaryStr) - 1
			if length < 0 {
				length = 0 // Protect against negative length
			}
			// Shift left by path length
			currentPath = new(big.Int).Lsh(currentPath, uint(length))
			// ((1n << length) - 1n)
			mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(length)), big.NewInt(1))
			// (currentPath << length) | (step.path & ((1n << length) - 1n))
			currentPath.Or(currentPath, new(big.Int).And(path, mask))
		}

		siblingHash := []byte{0} // Default empty sibling hash
		if len(step.Sibling) > 0 && step.Sibling[0] != "" {
			// Sibling is now just hash data without algorithm prefix, decode directly
			var err error
			siblingHash, err = hex.DecodeString(step.Sibling[0])
			if err != nil {
				return nil, fmt.Errorf("failed to decode sibling hash '%s': %w", step.Sibling[0], err)
			}
		}
		isRight := path.Bit(0) == 1

		if isRight {
			currentHash = hasher.Reset().AddData(siblingHash).AddData(hash).GetHash().RawHash
		} else {
			currentHash = hasher.Reset().AddData(hash).AddData(siblingHash).GetHash().RawHash
		}
	}

	pathValid := currentHash != nil && m.Root == NewDataHash(hasher.GetAlgorithm(), currentHash).ToHex()
	pathIncluded := requestId.Cmp(currentPath) == 0

	return &PathVerificationResult{
		PathValid:    pathValid,
		PathIncluded: pathIncluded,
		Result:       pathValid && pathIncluded,
	}, nil

}
