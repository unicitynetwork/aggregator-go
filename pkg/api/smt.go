package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
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

// HashAlgorithm represents the hashing algorithm to use
type HashAlgorithm int

const (
	SHA256 HashAlgorithm = iota
)

// DataHash represents a hash with algorithm imprint (matches TypeScript DataHash)
type DataHash struct {
	Algorithm HashAlgorithm
	Data      []byte
	Imprint   []byte
}

// NewDataHash creates a api.DataHash with algorithm imprint
func NewDataHash(algorithm HashAlgorithm, data []byte) *DataHash {
	imprint := make([]byte, len(data)+2)
	// Set algorithm bytes (SHA256 = 0, so 0x0000)
	imprint[0] = byte((int(algorithm) & 0xff00) >> 8)
	imprint[1] = byte(int(algorithm) & 0xff)
	copy(imprint[2:], data)

	return &DataHash{
		Algorithm: algorithm,
		Data:      append([]byte(nil), data...),
		Imprint:   imprint,
	}
}

// ToHex returns hex string of imprint (for compatibility)
func (h *DataHash) ToHex() string {
	return fmt.Sprintf("%x", h.Imprint)
}

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
			hash = Sha256Hash(append(pathBytes, bytes...))

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
			currentHash = Sha256Hash(append(siblingHash, hash...))
		} else {
			currentHash = Sha256Hash(append(hash, siblingHash...))
		}
	}

	pathValid := currentHash != nil && m.Root == NewDataHash(SHA256, currentHash).ToHex()
	pathIncluded := requestId.Cmp(currentPath) == 0

	return &PathVerificationResult{
		PathValid:    pathValid,
		PathIncluded: pathIncluded,
		Result:       pathValid && pathIncluded,
	}, nil

}

// sha256Hash performs SHA256 hashing
func Sha256Hash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}
