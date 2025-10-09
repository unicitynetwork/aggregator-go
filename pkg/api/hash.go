package api

import (
	"crypto/sha256"
	"crypto/sha3"
	"fmt"
	"hash"
	"log"
)

// HashAlgorithm identifies a hashing algorithm
type HashAlgorithm int

// Identifiers of known/supported hashing algorithms
const (
	SHA256   HashAlgorithm = 0 // SHA-2-256
	SHA3_256 HashAlgorithm = 1 // SHA-3-256
)

// DataHash represents a hash value combined with the algorithm identifier
type DataHash struct {
	Algorithm HashAlgorithm
	RawHash   []byte // Raw hash value
	imprint   []byte // Combines the algorithm identifier and the hash value
}

// NewDataHash creates a DataHash from an algorithm identifier and a hash value
func NewDataHash(algorithm HashAlgorithm, hash []byte) *DataHash {
	return &DataHash{
		Algorithm: algorithm,
		RawHash:   append([]byte(nil), hash...),
		// Imprint is computed on demand
	}
}

// GetImprint computes and caches the imprint representation of the hash value
func (h *DataHash) GetImprint() []byte {
	if h.imprint == nil {
		algorithm := uint(h.Algorithm)
		h.imprint = make([]byte, len(h.RawHash)+2)
		h.imprint[0] = byte(algorithm >> 8 & 0xff)
		h.imprint[1] = byte(algorithm & 0xff)
		copy(h.imprint[2:], h.RawHash)
	}
	return h.imprint
}

// ToHex returns the hex string representation of the hash imprint
func (h *DataHash) ToHex() string {
	return fmt.Sprintf("%x", h.GetImprint())
}

// DataHasher wraps a hash algorithm identifier and a corresponding hash function object
type DataHasher struct {
	algorithm HashAlgorithm // Identifies the hash algorithm
	hasher    hash.Hash     // The hasher object used internally
}

// NewDataHasher creates a new DataHasher using the given algorithm
func NewDataHasher(algorithm HashAlgorithm) *DataHasher {
	switch algorithm {
	case SHA256:
		return &DataHasher{
			algorithm: algorithm,
			hasher:    sha256.New(),
		}
	case SHA3_256:
		return &DataHasher{
			algorithm: algorithm,
			hasher:    sha3.New256(),
		}
	default:
		log.Printf("Unknown hash algorithm identifier %d\n", algorithm)
		return nil
	}
}

// GetAlgorithm returns the algorithm identifier
func (h *DataHasher) GetAlgorithm() HashAlgorithm {
	return h.algorithm
}

// AddData adds data to the hasher, returns the hasher for easy call chaining
func (h *DataHasher) AddData(data []byte) *DataHasher {
	h.hasher.Write(data) // hash.Hash.Write promises to never return errors
	return h
}

// AddCborBytes adds data to the hasher as CBOR byte string, returns the hasher for easy call chaining
func (h *DataHasher) AddCborBytes(data []byte) *DataHasher {
	return h.AddData(CborBytes(len(data))).AddData(data)
}

// GetHash finalizes the computation and returns the hash value
func (h *DataHasher) GetHash() *DataHash {
	return NewDataHash(h.algorithm, h.hasher.Sum(nil))
}

// Reset resets the hasher to initial state, returns the hasher for easy call chaining
func (h *DataHasher) Reset() *DataHasher {
	h.hasher.Reset()
	return h
}
