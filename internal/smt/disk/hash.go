package disk

import (
	"crypto/sha256"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	KeyBits  = api.StateTreeKeyLengthBits
	KeySize  = api.StateTreeKeyLengthBytes
	HashSize = api.SiblingSize
)

type Key [KeySize]byte
type Hash [HashSize]byte

func KeyFromBytes(data []byte) (Key, error) {
	var key Key
	if len(data) != KeySize {
		return key, fmt.Errorf("disk smt: invalid key length: got %d, want %d", len(data), KeySize)
	}
	copy(key[:], data)
	return key, nil
}

func HashFromBytes(data []byte) (Hash, error) {
	var hash Hash
	if len(data) != HashSize {
		return hash, fmt.Errorf("disk smt: invalid hash length: got %d, want %d", len(data), HashSize)
	}
	copy(hash[:], data)
	return hash, nil
}

// HashLeaf implements the yellowpaper SMT leaf hash:
// H(0x00 || key_32B || value).
func HashLeaf(key Key, value []byte) Hash {
	h := sha256.New()
	_, _ = h.Write([]byte{0x00})
	_, _ = h.Write(key[:])
	_, _ = h.Write(value)
	var out Hash
	h.Sum(out[:0])
	return out
}

// HashNode implements the yellowpaper SMT internal-node hash:
// H(0x01 || depth_1B || left_hash_32B || right_hash_32B).
func HashNode(left, right Hash, depth uint8) Hash {
	h := sha256.New()
	_, _ = h.Write([]byte{0x01, depth})
	_, _ = h.Write(left[:])
	_, _ = h.Write(right[:])
	var out Hash
	h.Sum(out[:0])
	return out
}

// EmptyRootHash matches the current Go v2 memory SMT empty-root rule:
// H(0x01 || 0x00). Unary roots with one child use the child hash directly.
func EmptyRootHash() Hash {
	h := sha256.New()
	_, _ = h.Write([]byte{0x01, 0x00})
	var out Hash
	h.Sum(out[:0])
	return out
}

// KeyBit returns bit d of key using the yellowpaper/Go v2 LSB-first key layout.
func KeyBit(key Key, d int) byte {
	if d < 0 || d >= KeyBits {
		panic(fmt.Sprintf("disk smt: key bit index out of range: %d", d))
	}
	return (key[d/8] >> (uint(d) % 8)) & 1
}
