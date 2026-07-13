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

// HashNode implements the yellowpaper v6a (Appendix C.3.2.2) internal-node
// hash: H(0x01 || depth_1B || region_32B || left_hash_32B || right_hash_32B).
// The region is the node's absolute key prefix at its bifurcation depth,
// packed big-endian with all bits at positions >= depth cleared.
func HashNode(left, right Hash, depth uint8, region PrefixBits) Hash {
	h := sha256.New()
	_, _ = h.Write([]byte{0x01, depth})
	_, _ = h.Write(region[:])
	_, _ = h.Write(left[:])
	_, _ = h.Write(right[:])
	var out Hash
	h.Sum(out[:0])
	return out
}

// RegionFromKey packs the depth-bit prefix of a descendant key into the
// canonical v6a region encoding: key bits 0..depth-1 in place, all bits at
// positions >= depth cleared. Under big-endian bit ordering the region shares
// the key's byte layout, so it is the key bytes with the depth suffix masked.
func RegionFromKey(key Key, depth uint8) PrefixBits {
	var region PrefixBits
	d := int(depth)
	byteLen := (d + 7) / 8
	copy(region[:byteLen], key[:byteLen])
	api.ClearSuffixBE(region[:], d)
	return region
}

// EmptyRootHash is the v6a empty-tree root (spec: root is ⊥): the all-zero
// hash, matching the JS/Java SDK v6a implementations. Unary roots with one
// child use the child hash directly.
func EmptyRootHash() Hash {
	return Hash{}
}

// KeyBit returns bit d of key using the yellowpaper big-endian bit layout:
// bit 0 is the MSB of byte 0, bit d = (key[d/8] >> (7 - d%8)) & 1. This is the
// bounds-checked disk-typed accessor over api.KeyBitBE.
func KeyBit(key Key, d int) byte {
	if d < 0 || d >= KeyBits {
		panic(fmt.Sprintf("disk smt: key bit index out of range: %d", d))
	}
	return api.KeyBitBE(key[:], d)
}
