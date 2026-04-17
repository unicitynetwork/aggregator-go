package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// SiblingSize is the fixed byte length of each sibling hash and of the
// leaf key / value hashes in an InclusionCert or ExclusionCert wire
// encoding. All supported SMT hash algorithms (SHA-256, SHA-3-256)
// produce 32-byte digests.
const SiblingSize = 32

// BitmapSize is the fixed byte length of the depth bitmap. The SMT
// key is StateTreeKeyLengthBits (256) bits, so the bitmap is 32 bytes.
const BitmapSize = StateTreeKeyLengthBytes

// maxDepth is the exclusive upper bound on tree depth indices. Depth
// values walked by the verifier are in [0, maxDepth).
const maxDepth = StateTreeKeyLengthBits

// Errors returned by certificate decoding and verification.
var (
	ErrCertTruncated        = errors.New("inclusion cert: truncated")
	ErrCertMisalignedSibs   = errors.New("inclusion cert: sibling bytes not aligned to 32")
	ErrCertBitmapMismatch   = errors.New("inclusion cert: sibling count does not match bitmap popcount")
	ErrCertRootMismatch     = errors.New("inclusion cert: root mismatch")
	ErrCertSiblingUnderflow = errors.New("inclusion cert: sibling underflow during verification")
	ErrCertKeyLength        = errors.New("inclusion cert: invalid key length")
	ErrCertRootLength       = errors.New("inclusion cert: invalid root length")
	ErrCertUnknownAlgo      = errors.New("inclusion cert: unknown hash algorithm")
	ErrExclusionNotImpl     = errors.New("exclusion cert: verification not yet implemented")
)

// InclusionCert is the decoded v2 inclusion certificate.
//
// Wire format (raw binary, no framing):
//
//	bitmap[32] || s_1[32] || ... || s_n[32]
//
// where n = popcount(bitmap). Siblings are in generation order
// (root-to-leaf): s_1 is the sibling at the shallowest depth with a
// bitmap bit set, s_n at the deepest. Verification walks depths
// 255..0 and consumes siblings from the end of the slice.
//
// The certificate carries no root, no key, and no value. Verification
// requires these to be supplied from the outer proof tuple:
//   - key  (sid)   — from the RPC request parameter.
//   - value (txhash) — from CertificationData.TransactionHash.
//   - root         — from UC.IR.h.
//
// See docs/inclusion-proof-wire.md for the full specification.
type InclusionCert struct {
	Bitmap   [BitmapSize]byte
	Siblings [][SiblingSize]byte
}

// MarshalBinary encodes the certificate to its wire form.
func (c *InclusionCert) MarshalBinary() ([]byte, error) {
	out := make([]byte, 0, BitmapSize+len(c.Siblings)*SiblingSize)
	out = append(out, c.Bitmap[:]...)
	for i := range c.Siblings {
		out = append(out, c.Siblings[i][:]...)
	}
	return out, nil
}

// UnmarshalBinary decodes the wire form into the certificate. The
// sibling count is validated against the bitmap popcount.
func (c *InclusionCert) UnmarshalBinary(data []byte) error {
	if len(data) < BitmapSize {
		return ErrCertTruncated
	}
	copy(c.Bitmap[:], data[:BitmapSize])
	rest := data[BitmapSize:]
	if len(rest)%SiblingSize != 0 {
		return ErrCertMisalignedSibs
	}
	actual := len(rest) / SiblingSize
	expected := bitmapPopcount(&c.Bitmap)
	if actual != expected {
		return fmt.Errorf("%w: have %d, want %d", ErrCertBitmapMismatch, actual, expected)
	}
	c.Siblings = make([][SiblingSize]byte, actual)
	for i := 0; i < actual; i++ {
		copy(c.Siblings[i][:], rest[i*SiblingSize:(i+1)*SiblingSize])
	}
	return nil
}

// Verify checks that applying the bitmap + siblings path on top of
// H_leaf(key, value) reproduces expectedRoot under the given hash
// algorithm.
//
// Parameters:
//   - key:          32-byte SMT key, LSB-first layout.
//   - value:        raw leaf value bytes (v2 inclusion proofs use the tx hash).
//   - expectedRoot: raw 32-byte root hash, taken from UC.IR.h.
//   - algo:         hash algorithm used by the SMT.
func (c *InclusionCert) Verify(key, value, expectedRoot []byte, algo HashAlgorithm) error {
	if len(key) != StateTreeKeyLengthBytes {
		return fmt.Errorf("%w: got %d, want %d", ErrCertKeyLength, len(key), StateTreeKeyLengthBytes)
	}

	// Leaf hash: H(0x00 || key || value).
	hasher := NewDataHasher(algo)
	if hasher == nil {
		return fmt.Errorf("%w: %d", ErrCertUnknownAlgo, algo)
	}
	hasher.Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(value)
	h := hasher.GetHash().RawHash

	return verifyBitmapPath(&c.Bitmap, c.Siblings, key, h, expectedRoot, algo)
}

func verifyBitmapPath(bitmap *[BitmapSize]byte, siblings [][SiblingSize]byte, key, startHash, expectedRoot []byte, algo HashAlgorithm) error {
	if len(startHash) != SiblingSize {
		return fmt.Errorf("%w: got %d, want %d", ErrCertRootLength, len(startHash), SiblingSize)
	}
	if len(expectedRoot) != SiblingSize {
		return fmt.Errorf("%w: got %d, want %d", ErrCertRootLength, len(expectedRoot), SiblingSize)
	}
	hasher := NewDataHasher(algo)
	if hasher == nil {
		return fmt.Errorf("%w: %d", ErrCertUnknownAlgo, algo)
	}

	// Walk depths from deepest to shallowest, consuming siblings from
	// the end of the slice. Depths with bitmap bit clear are skipped
	// (unary passthrough or off-path).
	h := append([]byte(nil), startHash...)
	j := len(siblings)
	for d := maxDepth - 1; d >= 0; d-- {
		if ((*bitmap)[d/8]>>(uint(d)%8))&1 == 0 {
			continue
		}
		if d/8 >= len(key) {
			return fmt.Errorf("%w: key too short for depth %d", ErrCertKeyLength, d)
		}
		if j == 0 {
			return ErrCertSiblingUnderflow
		}
		j--
		sibling := siblings[j][:]

		hasher.Reset().AddData([]byte{0x01, byte(d)})
		if keyBitAt(key, d) == 1 {
			// Descent went right at depth d → sibling is the left child.
			hasher.AddData(sibling).AddData(h)
		} else {
			// Descent went left at depth d → sibling is the right child.
			hasher.AddData(h).AddData(sibling)
		}
		h = hasher.GetHash().RawHash
	}
	if j != 0 {
		return fmt.Errorf("%w: %d siblings unused", ErrCertBitmapMismatch, j)
	}
	if !bytes.Equal(h, expectedRoot) {
		return ErrCertRootMismatch
	}
	return nil
}

// ExclusionCert is the decoded v2 non-inclusion certificate.
//
// Wire format (raw binary, no framing):
//
//	k_l[32] || h_l[32] || bitmap[32] || s_1[32] || ... || s_n[32]
//
// (k_l, h_l) is the witness leaf present in the tree at the position
// reached when routing the query key. bitmap + siblings describe the
// proof path from the root to that position, under the same root-to-
// leaf sibling ordering as InclusionCert.
//
// Verification semantics are not yet implemented in Go. The type and
// codec are frozen so clients can decode today; see
// docs/inclusion-proof-wire.md.
type ExclusionCert struct {
	KL       [SiblingSize]byte
	HL       [SiblingSize]byte
	Bitmap   [BitmapSize]byte
	Siblings [][SiblingSize]byte
}

// MarshalBinary encodes the exclusion certificate to its wire form.
func (c *ExclusionCert) MarshalBinary() ([]byte, error) {
	out := make([]byte, 0, 2*SiblingSize+BitmapSize+len(c.Siblings)*SiblingSize)
	out = append(out, c.KL[:]...)
	out = append(out, c.HL[:]...)
	out = append(out, c.Bitmap[:]...)
	for i := range c.Siblings {
		out = append(out, c.Siblings[i][:]...)
	}
	return out, nil
}

// UnmarshalBinary decodes the wire form into the exclusion certificate.
// The sibling count is validated against the bitmap popcount.
func (c *ExclusionCert) UnmarshalBinary(data []byte) error {
	const head = 2*SiblingSize + BitmapSize
	if len(data) < head {
		return ErrCertTruncated
	}
	copy(c.KL[:], data[:SiblingSize])
	copy(c.HL[:], data[SiblingSize:2*SiblingSize])
	copy(c.Bitmap[:], data[2*SiblingSize:head])
	rest := data[head:]
	if len(rest)%SiblingSize != 0 {
		return ErrCertMisalignedSibs
	}
	actual := len(rest) / SiblingSize
	expected := bitmapPopcount(&c.Bitmap)
	if actual != expected {
		return fmt.Errorf("%w: have %d, want %d", ErrCertBitmapMismatch, actual, expected)
	}
	c.Siblings = make([][SiblingSize]byte, actual)
	for i := 0; i < actual; i++ {
		copy(c.Siblings[i][:], rest[i*SiblingSize:(i+1)*SiblingSize])
	}
	return nil
}

// Verify is not yet implemented for exclusion certificates. The wire
// schema is frozen so clients can decode.
func (c *ExclusionCert) Verify(queryKey, expectedRoot []byte, algo HashAlgorithm) error {
	return ErrExclusionNotImpl
}

// bitmapPopcount counts the set bits in the 32-byte depth bitmap.
func bitmapPopcount(b *[BitmapSize]byte) int {
	total := 0
	for i := 0; i < BitmapSize; i += 8 {
		total += bits.OnesCount64(binary.LittleEndian.Uint64(b[i:]))
	}
	return total
}

// keyBitAt returns bit d of key under LSB-first byte layout:
// bit d is bit (d mod 8) of key[d / 8]. Matches PathToFixedBytes /
// FixedBytesToPath in state_id.go.
func keyBitAt(key []byte, d int) byte {
	return (key[d/8] >> (uint(d) % 8)) & 1
}
