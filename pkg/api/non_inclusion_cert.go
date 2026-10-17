package api

import (
	"bytes"
	"errors"
	"fmt"
)

// Non-inclusion certificate (get_non_inclusion_proof.v1).
//
// The certificate authenticates the terminal leaf reached by walking the queried key
// at every actual junction of the compressed radix tree; that terminal must be a
// different leaf. Wire (raw binary, no framing), bitmap first:
//
//	empty byte string                        the certified empty tree
//	bitmap[32] || sibling[32]*popcount(bitmap) || terminalKey[32] || terminalValue[32]
//
// Siblings are root to leaf. The query key is NOT part of the wire: callers bind it
// locally. Leaf hash H(00||key||value), node hash H(01||depth||region(terminalKey)||l||r),
// identical to InclusionCert. This type is distinct from ExclusionCert (terminal first,
// never verifiable), which it supersedes for non-inclusion.
const (
	// NonInclusionTerminalValueSize is the fixed terminal value size of the aggregator profile.
	NonInclusionTerminalValueSize = 32
	// MaxNonInclusionCertBytes is the largest valid nonempty certificate: 256 junctions.
	MaxNonInclusionCertBytes = BitmapSize + StateTreeKeyLengthBits*SiblingSize + StateTreeKeyLengthBytes + NonInclusionTerminalValueSize
	// minNonInclusionCertBytes is the bare terminal (zero bitmap).
	minNonInclusionCertBytes = BitmapSize + StateTreeKeyLengthBytes + NonInclusionTerminalValueSize
)

var (
	// ErrNonInclusionMalformed: the bytes are not in the exact certificate profile.
	ErrNonInclusionMalformed = errors.New("non-inclusion cert: malformed")
	// ErrNonInclusionInvalid: the certificate does not authenticate a terminal leaf
	// reached by the query under the root (path, branch direction, root, empty-tree rules).
	ErrNonInclusionInvalid = errors.New("non-inclusion cert: does not authenticate the root for this key")
	// ErrStateIncluded: the authenticated terminal leaf is the query itself, the state is present.
	ErrStateIncluded = errors.New("non-inclusion cert: the state is included at this root")
)

// NonInclusionCert is the decoded certificate. The zero value is not valid: use
// EmptyTreeNonInclusionCert, NewNonInclusionCert or UnmarshalBinary.
type NonInclusionCert struct {
	set  bool
	body *nonInclusionBody // nil: certified empty tree
}

type nonInclusionBody struct {
	bitmap   [BitmapSize]byte
	siblings [][SiblingSize]byte
	key      [StateTreeKeyLengthBytes]byte
	value    [NonInclusionTerminalValueSize]byte
}

// EmptyTreeNonInclusionCert is the distinguished certificate of the empty tree.
func EmptyTreeNonInclusionCert() *NonInclusionCert { return &NonInclusionCert{set: true} }

// NewNonInclusionCert builds a nonempty certificate. Siblings are root to leaf and
// their number must equal the bitmap popcount. Inputs are copied.
func NewNonInclusionCert(bitmap [BitmapSize]byte, siblings [][SiblingSize]byte, terminalKey, terminalValue []byte) (*NonInclusionCert, error) {
	if want := bitmapPopcount(&bitmap); len(siblings) != want {
		return nil, fmt.Errorf("%w: %d siblings for a bitmap of %d bits", ErrNonInclusionMalformed, len(siblings), want)
	}
	if len(terminalKey) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("%w: terminal key of %d bytes", ErrNonInclusionMalformed, len(terminalKey))
	}
	if len(terminalValue) != NonInclusionTerminalValueSize {
		return nil, fmt.Errorf("%w: terminal value of %d bytes", ErrNonInclusionMalformed, len(terminalValue))
	}
	b := &nonInclusionBody{bitmap: bitmap, siblings: append([][SiblingSize]byte(nil), siblings...)}
	copy(b.key[:], terminalKey)
	copy(b.value[:], terminalValue)
	return &NonInclusionCert{set: true, body: b}, nil
}

// UnmarshalBinary decodes the exact profile: length is checked against the bitmap before
// any path structure is allocated; trailing or missing bytes are errors.
func (c *NonInclusionCert) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		*c = NonInclusionCert{set: true}
		return nil
	}
	if len(data) > MaxNonInclusionCertBytes {
		return fmt.Errorf("%w: %d bytes exceed the %d byte limit", ErrNonInclusionMalformed, len(data), MaxNonInclusionCertBytes)
	}
	if len(data) < minNonInclusionCertBytes {
		return fmt.Errorf("%w: %d bytes", ErrNonInclusionMalformed, len(data))
	}
	var bm [BitmapSize]byte
	copy(bm[:], data)
	n := bitmapPopcount(&bm)
	if want := minNonInclusionCertBytes + n*SiblingSize; len(data) != want {
		return fmt.Errorf("%w: %d bytes for %d junctions (want %d)", ErrNonInclusionMalformed, len(data), n, want)
	}
	sibs := make([][SiblingSize]byte, n)
	off := BitmapSize
	for i := range sibs {
		copy(sibs[i][:], data[off:off+SiblingSize])
		off += SiblingSize
	}
	cert, err := NewNonInclusionCert(bm, sibs, data[off:off+StateTreeKeyLengthBytes], data[off+StateTreeKeyLengthBytes:])
	if err != nil {
		return err
	}
	*c = *cert
	return nil
}

// MarshalBinary encodes the certificate; the empty tree is the empty byte string.
func (c *NonInclusionCert) MarshalBinary() ([]byte, error) {
	if c == nil || !c.set {
		return nil, fmt.Errorf("%w: unset certificate", ErrNonInclusionMalformed)
	}
	if c.body == nil {
		return []byte{}, nil
	}
	b := c.body
	out := make([]byte, 0, minNonInclusionCertBytes+len(b.siblings)*SiblingSize)
	out = append(out, b.bitmap[:]...)
	for i := range b.siblings {
		out = append(out, b.siblings[i][:]...)
	}
	out = append(out, b.key[:]...)
	return append(out, b.value[:]...), nil
}

// IsEmptyTree reports whether this is the empty-tree certificate.
func (c *NonInclusionCert) IsEmptyTree() bool { return c != nil && c.set && c.body == nil }

// TerminalKey is the terminal leaf key (nil for the empty tree). Untrusted until verified.
func (c *NonInclusionCert) TerminalKey() []byte {
	if c == nil || c.body == nil {
		return nil
	}
	return bytes.Clone(c.body.key[:])
}

// TerminalValue is the terminal leaf value (nil for the empty tree). Untrusted until verified.
func (c *NonInclusionCert) TerminalValue() []byte {
	if c == nil || c.body == nil {
		return nil
	}
	return bytes.Clone(c.body.value[:])
}

// Junctions is the number of siblings.
func (c *NonInclusionCert) Junctions() int {
	if c == nil || c.body == nil {
		return 0
	}
	return len(c.body.siblings)
}

// VerifyAgainstTrustedRoot checks the relation against a root the caller has already
// authenticated (for example UC.InputRecord.Hash). It is NOT a complete proof
// verification: use pkg/noninclusion for that. root is the raw 32 byte root; an EMPTY
// root (nil or zero length) means the certified empty tree, and only the empty
// certificate verifies against it. The all-zero 32 byte hash is an ordinary root.
//
// It returns nil when the query is absent, ErrStateIncluded when the authenticated terminal
// is the query itself, ErrNonInclusionInvalid otherwise.
func (c *NonInclusionCert) VerifyAgainstTrustedRoot(key, root []byte, algo HashAlgorithm) error {
	if c == nil || !c.set {
		return fmt.Errorf("%w: unset certificate", ErrNonInclusionMalformed)
	}
	if len(key) != StateTreeKeyLengthBytes {
		return fmt.Errorf("%w: %v", ErrNonInclusionInvalid, ErrCertKeyLength)
	}
	switch {
	case len(root) == 0:
		if c.body != nil {
			return fmt.Errorf("%w: terminal certificate against the empty tree", ErrNonInclusionInvalid)
		}
		return nil
	case len(root) != SiblingSize:
		return fmt.Errorf("%w: %v", ErrNonInclusionInvalid, ErrCertRootLength)
	case c.body == nil:
		return fmt.Errorf("%w: empty-tree certificate against a nonempty root", ErrNonInclusionInvalid)
	}
	b := c.body
	hasher := NewDataHasher(algo)
	if hasher == nil {
		return fmt.Errorf("%w: %d", ErrCertUnknownAlgo, algo)
	}
	hasher.Reset().AddData([]byte{0x00}).AddData(b.key[:]).AddData(b.value[:])
	h := append([]byte(nil), hasher.GetHash().RawHash...)
	j := len(b.siblings)
	for d := maxDepth - 1; d >= 0; d-- {
		if KeyBitBE(b.bitmap[:], d) == 0 {
			continue
		}
		// the query must take the same branch as the terminal at every junction: an
		// authenticated path of another subtree proves nothing about this key
		if KeyBitBE(key, d) != KeyBitBE(b.key[:], d) {
			return fmt.Errorf("%w: branch at depth %d", ErrNonInclusionInvalid, d)
		}
		j--
		sib := b.siblings[j][:]
		hasher.Reset().AddData([]byte{0x01, byte(d)}).AddData(RegionFromKeyBytes(b.key[:], d))
		if KeyBitBE(b.key[:], d) == 1 {
			hasher.AddData(sib).AddData(h)
		} else {
			hasher.AddData(h).AddData(sib)
		}
		h = append(h[:0], hasher.GetHash().RawHash...)
	}
	if !bytes.Equal(h, root) {
		return fmt.Errorf("%w: root mismatch", ErrNonInclusionInvalid)
	}
	if bytes.Equal(b.key[:], key) {
		return ErrStateIncluded
	}
	return nil
}
