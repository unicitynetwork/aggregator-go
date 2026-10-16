// Package nifixture is the independent reference model of the non-inclusion
// corpus (testdata/non-inclusion/v1): a plain recursive radix sparse Merkle
// tree over a key set, the raw non-inclusion certificate walk, and a reference
// certificate verifier. It deliberately shares no code with the production
// trees (internal/smt, RocksDB) or with pkg/api, so the corpus is an
// independent oracle for them.
package nifixture

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/bits"
	"sort"
)

const (
	KeySize   = 32
	ValueSize = 32
	HashSize  = 32
	// MaxCertificateBytes is 96 + 32*256: the largest valid nonempty certificate (an included query
	// on a 256 deep path; an absent query has at most 255 junctions).
	MaxCertificateBytes = 32 + 32*256 + KeySize + ValueSize
)

// Leaf is one key/value pair of the tree.
type Leaf struct {
	Key   [KeySize]byte
	Value [ValueSize]byte
}

// bit is the MSB-first bit i of the key (bit 0 is the top bit of byte 0).
func bit(b []byte, i int) int { return int(b[i/8]>>(7-uint(i%8))) & 1 }

// region keeps the key bits [0,depth) and clears everything else.
func region(key []byte, depth int) [KeySize]byte {
	var r [KeySize]byte
	full, rem := depth/8, depth%8
	copy(r[:full], key[:full])
	if rem != 0 {
		r[full] = key[full] & (0xff << (8 - uint(rem)))
	}
	return r
}

func leafHash(key, value []byte) [HashSize]byte {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(key)
	h.Write(value)
	var out [HashSize]byte
	h.Sum(out[:0])
	return out
}

func nodeHash(depth int, key []byte, left, right [HashSize]byte) [HashSize]byte {
	h := sha256.New()
	h.Write([]byte{0x01, byte(depth)})
	r := region(key, depth)
	h.Write(r[:])
	h.Write(left[:])
	h.Write(right[:])
	var out [HashSize]byte
	h.Sum(out[:0])
	return out
}

// Tree is the reference tree. Nil root is the empty tree.
type Tree struct{ root *node }

type node struct {
	depth       int
	leaf        *Leaf
	left, right *node
	hash        [HashSize]byte
}

// Build builds the tree of the leaves; keys must be distinct.
func Build(leaves []Leaf) (*Tree, error) {
	if len(leaves) == 0 {
		return &Tree{}, nil
	}
	ls := append([]Leaf(nil), leaves...)
	sort.Slice(ls, func(i, j int) bool { return bytes.Compare(ls[i].Key[:], ls[j].Key[:]) < 0 })
	for i := 1; i < len(ls); i++ {
		if ls[i].Key == ls[i-1].Key {
			return nil, errors.New("duplicate key")
		}
	}
	return &Tree{root: build(ls)}, nil
}

func build(ls []Leaf) *node {
	if len(ls) == 1 {
		l := ls[0]
		return &node{leaf: &l, hash: leafHash(l.Key[:], l.Value[:])}
	}
	// sorted: the first bit where the extremes differ is the first split of the set
	first, last := ls[0].Key[:], ls[len(ls)-1].Key[:]
	d := 0
	for bit(first, d) == bit(last, d) {
		d++
	}
	i := sort.Search(len(ls), func(i int) bool { return bit(ls[i].Key[:], d) == 1 })
	n := &node{depth: d, left: build(ls[:i]), right: build(ls[i:])}
	n.hash = nodeHash(d, first, n.left.hash, n.right.hash)
	return n
}

// Root returns the root hash; ok is false for the empty tree.
func (t *Tree) Root() (root [HashSize]byte, ok bool) {
	if t.root == nil {
		return root, false
	}
	return t.root.hash, true
}

// Contains reports whether the key is a leaf of the tree.
func (t *Tree) Contains(key [KeySize]byte) bool {
	n := t.root
	for n != nil && n.leaf == nil {
		if bit(key[:], n.depth) == 0 {
			n = n.left
		} else {
			n = n.right
		}
	}
	return n != nil && n.leaf.Key == key
}

// Certificate is the raw non-inclusion certificate of the query: the terminal
// leaf reached by walking the query at every actual junction. An empty tree
// yields the empty certificate. For a key which is in the tree the terminal is
// the key's own leaf (the verifier reports state included).
func (t *Tree) Certificate(query [KeySize]byte) []byte {
	if t.root == nil {
		return []byte{}
	}
	var bitmap [32]byte
	var sibs [][HashSize]byte
	n := t.root
	for n.leaf == nil {
		bitmap[n.depth/8] |= 0x80 >> uint(n.depth%8)
		if bit(query[:], n.depth) == 0 {
			sibs = append(sibs, n.right.hash)
			n = n.left
		} else {
			sibs = append(sibs, n.left.hash)
			n = n.right
		}
	}
	out := append([]byte(nil), bitmap[:]...)
	for _, s := range sibs {
		out = append(out, s[:]...)
	}
	out = append(out, n.leaf.Key[:]...)
	return append(out, n.leaf.Value[:]...)
}

// Verdicts of the reference verifier.
var (
	ErrMalformed      = errors.New("malformed certificate")
	ErrInvalid        = errors.New("certificate does not authenticate the root")
	ErrStateIncluded  = errors.New("state is included")
	ErrEmptyMismatch  = errors.New("empty certificate against a nonempty root")
	errWrongDirection = errors.New("query and terminal branch differently")
)

// Verify is the reference relation check. root is nil for the certified empty
// tree. A nil error means: the certificate authenticates a terminal leaf under
// root and the query is absent.
func Verify(cert []byte, query [KeySize]byte, root *[HashSize]byte) error {
	if len(cert) == 0 {
		if root != nil {
			return ErrEmptyMismatch
		}
		return nil
	}
	if root == nil {
		return ErrInvalid
	}
	if len(cert) < 32+KeySize+ValueSize || len(cert) > MaxCertificateBytes {
		return ErrMalformed
	}
	pop := 0
	for _, b := range cert[:32] {
		pop += bits.OnesCount8(b)
	}
	if len(cert) != 32+32*pop+KeySize+ValueSize {
		return ErrMalformed
	}
	sibs := cert[32 : 32+32*pop]
	tk := cert[32+32*pop : 32+32*pop+KeySize]
	tv := cert[32+32*pop+KeySize:]
	h := leafHash(tk, tv)
	j := pop
	for d := 255; d >= 0; d-- {
		if bit(cert[:32], d) == 0 {
			continue
		}
		if bit(query[:], d) != bit(tk, d) {
			return fmt.Errorf("%w: depth %d", ErrInvalid, d)
		}
		j--
		var sib [HashSize]byte
		copy(sib[:], sibs[j*32:(j+1)*32])
		if bit(tk, d) == 1 {
			h = nodeHash(d, tk, sib, h)
		} else {
			h = nodeHash(d, tk, h, sib)
		}
	}
	if h != *root {
		return ErrInvalid
	}
	if bytes.Equal(tk, query[:]) {
		return ErrStateIncluded
	}
	return nil
}

// InclusionCertificate returns the raw inclusion certificate (bitmap || siblings, root to leaf)
// of a key which is a leaf of the tree; ok is false when it is not.
func (t *Tree) InclusionCertificate(key [KeySize]byte) ([]byte, bool) {
	if !t.Contains(key) {
		return nil, false
	}
	c := t.Certificate(key)
	return c[:len(c)-KeySize-ValueSize], true
}
