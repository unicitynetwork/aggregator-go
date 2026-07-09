package smt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// This file is an independent, self-contained big-endian SMT reference used to
// anchor the production SMT to the yellowpaper spec (issue #169). It shares no
// code with the production packing/traversal path: bits are read big-endian by
// hand and every hash is computed with crypto/sha256 directly. Agreement
// between this reference and production for a set of trees — including
// convention-asymmetric canary keys — proves the production tree is big-endian
// correct, not merely internally self-consistent.

type beRefLeaf struct {
	key []byte
	val []byte
}

func beRefKeyBit(key []byte, d int) int { return int((key[d/8] >> (7 - uint(d)%8)) & 1) }

func beRefLeafHash(key, val []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(key)
	h.Write(val)
	return h.Sum(nil)
}

func beRefNodeHash(depth int, region, left, right []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x01, byte(depth)})
	h.Write(region)
	h.Write(left)
	h.Write(right)
	return h.Sum(nil)
}

func beRefRegion(key []byte, depth int) []byte {
	r := make([]byte, 32)
	for d := 0; d < depth; d++ {
		if beRefKeyBit(key, d) == 1 {
			r[d/8] |= 0x80 >> (uint(d) % 8)
		}
	}
	return r
}

// beRefBuild builds the v6a big-endian radix tree for leaves that all share
// bits [0, startBit) and returns its node hash. Single leaf returns the leaf
// hash (unary passthrough), matching the production root of a ≥1-leaf tree.
func beRefBuild(leaves []beRefLeaf, startBit int) []byte {
	if len(leaves) == 1 {
		return beRefLeafHash(leaves[0].key, leaves[0].val)
	}
	split := startBit
	for {
		b := beRefKeyBit(leaves[0].key, split)
		same := true
		for _, lf := range leaves {
			if beRefKeyBit(lf.key, split) != b {
				same = false
				break
			}
		}
		if !same {
			break
		}
		split++
	}
	var left, right []beRefLeaf
	for _, lf := range leaves {
		if beRefKeyBit(lf.key, split) == 0 {
			left = append(left, lf)
		} else {
			right = append(right, lf)
		}
	}
	region := beRefRegion(leaves[0].key, split)
	return beRefNodeHash(split, region, beRefBuild(left, split+1), beRefBuild(right, split+1))
}

func beRefKey(b ...byte) []byte {
	key := make([]byte, 32)
	copy(key, b)
	return key
}

func TestBigEndianReferenceMatchesProduction(t *testing.T) {
	cases := []struct {
		name   string
		leaves []beRefLeaf
	}{
		{"shallow_split_bit7", []beRefLeaf{
			{beRefKey(0x00), []byte("left")}, {beRefKey(0x01), []byte("right")},
		}},
		{"deep_split_bit255", []beRefLeaf{
			{beRefKey(), beRefKey(1)}, {beRefKeyLast(0x01), beRefKey(2)},
		}},
		// Asymmetric canary: 0x80 (bit 0) vs 0x01 (bit 7) — a half-flipped
		// implementation splits these at a different depth than big-endian.
		{"asymmetric_bit0_vs_bit7", []beRefLeaf{
			{beRefKey(0x80), []byte("a")}, {beRefKey(0x01), []byte("b")}, {beRefKey(0x00), []byte("c")},
		}},
		{"multi_leaf", []beRefLeaf{
			{beRefKey(0x00), []byte("value0")},
			{beRefKey(0x04), []byte("value1")},
			{beRefKey(0x01), []byte("value2")},
			{beRefKey(0x00, 0x01), []byte("value3")},
			{beRefKey(0x00, 0x00, 0x01), []byte("value4")},
		}},
		{"js_parity_single_byte_keys", func() []beRefLeaf {
			firstBytes := []byte{0b10010000, 0b00000000, 0b00010000, 0b10000000, 0b01100000, 0b00010100}
			var ls []beRefLeaf
			for i, b := range firstBytes {
				ls = append(ls, beRefLeaf{beRefKey(b), []byte(fmt.Sprintf("value%d", i))})
			}
			return ls
		}()},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := NewSparseMerkleTree(api.SHA256, 256)
			for _, lf := range c.leaves {
				p, err := api.FixedBytesToPath(lf.key, 256)
				require.NoError(t, err)
				require.NoError(t, tree.AddLeaf(p, lf.val))
			}
			want := hex.EncodeToString(beRefBuild(c.leaves, 0))
			require.Equal(t, want, tree.GetRootHashHex(),
				"production root must equal independent big-endian reference")

			// Every leaf's inclusion cert must verify against the root.
			for _, lf := range c.leaves {
				cert, err := tree.GetInclusionCert(lf.key)
				require.NoError(t, err)
				require.NoError(t, cert.Verify(lf.key, lf.val, tree.GetRootHashRaw(), api.SHA256))
			}
		})
	}
}

func beRefKeyLast(b byte) []byte {
	key := make([]byte, 32)
	key[31] = b
	return key
}
