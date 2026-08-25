package smt

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ---------------------------------------------------------------------------
// Literal transcription of appendix-hashtrees.tex Sec. C.3.7.1
// (rsmt_verify_inclusion) + C.3.2.1/C.3.2.2 (leaf/node hash) + C.3.6 (cert
// path bit numbering). Written from the paper only, no reuse of pkg/api.
// ---------------------------------------------------------------------------

// bit d of a big-endian bit string: k[0] is MSB of byte 0.
func specBit(buf []byte, d int) byte { return (buf[d/8] >> (7 - uint(d)%8)) & 1 }

func specPopcount(bm []byte) int {
	n := 0
	for d := 0; d < 8*len(bm); d++ {
		n += int(specBit(bm, d))
	}
	return n
}

// <p> for p = k[0..d): 256-bit BE string, p in the first d bits, rest zero.
func specRegion(k []byte, d int) []byte {
	p := make([]byte, 32)
	for i := 0; i < d; i++ {
		if specBit(k, i) == 1 {
			p[i/8] |= 0x80 >> (uint(i) % 8)
		}
	}
	return p
}

func specLeafHash(k, v []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x00})
	h.Write(k)
	h.Write(v)
	return h.Sum(nil)
}

func specNodeHash(hL, hR []byte, d int, p []byte) []byte {
	h := sha256.New()
	h.Write([]byte{0x01})
	h.Write([]byte{byte(d)})
	h.Write(p)
	h.Write(hL)
	h.Write(hR)
	return h.Sum(nil)
}

func specVerifyInclusion(bitmap []byte, siblings [][]byte, rho, k, v []byte) bool {
	if len(bitmap) != 32 || len(k) != 32 || len(rho) != 32 {
		return false
	}
	if len(siblings) != specPopcount(bitmap) {
		return false
	}
	h := specLeafHash(k, v)
	j := len(siblings)
	for d := 255; d >= 0; d-- {
		if specBit(bitmap, d) == 0 {
			continue
		}
		j--
		s := siblings[j]
		if len(s) != 32 {
			return false
		}
		p := specRegion(k, d)
		var hL, hR []byte
		if specBit(k, d) == 1 {
			hL, hR = s, h
		} else {
			hL, hR = h, s
		}
		h = specNodeHash(hL, hR, d, p)
	}
	return j == 0 && bytes.Equal(h, rho)
}

func certToSpec(c *api.InclusionCert) ([]byte, [][]byte) {
	sibs := make([][]byte, len(c.Siblings))
	for i := range c.Siblings {
		sibs[i] = append([]byte(nil), c.Siblings[i][:]...)
	}
	return append([]byte(nil), c.Bitmap[:]...), sibs
}

// ---------------------------------------------------------------------------

type kv struct{ k, v []byte }

func buildRandomTree(t *testing.T, n int) (*SparseMerkleTree, []kv) {
	t.Helper()
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	out := make([]kv, 0, n)
	seen := map[string]bool{}
	for len(out) < n {
		k := make([]byte, 32)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		v := make([]byte, 32)
		if _, err := rand.Read(v); err != nil {
			t.Fatal(err)
		}
		p, err := api.FixedBytesToPath(k, api.StateTreeKeyLengthBits)
		if err != nil {
			t.Fatal(err)
		}
		if err := tree.AddLeaf(p, v); err != nil {
			t.Fatal(err)
		}
		out = append(out, kv{k, v})
	}
	return tree, out
}

// TestSpec_GeneratedCertsVerifyUnderSpec: every cert the implementation
// generates must be accepted by the literal spec verifier.
func TestSpec_GeneratedCertsVerifyUnderSpec(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 33, 200} {
		tree, leaves := buildRandomTree(t, n)
		root := tree.GetRootHashRaw()
		for _, l := range leaves {
			cert, err := tree.GetInclusionCert(l.k)
			if err != nil {
				t.Fatalf("n=%d GetInclusionCert: %v", n, err)
			}
			bm, sibs := certToSpec(cert)
			if !specVerifyInclusion(bm, sibs, root, l.k, l.v) {
				t.Fatalf("n=%d: spec verifier REJECTED an implementation-generated cert for key %x", n, l.k)
			}
			if err := cert.Verify(l.k, l.v, root, api.SHA256); err != nil {
				t.Fatalf("n=%d: impl verifier rejected own cert: %v", n, err)
			}
		}
	}
}

// TestSpec_ImplAgreesWithSpecOnMutations: for a corpus of mutated certs the
// impl verifier and the spec verifier must give the same answer.
func TestSpec_ImplAgreesWithSpecOnMutations(t *testing.T) {
	tree, leaves := buildRandomTree(t, 64)
	root := tree.GetRootHashRaw()

	type tc struct {
		name string
		mut  func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte)
	}
	cases := []tc{
		{"unchanged", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) { return c, k, v }},
		{"extra sibling appended (no bitmap bit)", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			d := *c
			d.Siblings = append(append([][32]byte{}, c.Siblings...), [32]byte{})
			return &d, k, v
		}},
		{"sibling dropped (bitmap unchanged)", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			if len(c.Siblings) == 0 {
				return c, k, v
			}
			d := *c
			d.Siblings = append([][32]byte{}, c.Siblings[:len(c.Siblings)-1]...)
			return &d, k, v
		}},
		{"bitmap bit flipped on at unused depth", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			d := *c
			for depth := 255; depth >= 0; depth-- {
				if api.KeyBitBE(d.Bitmap[:], depth) == 0 {
					api.SetBitBE(d.Bitmap[:], depth)
					break
				}
			}
			return &d, k, v
		}},
		{"siblings reversed", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			d := *c
			s := append([][32]byte{}, c.Siblings...)
			for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
				s[i], s[j] = s[j], s[i]
			}
			d.Siblings = s
			return &d, k, v
		}},
		{"wrong value", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			w := append([]byte(nil), v...)
			w[0] ^= 0xff
			return c, k, w
		}},
		{"wrong key", func(c *api.InclusionCert, k, v []byte) (*api.InclusionCert, []byte, []byte) {
			w := append([]byte(nil), k...)
			w[31] ^= 0x01
			return c, w, v
		}},
	}

	for _, l := range leaves[:16] {
		base, err := tree.GetInclusionCert(l.k)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cases {
			mc, k, v := c.mut(base, l.k, l.v)
			bm, sibs := certToSpec(mc)
			specOK := specVerifyInclusion(bm, sibs, root, k, v)
			implOK := mc.Verify(k, v, root, api.SHA256) == nil
			if specOK != implOK {
				t.Errorf("DIVERGENCE [%s] key=%x: spec=%v impl=%v", c.name, l.k, specOK, implOK)
			}
		}
	}
}

// TestSpec_RootHashMatchesSpecReconstruction: recompute the tree root purely
// from the spec (leaf hashes + certificate paths) for a 2-leaf tree.
func TestSpec_TwoLeafRootFromSpec(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	kA := make([]byte, 32)
	kB := make([]byte, 32)
	kA[0] = 0x00 // bit0=0
	kB[0] = 0x80 // bit0=1
	vA := []byte("a")
	vB := []byte("b")
	pA, _ := api.FixedBytesToPath(kA, 256)
	pB, _ := api.FixedBytesToPath(kB, 256)
	if err := tree.AddLeaf(pA, vA); err != nil {
		t.Fatal(err)
	}
	if err := tree.AddLeaf(pB, vB); err != nil {
		t.Fatal(err)
	}
	root := tree.GetRootHashRaw()
	// spec: junction at depth 0, region = empty (all-zero 32 bytes)
	want := specNodeHash(specLeafHash(kA, vA), specLeafHash(kB, vB), 0, make([]byte, 32))
	if !bytes.Equal(root, want) {
		t.Fatalf("root mismatch\n impl %x\n spec %x", root, want)
	}
	certA, err := tree.GetInclusionCert(kA)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("certA bitmap=%x siblings=%d", certA.Bitmap, len(certA.Siblings))
}
