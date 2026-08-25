package smt

import (
	"bytes"
	"crypto/rand"
	mrand "math/rand"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// specRoot computes the root the SPEC verifier would accept for a given
// (bitmap, siblings, key, value) — i.e. runs the spec reconstruction.
func specRoot(bitmap []byte, siblings [][]byte, k, v []byte) []byte {
	h := specLeafHash(k, v)
	j := len(siblings)
	for d := 255; d >= 0; d-- {
		if specBit(bitmap, d) == 0 {
			continue
		}
		j--
		s := siblings[j]
		p := specRegion(k, d)
		var hL, hR []byte
		if specBit(k, d) == 1 {
			hL, hR = s, h
		} else {
			hL, hR = h, s
		}
		h = specNodeHash(hL, hR, d, p)
	}
	return h
}

// Spec-constructed certificates (arbitrary junction depth sets, including 0
// and 255, arbitrary value lengths) must be accepted by the implementation
// verifier and survive a wire round-trip.
func TestSpec_ImplAcceptsSpecConstructedCerts(t *testing.T) {
	rng := mrand.New(mrand.NewSource(42))
	for iter := 0; iter < 5000; iter++ {
		var bm [32]byte
		depths := map[int]bool{}
		nd := rng.Intn(8)
		if iter < 3 {
			// force the extreme depths at least once each
			depths[0] = true
			depths[255] = true
		}
		for i := 0; i < nd; i++ {
			depths[rng.Intn(256)] = true
		}
		ordered := make([]int, 0, len(depths))
		for d := 0; d < 256; d++ {
			if depths[d] {
				api.SetBitBE(bm[:], d)
				ordered = append(ordered, d)
			}
		}
		sibs := make([][32]byte, len(ordered))
		for i := range sibs {
			rand.Read(sibs[i][:])
		}
		k := make([]byte, 32)
		rand.Read(k)
		v := make([]byte, rng.Intn(64))
		rand.Read(v)

		specSibs := make([][]byte, len(sibs))
		for i := range sibs {
			specSibs[i] = sibs[i][:]
		}
		root := specRoot(bm[:], specSibs, k, v)

		cert := &api.InclusionCert{Bitmap: bm, Siblings: sibs}
		if err := cert.Verify(k, v, root, api.SHA256); err != nil {
			t.Fatalf("iter %d: impl REJECTED a spec-valid cert (depths=%v): %v", iter, ordered, err)
		}

		// wire round trip: bitmap[32] || s_1..s_n, total 32+32n bytes
		wire, err := cert.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) != 32+32*len(sibs) {
			t.Fatalf("iter %d: wire length %d, want %d", iter, len(wire), 32+32*len(sibs))
		}
		if !bytes.Equal(wire[:32], bm[:]) {
			t.Fatalf("iter %d: bitmap is not the first 32 wire bytes", iter)
		}
		var back api.InclusionCert
		if err := back.UnmarshalBinary(wire); err != nil {
			t.Fatalf("iter %d: round trip decode: %v", iter, err)
		}
		if err := back.Verify(k, v, root, api.SHA256); err != nil {
			t.Fatalf("iter %d: decoded cert rejected: %v", iter, err)
		}

		// trailing garbage / truncation must be rejected
		if err := (&api.InclusionCert{}).UnmarshalBinary(append(append([]byte(nil), wire...), 0x00)); err == nil {
			t.Fatalf("iter %d: decoder accepted 1 trailing byte", iter)
		}
		if err := (&api.InclusionCert{}).UnmarshalBinary(append(append([]byte(nil), wire...), make([]byte, 32)...)); err == nil {
			t.Fatalf("iter %d: decoder accepted an extra 32-byte sibling", iter)
		}
		if err := (&api.InclusionCert{}).UnmarshalBinary(wire[:len(wire)-1]); err == nil {
			t.Fatalf("iter %d: decoder accepted a truncated cert", iter)
		}
	}
}

// A junction at depth 255 (keys differing only in the final bit) must be
// generated and verified correctly end to end.
func TestSpec_Depth255Junction(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	kA := bytes.Repeat([]byte{0xAB}, 32)
	kB := append([]byte(nil), kA...)
	kB[31] ^= 0x01 // differ only at bit 255
	vA, vB := []byte("A"), []byte("B")
	pA, _ := api.FixedBytesToPath(kA, 256)
	pB, _ := api.FixedBytesToPath(kB, 256)
	if err := tree.AddLeaf(pA, vA); err != nil {
		t.Fatal(err)
	}
	if err := tree.AddLeaf(pB, vB); err != nil {
		t.Fatal(err)
	}
	root := tree.GetRootHashRaw()
	cert, err := tree.GetInclusionCert(kA)
	if err != nil {
		t.Fatal(err)
	}
	if api.KeyBitBE(cert.Bitmap[:], 255) != 1 || len(cert.Siblings) != 1 {
		t.Fatalf("expected a single junction at depth 255, bitmap=%x n=%d", cert.Bitmap, len(cert.Siblings))
	}
	bm, sibs := certToSpec(cert)
	if !specVerifyInclusion(bm, sibs, root, kA, vA) {
		t.Fatal("spec verifier rejected depth-255 cert")
	}
	// kA has bit 255 = 1 (0xAB) so kA is the right child, kB (0xAA) the left.
	want := specNodeHash(specLeafHash(kB, vB), specLeafHash(kA, vA), 255, specRegion(kA, 255))
	if !bytes.Equal(root, want) {
		t.Fatalf("root mismatch at depth 255:\n impl %x\n spec %x", root, want)
	}
}
