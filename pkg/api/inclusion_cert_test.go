package api

import (
	"bytes"
	"errors"
	"testing"
)

// hashLeafRaw computes H(0x00 || key || value) for test fixtures.
func hashLeafRaw(t *testing.T, algo HashAlgorithm, key, value []byte) []byte {
	t.Helper()
	h := NewDataHasher(algo)
	h.AddData([]byte{0x00}).AddData(key).AddData(value)
	return h.GetHash().RawHash
}

// hashNodeRaw computes H(0x01 || depth || left || right) for test fixtures.
func hashNodeRaw(t *testing.T, algo HashAlgorithm, depth byte, left, right []byte) []byte {
	t.Helper()
	h := NewDataHasher(algo)
	h.AddData([]byte{0x01, depth}).AddData(left).AddData(right)
	return h.GetHash().RawHash
}

func TestInclusionCertVerify_EmptyBitmap_SingleLeafTree(t *testing.T) {
	// Single-leaf tree: no internal nodes, no siblings. Root is the
	// leaf hash directly.
	key := bytes.Repeat([]byte{0x01}, StateTreeKeyLengthBytes)
	value := []byte("hello")
	root := hashLeafRaw(t, SHA256, key, value)

	cert := &InclusionCert{} // bitmap zero, siblings nil
	if err := cert.Verify(key, value, root, SHA256); err != nil {
		t.Fatalf("verify empty-bitmap cert: %v", err)
	}
}

func TestInclusionCertVerify_SingleSiblingAtDepth0(t *testing.T) {
	// Two-leaf tree diverging at depth 0. Our key has bit 0 = 0, so
	// we went left at depth 0. Sibling is the right child.
	key := make([]byte, StateTreeKeyLengthBytes) // all zeros → bit 0 = 0
	value := []byte("left leaf")
	siblingHash := bytes.Repeat([]byte{0xAB}, SiblingSize)

	leafHash := hashLeafRaw(t, SHA256, key, value)
	root := hashNodeRaw(t, SHA256, 0, leafHash, siblingHash)

	cert := &InclusionCert{}
	cert.Bitmap[0] = 0x01 // depth 0
	var s [SiblingSize]byte
	copy(s[:], siblingHash)
	cert.Siblings = append(cert.Siblings, s)

	if err := cert.Verify(key, value, root, SHA256); err != nil {
		t.Fatalf("verify single-sibling cert: %v", err)
	}
}

func TestInclusionCertVerify_TwoSiblingsRootToLeafWireOrder(t *testing.T) {
	// Proof path touches depth 3 (shallower) and depth 7 (deeper).
	// Wire order must be root → leaf: siblings[0] at depth 3,
	// siblings[1] at depth 7. Verification consumes from the end:
	// depth 7 first (sib7), then depth 3 (sib3).

	// Key byte 0 = 0b0000_1000 → bit 3 = 1, bit 7 = 0.
	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0b0000_1000
	value := []byte("v")

	sib3 := bytes.Repeat([]byte{0x33}, SiblingSize)
	sib7 := bytes.Repeat([]byte{0x77}, SiblingSize)

	leaf := hashLeafRaw(t, SHA256, key, value)
	// d=7: bit 7 = 0 → went left → sibling right
	h7 := hashNodeRaw(t, SHA256, 7, leaf, sib7)
	// d=3: bit 3 = 1 → went right → sibling left
	h3 := hashNodeRaw(t, SHA256, 3, sib3, h7)
	root := h3

	cert := &InclusionCert{}
	cert.Bitmap[0] = 0b1000_1000 // bits 3 and 7
	var s3, s7 [SiblingSize]byte
	copy(s3[:], sib3)
	copy(s7[:], sib7)
	// Canonical root-to-leaf wire order: shallowest first.
	cert.Siblings = append(cert.Siblings, s3, s7)

	if err := cert.Verify(key, value, root, SHA256); err != nil {
		t.Fatalf("verify two-sibling cert: %v", err)
	}
}

func TestInclusionCertVerify_WrongSiblingOrderFails(t *testing.T) {
	// Same setup as the two-sibling test but with siblings swapped
	// into leaf-to-root order. Must fail.
	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0b0000_1000
	value := []byte("v")

	sib3 := bytes.Repeat([]byte{0x33}, SiblingSize)
	sib7 := bytes.Repeat([]byte{0x77}, SiblingSize)

	leaf := hashLeafRaw(t, SHA256, key, value)
	h7 := hashNodeRaw(t, SHA256, 7, leaf, sib7)
	h3 := hashNodeRaw(t, SHA256, 3, sib3, h7)
	root := h3

	cert := &InclusionCert{}
	cert.Bitmap[0] = 0b1000_1000
	var s3, s7 [SiblingSize]byte
	copy(s3[:], sib3)
	copy(s7[:], sib7)
	// Wrong order: deepest first.
	cert.Siblings = append(cert.Siblings, s7, s3)

	if err := cert.Verify(key, value, root, SHA256); err == nil {
		t.Fatal("expected verify to fail with wrong sibling order")
	}
}

func TestInclusionCertVerify_DepthSpanning8Bytes(t *testing.T) {
	// Single sibling at depth 200 to exercise cross-byte bitmap
	// addressing and large depth encoding.
	const depth = 200

	// Set bit 200 of the key so we went right at depth 200.
	key := make([]byte, StateTreeKeyLengthBytes)
	key[depth/8] |= 1 << (depth % 8)
	value := []byte("deep")

	siblingHash := bytes.Repeat([]byte{0x5A}, SiblingSize)
	leafHash := hashLeafRaw(t, SHA256, key, value)
	// bit 200 = 1 → went right → sibling is left
	root := hashNodeRaw(t, SHA256, byte(depth), siblingHash, leafHash)

	cert := &InclusionCert{}
	cert.Bitmap[depth/8] = 1 << (depth % 8)
	var s [SiblingSize]byte
	copy(s[:], siblingHash)
	cert.Siblings = append(cert.Siblings, s)

	if err := cert.Verify(key, value, root, SHA256); err != nil {
		t.Fatalf("verify depth-%d cert: %v", depth, err)
	}
}

func TestInclusionCertVerify_WrongValueFails(t *testing.T) {
	key := bytes.Repeat([]byte{0x01}, StateTreeKeyLengthBytes)
	root := hashLeafRaw(t, SHA256, key, []byte("right"))

	cert := &InclusionCert{}
	if err := cert.Verify(key, []byte("wrong"), root, SHA256); err == nil {
		t.Fatal("expected verify to fail on wrong value")
	}
}

func TestInclusionCertVerify_WrongRootFails(t *testing.T) {
	key := bytes.Repeat([]byte{0x01}, StateTreeKeyLengthBytes)
	value := []byte("v")
	fakeRoot := bytes.Repeat([]byte{0xFF}, SiblingSize)

	cert := &InclusionCert{}
	err := cert.Verify(key, value, fakeRoot, SHA256)
	if !errors.Is(err, ErrCertRootMismatch) {
		t.Fatalf("expected ErrCertRootMismatch, got %v", err)
	}
}

func TestInclusionCertVerify_InvalidKeyLength(t *testing.T) {
	cert := &InclusionCert{}
	err := cert.Verify([]byte{1, 2, 3}, []byte("v"), bytes.Repeat([]byte{0}, 32), SHA256)
	if !errors.Is(err, ErrCertKeyLength) {
		t.Fatalf("expected ErrCertKeyLength, got %v", err)
	}
}

func TestInclusionCertVerify_InvalidRootLength(t *testing.T) {
	key := bytes.Repeat([]byte{0x01}, StateTreeKeyLengthBytes)
	cert := &InclusionCert{}
	err := cert.Verify(key, []byte("v"), []byte{0}, SHA256)
	if !errors.Is(err, ErrCertRootLength) {
		t.Fatalf("expected ErrCertRootLength, got %v", err)
	}
}

func TestInclusionCertVerify_UnknownAlgorithm(t *testing.T) {
	key := bytes.Repeat([]byte{0x01}, StateTreeKeyLengthBytes)
	root := bytes.Repeat([]byte{0}, SiblingSize)
	cert := &InclusionCert{}
	err := cert.Verify(key, []byte("v"), root, HashAlgorithm(99))
	if !errors.Is(err, ErrCertUnknownAlgo) {
		t.Fatalf("expected ErrCertUnknownAlgo, got %v", err)
	}
}

func TestInclusionCertRoundTrip(t *testing.T) {
	cert := &InclusionCert{}
	cert.Bitmap[0] = 0b1010_1010
	cert.Bitmap[5] = 0xFF
	cert.Bitmap[31] = 0x80

	n := bitmapPopcount(&cert.Bitmap)
	cert.Siblings = make([][SiblingSize]byte, n)
	for i := 0; i < n; i++ {
		for j := range cert.Siblings[i] {
			cert.Siblings[i][j] = byte(i*7 + j)
		}
	}

	wire, err := cert.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	wantLen := BitmapSize + n*SiblingSize
	if len(wire) != wantLen {
		t.Fatalf("wire length: got %d want %d", len(wire), wantLen)
	}

	var got InclusionCert
	if err := got.UnmarshalBinary(wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Bitmap != cert.Bitmap {
		t.Fatalf("bitmap mismatch")
	}
	if len(got.Siblings) != n {
		t.Fatalf("sibling count: got %d want %d", len(got.Siblings), n)
	}
	for i := range cert.Siblings {
		if got.Siblings[i] != cert.Siblings[i] {
			t.Fatalf("sibling %d mismatch", i)
		}
	}
}

func TestInclusionCertRoundTrip_EmptyBitmap(t *testing.T) {
	cert := &InclusionCert{}
	wire, err := cert.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(wire) != BitmapSize {
		t.Fatalf("empty cert wire length: got %d want %d", len(wire), BitmapSize)
	}
	var got InclusionCert
	if err := got.UnmarshalBinary(wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Siblings) != 0 {
		t.Fatalf("expected 0 siblings, got %d", len(got.Siblings))
	}
}

func TestInclusionCertDecode_Truncated(t *testing.T) {
	short := make([]byte, 10)
	var cert InclusionCert
	if err := cert.UnmarshalBinary(short); !errors.Is(err, ErrCertTruncated) {
		t.Fatalf("expected ErrCertTruncated, got %v", err)
	}
}

func TestInclusionCertDecode_MisalignedSiblings(t *testing.T) {
	data := make([]byte, BitmapSize+17) // bitmap + partial sibling
	var cert InclusionCert
	if err := cert.UnmarshalBinary(data); !errors.Is(err, ErrCertMisalignedSibs) {
		t.Fatalf("expected ErrCertMisalignedSibs, got %v", err)
	}
}

func TestInclusionCertDecode_BitmapMismatch(t *testing.T) {
	// Bitmap claims 1 set bit; wire carries 2 siblings.
	data := make([]byte, BitmapSize+2*SiblingSize)
	data[0] = 0x01
	var cert InclusionCert
	if err := cert.UnmarshalBinary(data); !errors.Is(err, ErrCertBitmapMismatch) {
		t.Fatalf("expected ErrCertBitmapMismatch, got %v", err)
	}
}

func TestBitmapPopcount(t *testing.T) {
	cases := []struct {
		name string
		set  map[int]bool
		want int
	}{
		{"empty", nil, 0},
		{"one bit byte 0", map[int]bool{0: true}, 1},
		{"one bit byte 31", map[int]bool{255: true}, 1},
		{"multi", map[int]bool{0: true, 7: true, 8: true, 200: true, 255: true}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b [BitmapSize]byte
			for d := range tc.set {
				b[d/8] |= 1 << (d % 8)
			}
			got := bitmapPopcount(&b)
			if got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestKeyBitAt(t *testing.T) {
	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0b1010_0101
	key[1] = 0x01  // bit 8 set
	key[31] = 0x80 // bit 255 set

	checks := []struct {
		pos  int
		want byte
	}{
		{0, 1}, {1, 0}, {2, 1}, {5, 1}, {7, 1},
		{8, 1}, {9, 0},
		{255, 1},
	}
	for _, c := range checks {
		if got := keyBitAt(key, c.pos); got != c.want {
			t.Errorf("keyBitAt(%d) = %d, want %d", c.pos, got, c.want)
		}
	}
}

func TestExclusionCertRoundTrip(t *testing.T) {
	cert := &ExclusionCert{}
	for i := range cert.KL {
		cert.KL[i] = byte(i)
	}
	for i := range cert.HL {
		cert.HL[i] = byte(i + 100)
	}
	cert.Bitmap[2] = 0b0000_0101 // 2 set bits
	cert.Siblings = make([][SiblingSize]byte, 2)
	for i := range cert.Siblings[0] {
		cert.Siblings[0][i] = 0xAA
	}
	for i := range cert.Siblings[1] {
		cert.Siblings[1][i] = 0xBB
	}

	wire, err := cert.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	wantLen := 2*SiblingSize + BitmapSize + 2*SiblingSize
	if len(wire) != wantLen {
		t.Fatalf("wire length: got %d want %d", len(wire), wantLen)
	}

	var got ExclusionCert
	if err := got.UnmarshalBinary(wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.KL != cert.KL {
		t.Fatalf("KL mismatch")
	}
	if got.HL != cert.HL {
		t.Fatalf("HL mismatch")
	}
	if got.Bitmap != cert.Bitmap {
		t.Fatalf("bitmap mismatch")
	}
	if len(got.Siblings) != 2 {
		t.Fatalf("sibling count: got %d want 2", len(got.Siblings))
	}
	for i := range cert.Siblings {
		if got.Siblings[i] != cert.Siblings[i] {
			t.Fatalf("sibling %d mismatch", i)
		}
	}
}

func TestExclusionCertDecode_Truncated(t *testing.T) {
	short := make([]byte, 2*SiblingSize+BitmapSize-1) // 1 byte short of header
	var cert ExclusionCert
	if err := cert.UnmarshalBinary(short); !errors.Is(err, ErrCertTruncated) {
		t.Fatalf("expected ErrCertTruncated, got %v", err)
	}
}

func TestExclusionCertVerify_NotImplemented(t *testing.T) {
	cert := &ExclusionCert{}
	key := bytes.Repeat([]byte{0}, StateTreeKeyLengthBytes)
	root := bytes.Repeat([]byte{0}, SiblingSize)
	if err := cert.Verify(key, root, SHA256); !errors.Is(err, ErrExclusionNotImpl) {
		t.Fatalf("expected ErrExclusionNotImpl, got %v", err)
	}
}

func TestComposeInclusionCert_Success(t *testing.T) {
	key := make([]byte, StateTreeKeyLengthBytes) // shard prefix 00, child bits also 0
	value := []byte("leaf-value")

	childSibling := bytes.Repeat([]byte{0x55}, SiblingSize)
	parentSibling := bytes.Repeat([]byte{0x99}, SiblingSize)

	leaf := hashLeafRaw(t, SHA256, key, value)
	childRoot := hashNodeRaw(t, SHA256, 5, leaf, childSibling)
	parentRoot := hashNodeRaw(t, SHA256, 1, childRoot, parentSibling)

	child := &InclusionCert{}
	child.Bitmap[5/8] |= 1 << (5 % 8)
	var childS [SiblingSize]byte
	copy(childS[:], childSibling)
	child.Siblings = append(child.Siblings, childS)

	parent := &InclusionCert{}
	parent.Bitmap[1/8] |= 1 << (1 % 8)
	var parentS [SiblingSize]byte
	copy(parentS[:], parentSibling)
	parent.Siblings = append(parent.Siblings, parentS)
	parentBytes, err := parent.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal parent cert: %v", err)
	}

	fragment := &ParentInclusionFragment{
		CertificateBytes: parentBytes,
		ShardLeafValue:   append([]byte(nil), childRoot...),
	}

	composed, err := ComposeInclusionCert(fragment, child, childRoot)
	if err != nil {
		t.Fatalf("compose inclusion cert: %v", err)
	}
	if err := composed.Verify(key, value, parentRoot, SHA256); err != nil {
		t.Fatalf("verify composed cert: %v", err)
	}
	if got := len(composed.Siblings); got != 2 {
		t.Fatalf("composed sibling count: got %d want 2", got)
	}
	if composed.Siblings[0] != parentS {
		t.Fatalf("parent sibling not first in composed cert")
	}
	if composed.Siblings[1] != childS {
		t.Fatalf("child sibling not second in composed cert")
	}
}

func TestComposeInclusionCert_RejectsMalformedParentFragment(t *testing.T) {
	child := &InclusionCert{}
	fragment := &ParentInclusionFragment{
		CertificateBytes: []byte{0x01, 0x02},
		ShardLeafValue:   bytes.Repeat([]byte{0xAA}, SiblingSize),
	}
	_, err := ComposeInclusionCert(fragment, child, bytes.Repeat([]byte{0xAA}, SiblingSize))
	if err == nil {
		t.Fatal("expected malformed parent fragment to fail")
	}
	if !errors.Is(err, ErrCertTruncated) {
		t.Fatalf("expected ErrCertTruncated, got %v", err)
	}
}

func TestComposeInclusionCert_RejectsChildRootMismatch(t *testing.T) {
	parent := &InclusionCert{}
	parentBytes, err := parent.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal parent cert: %v", err)
	}
	fragment := &ParentInclusionFragment{
		CertificateBytes: parentBytes,
		ShardLeafValue:   bytes.Repeat([]byte{0xAA}, SiblingSize),
	}
	_, err = ComposeInclusionCert(fragment, &InclusionCert{}, bytes.Repeat([]byte{0xBB}, SiblingSize))
	if !errors.Is(err, ErrCertChildRootMismatch) {
		t.Fatalf("expected ErrCertChildRootMismatch, got %v", err)
	}
}

func TestComposeInclusionCert_RejectsDepthOverlap(t *testing.T) {
	child := &InclusionCert{}
	child.Bitmap[5/8] |= 1 << (5 % 8)
	var childS [SiblingSize]byte
	child.Siblings = append(child.Siblings, childS)

	parent := &InclusionCert{}
	parent.Bitmap[5/8] |= 1 << (5 % 8)
	var parentS [SiblingSize]byte
	parent.Siblings = append(parent.Siblings, parentS)
	parentBytes, err := parent.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal parent cert: %v", err)
	}

	childRoot := bytes.Repeat([]byte{0x11}, SiblingSize)
	fragment := &ParentInclusionFragment{
		CertificateBytes: parentBytes,
		ShardLeafValue:   append([]byte(nil), childRoot...),
	}
	_, err = ComposeInclusionCert(fragment, child, childRoot)
	if !errors.Is(err, ErrCertDepthOverlap) {
		t.Fatalf("expected ErrCertDepthOverlap, got %v", err)
	}
}

func TestComposeInclusionCert_RejectsParentDeeperThanChild(t *testing.T) {
	child := &InclusionCert{}
	child.Bitmap[3/8] |= 1 << (3 % 8)
	var childS [SiblingSize]byte
	child.Siblings = append(child.Siblings, childS)

	parent := &InclusionCert{}
	parent.Bitmap[7/8] |= 1 << (7 % 8)
	var parentS [SiblingSize]byte
	parent.Siblings = append(parent.Siblings, parentS)
	parentBytes, err := parent.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal parent cert: %v", err)
	}

	childRoot := bytes.Repeat([]byte{0x22}, SiblingSize)
	fragment := &ParentInclusionFragment{
		CertificateBytes: parentBytes,
		ShardLeafValue:   append([]byte(nil), childRoot...),
	}
	_, err = ComposeInclusionCert(fragment, child, childRoot)
	if !errors.Is(err, ErrCertDepthOrder) {
		t.Fatalf("expected ErrCertDepthOrder, got %v", err)
	}
}
