package nifixture

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestRugregatorTwoLeafGoldenVector(t *testing.T) {
	var l0, l1 Leaf
	l0.Value = [32]byte(bytes.Repeat([]byte{1}, 32))
	l1.Value = [32]byte(bytes.Repeat([]byte{2}, 32))
	l1.Key[0] = 0x80
	tree, err := Build([]Leaf{l0, l1})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Root()
	if got := hex.EncodeToString(root[:]); got != "5158a5f4029511f5312b7a147c9da48b9281322d1b893a10a25b2c17e2197a53" {
		t.Fatalf("root %s", got)
	}
	var q [32]byte
	q[0] = 1
	cert := tree.Certificate(q)
	want := "8000000000000000000000000000000000000000000000000000000000000000d79ae116bb8c8604819adc35c2a71b7c3acf89cd1be8a382fbedbd6905ac20e700000000000000000000000000000000000000000000000000000000000000000101010101010101010101010101010101010101010101010101010101010101"
	if hex.EncodeToString(cert) != want {
		t.Fatalf("cert %x", cert)
	}
	if err := Verify(cert, q, &root); err != nil {
		t.Fatal(err)
	}
}
