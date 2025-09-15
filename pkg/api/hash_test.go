package api

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataHashGetImprint(t *testing.T) {
	h := NewDataHash(SHA256, []byte{1, 2, 3})
	expected := []byte{0, 0, 1, 2, 3}
	assert.Equal(t, expected, h.GetImprint(), "Wrong imprint on first call")
	assert.Equal(t, expected, h.imprint, "Imprint should be cached")
	assert.Equal(t, expected, h.GetImprint(), "Wrong imprint on second call")
}

func TestDataHashToHex(t *testing.T) {
	h := NewDataHash(SHA256, []byte{1, 2, 3})
	assert.Equal(t, "0000010203", h.ToHex())
}

func TestNewDataHasher(t *testing.T) {
	t.Run("Invalid algorithm identifier", func(t *testing.T) {
		h := NewDataHasher(-1)
		assert.Nil(t, h)
	})

	t.Run("SHA256 algorithm identifier", func(t *testing.T) {
		h := NewDataHasher(SHA256)
		assert.NotNil(t, h)
		assert.Equal(t, SHA256, h.GetAlgorithm())
	})
}

func TestSHA256Hashing(t *testing.T) {
	t.Run("No input", func(t *testing.T) {
		h := NewDataHasher(SHA256)
		res := h.GetHash().ToHex()
		expected := "0000e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		assert.Equal(t, expected, res)
	})

	// Some NIST test vectors
	vectors := []struct {
		data string
		hash string
	}{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"d3", "28969cdfa74a12c82f3bad960b0b000aca2ac329deea5c2328ebc6f2ba9802c1"},
		{"11af", "5ca7133fa735326081558ac312c620eeca9970d1e70a4b95533d956f072d1f98"},
		{"b4190e", "dff2e73091f6c05e528896c4c831b9448653dc2ff043528f6769437bc7b975c2"},
	}
	for i, v := range vectors {
		t.Run(fmt.Sprintf("Input %d", i), func(t *testing.T) {
			h := NewDataHasher(SHA256)
			d, _ := hex.DecodeString(v.data)
			h.AddData(d)
			res := h.GetHash().ToHex()
			assert.Equal(t, "0000"+v.hash, res)
		})
	}

	t.Run("Chaining", func(t *testing.T) {
		h1 := NewDataHasher(SHA256)
		res1 := h1.AddData([]byte{0, 1, 2}).GetHash()
		h2 := NewDataHasher(SHA256)
		res2 := h2.AddData([]byte{0}).AddData([]byte{1}).AddData([]byte{2}).GetHash()
		assert.Equal(t, res1, res2)
	})

	t.Run("Reset", func(t *testing.T) {
		d := []byte{1, 2, 3}
		h := NewDataHasher(SHA256)
		res1 := h.GetHash().ToHex()
		h.AddData(d)
		res2 := h.GetHash().ToHex()
		h.Reset()
		res3 := h.GetHash().ToHex()
		h.AddData(d)
		res4 := h.GetHash().ToHex()
		assert.NotEqual(t, res1, res2) // Adding data mush change the hash
		assert.Equal(t, res1, res3)    // Resetting must restore inital state
		assert.Equal(t, res2, res4)    // State must change identically after reset
	})
}
