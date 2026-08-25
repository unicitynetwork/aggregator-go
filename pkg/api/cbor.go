// Some quick-and-dirty helpers for CBOR encoding

package api

// cborTag returns the CBOR tag for major type t and parameter value n
func cborTag(t, n int) []byte {
	if n < 0 {
		panic("Negative CBOR tag parameter value")
	}
	if n <= 23 {
		return []byte{byte(t<<5 + n)}
	}
	if n <= 0xff {
		return []byte{byte(t<<5 + 24), byte(n)}
	}
	if n <= 0xffff {
		return []byte{byte(t<<5 + 25), byte(n >> 8 & 0xff), byte(n & 0xff)}
	}
	if n <= 0xffffffff {
		return []byte{byte(t<<5 + 26),
			byte(n >> 24 & 0xff), byte(n >> 16 & 0xff), byte(n >> 8 & 0xff), byte(n & 0xff)}
	}
	return []byte{byte(t<<5 + 27),
		byte(n >> 56 & 0xff), byte(n >> 48 & 0xff), byte(n >> 40 & 0xff), byte(n >> 32 & 0xff),
		byte(n >> 24 & 0xff), byte(n >> 16 & 0xff), byte(n >> 8 & 0xff), byte(n & 0xff)}
}

// CborBytes returns the CBOR tag for "byte string of n bytes"
func CborBytes(n int) []byte {
	return cborTag(2, n)
}

// CborArray returns the CBOR tag for "array of n elements"
func CborArray(n int) []byte {
	return cborTag(4, n)
}

// CborUint returns the CBOR encoding of an unsigned integer.
//
// This does not route through cborTag, whose parameter is an int: converting a
// uint64 there truncates above 2^31 on 32-bit platforms and wraps negative
// above 2^63 anywhere, which cborTag rejects by panicking. Both reach hashing
// paths such as LeafValue, where the input is not always ours to bound.
func CborUint(n uint64) []byte {
	switch {
	case n <= 23:
		return []byte{byte(n)}
	case n <= 0xff:
		return []byte{24, byte(n)}
	case n <= 0xffff:
		return []byte{25, byte(n >> 8), byte(n)}
	case n <= 0xffffffff:
		return []byte{26, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	default:
		return []byte{27,
			byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
			byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	}
}

// CborNull returns the CBOR tag for null
func CborNull() []byte {
	return cborTag(7, 22)
}
