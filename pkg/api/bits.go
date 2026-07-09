package api

// Big-endian bit-string helpers (yellowpaper "Radix Sparse Merkle Trees",
// big-endian bit strings). A key/region/bitmap is a 256-bit big-endian bit
// string: bit 0 is the most-significant bit of byte 0, bit 255 the
// least-significant bit of byte 31. Bit d lives in byte d/8 at in-byte
// position 7-(d%8).
//
// Every big-endian bit access in pkg/api and internal/smt routes through these
// three helpers (or the disk-typed wrappers that call them) so a single audit
// point governs the whole bit convention.

// KeyBitBE returns bit d of a big-endian bit string:
// bit d = (buf[d/8] >> (7 - d%8)) & 1.
func KeyBitBE(buf []byte, d int) byte {
	return (buf[d/8] >> (7 - uint(d)%8)) & 1
}

// SetBitBE sets bit d of a big-endian bit string in place.
func SetBitBE(buf []byte, d int) {
	buf[d/8] |= 0x80 >> (uint(d) % 8)
}

// ClearSuffixBE zeroes every bit at position >= bitLen in the big-endian bit
// string buf. The boundary byte keeps its high (bitLen%8) bits; all bytes
// wholly beyond bitLen are zeroed. bitLen must be in [0, len(buf)*8].
func ClearSuffixBE(buf []byte, bitLen int) {
	if bitLen < 0 {
		bitLen = 0
	}
	byteLen := (bitLen + 7) / 8
	if rem := bitLen % 8; rem != 0 && byteLen-1 < len(buf) {
		buf[byteLen-1] &= byte(0xFF << (8 - uint(rem)))
	}
	for i := byteLen; i < len(buf); i++ {
		buf[i] = 0
	}
}
