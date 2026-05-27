package disk

import "fmt"

// CompressedPath stores an internal node's common-prefix bits. The bits are
// navigation metadata only; they are not part of yellowpaper node hashes.
type CompressedPath struct {
	len  uint8
	bits [KeySize]byte
}

func EmptyPath() CompressedPath {
	return CompressedPath{}
}

func NewCompressedPathFromKeyRange(key Key, startBit, bitLen int) (CompressedPath, error) {
	if bitLen < 0 || bitLen > 255 {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid compressed path length %d", bitLen)
	}
	if startBit < 0 || startBit+bitLen > KeyBits {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid key range start=%d len=%d", startBit, bitLen)
	}

	path := CompressedPath{len: uint8(bitLen)}
	for i := 0; i < bitLen; i++ {
		if KeyBit(key, startBit+i) != 0 {
			path.bits[i/8] |= 1 << (uint(i) % 8)
		}
	}
	return path, nil
}

func NewCompressedPathFromRaw(bitLen int, raw []byte) (CompressedPath, error) {
	if bitLen < 0 || bitLen > 255 {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid compressed path length %d", bitLen)
	}
	byteLen := prefixByteLen(bitLen)
	if len(raw) != byteLen {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid compressed path bytes: got %d, want %d", len(raw), byteLen)
	}

	path := CompressedPath{len: uint8(bitLen)}
	copy(path.bits[:], raw)
	if !path.hasCanonicalUnusedBits() {
		return CompressedPath{}, fmt.Errorf("disk smt: compressed path has non-zero unused bits")
	}
	return path, nil
}

func NewCompressedPathFromPathRange(src CompressedPath, start, bitLen int) (CompressedPath, error) {
	if bitLen < 0 || bitLen > 255 {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid compressed path length %d", bitLen)
	}
	if start < 0 || start+bitLen > src.Len() {
		return CompressedPath{}, fmt.Errorf("disk smt: invalid compressed path range start=%d len=%d sourceLen=%d", start, bitLen, src.Len())
	}

	path := CompressedPath{len: uint8(bitLen)}
	for i := 0; i < bitLen; i++ {
		if src.BitAt(start+i) != 0 {
			path.bits[i/8] |= 1 << (uint(i) % 8)
		}
	}
	return path, nil
}

func (p CompressedPath) Len() int {
	return int(p.len)
}

func (p CompressedPath) BitAt(pos int) byte {
	if pos < 0 || pos >= int(p.len) {
		panic(fmt.Sprintf("disk smt: compressed path bit index out of range: %d", pos))
	}
	return (p.bits[pos/8] >> (uint(pos) % 8)) & 1
}

func (p CompressedPath) Bytes() []byte {
	byteLen := prefixByteLen(int(p.len))
	out := make([]byte, byteLen)
	copy(out, p.bits[:byteLen])
	return out
}

func (p CompressedPath) Equal(other CompressedPath) bool {
	if p.len != other.len {
		return false
	}
	byteLen := prefixByteLen(int(p.len))
	for i := 0; i < byteLen; i++ {
		if p.bits[i] != other.bits[i] {
			return false
		}
	}
	return true
}

func (p CompressedPath) hasCanonicalUnusedBits() bool {
	bitLen := int(p.len)
	byteLen := prefixByteLen(bitLen)
	for i := byteLen; i < len(p.bits); i++ {
		if p.bits[i] != 0 {
			return false
		}
	}
	if rem := bitLen % 8; rem != 0 {
		mask := byte(1<<uint(rem)) - 1
		return p.bits[byteLen-1]&^mask == 0
	}
	return true
}

func prefixByteLen(bitLen int) int {
	return (bitLen + 7) / 8
}
