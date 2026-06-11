package cbor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrNotCanonical is returned when CBOR input is not encoded in canonical
// Core Deterministic form.
var ErrNotCanonical = errors.New("CBOR is not canonical")

// ValidateCoreDeterministic rejects CBOR input that is not encoded in
// canonical Core Deterministic form.
func ValidateCoreDeterministic(data []byte) error {
	w := canonicalValidator{data: data}
	if err := w.readItem(0); err != nil {
		return err
	}
	if w.pos != len(w.data) {
		return fmt.Errorf("%w: trailing data", ErrNotCanonical)
	}
	return nil
}

const maxCanonicalDepth = 64

const (
	majorUnsignedInt byte = 0
	majorNegativeInt byte = 1
	majorByteString  byte = 2
	majorTextString  byte = 3
	majorArray       byte = 4
	majorMap         byte = 5
	majorTag         byte = 6
	majorSimpleFloat byte = 7

	additionalOneByte    byte = 24
	additionalTwoBytes   byte = 25
	additionalFourBytes  byte = 26
	additionalEightBytes byte = 27
)

type canonicalValidator struct {
	data []byte
	pos  int
}

func (w *canonicalValidator) readItem(depth int) error {
	if depth > maxCanonicalDepth {
		return fmt.Errorf("%w: nesting too deep", ErrNotCanonical)
	}

	ib, err := w.readByte()
	if err != nil {
		return err
	}
	major := ib >> 5
	ai := ib & 0x1f
	if major == majorSimpleFloat {
		return w.readSimpleOrFloat(ai)
	}

	arg, err := w.readArgument(ai)
	if err != nil {
		return err
	}

	switch major {
	case majorUnsignedInt, majorNegativeInt:
		return nil
	case majorByteString, majorTextString:
		return w.skip(arg)
	case majorArray:
		for i := uint64(0); i < arg; i++ {
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case majorMap:
		var prevStart, prevEnd int
		for i := uint64(0); i < arg; i++ {
			keyStart := w.pos
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
			keyEnd := w.pos
			if i > 0 && bytes.Compare(w.data[prevStart:prevEnd], w.data[keyStart:keyEnd]) >= 0 {
				return fmt.Errorf("%w: map keys not in core deterministic order", ErrNotCanonical)
			}
			prevStart, prevEnd = keyStart, keyEnd
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case majorTag:
		return w.readItem(depth + 1)
	default:
		return fmt.Errorf("%w: invalid major type", ErrNotCanonical)
	}
}

func (w *canonicalValidator) readByte() (byte, error) {
	if w.pos >= len(w.data) {
		return 0, fmt.Errorf("%w: premature end of data", ErrNotCanonical)
	}
	b := w.data[w.pos]
	w.pos++
	return b, nil
}

// readArgument reads the CBOR argument used by major types 0 through 6 and
// enforces preferred serialization: integers, lengths, and tags must use the
// shortest possible additional-information form.
func (w *canonicalValidator) readArgument(ai byte) (uint64, error) {
	switch {
	case ai < additionalOneByte:
		return uint64(ai), nil
	case ai == additionalOneByte:
		b, err := w.readByte()
		if err != nil {
			return 0, err
		}
		if b < additionalOneByte {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrNotCanonical)
		}
		return uint64(b), nil
	case ai == additionalTwoBytes:
		v, err := w.readUint(2)
		if err != nil {
			return 0, err
		}
		if v < 1<<8 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrNotCanonical)
		}
		return v, nil
	case ai == additionalFourBytes:
		v, err := w.readUint(4)
		if err != nil {
			return 0, err
		}
		if v < 1<<16 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrNotCanonical)
		}
		return v, nil
	case ai == additionalEightBytes:
		v, err := w.readUint(8)
		if err != nil {
			return 0, err
		}
		if v < 1<<32 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrNotCanonical)
		}
		return v, nil
	default:
		return 0, fmt.Errorf("%w: indefinite or reserved additional information", ErrNotCanonical)
	}
}

func (w *canonicalValidator) readUint(n int) (uint64, error) {
	if len(w.data)-w.pos < n {
		return 0, fmt.Errorf("%w: premature end of data", ErrNotCanonical)
	}
	var v uint64
	switch n {
	case 2:
		v = uint64(binary.BigEndian.Uint16(w.data[w.pos:]))
	case 4:
		v = uint64(binary.BigEndian.Uint32(w.data[w.pos:]))
	case 8:
		v = binary.BigEndian.Uint64(w.data[w.pos:])
	default:
		for i := 0; i < n; i++ {
			v = (v << 8) | uint64(w.data[w.pos+i])
		}
	}
	w.pos += n
	return v, nil
}

func (w *canonicalValidator) skip(n uint64) error {
	if n > uint64(len(w.data)-w.pos) {
		return fmt.Errorf("%w: premature end of data", ErrNotCanonical)
	}
	w.pos += int(n)
	return nil
}

func (w *canonicalValidator) readSimpleOrFloat(ai byte) error {
	switch {
	case ai < additionalOneByte:
		return nil
	case ai == additionalOneByte:
		b, err := w.readByte()
		if err != nil {
			return err
		}
		if b < 32 {
			return fmt.Errorf("%w: reserved or non-shortest simple value encoding", ErrNotCanonical)
		}
		return nil
	case ai == additionalTwoBytes, ai == additionalFourBytes, ai == additionalEightBytes:
		return fmt.Errorf("%w: floating point values are not supported by this canonical CBOR validator", ErrNotCanonical)
	default:
		return fmt.Errorf("%w: invalid simple value encoding", ErrNotCanonical)
	}
}
