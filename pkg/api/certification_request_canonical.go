package api

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

// ErrCertificationRequestNotCanonical is returned by
// UnmarshalCanonicalCertificationRequestCBOR when the input is not encoded in
// canonical (Core Deterministic) CBOR form. Non-canonical encodings are
// rejected at the public API boundary so the same logical request cannot be
// expressed by multiple distinct byte sequences.
var ErrCertificationRequestNotCanonical = errors.New("certification request CBOR is not canonical")

// UnmarshalCanonicalCertificationRequestCBOR decodes data into out and rejects
// the input if it is not in canonical Core Deterministic CBOR form.
func UnmarshalCanonicalCertificationRequestCBOR(data []byte, out *CertificationRequest) error {
	if out == nil {
		return errors.New("nil CertificationRequest output")
	}
	if err := validateCanonicalCBOR(data); err != nil {
		return err
	}
	if err := types.Cbor.Unmarshal(data, out); err != nil {
		return err
	}
	if out.Version != 1 {
		return fmt.Errorf("unsupported CertificationRequest version: %d", out.Version)
	}
	if out.CertificationData.Version != 1 {
		return fmt.Errorf("unsupported CertificationData version: %d", out.CertificationData.Version)
	}
	return nil
}

const maxCanonicalCBORDepth = 64

const (
	cborMajorUnsignedInt byte = 0
	cborMajorNegativeInt byte = 1
	cborMajorByteString  byte = 2
	cborMajorTextString  byte = 3
	cborMajorArray       byte = 4
	cborMajorMap         byte = 5
	cborMajorTag         byte = 6
	cborMajorSimpleFloat byte = 7

	cborAdditionalOneByte       byte = 24
	cborAdditionalTwoBytes      byte = 25
	cborAdditionalFourBytes     byte = 26
	cborAdditionalEightBytes    byte = 27
	cborAdditionalIndefiniteLen byte = 31
)

type canonicalCBORValidator struct {
	data []byte
	pos  int
}

func validateCanonicalCBOR(data []byte) error {
	w := canonicalCBORValidator{data: data}
	if err := w.readItem(0); err != nil {
		return err
	}
	if w.pos != len(w.data) {
		return fmt.Errorf("%w: trailing data", ErrCertificationRequestNotCanonical)
	}
	return nil
}

func (w *canonicalCBORValidator) readItem(depth int) error {
	if depth > maxCanonicalCBORDepth {
		return fmt.Errorf("%w: nesting too deep", ErrCertificationRequestNotCanonical)
	}

	ib, err := w.readByte()
	if err != nil {
		return err
	}
	major := ib >> 5
	ai := ib & 0x1f
	if major == cborMajorSimpleFloat {
		return w.readSimpleOrFloat(ai)
	}

	arg, err := w.readArgument(ai)
	if err != nil {
		return err
	}

	switch major {
	case cborMajorUnsignedInt, cborMajorNegativeInt:
		return nil
	case cborMajorByteString, cborMajorTextString:
		return w.skip(arg)
	case cborMajorArray:
		for i := uint64(0); i < arg; i++ {
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case cborMajorMap:
		var prevStart, prevEnd int
		for i := uint64(0); i < arg; i++ {
			keyStart := w.pos
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
			keyEnd := w.pos
			// CertificationRequest has no map fields today. If that changes,
			// enforce the same RFC 8949 Core Deterministic map ordering used by
			// types.Cbor.Marshal: bytewise lexical order of encoded keys.
			if i > 0 && bytes.Compare(w.data[prevStart:prevEnd], w.data[keyStart:keyEnd]) >= 0 {
				return fmt.Errorf("%w: map keys not in core deterministic order", ErrCertificationRequestNotCanonical)
			}
			prevStart, prevEnd = keyStart, keyEnd
			if err := w.readItem(depth + 1); err != nil {
				return err
			}
		}
		return nil
	case cborMajorTag:
		return w.readItem(depth + 1)
	default:
		return fmt.Errorf("%w: invalid major type", ErrCertificationRequestNotCanonical)
	}
}

func (w *canonicalCBORValidator) readByte() (byte, error) {
	if w.pos >= len(w.data) {
		return 0, fmt.Errorf("%w: premature end of data", ErrCertificationRequestNotCanonical)
	}
	b := w.data[w.pos]
	w.pos++
	return b, nil
}

// readArgument reads the CBOR argument used by major types 0 through 6 and
// enforces preferred serialization: integers, lengths, and tags must use the
// shortest possible additional-information form.
func (w *canonicalCBORValidator) readArgument(ai byte) (uint64, error) {
	switch {
	case ai < cborAdditionalOneByte:
		return uint64(ai), nil
	case ai == cborAdditionalOneByte:
		b, err := w.readByte()
		if err != nil {
			return 0, err
		}
		if b < cborAdditionalOneByte {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrCertificationRequestNotCanonical)
		}
		return uint64(b), nil
	case ai == cborAdditionalTwoBytes:
		v, err := w.readUint(2)
		if err != nil {
			return 0, err
		}
		if v < 1<<8 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrCertificationRequestNotCanonical)
		}
		return v, nil
	case ai == cborAdditionalFourBytes:
		v, err := w.readUint(4)
		if err != nil {
			return 0, err
		}
		if v < 1<<16 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrCertificationRequestNotCanonical)
		}
		return v, nil
	case ai == cborAdditionalEightBytes:
		v, err := w.readUint(8)
		if err != nil {
			return 0, err
		}
		if v < 1<<32 {
			return 0, fmt.Errorf("%w: non-shortest argument encoding", ErrCertificationRequestNotCanonical)
		}
		return v, nil
	default:
		return 0, fmt.Errorf("%w: indefinite or reserved additional information", ErrCertificationRequestNotCanonical)
	}
}

func (w *canonicalCBORValidator) readUint(n int) (uint64, error) {
	if len(w.data)-w.pos < n {
		return 0, fmt.Errorf("%w: premature end of data", ErrCertificationRequestNotCanonical)
	}
	var v uint64
	for i := 0; i < n; i++ {
		v = (v << 8) | uint64(w.data[w.pos+i])
	}
	w.pos += n
	return v, nil
}

func (w *canonicalCBORValidator) skip(n uint64) error {
	if n > uint64(len(w.data)-w.pos) {
		return fmt.Errorf("%w: premature end of data", ErrCertificationRequestNotCanonical)
	}
	w.pos += int(n)
	return nil
}

func (w *canonicalCBORValidator) readSimpleOrFloat(ai byte) error {
	switch {
	case ai < cborAdditionalOneByte:
		return nil
	case ai == cborAdditionalOneByte:
		b, err := w.readByte()
		if err != nil {
			return err
		}
		if b < cborAdditionalOneByte {
			return fmt.Errorf("%w: reserved or non-shortest simple value encoding", ErrCertificationRequestNotCanonical)
		}
		return nil
	case ai == cborAdditionalTwoBytes, ai == cborAdditionalFourBytes, ai == cborAdditionalEightBytes:
		return fmt.Errorf("%w: floating point values are not valid certification request CBOR", ErrCertificationRequestNotCanonical)
	default:
		return fmt.Errorf("%w: invalid simple value encoding", ErrCertificationRequestNotCanonical)
	}
}
