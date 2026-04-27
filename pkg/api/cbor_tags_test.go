package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// cborTagPrefix returns the raw CBOR bytes that encode `tag` as a tag head
// followed by a major-4 array-of-N header. Used by wire-format tests to
// assert the exact tag/arity of the emitted CBOR.
func cborTagPrefix(t *testing.T, tag types.CborTag, arrayLen int) []byte {
	t.Helper()
	require.Less(t, arrayLen, 24, "arrayLen %d exceeds single-byte array header", arrayLen)
	// Tags we use live in 0x4000..0xFFFF; encoded with major 6 + additional-info 25 + 2-byte big-endian.
	require.Less(t, uint64(tag), uint64(1)<<16, "tag %d too large for 2-byte encoding", tag)
	return []byte{
		0xd9, byte(tag >> 8), byte(tag),
		0x80 | byte(arrayLen), // array header
	}
}

func TestCertificationRequest_WireFormat(t *testing.T) {
	req := &CertificationRequest{
		StateID:           RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000"),
		CertificationData: createCertData(t),
	}

	b, err := types.Cbor.Marshal(req)
	require.NoError(t, err)

	prefix := cborTagPrefix(t, CertificationRequestTag, 4)
	require.Equal(t, prefix, b[:len(prefix)], "outer tag + array header mismatch")
	// Version slot is the first element of the array, encoded as uint 1 (0x01).
	require.Equal(t, byte(0x01), b[len(prefix)], "Version slot should be 1")
}

func TestCertificationRequest_RejectsWrongTag(t *testing.T) {
	req := &CertificationRequest{
		StateID:           RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000"),
		CertificationData: createCertData(t),
	}
	b, err := types.Cbor.Marshal(req)
	require.NoError(t, err)

	// Flip the tag from CertificationRequestTag (39030) to CertificationDataTag (39031).
	b[2] ^= 0x01

	var decoded CertificationRequest
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag")
}

func TestCertificationRequest_RejectsWrongVersion(t *testing.T) {
	req := &CertificationRequest{
		StateID:           RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000"),
		CertificationData: createCertData(t),
	}
	b, err := types.Cbor.Marshal(req)
	require.NoError(t, err)

	// Version byte lives right after the 3-byte tag head and 1-byte array header.
	b[4] = 0x02

	var decoded CertificationRequest
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

func TestCertificationData_WireFormat(t *testing.T) {
	cd := createCertData(t)

	b, err := types.Cbor.Marshal(&cd)
	require.NoError(t, err)

	prefix := cborTagPrefix(t, CertificationDataTag, 5)
	require.Equal(t, prefix, b[:len(prefix)])
	require.Equal(t, byte(0x01), b[len(prefix)], "Version slot should be 1")
}

func TestCertificationData_RejectsWrongTag(t *testing.T) {
	cd := createCertData(t)
	b, err := types.Cbor.Marshal(&cd)
	require.NoError(t, err)
	b[2] ^= 0x02 // flip to some unrelated tag

	var decoded CertificationData
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag")
}

func TestCertificationData_RejectsWrongVersion(t *testing.T) {
	cd := createCertData(t)
	b, err := types.Cbor.Marshal(&cd)
	require.NoError(t, err)
	b[4] = 0x09

	var decoded CertificationData
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

func TestPredicate_WireFormat(t *testing.T) {
	pred := NewPayToPublicKeyPredicate([]byte{0x01, 0x02, 0x03})

	b, err := types.Cbor.Marshal(pred)
	require.NoError(t, err)

	// Predicate is tag-wrapped with a 3-element array and carries NO Version slot.
	prefix := cborTagPrefix(t, PredicateTag, 3)
	require.Equal(t, prefix, b[:len(prefix)])
	// First array element is Engine (uint 1).
	require.Equal(t, byte(0x01), b[len(prefix)], "Engine slot should be 1 (no Version field)")
}

func TestPredicate_RejectsWrongTag(t *testing.T) {
	pred := NewPayToPublicKeyPredicate([]byte{0x01, 0x02, 0x03})
	b, err := types.Cbor.Marshal(pred)
	require.NoError(t, err)
	b[2] ^= 0x01 // flip to a neighbouring tag

	var decoded Predicate
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag")
}

func TestInclusionProofV2_WireFormat(t *testing.T) {
	cd := createCertData(t)
	proof := &InclusionProofV2{
		CertificationData: &cd,
		CertificateBytes:  HexBytes{0x01, 0x02},
		// UnicityCertificate is raw CBOR; an empty byte-string is valid CBOR.
		UnicityCertificate: types.RawCBOR{0x40},
	}

	b, err := types.Cbor.Marshal(proof)
	require.NoError(t, err)

	prefix := cborTagPrefix(t, InclusionProofTag, 4)
	require.Equal(t, prefix, b[:len(prefix)])
	require.Equal(t, byte(0x01), b[len(prefix)], "Version slot should be 1")
}

func TestInclusionProofV2_RejectsWrongTag(t *testing.T) {
	cd := createCertData(t)
	proof := &InclusionProofV2{
		CertificationData:  &cd,
		CertificateBytes:   HexBytes{0x01, 0x02},
		UnicityCertificate: types.RawCBOR{0x40},
	}
	b, err := types.Cbor.Marshal(proof)
	require.NoError(t, err)
	b[2] ^= 0x01

	var decoded InclusionProofV2
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag")
}

func TestInclusionProofV2_RejectsWrongVersion(t *testing.T) {
	cd := createCertData(t)
	proof := &InclusionProofV2{
		CertificationData:  &cd,
		CertificateBytes:   HexBytes{0x01, 0x02},
		UnicityCertificate: types.RawCBOR{0x40},
	}
	b, err := types.Cbor.Marshal(proof)
	require.NoError(t, err)
	b[4] = 0x07

	var decoded InclusionProofV2
	err = types.Cbor.Unmarshal(b, &decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}
