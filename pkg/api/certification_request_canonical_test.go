package api

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// canonicalCertificationRequestFixture builds a well-formed canonical
// CertificationRequest and returns both the struct and its canonical CBOR
// encoding. Mutations of the byte slice in tests are surgical edits intended
// to break canonicality while leaving the structure decodable.
func canonicalCertificationRequestFixture(t testing.TB) (*CertificationRequest, []byte) {
	t.Helper()
	publicKey, err := NewHexBytesFromString("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
	require.NoError(t, err)
	witness, err := NewHexBytesFromString("a0b37f8fba683cc68f6574cd43b39f0343a50008bf6ccea9d13231d9e7e2e1e411edc8d307254296264aebfc3dc76cd8b668373a072fd64665b50000e9fcce5201")
	require.NoError(t, err)

	req := &CertificationRequest{
		StateID: RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000"),
		CertificationData: CertificationData{
			Version:         1,
			OwnerPredicate:  NewPayToPublicKeyPredicate(publicKey),
			SourceStateHash: RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000000"),
			TransactionHash: RequireNewImprintV2("0000000000000000000000000000000000000000000000000000000000000001"),
			Witness:         witness,
		},
	}
	b, err := types.Cbor.Marshal(req)
	require.NoError(t, err)
	return req, b
}

func TestUnmarshalCanonicalCertificationRequestCBOR_AcceptsCanonical(t *testing.T) {
	want, canonical := canonicalCertificationRequestFixture(t)

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(canonical, &got)
	require.NoError(t, err)

	require.Equal(t, want.StateID, got.StateID)
	require.Equal(t, want.CertificationData, got.CertificationData)
}

func TestUnmarshalCanonicalCertificationRequestCBOR_NilOutput(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	err := UnmarshalCanonicalCertificationRequestCBOR(canonical, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func TestUnmarshalCanonicalCertificationRequestCBOR_RejectsTrailingData(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	tainted := append(append([]byte{}, canonical...), 0xf6) // append CBOR null

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

// TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalVersionInteger
// verifies that a decode-succeeds-but-non-canonical input is rejected. We
// re-encode the Version uint (canonical: 0x01) using the 1-byte length form
// (0x18 0x01), which is decoded as uint(1) but is not shortest-form.
func TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalVersionInteger(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	// Outer wrapper: 0xd9 0x98 0x76 (tag 39030 = CertificationRequestTag),
	// then 0x84 (array of 4), then Version slot = 0x01 at offset 4.
	require.Equal(t, byte(0x01), canonical[4], "fixture invariant: Version byte at offset 4")

	tainted := make([]byte, 0, len(canonical)+1)
	tainted = append(tainted, canonical[:4]...)
	tainted = append(tainted, 0x18, 0x01) // uint(1) in 1-byte form, non-canonical
	tainted = append(tainted, canonical[5:]...)

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalTag(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	require.Equal(t, []byte{0xd9, 0x98, 0x76}, canonical[:3], "fixture invariant: request tag")

	tainted := make([]byte, 0, len(canonical)+2)
	tainted = append(tainted, 0xda, 0x00, 0x00, 0x98, 0x76) // tag 39030 in 4-byte form
	tainted = append(tainted, canonical[3:]...)

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

// TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalByteStringLength
// re-encodes the 32-byte StateID's length header in a non-shortest form. The
// canonical header for a 32-byte byte string is 0x58 0x20; the 2-byte form
// 0x59 0x00 0x20 decodes identically but is not canonical.
func TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalByteStringLength(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	// Locate the StateID byte string by its canonical header. It immediately
	// follows the Version slot at offset 4 (single-byte uint 1).
	stateIDHeader := []byte{0x58, 0x20}
	idx := bytes.Index(canonical, stateIDHeader)
	require.Equal(t, 5, idx, "fixture invariant: StateID header at offset 5")

	tainted := make([]byte, 0, len(canonical)+1)
	tainted = append(tainted, canonical[:idx]...)
	tainted = append(tainted, 0x59, 0x00, 0x20) // length 32 in 2-byte form
	tainted = append(tainted, canonical[idx+2:]...)

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

// TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalNestedPredicateInteger
// surfaces non-canonical encoding inside the nested OwnerPredicate. The
// predicate's Engine field is uint(1) and canonically encoded as 0x01. We
// re-encode it as 0x18 0x01, which still decodes but is not canonical.
func TestUnmarshalCanonicalCertificationRequestCBOR_RejectsNonCanonicalNestedPredicateInteger(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	// Find the nested Predicate tag head (39032 = 0x9878), followed by 0x83
	// (array of 3) and then the Engine slot encoded as 0x01.
	predicateMarker := []byte{0xd9, 0x98, 0x78, 0x83, 0x01}
	idx := bytes.Index(canonical, predicateMarker)
	require.GreaterOrEqual(t, idx, 0, "fixture invariant: nested predicate marker not found")

	enginePos := idx + 4
	require.Equal(t, byte(0x01), canonical[enginePos])

	tainted := make([]byte, 0, len(canonical)+1)
	tainted = append(tainted, canonical[:enginePos]...)
	tainted = append(tainted, 0x18, 0x01)
	tainted = append(tainted, canonical[enginePos+1:]...)

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func TestUnmarshalCanonicalCertificationRequestCBOR_IndefiniteLengthArray(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	require.Equal(t, byte(0x84), canonical[3], "fixture invariant: outer array header at offset 3")

	tainted := make([]byte, 0, len(canonical)+1)
	tainted = append(tainted, canonical[:3]...)
	tainted = append(tainted, 0x9f) // indefinite-length array
	tainted = append(tainted, canonical[4:]...)
	tainted = append(tainted, 0xff) // break

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func TestUnmarshalCanonicalCertificationRequestCBOR_VersionZero(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	require.Equal(t, byte(0x01), canonical[4], "fixture invariant: Version byte at offset 4")
	tainted := append([]byte{}, canonical...)
	tainted[4] = 0x00 // uint(0), shortest-form, decodes to Version=0

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported CertificationRequest version: 0")
}

func TestUnmarshalCanonicalCertificationRequestCBOR_NestedVersionZero(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	certDataMarker := []byte{0xd9, 0x98, 0x77, 0x85, 0x01}
	idx := bytes.Index(canonical, certDataMarker)
	require.GreaterOrEqual(t, idx, 0, "fixture invariant: nested certification data marker not found")

	versionPos := idx + 4
	tainted := append([]byte{}, canonical...)
	tainted[versionPos] = 0x00

	var got CertificationRequest
	err := UnmarshalCanonicalCertificationRequestCBOR(tainted, &got)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported CertificationData version: 0")
}

func TestCertificationRequest_UnmarshalJSON_RoutesThroughCanonicalCheck(t *testing.T) {
	_, canonical := canonicalCertificationRequestFixture(t)

	// Build the JSON-RPC params shape: a JSON string of the hex-encoded canonical CBOR.
	hexBytes := HexBytes(canonical)
	jsonBytes, err := hexBytes.MarshalJSON()
	require.NoError(t, err)

	var got CertificationRequest
	require.NoError(t, got.UnmarshalJSON(jsonBytes))

	// Tamper with the hex payload to introduce non-canonical version encoding
	// and confirm UnmarshalJSON rejects it.
	tainted := make([]byte, 0, len(canonical)+1)
	tainted = append(tainted, canonical[:4]...)
	tainted = append(tainted, 0x18, 0x01)
	tainted = append(tainted, canonical[5:]...)

	taintedJSON, err := HexBytes(tainted).MarshalJSON()
	require.NoError(t, err)

	var rejected CertificationRequest
	err = rejected.UnmarshalJSON(taintedJSON)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func TestValidateCanonicalCBOR_MapKeyOrdering(t *testing.T) {
	require.NoError(t, validateCanonicalCBOR([]byte{
		0xa2,
		0x01, 0x61, 'a',
		0x02, 0x61, 'b',
	}))

	err := validateCanonicalCBOR([]byte{
		0xa2,
		0x02, 0x61, 'b',
		0x01, 0x61, 'a',
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)

	err = validateCanonicalCBOR([]byte{
		0xa2,
		0x01, 0x61, 'a',
		0x01, 0x61, 'b',
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCertificationRequestNotCanonical)
}

func FuzzValidateCanonicalCBOR(f *testing.F) {
	_, canonical := canonicalCertificationRequestFixture(f)
	f.Add(canonical)
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add([]byte{0x9f, 0xff})
	f.Add([]byte{0xa2, 0x02, 0x61, 'b', 0x01, 0x61, 'a'})

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = validateCanonicalCBOR(data)
	})
}
