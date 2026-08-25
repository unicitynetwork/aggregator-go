package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// CertificationData.UnmarshalCBOR relies on the toarray decode to reject a
// wrong tag and a wrong element count, rather than probing the payload first.
// These cases pin that validation surface so it cannot be weakened silently.
func TestCertificationDataUnmarshalValidationSurface(t *testing.T) {
	type sixField struct {
		_               struct{} `cbor:",toarray"`
		Version         types.Version
		OwnerPredicate  Predicate
		SourceStateHash SourceStateHash
		TransactionHash TransactionHash
		ExpiresAt       *uint64
		Witness         HexBytes
	}
	type fiveField struct {
		_               struct{} `cbor:",toarray"`
		Version         types.Version
		OwnerPredicate  Predicate
		SourceStateHash SourceStateHash
		TransactionHash TransactionHash
		Witness         HexBytes
	}
	type sevenField struct {
		_               struct{} `cbor:",toarray"`
		Version         types.Version
		OwnerPredicate  Predicate
		SourceStateHash SourceStateHash
		TransactionHash TransactionHash
		ExpiresAt       *uint64
		Witness         HexBytes
		Extra           uint64
	}

	predicate := Predicate{Engine: 1, Code: []byte{0x01}, Params: []byte{0x02}}
	sourceStateHash := RequireNewImprintV2("cd60000000000000000000000000000000000000000000000000000000000000")
	transactionHash := RequireNewImprintV2("cd61000000000000000000000000000000000000000000000000000000000000")
	witness := HexBytes(make([]byte, 65))

	base := sixField{
		Version: CertificationDataVersion, OwnerPredicate: predicate,
		SourceStateHash: sourceStateHash, TransactionHash: transactionHash,
		ExpiresAt: Uint64Ptr(1755003600), Witness: witness,
	}
	marshal := func(tag uint64, v any) []byte {
		b, err := types.Cbor.MarshalTaggedValue(tag, v)
		require.NoError(t, err)
		return b
	}

	nilExpiry := base
	nilExpiry.ExpiresAt = nil
	v1 := base
	v1.Version = 1
	v3 := base
	v3.Version = 3
	v0 := base
	v0.Version = 0

	tests := []struct {
		name   string
		data   []byte
		accept bool
	}{
		{"explicit ExpiresAt", marshal(CertificationDataTag, &base), true},
		{"absent ExpiresAt", marshal(CertificationDataTag, &nilExpiry), true},
		{"version 0", marshal(CertificationDataTag, &v0), false},
		{"version 1", marshal(CertificationDataTag, &v1), false},
		{"version 3", marshal(CertificationDataTag, &v3), false},
		{"wrong tag", marshal(CertificationRequestTag, &base), false},
		{"too few fields", marshal(CertificationDataTag, &fiveField{
			Version: CertificationDataVersion, OwnerPredicate: predicate,
			SourceStateHash: sourceStateHash, TransactionHash: transactionHash, Witness: witness,
		}), false},
		{"too many fields", marshal(CertificationDataTag, &sevenField{
			Version: CertificationDataVersion, OwnerPredicate: predicate,
			SourceStateHash: sourceStateHash, TransactionHash: transactionHash,
			ExpiresAt: Uint64Ptr(1755003600), Witness: witness, Extra: 9,
		}), false},
		{"tagged non-array", marshal(CertificationDataTag, uint64(7)), false},
		{"empty", []byte{}, false},
		{"garbage", []byte{0xff, 0xff, 0xff}, false},
		{"truncated", marshal(CertificationDataTag, &base)[:5], false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cd CertificationData
			err := cd.UnmarshalCBOR(tt.data)
			if tt.accept {
				require.NoError(t, err)
				require.Equal(t, CertificationDataVersion, cd.Version)
				return
			}
			require.Error(t, err)
		})
	}
}

// An absent deadline must survive a decode/encode round trip as absent, not as
// zero: zero is a legal instant, so the two cannot share a representation.
func TestCertificationDataAbsentExpiresAtRoundTrips(t *testing.T) {
	original := &CertificationData{
		Version:         CertificationDataVersion,
		OwnerPredicate:  Predicate{Engine: 1, Code: []byte{0x01}, Params: []byte{0x02}},
		SourceStateHash: RequireNewImprintV2("cd60000000000000000000000000000000000000000000000000000000000000"),
		TransactionHash: RequireNewImprintV2("cd61000000000000000000000000000000000000000000000000000000000000"),
		Witness:         make([]byte, 65),
	}
	encoded, err := original.MarshalCBOR()
	require.NoError(t, err)

	var decoded CertificationData
	require.NoError(t, decoded.UnmarshalCBOR(encoded))
	require.Nil(t, decoded.ExpiresAt)

	zero := *original
	zero.ExpiresAt = Uint64Ptr(0)
	zeroEncoded, err := zero.MarshalCBOR()
	require.NoError(t, err)
	require.NotEqual(t, encoded, zeroEncoded, "absent and zero must not encode identically")
}
