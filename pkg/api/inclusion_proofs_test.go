package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func hexID(n int) string {
	return fmt.Sprintf("%064x", n+1)
}

func idList(n int) string {
	var s []string
	for i := range n {
		s = append(s, `"`+hexID(i)+`"`)
	}
	return "[" + strings.Join(s, ",") + "]"
}

func requireParamsErr(t *testing.T, params, detail string, limit int) {
	t.Helper()
	_, err := ParseGetInclusionProofsRequestV1([]byte(params))
	var pe *InclusionProofsParamsError
	require.True(t, errors.As(err, &pe), "params %.80s: %v", params, err)
	require.Equal(t, detail, pe.Detail)
	require.Equal(t, limit, pe.Limit)
}

func TestParseInclusionProofsRequest_Valid(t *testing.T) {
	upper := strings.ToUpper(hexID(1))
	r, err := ParseGetInclusionProofsRequestV1([]byte(`{"stateIds":["` + hexID(0) + `","0x` + upper + `"]}`))
	require.NoError(t, err)
	require.Len(t, r.StateIDs, 2)
	require.Nil(t, r.Root, "omitted stateRoot is latest")
	require.Equal(t, StateTreeKeyLengthBytes, len(r.StateIDs[1]))

	r, err = ParseGetInclusionProofsRequestV1([]byte(`{"stateRoot":"latest","stateIds":` + idList(256) + `}`))
	require.NoError(t, err)
	require.Len(t, r.StateIDs, 256)
	require.Nil(t, r.Root)

	r, err = ParseGetInclusionProofsRequestV1([]byte(`{"stateIds":` + idList(1) + `,"stateRoot":"0x` + strings.ToUpper(hexID(77)) + `"}`))
	require.NoError(t, err)
	require.Equal(t, mustHex(t, hexID(77)), []byte(r.Root))
}

func mustHex(t *testing.T, s string) []byte {
	b, err := NewHexBytesFromString(s)
	require.NoError(t, err)
	return b
}

func TestParseInclusionProofsRequest_Rejections(t *testing.T) {
	ok := idList(1)
	for name, tc := range map[string]struct {
		params, detail string
		limit          int
	}{
		"positional":        {`["` + hexID(0) + `"]`, DetailMalformedParams, 0},
		"null":              {`null`, DetailMalformedParams, 0},
		"string":            {`"x"`, DetailMalformedParams, 0},
		"empty object":      {`{}`, DetailMalformedParams, 0},
		"empty list":        {`{"stateIds":[]}`, DetailMalformedParams, 0},
		"null list":         {`{"stateIds":null}`, DetailMalformedParams, 0},
		"object list":       {`{"stateIds":{}}`, DetailMalformedParams, 0},
		"number entry":      {`{"stateIds":[1]}`, DetailMalformedParams, 0},
		"unknown field":     {`{"stateIds":` + ok + `,"extra":1}`, DetailMalformedParams, 0},
		"duplicate key":     {`{"stateIds":` + ok + `,"stateIds":` + ok + `}`, DetailMalformedParams, 0},
		"trailing data":     {`{"stateIds":` + ok + `} {}`, DetailMalformedParams, 0},
		"short id":          {`{"stateIds":["abcd"]}`, DetailInvalidStateID, 0},
		"long id":           {`{"stateIds":["` + hexID(0) + `00"]}`, DetailInvalidStateID, 0},
		"non hex id":        {`{"stateIds":["` + strings.Repeat("zz", 32) + `"]}`, DetailInvalidStateID, 0},
		"duplicate id":      {`{"stateIds":["` + hexID(3) + `","` + hexID(4) + `","0x` + strings.ToUpper(hexID(3)) + `"]}`, DetailDuplicateStateID, 0},
		"257 ids":           {`{"stateIds":` + idList(257) + `}`, DetailStateCount, 256},
		"null root":         {`{"stateIds":` + ok + `,"stateRoot":null}`, DetailInvalidStateRoot, 0},
		"empty root":        {`{"stateIds":` + ok + `,"stateRoot":""}`, DetailInvalidStateRoot, 0},
		"other keyword":     {`{"stateIds":` + ok + `,"stateRoot":"earliest"}`, DetailInvalidStateRoot, 0},
		"uppercase keyword": {`{"stateIds":` + ok + `,"stateRoot":"LATEST"}`, DetailInvalidStateRoot, 0},
		"short root":        {`{"stateIds":` + ok + `,"stateRoot":"abcd"}`, DetailInvalidStateRoot, 0},
		"number root":       {`{"stateIds":` + ok + `,"stateRoot":5}`, DetailInvalidStateRoot, 0},
		"params too big":    {`{"stateIds":` + ok + `,"pad":"` + strings.Repeat("a", InclusionProofsMaxParamsBytes) + `"}`, DetailRequestBytes, InclusionProofsMaxParamsBytes},
	} {
		t.Run(name, func(t *testing.T) { requireParamsErr(t, tc.params, tc.detail, tc.limit) })
	}
	// omitted params
	_, err := ParseGetInclusionProofsRequestV1(nil)
	require.Error(t, err)
}

// The ID count is enforced while streaming: a huge list is cut at the 257th entry.
func TestParseInclusionProofsRequest_CountsBeforeMaterializing(t *testing.T) {
	n := InclusionProofsMaxParamsBytes / 70
	require.Greater(t, n, 256)
	requireParamsErr(t, `{"stateIds":`+idList(n)+`}`, DetailStateCount, 256)
}

func TestInclusionProofsResponseJSONShape(t *testing.T) {
	b, err := json.Marshal(&GetInclusionProofsResponseV1{Proofs: []*GetInclusionProofResponseV2{}})
	require.NoError(t, err)
	require.Equal(t, `{"proofs":[]}`, string(b))
	b, err = json.Marshal(&GetInclusionProofsResponseV1{Proofs: []*GetInclusionProofResponseV2{}, PartitionView: HexBytes{0xab}})
	require.NoError(t, err)
	require.Equal(t, `{"proofs":[],"partitionView":"ab"}`, string(b))
}

func TestAppendJSONBoundedMatchesMarshalAndStopsEarly(t *testing.T) {
	mk := func(n, witness int) *GetInclusionProofsResponseV1 {
		proofs := make([]*GetInclusionProofResponseV2, n)
		for i := range proofs {
			rt := uint64(7)
			proofs[i] = &GetInclusionProofResponseV2{BlockNumber: uint64(i), InclusionProof: &InclusionProofV2{
				CertificationData: &CertificationData{Version: CertificationDataVersion, OwnerPredicate: Predicate{Engine: 1, Code: []byte{1}, Params: []byte{2}},
					SourceStateHash: bytes.Repeat([]byte{3}, 32), TransactionHash: RequireNewImprintV2(strings.Repeat("22", 32)), Witness: bytes.Repeat([]byte{4}, witness)},
				ReferenceTime: &rt, CertificateBytes: make([]byte, BitmapSize), UnicityCertificate: []byte{0x01}}}
		}
		return &GetInclusionProofsResponseV1{Proofs: proofs}
	}
	for _, r := range []*GetInclusionProofsResponseV1{mk(0, 1), mk(3, 65), {Proofs: mk(2, 10).Proofs, PartitionView: HexBytes{1, 2, 0xff}}} {
		want, err := json.Marshal(r)
		require.NoError(t, err)
		got, err := r.AppendJSONBounded(nil, len(want))
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
		_, err = r.AppendJSONBounded(nil, len(want)-1)
		require.ErrorIs(t, err, ErrBoundedOutputExceeded)
	}
}
