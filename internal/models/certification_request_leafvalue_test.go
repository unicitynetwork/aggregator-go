package models

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The v2 leaf value binds the round's reference time, so the same request
// yields a different leaf in a different round.
func TestCertificationRequestLeafValue_V2BindsTheReferenceTime(t *testing.T) {
	txRaw := "11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"
	const referenceTime uint64 = 1755000000

	reqRaw := &CertificationRequest{
		Version: 2,
		CertificationData: CertificationData{
			TransactionHash: api.RequireNewImprintV2(txRaw),
		},
	}

	leafRaw, err := reqRaw.LeafValue(referenceTime)
	require.NoError(t, err)
	require.Equal(t, api.LeafValue(api.RequireNewImprintV2(txRaw).DataBytes(), referenceTime), leafRaw)
	require.NotEqual(t, api.RequireNewImprintV2(txRaw), api.ImprintV2(leafRaw))

	laterLeaf, err := reqRaw.LeafValue(referenceTime + 1)
	require.NoError(t, err)
	require.NotEqual(t, leafRaw, laterLeaf)
}

// Materialising a leaf records the reference time it was built from, so the
// record and the served proof report the same value.
func TestCertificationRequestLeafValue_RecordsTheReferenceTime(t *testing.T) {
	req := &CertificationRequest{Version: 2}
	require.Zero(t, req.ReferenceTime)
}
