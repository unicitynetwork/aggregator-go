package models

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestCertificationRequestLeafValue_V2UsesTransactionHashBytes(t *testing.T) {
	txRaw := "11223344556677889900aabbccddeeff00112233445566778899aabbccddeeff"

	reqRaw := &CertificationRequest{
		Version: 2,
		CertificationData: CertificationData{
			TransactionHash: api.RequireNewImprintV2(txRaw),
		},
	}
	leafRaw, err := reqRaw.LeafValue()
	require.NoError(t, err)
	require.Equal(t, api.RequireNewImprintV2(txRaw), api.ImprintV2(leafRaw))
}
