package service

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The get_block_records response is documented in README.md. A consumer needs
// referenceTime to rebuild the certified leaf value and expiresAt to check the
// request deadline, so silently dropping either makes the record unverifiable.
// This pins the exact key set the endpoint emits.
func TestBlockRecordWireShape(t *testing.T) {
	expiresAt := uint64(1755003600)
	record := &models.AggregatorRecord{
		StateID: api.RequireNewImprintV2("c7aa6962316c0eeb1469dc3d7793e39e140c005e6eea0e188dcc73035d765937"),
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{0x01}, Params: []byte{0x02, 0x03}},
			SourceStateHash: api.RequireNewImprintV2("539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647"),
			TransactionHash: api.RequireNewImprintV2("c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"),
			ExpiresAt:       &expiresAt,
			Witness:         []byte{0x04, 0x05},
		},
		ReferenceTime: 1755000000,
		BlockNumber:   api.NewBigInt(big.NewInt(123)),
		LeafIndex:     api.NewBigInt(big.NewInt(0)),
		CreatedAt:     api.NewTimestamp(time.UnixMilli(1734435600000).UTC()),
	}

	finalizedAt := api.NewTimestamp(time.UnixMilli(1734435601000).UTC())
	encoded, err := json.Marshal(modelToAPIAggregatorRecord(record, finalizedAt))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.ElementsMatch(t,
		[]string{"stateId", "certificationData", "referenceTime", "blockNumber", "leafIndex", "createdAt", "finalizedAt"},
		keysOf(decoded),
		"get_block_records record keys changed; update README.md to match")

	certData, ok := decoded["certificationData"].(map[string]any)
	require.True(t, ok)
	require.ElementsMatch(t,
		[]string{"version", "ownerPredicate", "sourceStateHash", "transactionHash", "expiresAt", "witness"},
		keysOf(certData),
		"certificationData keys changed; update README.md to match")

	require.EqualValues(t, 1755000000, decoded["referenceTime"])
	require.EqualValues(t, 1755003600, certData["expiresAt"])
	require.EqualValues(t, api.CertificationDataVersion, certData["version"])
	require.Equal(t, "1734435601000", decoded["finalizedAt"])

	// An absent deadline stays absent rather than becoming zero: the service
	// assigns its own, but that value is not part of the certified record.
	record.CertificationData.ExpiresAt = nil
	encoded, err = json.Marshal(modelToAPIAggregatorRecord(record, nil))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	certData, ok = decoded["certificationData"].(map[string]any)
	require.True(t, ok)
	require.Nil(t, certData["expiresAt"])
	require.Nil(t, decoded["finalizedAt"])
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
