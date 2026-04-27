package models

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestAggregatorRecordSerialization(t *testing.T) {
	// Create AggregatorRecord
	originalStateID := api.RequireNewImprintV2("a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890")
	originalTransactionHash := api.RequireNewImprintV2("b1b2c3d4e5f67890b1b2c3d4e5f67890b1b2c3d4e5f67890b1b2c3d4e5f67890")
	originalBlockNumber, err := api.NewBigIntFromString("123")
	require.NoError(t, err)
	originalLeafIndex, err := api.NewBigIntFromString("456")
	require.NoError(t, err)
	record := &AggregatorRecord{
		StateID: originalStateID,
		CertificationData: CertificationData{
			OwnerPredicate:  api.Predicate{},
			SourceStateHash: api.RequireNewImprintV2("cd60000000000000000000000000000000000000000000000000000000000000"),
			TransactionHash: originalTransactionHash,
			Witness:         api.HexBytes("abcdef12"),
		},
		AggregateRequestCount: 1,
		BlockNumber:           originalBlockNumber,
		LeafIndex:             originalLeafIndex,
		CreatedAt:             api.Now(),
		FinalizedAt:           api.Now(),
	}

	// Convert to BSON
	bsonRecord, err := record.ToBSON()
	require.NoError(t, err)
	require.NotNil(t, bsonRecord)

	// Verify StateID in BSON format
	require.Equal(t, originalStateID.String(), bsonRecord.StateID)

	// Convert back from BSON
	unmarshaledRecord, err := bsonRecord.FromBSON()
	require.NoError(t, err)
	require.NotNil(t, unmarshaledRecord)

	// Verify StateID and transaction hash are preserved
	require.Equal(t, originalStateID, unmarshaledRecord.StateID)
	require.Equal(t, originalBlockNumber.String(), unmarshaledRecord.BlockNumber.String())
	require.Equal(t, originalLeafIndex.String(), unmarshaledRecord.LeafIndex.String())
}
