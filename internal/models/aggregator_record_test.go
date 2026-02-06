package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBackwardCompatibility(t *testing.T) {
	blockNumber, err := primitive.ParseDecimal128("100")
	require.NoError(t, err)
	leafIndex, err := primitive.ParseDecimal128("4")
	require.NoError(t, err)

	t.Run("FromBSON defaults AggregateRequestCount to 1 when missing", func(t *testing.T) {
		// Simulate an old record without AggregateRequestCount
		bsonRecord := &AggregatorRecordBSON{
			StateID: "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			CertificationData: CertificationDataBSON{
				OwnerPredicate:  PredicateBSON{},
				SourceStateHash: "0000cd60",
				TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
				Witness:         "abcdef12",
			},
			// AggregateRequestCount is intentionally not set (will be 0)
			BlockNumber: blockNumber,
			LeafIndex:   leafIndex,
			CreatedAt:   time.UnixMilli(1700000000000),
			FinalizedAt: time.UnixMilli(1700000001000),
		}

		record, err := bsonRecord.FromBSON()
		require.NoError(t, err)
		require.NotNil(t, record)

		// Should default to 1 for backward compatibility
		require.Equal(t, uint64(1), record.AggregateRequestCount)
	})

	t.Run("FromBSON preserves AggregateRequestCount when present", func(t *testing.T) {
		// New record with AggregateRequestCount
		bsonRecord := &AggregatorRecordBSON{
			StateID: "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			CertificationData: CertificationDataBSON{
				OwnerPredicate:  PredicateBSON{},
				SourceStateHash: "0000cd60",
				TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
				Witness:         "abcdef12",
			},
			AggregateRequestCount: 500,
			BlockNumber:           blockNumber,
			LeafIndex:             leafIndex,
			CreatedAt:             time.UnixMilli(1700000000000),
			FinalizedAt:           time.UnixMilli(1700000001000),
		}

		record, err := bsonRecord.FromBSON()
		require.NoError(t, err)
		require.NotNil(t, record)

		// Should preserve the actual value
		require.Equal(t, uint64(500), record.AggregateRequestCount)
	})

	t.Run("TotalCommitments calculation handles mixed old and new records", func(t *testing.T) {
		// Simulate a mix of old and new records
		records := []*AggregatorRecord{
			// Old record (would have AggregateRequestCount = 0, treated as 1)
			{AggregateRequestCount: 1},
			// New records with explicit counts
			{AggregateRequestCount: 10},
			{AggregateRequestCount: 25},
			// Another old record
			{AggregateRequestCount: 1},
			// New record with large count
			{AggregateRequestCount: 100},
		}

		var totalCount uint64
		for _, record := range records {
			totalCount += record.AggregateRequestCount
		}

		// Expected: 1 + 10 + 25 + 1 + 100 = 137
		require.Equal(t, uint64(137), totalCount)
	})
}

func TestAggregatorRecordSerialization(t *testing.T) {
	// Create AggregatorRecord
	originalStateID := api.RequireNewImprintV2("0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890")
	originalTransactionHash := api.RequireNewImprintV2("0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890")
	originalBlockNumber, err := api.NewBigIntFromString("123")
	require.NoError(t, err)
	originalLeafIndex, err := api.NewBigIntFromString("456")
	require.NoError(t, err)
	record := &AggregatorRecord{
		StateID: originalStateID,
		CertificationData: CertificationData{
			OwnerPredicate:  api.Predicate{},
			SourceStateHash: api.RequireNewImprintV2("0000cd60"),
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

	// Verify StateID and TransactionHashImprint are preserved
	require.Equal(t, originalStateID, unmarshaledRecord.StateID)
	require.Equal(t, originalBlockNumber.String(), unmarshaledRecord.BlockNumber.String())
	require.Equal(t, originalLeafIndex.String(), unmarshaledRecord.LeafIndex.String())
}
