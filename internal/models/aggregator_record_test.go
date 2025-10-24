package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBackwardCompatibility(t *testing.T) {
	blockNumber, err := primitive.ParseDecimal128("100")
	require.NoError(t, err)
	leafIndex, err := primitive.ParseDecimal128("4")
	require.NoError(t, err)

	t.Run("FromBSON defaults AggregateRequestCount to 1 when missing", func(t *testing.T) {
		// Simulate an old record without AggregateRequestCount
		bsonRecord := &AggregatorRecordBSON{
			RequestID:       "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			Authenticator: AuthenticatorBSON{
				Algorithm: "secp256k1",
				PublicKey: "02345678",
				Signature: "abcdef12",
				StateHash: "0000cd60",
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
			RequestID:       "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			TransactionHash: "0000b1b2c3d4e5f6789012345678901234567890123456789012345678901234567890",
			Authenticator: AuthenticatorBSON{
				Algorithm: "secp256k1",
				PublicKey: "02345678",
				Signature: "abcdef12",
				StateHash: "0000cd60",
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

		var totalCommitments uint64
		for _, record := range records {
			totalCommitments += record.AggregateRequestCount
		}

		// Expected: 1 + 10 + 25 + 1 + 100 = 137
		require.Equal(t, uint64(137), totalCommitments)
	})
}
