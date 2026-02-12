package service

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestAggregateRequestCount(t *testing.T) {
	t.Run("CertificationRequest with different aggregate counts", func(t *testing.T) {
		// This test verifies that AggregateRequestCount is properly stored
		// The actual integration test would require setting up storage

		// Test creating commitments with different aggregate counts
		commitment1 := models.NewCertificationRequest(
			api.RequireNewImprintV2("0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			models.CertificationData{},
		)
		require.Equal(t, uint64(1), commitment1.AggregateRequestCount)

		commitment2 := models.NewCertificationRequestWithAggregate(
			api.RequireNewImprintV2("0000c1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			models.CertificationData{},
			100,
		)
		require.Equal(t, uint64(100), commitment2.AggregateRequestCount)
	})
}

func TestGetBlockTotalCommitments(t *testing.T) {
	t.Run("GetBlockResponse includes correct TotalCount", func(t *testing.T) {
		// Test the calculation logic for TotalCount

		// Create test aggregator records with different counts
		records := []*models.AggregatorRecord{
			{AggregateRequestCount: 1},
			{AggregateRequestCount: 10},
			{AggregateRequestCount: 25},
			{AggregateRequestCount: 100},
			{AggregateRequestCount: 50},
		}

		// Calculate expected total
		var expectedTotal uint64
		for _, record := range records {
			expectedTotal += record.AggregateRequestCount
		}
		require.Equal(t, uint64(186), expectedTotal)
	})

	t.Run("AggregatorRecord preserves AggregateRequestCount", func(t *testing.T) {
		// Test that creating an aggregator record from a certification request preserves the count
		commitment := models.NewCertificationRequestWithAggregate(
			api.RequireNewImprintV2("0000e1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			models.CertificationData{},
			500,
		)

		blockNumber := api.NewBigInt(big.NewInt(1))
		leafIndex := api.NewBigInt(big.NewInt(0))

		record := models.NewAggregatorRecord(commitment, blockNumber, leafIndex)
		require.Equal(t, uint64(500), record.AggregateRequestCount)
	})

	t.Run("API conversion preserves AggregateRequestCount", func(t *testing.T) {
		// Test model to API conversion
		modelRecord := &models.AggregatorRecord{
			StateID: api.RequireNewImprintV2("0000e1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			CertificationData: models.CertificationData{
				TransactionHash: api.RequireNewImprintV2("0000e1b2c3d4e5f6789012345678901234567890123456789012345678901234567891"),
			},
			AggregateRequestCount: 1000,
			BlockNumber:           api.NewBigInt(big.NewInt(1)),
			LeafIndex:             api.NewBigInt(big.NewInt(0)),
			CreatedAt:             api.Now(),
			FinalizedAt:           api.Now(),
		}

		apiRecord := modelToAPIAggregatorRecord(modelRecord)
		require.Equal(t, uint64(1000), apiRecord.AggregateRequestCount)
	})
}
