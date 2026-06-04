package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestProofMetadataCacheUpdateRefreshesEvictionOrder(t *testing.T) {
	cache := newProofMetadataCache(2)

	block1 := testCacheBlock(1, 0x01)
	block2 := testCacheBlock(2, 0x02)
	block3 := testCacheBlock(3, 0x03)
	block4 := testCacheBlock(4, 0x04)
	stateA := testCacheStateID(0x0a)
	stateB := testCacheStateID(0x0b)
	stateC := testCacheStateID(0x0c)

	recordA1 := testCacheRecord(stateA, 1)
	recordB := testCacheRecord(stateB, 2)
	recordA2 := testCacheRecord(stateA, 3)
	recordC := testCacheRecord(stateC, 4)

	cache.add(block1, []*models.AggregatorRecord{recordA1})
	cache.add(block2, []*models.AggregatorRecord{recordB})
	cache.add(block3, []*models.AggregatorRecord{recordA2})
	cache.add(block4, []*models.AggregatorRecord{recordC})

	block, record, ok := cache.get(stateA, block3.RootHash)
	require.True(t, ok)
	require.Same(t, block3, block)
	require.Same(t, recordA2, record)

	_, _, ok = cache.get(stateB, block2.RootHash)
	require.False(t, ok)

	block, record, ok = cache.get(stateC, block4.RootHash)
	require.True(t, ok)
	require.Same(t, block4, block)
	require.Same(t, recordC, record)
}

func testCacheBlock(index uint64, rootByte byte) *models.Block {
	rootHash := make(api.HexBytes, api.StateTreeKeyLengthBytes)
	rootHash[0] = rootByte
	return &models.Block{
		Index:    api.NewBigIntFromUint64(index),
		RootHash: rootHash,
	}
}

func testCacheRecord(stateID api.StateID, blockNumber uint64) *models.AggregatorRecord {
	return &models.AggregatorRecord{
		StateID:     stateID,
		BlockNumber: api.NewBigIntFromUint64(blockNumber),
		LeafIndex:   api.NewBigIntFromUint64(0),
	}
}

func testCacheStateID(seed byte) api.StateID {
	stateID := make(api.StateID, api.StateTreeKeyLengthBytes)
	stateID[len(stateID)-1] = seed
	return stateID
}
