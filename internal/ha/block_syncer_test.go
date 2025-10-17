package ha

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBlockSync(t *testing.T) {
	storage := testutil.SetupTestStorage(t, config.Config{
		Database: config.DatabaseConfig{
			Database:               "test_block_sync",
			ConnectTimeout:         30 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            100,
			MinPoolSize:            5,
			MaxConnIdleTime:        5 * time.Minute,
		},
	})

	ctx := context.Background()
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// create block syncer
	smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	threadSafeSMT := smt.NewThreadSafeSMT(smtInstance)
	stateTracker := state.NewSyncStateTracker()
	syncer := newBlockSyncer(testLogger, storage, threadSafeSMT, 0, stateTracker)

	// simulate leader creating a block
	rootHash := createBlock(ctx, t, storage)

	// trigger the sync
	err = syncer.syncToLatestBlock(ctx)
	require.NoError(t, err)

	// SMT root hash should match persisted block root hash after block sync
	require.Equal(t, rootHash.String(), threadSafeSMT.GetRootHash())
	require.Equal(t, big.NewInt(1), stateTracker.GetLastSyncedBlock())
}

func createBlock(ctx context.Context, t *testing.T, storage *mongodb.Storage) api.HexBytes {
	blockNumber := api.NewBigInt(big.NewInt(1))
	testCommitments := []*models.Commitment{
		testutil.CreateTestCommitment(t, "request_1"),
		testutil.CreateTestCommitment(t, "request_2"),
		testutil.CreateTestCommitment(t, "request_3"),
	}

	// persist aggregator records
	leaves := make([]*smt.Leaf, len(testCommitments))
	records := make([]*models.AggregatorRecord, len(testCommitments))
	for i, c := range testCommitments {
		path, err := c.RequestID.GetPath(0)
		require.NoError(t, err)

		val, err := c.CreateLeafValue()
		require.NoError(t, err)

		leaves[i] = &smt.Leaf{Path: path, Value: val}
		records[i] = models.NewAggregatorRecord(c, blockNumber, api.NewBigInt(big.NewInt(int64(i))))

		err = storage.AggregatorRecordStorage().Store(ctx, records[i])
		require.NoError(t, err)
	}

	// persist smt nodes
	smtNodes := make([]*models.SmtNode, len(leaves))
	for i, leaf := range leaves {
		key := api.NewHexBytes(leaf.Path.Bytes())
		value := api.NewHexBytes(leaf.Value)
		smtNodes[i] = models.NewSmtNode(key, value)
	}
	err := storage.SmtStorage().StoreBatch(ctx, smtNodes)
	require.NoError(t, err)

	// compute rootHash
	tmpSMT := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	require.NoError(t, tmpSMT.AddLeaves(leaves))
	rootHash := api.NewHexBytes(tmpSMT.GetRootHash())

	// persist block
	block := models.NewBlock(blockNumber, "unicity", 0, "1.0", "mainnet", rootHash, nil, nil, nil)
	err = storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err)

	// persist block records (mapping request IDs)
	reqIDs := make([]api.RequestID, len(testCommitments))
	for i, c := range testCommitments {
		reqIDs[i] = c.RequestID
	}
	blockRecords := models.NewBlockRecords(blockNumber, reqIDs)
	err = storage.BlockRecordsStorage().Store(ctx, blockRecords)
	require.NoError(t, err)

	return rootHash
}
