package round

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type MockLeaderSelector struct {
	isLeader bool
}

func (m MockLeaderSelector) IsLeader(ctx context.Context) (bool, error) {
	return m.isLeader, nil
}

func TestBlockSync(t *testing.T) {
	storage, cleanup := testutil.SetupTestStorage(t, config.Config{
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
	defer cleanup()

	ctx := context.Background()
	cfg := &config.Config{
		Processing: config.ProcessingConfig{RoundDuration: 100 * time.Millisecond},
		HA:         config.HAConfig{Enabled: true},
		BFT:        config.BFTConfig{Enabled: false},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	mockLeader := &MockLeaderSelector{isLeader: false}
	rm, err := NewRoundManager(ctx, cfg, testLogger, storage.CommitmentQueue(), storage, mockLeader)
	require.NoError(t, err)

	require.NoError(t, rm.Start(ctx))
	defer func() {
		if err := rm.Stop(ctx); err != nil {
			t.Logf("Failed to stop RoundManager: %v", err)
		}
	}()

	// simulate leader creating a block
	rootHash := createBlock(ctx, t, storage, rm)

	// wait for follower's blockSync to replay storage into smt
	require.Eventually(t, func() bool {
		return rm.GetSMT().GetRootHash() == rootHash.String()
	}, 5*time.Second, 100*time.Millisecond,
		"SMT root hash should eventually match persisted block root hash after block sync")

	require.Equal(t, big.NewInt(1), rm.getLastSyncedRoundNumber())
}

func createBlock(ctx context.Context, t *testing.T, storage *mongodb.Storage, rm *RoundManager) api.HexBytes {
	blockNumber := api.NewBigInt(big.NewInt(1))
	testCommitments := []*models.Commitment{
		createTestCommitment(t, "request_1"),
		createTestCommitment(t, "request_2"),
		createTestCommitment(t, "request_3"),
	}

	// persist aggregator records
	leaves := make([]*smt.Leaf, len(testCommitments))
	records := make([]*models.AggregatorRecord, len(testCommitments))
	for i, c := range testCommitments {
		path, err := c.RequestID.GetPath()
		require.NoError(t, err)

		val, err := rm.createLeafValue(c)
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
	tmpSMT := smt.NewSparseMerkleTree(api.SHA256)
	require.NoError(t, tmpSMT.AddLeaves(leaves))
	rootHash := api.NewHexBytes(tmpSMT.GetRootHash())

	// persist block
	block := models.NewBlock(blockNumber, "unicity", "1.0", "mainnet", rootHash, nil)
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
