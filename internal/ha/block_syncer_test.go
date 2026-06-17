package ha

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type mockLeaderSelector struct {
	isLeader atomic.Bool
}

func (m *mockLeaderSelector) IsLeader(_ context.Context) (bool, error) {
	return m.isLeader.Load(), nil
}

func TestBlockSyncer(t *testing.T) {
	ctx := t.Context()
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

	cfg := &config.Config{
		Processing: config.ProcessingConfig{RoundDuration: 100 * time.Millisecond},
		HA:         config.HAConfig{Enabled: true},
		BFT:        config.BFTConfig{Enabled: false},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// initialize block syncer with isLeader=false
	mockLeader := &mockLeaderSelector{}
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	stateTracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testLogger, mockLeader, storage, smtbackend.NewMemoryBackend(smtInstance), 0, cfg.Processing.RoundDuration, stateTracker)

	// simulate leader creating a block
	rootHash := createBlock(t, storage, 1)

	// start the block syncer
	syncer.Start(ctx)
	defer syncer.Stop()

	// wait for block syncer to start
	time.Sleep(2 * cfg.Processing.RoundDuration)

	// SMT root hash should match persisted block root hash after block sync
	require.Equal(t, rootHash.String(), api.HexBytes(smtInstance.GetRootHashRaw()).String())
	require.Equal(t, big.NewInt(1), stateTracker.GetLastSyncedBlock())

	// verify the blocks are not synced if node is leader
	mockLeader.isLeader.Store(true)
	createBlock(t, storage, 2)
	time.Sleep(2 * cfg.Processing.RoundDuration)
	require.Equal(t, rootHash.String(), api.HexBytes(smtInstance.GetRootHashRaw()).String())
	require.Equal(t, big.NewInt(1), stateTracker.GetLastSyncedBlock())

}

func TestBlockSyncerDiskReplaysNonEmptyBlockFromCommittedState(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	root1 := fixture.addBlock(t, 1, 2)
	root2 := fixture.addBlock(t, 2, 1)

	backend := newFakeDiskBackend()
	commitFixtureBlockToBackend(t, ctx, backend, fixture.storage, 1)
	stateTracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, stateTracker)

	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	committed, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), committed.BlockNumber.Int64())
	require.Equal(t, root2.String(), api.HexBytes(committed.RootHash).String())
	require.NotEqual(t, root1.String(), root2.String())
	require.Equal(t, big.NewInt(2), stateTracker.GetLastSyncedBlock())
	require.Equal(t, int32(1), backend.published.Load())
}

func TestBlockSyncerDiskUsesCommittedStateCursorAndAdvancesEmptyBlock(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	root1 := fixture.addBlock(t, 1, 2)
	root2 := fixture.addBlock(t, 2, 0)
	require.Equal(t, root1.String(), root2.String())

	backend := newFakeDiskBackend()
	commitFixtureBlockToBackend(t, ctx, backend, fixture.storage, 1)
	stateTracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, stateTracker)

	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	committed, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), committed.BlockNumber.Int64())
	require.Equal(t, root2.String(), api.HexBytes(committed.RootHash).String())
	require.Equal(t, big.NewInt(2), stateTracker.GetLastSyncedBlock())
	require.Equal(t, int32(1), backend.published.Load())
}

func TestBlockSyncerDiskRootMismatchFailsBeforeCommit(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	root1 := fixture.addBlock(t, 1, 2)
	fixture.addBlock(t, 2, 1)
	fixture.storage.blocks.byNumber["2"].RootHash = api.HexBytes(bytesOf(32, 9))

	backend := newFakeDiskBackend()
	commitFixtureBlockToBackend(t, ctx, backend, fixture.storage, 1)
	stateTracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, stateTracker)

	err := syncer.SyncToLatestBlock(ctx)
	require.ErrorContains(t, err, "does not match")
	require.True(t, errors.Is(err, ErrDiskSMTDiverged))

	committed, stateErr := backend.CommittedState(ctx)
	require.NoError(t, stateErr)
	require.Equal(t, int64(1), committed.BlockNumber.Int64())
	require.Equal(t, root1.String(), api.HexBytes(committed.RootHash).String())
	require.Equal(t, big.NewInt(0), stateTracker.GetLastSyncedBlock())
	require.Equal(t, int32(0), backend.published.Load())
}

func TestBlockSyncerDiskSyncToBlockStopsAtFixedTarget(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	root1 := fixture.addBlock(t, 1, 1)
	root2 := fixture.addBlock(t, 2, 1)
	root3 := fixture.addBlock(t, 3, 1)

	backend := newFakeDiskBackend()
	commitFixtureBlockToBackend(t, ctx, backend, fixture.storage, 1)
	stateTracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, stateTracker)

	target, err := fixture.storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	require.NoError(t, syncer.syncToBlock(ctx, target))

	committed, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), committed.BlockNumber.Int64())
	require.Equal(t, root2.String(), api.HexBytes(committed.RootHash).String())
	require.NotEqual(t, root1.String(), root2.String())
	require.NotEqual(t, root3.String(), api.HexBytes(committed.RootHash).String())
	require.Equal(t, big.NewInt(2), stateTracker.GetLastSyncedBlock())
	require.Equal(t, int32(1), backend.published.Load())
}

func TestBlockSyncerSyncGateHonorsContextCancellation(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	fixture.addBlock(t, 1, 1)

	backend := newFakeDiskBackend()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, state.NewSyncStateTracker())

	release, err := syncer.acquireSync(ctx)
	require.NoError(t, err)
	defer release()

	syncCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := syncer.SyncToLatestFinalizedBlock(syncCtx)
		errCh <- err
	}()

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "sync did not return after context cancellation")
	}
}

func createBlock(t *testing.T, storage *mongodb.Storage, blockNum int64) api.HexBytes {
	ctx := t.Context()
	blockNumber := api.NewBigInt(big.NewInt(blockNum))
	testCommitments := []*models.CertificationRequest{
		testutil.CreateTestCertificationRequest(t, "request_1"),
		testutil.CreateTestCertificationRequest(t, "request_2"),
		testutil.CreateTestCertificationRequest(t, "request_3"),
	}

	// persist aggregator records
	leaves := make([]*smt.Leaf, len(testCommitments))
	records := make([]*models.AggregatorRecord, len(testCommitments))
	proposalID := fmt.Sprintf("proposal-%s", blockNumber.String())
	for i, c := range testCommitments {
		path, err := c.StateID.GetPath()
		require.NoError(t, err)

		val, err := c.LeafValue()
		require.NoError(t, err)

		leaves[i] = &smt.Leaf{Path: path, Value: val}
		records[i] = models.NewAggregatorRecord(c, blockNumber, api.NewBigInt(big.NewInt(int64(i))))
		records[i].ProposalID = proposalID

		err = storage.AggregatorRecordStorage().Store(ctx, records[i])
		require.NoError(t, err)
	}

	// persist smt nodes
	smtNodes := make([]*models.SmtNode, len(leaves))
	for i, leaf := range leaves {
		keyBytes, err := api.PathToFixedBytes(leaf.Path, api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		key := api.NewHexBytes(keyBytes)
		value := api.NewHexBytes(leaf.Value)
		smtNodes[i] = models.NewSmtNode(key, value)
	}
	err := storage.SmtStorage().StoreBatch(ctx, smtNodes)
	require.NoError(t, err)

	// compute rootHash
	tmpSMT := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	require.NoError(t, tmpSMT.AddLeaves(leaves))
	rootHash := api.HexBytes(tmpSMT.GetRootHashRaw())

	// persist block
	block := models.NewBlock(blockNumber, "unicity", 0, "1.0", "mainnet", rootHash, nil, nil)
	block.Finalized = true // Mark as finalized so GetLatestNumber finds it
	block.ProposalID = proposalID
	err = storage.BlockStorage().Store(ctx, block)
	require.NoError(t, err)

	return rootHash
}

func testBlockSyncerLogger(t *testing.T) *logger.Logger {
	t.Helper()
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	return log
}

type fakeDiskBackend struct {
	*smtbackend.MemoryBackend
	published atomic.Int32
}

func newFakeDiskBackend() *fakeDiskBackend {
	return &fakeDiskBackend{
		MemoryBackend: smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))),
	}
}

func (b *fakeDiskBackend) IsDiskBackedSMT() bool {
	return true
}

func (b *fakeDiskBackend) RefreshPublishedProofView(context.Context, []byte) error {
	b.published.Add(1)
	return nil
}

type blockSyncerFixture struct {
	storage *blockSyncerTestStorage
	tree    *smt.SparseMerkleTree
}

func newBlockSyncerFixture(t *testing.T) *blockSyncerFixture {
	t.Helper()
	return &blockSyncerFixture{
		storage: newBlockSyncerTestStorage(),
		tree:    smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits),
	}
}

func (f *blockSyncerFixture) addBlock(t *testing.T, blockNum int64, commitmentCount int) api.HexBytes {
	t.Helper()
	ctx := t.Context()
	blockNumber := api.NewBigInt(big.NewInt(blockNum))
	leaves := make([]*smt.Leaf, 0, commitmentCount)
	nodes := make([]*models.SmtNode, 0, commitmentCount)
	records := make([]*models.AggregatorRecord, 0, commitmentCount)
	proposalID := fmt.Sprintf("proposal-%d", blockNum)

	for i := 0; i < commitmentCount; i++ {
		c := testutil.CreateTestCertificationRequest(t, fmt.Sprintf("block_%d_request_%d", blockNum, i))
		path, err := c.StateID.GetPath()
		require.NoError(t, err)
		value, err := c.LeafValue()
		require.NoError(t, err)
		key, err := c.StateID.GetTreeKey()
		require.NoError(t, err)

		leaves = append(leaves, &smt.Leaf{Path: path, Value: value})
		nodes = append(nodes, models.NewSmtNode(api.HexBytes(key), api.HexBytes(value)))
		record := models.NewAggregatorRecord(c, blockNumber, api.NewBigInt(big.NewInt(int64(i))))
		record.ProposalID = proposalID
		records = append(records, record)
	}

	if len(leaves) > 0 {
		require.NoError(t, f.tree.AddLeaves(leaves))
	}
	rootHash := api.HexBytes(f.tree.GetRootHashRaw())

	block := models.NewBlock(blockNumber, "unicity", 0, "1.0", "test", rootHash, nil, nil)
	block.Finalized = true
	block.ProposalID = proposalID
	require.NoError(t, f.storage.BlockStorage().Store(ctx, block))
	require.NoError(t, f.storage.AggregatorRecordStorage().StoreBatch(ctx, records))
	require.NoError(t, f.storage.SmtStorage().StoreBatch(ctx, nodes))
	return rootHash
}

func commitFixtureBlockToBackend(t *testing.T, ctx context.Context, backend smtbackend.Backend, storage *blockSyncerTestStorage, blockNum int64) {
	t.Helper()
	blockNumber := api.NewBigInt(big.NewInt(blockNum))
	block, err := storage.BlockStorage().GetByNumber(ctx, blockNumber)
	require.NoError(t, err)
	require.NotNil(t, block)
	records, err := storage.AggregatorRecordStorage().GetByBlockNumber(ctx, blockNumber)
	require.NoError(t, err)

	leaves, err := replayLeavesForAggregatorRecords(records)
	require.NoError(t, err)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(leaves)))
	require.Equal(t, block.RootHash.String(), api.HexBytes(result.CandidateRoot).String())
	require.NoError(t, snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockNumber, RootHash: block.RootHash}))
}

func bytesOf(n int, value byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = value
	}
	return out
}

type blockSyncerTestStorage struct {
	blocks   *blockSyncerTestBlockStorage
	records  *blockSyncerTestAggregatorRecordStorage
	smtNodes *blockSyncerTestSMTStorage
}

func newBlockSyncerTestStorage() *blockSyncerTestStorage {
	return &blockSyncerTestStorage{
		blocks:   &blockSyncerTestBlockStorage{byNumber: make(map[string]*models.Block)},
		records:  &blockSyncerTestAggregatorRecordStorage{byBlock: make(map[string][]*models.AggregatorRecord)},
		smtNodes: &blockSyncerTestSMTStorage{byKey: make(map[string]*models.SmtNode)},
	}
}

func (s *blockSyncerTestStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage {
	return s.records
}

func (s *blockSyncerTestStorage) BlockStorage() interfaces.BlockStorage {
	return s.blocks
}

func (s *blockSyncerTestStorage) SmtStorage() interfaces.SmtStorage {
	return s.smtNodes
}

func (s *blockSyncerTestStorage) LeadershipStorage() interfaces.LeadershipStorage {
	return nil
}

func (s *blockSyncerTestStorage) TrustBaseStorage() interfaces.TrustBaseStorage {
	return nil
}

func (s *blockSyncerTestStorage) Initialize(context.Context) error { return nil }
func (s *blockSyncerTestStorage) Ping(context.Context) error       { return nil }
func (s *blockSyncerTestStorage) Close(context.Context) error      { return nil }
func (s *blockSyncerTestStorage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type blockSyncerTestBlockStorage struct {
	byNumber map[string]*models.Block
}

func (s *blockSyncerTestBlockStorage) Store(_ context.Context, block *models.Block) error {
	s.byNumber[block.Index.String()] = block
	return nil
}

func (s *blockSyncerTestBlockStorage) GetByNumber(_ context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	block := s.byNumber[blockNumber.String()]
	if block == nil || !block.Finalized {
		return nil, nil
	}
	return block, nil
}

func (s *blockSyncerTestBlockStorage) GetLatest(context.Context) (*models.Block, error) {
	var latest *models.Block
	for _, block := range s.byNumber {
		if !block.Finalized {
			continue
		}
		if latest == nil || block.Index.Cmp(latest.Index.Int) > 0 {
			latest = block
		}
	}
	return latest, nil
}

func (s *blockSyncerTestBlockStorage) GetLatestNumber(ctx context.Context) (*api.BigInt, error) {
	latest, err := s.GetLatest(ctx)
	if err != nil || latest == nil {
		return nil, err
	}
	return latest.Index, nil
}

func (s *blockSyncerTestBlockStorage) GetLatestByRootHash(context.Context, api.HexBytes) (*models.Block, error) {
	return nil, nil
}

func (s *blockSyncerTestBlockStorage) Count(context.Context) (int64, error) {
	return int64(len(s.byNumber)), nil
}

func (s *blockSyncerTestBlockStorage) GetRange(_ context.Context, fromBlock, toBlock *api.BigInt) ([]*models.Block, error) {
	var out []*models.Block
	for _, block := range s.byNumber {
		if !block.Finalized {
			continue
		}
		if block.Index.Cmp(fromBlock.Int) < 0 || block.Index.Cmp(toBlock.Int) > 0 {
			continue
		}
		out = append(out, block)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Index.Cmp(out[j].Index.Int) < 0
	})
	return out, nil
}

func (s *blockSyncerTestBlockStorage) GetNextFinalizedAfter(_ context.Context, afterBlock, toBlock *api.BigInt) (*models.Block, error) {
	var next *models.Block
	for _, block := range s.byNumber {
		if !block.Finalized {
			continue
		}
		if block.Index.Cmp(afterBlock.Int) <= 0 || block.Index.Cmp(toBlock.Int) > 0 {
			continue
		}
		if next == nil || block.Index.Cmp(next.Index.Int) < 0 {
			next = block
		}
	}
	return next, nil
}

func (s *blockSyncerTestBlockStorage) SetFinalized(_ context.Context, blockNumber *api.BigInt, finalized bool) error {
	if block := s.byNumber[blockNumber.String()]; block != nil {
		block.Finalized = finalized
	}
	return nil
}

func (s *blockSyncerTestBlockStorage) GetUnfinalized(context.Context) ([]*models.Block, error) {
	var out []*models.Block
	for _, block := range s.byNumber {
		if !block.Finalized {
			out = append(out, block)
		}
	}
	return out, nil
}

type blockSyncerTestAggregatorRecordStorage struct {
	byBlock map[string][]*models.AggregatorRecord
}

func (s *blockSyncerTestAggregatorRecordStorage) Store(_ context.Context, record *models.AggregatorRecord) error {
	s.byBlock[record.BlockNumber.String()] = append(s.byBlock[record.BlockNumber.String()], record)
	return nil
}

func (s *blockSyncerTestAggregatorRecordStorage) StoreBatch(ctx context.Context, records []*models.AggregatorRecord) error {
	for _, record := range records {
		if err := s.Store(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (s *blockSyncerTestAggregatorRecordStorage) GetByStateID(_ context.Context, stateID api.StateID) (*models.AggregatorRecord, error) {
	for _, records := range s.byBlock {
		for _, record := range records {
			if record != nil && string(record.StateID) == string(stateID) {
				return record, nil
			}
		}
	}
	return nil, nil
}

func (s *blockSyncerTestAggregatorRecordStorage) GetByBlockNumber(_ context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	records := append([]*models.AggregatorRecord(nil), s.byBlock[blockNumber.String()]...)
	sort.SliceStable(records, func(i, j int) bool {
		left := records[i].LeafIndex
		right := records[j].LeafIndex
		if left == nil || left.Int == nil {
			return right != nil && right.Int != nil
		}
		if right == nil || right.Int == nil {
			return false
		}
		return left.Int.Cmp(right.Int) < 0
	})
	return records, nil
}

func (s *blockSyncerTestAggregatorRecordStorage) Count(context.Context) (int64, error) {
	var count int64
	for _, records := range s.byBlock {
		count += int64(len(records))
	}
	return count, nil
}

func (s *blockSyncerTestAggregatorRecordStorage) GetExistingStateIDs(_ context.Context, stateIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(stateIDs))
	wanted := make(map[string]struct{}, len(stateIDs))
	for _, stateID := range stateIDs {
		wanted[stateID] = struct{}{}
	}
	for _, records := range s.byBlock {
		for _, record := range records {
			if record == nil {
				continue
			}
			key := record.StateID.String()
			if _, ok := wanted[key]; ok {
				out[key] = true
			}
		}
	}
	return out, nil
}

type blockSyncerTestSMTStorage struct {
	byKey map[string]*models.SmtNode
}

func (s *blockSyncerTestSMTStorage) Store(_ context.Context, node *models.SmtNode) error {
	s.byKey[node.Key.String()] = node
	return nil
}

func (s *blockSyncerTestSMTStorage) StoreBatch(_ context.Context, nodes []*models.SmtNode) error {
	for _, node := range nodes {
		s.byKey[node.Key.String()] = node
	}
	return nil
}

func (s *blockSyncerTestSMTStorage) UpsertBatch(ctx context.Context, nodes []*models.SmtNode) error {
	return s.StoreBatch(ctx, nodes)
}

func (s *blockSyncerTestSMTStorage) GetByKey(_ context.Context, key api.HexBytes) (*models.SmtNode, error) {
	return s.byKey[key.String()], nil
}

func (s *blockSyncerTestSMTStorage) GetByKeys(_ context.Context, keys []api.HexBytes) ([]*models.SmtNode, error) {
	out := make([]*models.SmtNode, 0, len(keys))
	for _, key := range keys {
		if node := s.byKey[key.String()]; node != nil {
			out = append(out, node)
		}
	}
	return out, nil
}

func (s *blockSyncerTestSMTStorage) Delete(_ context.Context, key api.HexBytes) error {
	delete(s.byKey, key.String())
	return nil
}

func (s *blockSyncerTestSMTStorage) DeleteBatch(_ context.Context, keys []api.HexBytes) error {
	for _, key := range keys {
		delete(s.byKey, key.String())
	}
	return nil
}

func (s *blockSyncerTestSMTStorage) Count(context.Context) (int64, error) {
	return int64(len(s.byKey)), nil
}

func (s *blockSyncerTestSMTStorage) EstimatedCount(ctx context.Context) (int64, error) {
	return s.Count(ctx)
}

func (s *blockSyncerTestSMTStorage) GetAll(context.Context) ([]*models.SmtNode, error) {
	out := make([]*models.SmtNode, 0, len(s.byKey))
	for _, node := range s.byKey {
		out = append(out, node)
	}
	return out, nil
}

func (s *blockSyncerTestSMTStorage) GetChunked(context.Context, int, int) ([]*models.SmtNode, error) {
	return nil, nil
}

func (s *blockSyncerTestSMTStorage) GetExistingKeys(_ context.Context, keys []string) (map[string]bool, error) {
	out := make(map[string]bool, len(keys))
	for _, key := range keys {
		_, out[key] = s.byKey[key]
	}
	return out, nil
}

var (
	_ interfaces.Storage            = (*blockSyncerTestStorage)(nil)
	_ interfaces.BlockStorage       = (*blockSyncerTestBlockStorage)(nil)
	_ interfaces.SmtStorage         = (*blockSyncerTestSMTStorage)(nil)
	_ smtbackend.DiskBacked         = (*fakeDiskBackend)(nil)
	_ smtbackend.ProofViewPublisher = (*fakeDiskBackend)(nil)
)
