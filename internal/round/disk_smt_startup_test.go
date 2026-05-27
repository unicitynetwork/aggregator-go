package round

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	diskstore "github.com/unicitynetwork/aggregator-go/internal/smt/disk/store"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskSMTStartupFreshNoBlocks(t *testing.T) {
	ctx := context.Background()
	rm := newDiskStartupRoundManager(t, newTestDiskBackendForStartup(t), &diskStartupStorage{})

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Nil(t, block)
}

func TestDiskSMTStartupMatchingLatestBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf})
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block.Uint64())
}

func TestDiskSMTStartupRootMismatchFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf})
	wrongRoot := append([]byte(nil), root...)
	wrongRoot[0] ^= 0xff
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, wrongRoot)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "root mismatch")
}

func TestDiskSMTStartupEmptyDiskWithHistoryFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, diskStartupLeaf(1, 11).Value)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "disk SMT is empty")
}

func TestDiskSMTStartupBoundedReplay(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})

	leaf2 := diskStartupLeaf(2, 22)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2})
	require.NoError(t, err)
	root2 := result.CandidateRoot
	snapshot.Discard(ctx)

	stateID2 := api.StateID(leaf2.Key)
	storage := &diskStartupStorage{
		blocks:  newDiskStartupBlockStorage(diskStartupBlock(1, root1), diskStartupBlock(2, root2)),
		records: newDiskStartupBlockRecordsStorage(models.NewBlockRecords(api.NewBigIntFromUint64(2), []api.StateID{stateID2})),
		smt:     newDiskStartupSMTStorage(models.NewSmtNode(leaf2.Key, leaf2.Value)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.config.SMT.StartupReplayLimitBlocks = 1

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.Uint64())

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.BlockNumber.Uint64())
	require.Equal(t, root2, state.RootHash)
}

func TestDiskSMTStartupReplayLimitExceeded(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root1), diskStartupBlock(3, root1)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.config.SMT.StartupReplayLimitBlocks = 1

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "exceeding SMT_STARTUP_REPLAY_LIMIT_BLOCKS")
}

func TestDiskSMTStartupDiskAheadFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 2, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "ahead of latest finalized block")
}

func newTestDiskBackendForStartup(t *testing.T) *smtbackend.DiskBackend {
	t.Helper()
	store, err := diskstore.Open(t.TempDir(), diskstore.Options{})
	require.NoError(t, err)
	backend, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	return backend
}

func newDiskStartupRoundManager(t *testing.T, backend smtbackend.Backend, storage interfaces.Storage) *RoundManager {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	return &RoundManager{
		config: &config.Config{
			SMT: config.SMTConfig{
				StartupReplayLimitBlocks: 100,
			},
		},
		logger:       log,
		storage:      storage,
		smtBackend:   backend,
		stateTracker: state.NewSyncStateTracker(),
	}
}

func diskStartupLeaf(keyByte, valueByte byte) smtbackend.LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[len(key)-1] = keyByte
	value := make([]byte, 32)
	value[len(value)-1] = valueByte
	return smtbackend.LeafInput{Key: key, Value: value}
}

func commitDiskStartupLeaves(t *testing.T, ctx context.Context, backend smtbackend.Backend, blockNumber uint64, leaves []smtbackend.LeafInput) []byte {
	t.Helper()
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(leaves)))
	require.NoError(t, snapshot.Commit(ctx, smtbackend.CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(blockNumber),
		RootHash:    result.CandidateRoot,
	}))
	return result.CandidateRoot
}

func diskStartupBlock(number uint64, root []byte) *models.Block {
	block := models.NewBlock(
		api.NewBigIntFromUint64(number),
		"test-chain",
		0,
		"1",
		"test",
		api.HexBytes(append([]byte(nil), root...)),
		nil,
		nil,
	)
	block.Finalized = true
	return block
}

type diskStartupStorage struct {
	blocks  *diskStartupBlockStorage
	records *diskStartupBlockRecordsStorage
	smt     *diskStartupSMTStorage
}

func (s *diskStartupStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage { return nil }
func (s *diskStartupStorage) BlockStorage() interfaces.BlockStorage {
	if s.blocks == nil {
		return newDiskStartupBlockStorage()
	}
	return s.blocks
}
func (s *diskStartupStorage) SmtStorage() interfaces.SmtStorage {
	if s.smt == nil {
		return newDiskStartupSMTStorage()
	}
	return s.smt
}
func (s *diskStartupStorage) BlockRecordsStorage() interfaces.BlockRecordsStorage {
	if s.records == nil {
		return newDiskStartupBlockRecordsStorage()
	}
	return s.records
}
func (s *diskStartupStorage) LeadershipStorage() interfaces.LeadershipStorage { return nil }
func (s *diskStartupStorage) TrustBaseStorage() interfaces.TrustBaseStorage   { return nil }
func (s *diskStartupStorage) Initialize(context.Context) error                { return nil }
func (s *diskStartupStorage) Ping(context.Context) error                      { return nil }
func (s *diskStartupStorage) Close(context.Context) error                     { return nil }
func (s *diskStartupStorage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type diskStartupBlockStorage struct {
	blocks map[string]*models.Block
	latest *models.Block
}

func newDiskStartupBlockStorage(blocks ...*models.Block) *diskStartupBlockStorage {
	out := &diskStartupBlockStorage{blocks: make(map[string]*models.Block)}
	for _, block := range blocks {
		out.blocks[block.Index.String()] = block
		if out.latest == nil || block.Index.Cmp(out.latest.Index.Int) > 0 {
			out.latest = block
		}
	}
	return out
}

func (s *diskStartupBlockStorage) Store(context.Context, *models.Block) error { return nil }
func (s *diskStartupBlockStorage) GetByNumber(_ context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	return s.blocks[blockNumber.String()], nil
}
func (s *diskStartupBlockStorage) GetLatest(context.Context) (*models.Block, error) {
	return s.latest, nil
}
func (s *diskStartupBlockStorage) GetLatestNumber(context.Context) (*api.BigInt, error) {
	if s.latest == nil {
		return nil, nil
	}
	return s.latest.Index, nil
}
func (s *diskStartupBlockStorage) GetLatestByRootHash(context.Context, api.HexBytes) (*models.Block, error) {
	return nil, nil
}
func (s *diskStartupBlockStorage) Count(context.Context) (int64, error) {
	return int64(len(s.blocks)), nil
}
func (s *diskStartupBlockStorage) GetRange(context.Context, *api.BigInt, *api.BigInt) ([]*models.Block, error) {
	return nil, nil
}
func (s *diskStartupBlockStorage) SetFinalized(context.Context, *api.BigInt, bool) error {
	return nil
}
func (s *diskStartupBlockStorage) GetUnfinalized(context.Context) ([]*models.Block, error) {
	return nil, nil
}

type diskStartupBlockRecordsStorage struct {
	records map[string]*models.BlockRecords
}

func newDiskStartupBlockRecordsStorage(records ...*models.BlockRecords) *diskStartupBlockRecordsStorage {
	out := &diskStartupBlockRecordsStorage{records: make(map[string]*models.BlockRecords)}
	for _, record := range records {
		out.records[record.BlockNumber.String()] = record
	}
	return out
}

func (s *diskStartupBlockRecordsStorage) Store(context.Context, *models.BlockRecords) error {
	return nil
}
func (s *diskStartupBlockRecordsStorage) GetByBlockNumber(_ context.Context, blockNumber *api.BigInt) (*models.BlockRecords, error) {
	return s.records[blockNumber.String()], nil
}
func (s *diskStartupBlockRecordsStorage) Count(context.Context) (int64, error) {
	return int64(len(s.records)), nil
}
func (s *diskStartupBlockRecordsStorage) GetNextBlock(context.Context, *api.BigInt) (*models.BlockRecords, error) {
	return nil, nil
}
func (s *diskStartupBlockRecordsStorage) GetLatestBlockNumber(context.Context) (*api.BigInt, error) {
	return nil, nil
}

type diskStartupSMTStorage struct {
	nodes map[string]*models.SmtNode
}

func newDiskStartupSMTStorage(nodes ...*models.SmtNode) *diskStartupSMTStorage {
	out := &diskStartupSMTStorage{nodes: make(map[string]*models.SmtNode)}
	for _, node := range nodes {
		out.nodes[node.Key.String()] = node
	}
	return out
}

func (s *diskStartupSMTStorage) Store(context.Context, *models.SmtNode) error { return nil }
func (s *diskStartupSMTStorage) StoreBatch(context.Context, []*models.SmtNode) error {
	return nil
}
func (s *diskStartupSMTStorage) UpsertBatch(context.Context, []*models.SmtNode) error {
	return nil
}
func (s *diskStartupSMTStorage) GetByKey(_ context.Context, key api.HexBytes) (*models.SmtNode, error) {
	return s.nodes[key.String()], nil
}
func (s *diskStartupSMTStorage) GetByKeys(_ context.Context, keys []api.HexBytes) ([]*models.SmtNode, error) {
	nodes := make([]*models.SmtNode, 0, len(keys))
	for _, key := range keys {
		node := s.nodes[key.String()]
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}
func (s *diskStartupSMTStorage) Delete(context.Context, api.HexBytes) error        { return nil }
func (s *diskStartupSMTStorage) DeleteBatch(context.Context, []api.HexBytes) error { return nil }
func (s *diskStartupSMTStorage) Count(context.Context) (int64, error) {
	return int64(len(s.nodes)), nil
}
func (s *diskStartupSMTStorage) EstimatedCount(context.Context) (int64, error) {
	return int64(len(s.nodes)), nil
}
func (s *diskStartupSMTStorage) GetAll(context.Context) ([]*models.SmtNode, error) {
	return nil, errors.New("not implemented")
}
func (s *diskStartupSMTStorage) GetChunked(context.Context, int, int) ([]*models.SmtNode, error) {
	return nil, errors.New("not implemented")
}
func (s *diskStartupSMTStorage) GetExistingKeys(context.Context, []string) (map[string]bool, error) {
	return nil, errors.New("not implemented")
}

var (
	_ interfaces.Storage             = (*diskStartupStorage)(nil)
	_ interfaces.BlockStorage        = (*diskStartupBlockStorage)(nil)
	_ interfaces.BlockRecordsStorage = (*diskStartupBlockRecordsStorage)(nil)
	_ interfaces.SmtStorage          = (*diskStartupSMTStorage)(nil)
)
