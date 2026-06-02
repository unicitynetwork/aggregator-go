package round

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	testsharding "github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type signalingRootAggregatorClient struct {
	inner              *testsharding.RootAggregatorClientStub
	firstProofReturned chan struct{}
	firstProofOnce     sync.Once
}

type blockingProofRootAggregatorClient struct {
	inner            *testsharding.RootAggregatorClientStub
	proofPollStarted chan struct{}
	releaseProof     chan struct{}
	startOnce        sync.Once
}

type countingProofRootAggregatorClient struct {
	inner         *testsharding.RootAggregatorClientStub
	proofReturned chan int
	mu            sync.Mutex
	proofCount    int
}

type staleThenFreshRootAggregatorClient struct {
	mu               sync.Mutex
	submittedRoot    api.HexBytes
	staleParentRound uint64
	staleRootRound   uint64
	freshParentRound uint64
	freshRootRound   uint64
	proofPollStarted chan struct{}
	releaseFresh     chan struct{}
	startOnce        sync.Once
	proofPolls       int
}

func newSignalingRootAggregatorClient() *signalingRootAggregatorClient {
	return &signalingRootAggregatorClient{
		inner:              testsharding.NewRootAggregatorClientStub(),
		firstProofReturned: make(chan struct{}),
	}
}

func newBlockingProofRootAggregatorClient() *blockingProofRootAggregatorClient {
	return &blockingProofRootAggregatorClient{
		inner:            testsharding.NewRootAggregatorClientStub(),
		proofPollStarted: make(chan struct{}),
		releaseProof:     make(chan struct{}),
	}
}

func newCountingProofRootAggregatorClient() *countingProofRootAggregatorClient {
	return &countingProofRootAggregatorClient{
		inner:         testsharding.NewRootAggregatorClientStub(),
		proofReturned: make(chan int, 16),
	}
}

func newStaleThenFreshRootAggregatorClient(staleParentRound, staleRootRound, freshParentRound, freshRootRound uint64) *staleThenFreshRootAggregatorClient {
	return &staleThenFreshRootAggregatorClient{
		staleParentRound: staleParentRound,
		staleRootRound:   staleRootRound,
		freshParentRound: freshParentRound,
		freshRootRound:   freshRootRound,
		proofPollStarted: make(chan struct{}),
		releaseFresh:     make(chan struct{}),
	}
}

func (c *signalingRootAggregatorClient) SubmitShardRoot(ctx context.Context, request *api.SubmitShardRootRequest) error {
	return c.inner.SubmitShardRoot(ctx, request)
}

func (c *signalingRootAggregatorClient) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	proof, err := c.inner.GetShardProof(ctx, request)
	if err == nil && proof != nil {
		c.firstProofOnce.Do(func() {
			close(c.firstProofReturned)
		})
	}
	return proof, err
}

func (c *signalingRootAggregatorClient) CheckHealth(ctx context.Context) error {
	return c.inner.CheckHealth(ctx)
}

func (c *blockingProofRootAggregatorClient) SubmitShardRoot(ctx context.Context, request *api.SubmitShardRootRequest) error {
	return c.inner.SubmitShardRoot(ctx, request)
}

func (c *blockingProofRootAggregatorClient) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	c.startOnce.Do(func() {
		close(c.proofPollStarted)
	})

	select {
	case <-c.releaseProof:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.inner.GetShardProof(ctx, request)
}

func (c *blockingProofRootAggregatorClient) CheckHealth(ctx context.Context) error {
	return c.inner.CheckHealth(ctx)
}

func (c *countingProofRootAggregatorClient) SubmitShardRoot(ctx context.Context, request *api.SubmitShardRootRequest) error {
	return c.inner.SubmitShardRoot(ctx, request)
}

func (c *countingProofRootAggregatorClient) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	proof, err := c.inner.GetShardProof(ctx, request)
	if err == nil && proof != nil {
		c.mu.Lock()
		c.proofCount++
		count := c.proofCount
		c.mu.Unlock()

		select {
		case c.proofReturned <- count:
		default:
		}
	}
	return proof, err
}

func (c *countingProofRootAggregatorClient) CheckHealth(ctx context.Context) error {
	return c.inner.CheckHealth(ctx)
}

func (c *staleThenFreshRootAggregatorClient) SubmitShardRoot(ctx context.Context, request *api.SubmitShardRootRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.submittedRoot = request.RootHash
	return nil
}

func (c *staleThenFreshRootAggregatorClient) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	c.startOnce.Do(func() {
		close(c.proofPollStarted)
	})

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.submittedRoot) == 0 {
		return nil, nil
	}

	c.proofPolls++
	parentRound := c.staleParentRound
	rootRound := c.staleRootRound

	select {
	case <-c.releaseFresh:
		parentRound = c.freshParentRound
		rootRound = c.freshRootRound
	default:
	}

	uc, err := testProofUC(parentRound, rootRound, c.submittedRoot)
	if err != nil {
		return nil, err
	}

	return &api.RootShardInclusionProof{
		ParentFragment: &api.ParentInclusionFragment{
			CertificateBytes: api.NewHexBytes(make([]byte, api.BitmapSize)),
			ShardLeafValue:   api.NewHexBytes(c.submittedRoot),
		},
		BlockNumber:        parentRound,
		UnicityCertificate: uc,
	}, nil
}

func (c *staleThenFreshRootAggregatorClient) CheckHealth(ctx context.Context) error {
	return nil
}

func (c *staleThenFreshRootAggregatorClient) ProofPolls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.proofPolls
}

func testProofUC(parentRound, rootRound uint64, rootHash api.HexBytes) (api.HexBytes, error) {
	uc := types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			RoundNumber: parentRound,
			Hash:        hex.Bytes(rootHash),
		},
		UnicitySeal: &types.UnicitySeal{
			RootChainRoundNumber: rootRound,
		},
	}

	ucBytes, err := types.Cbor.Marshal(uc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal test proof UC: %w", err)
	}

	return api.NewHexBytes(ucBytes), nil
}

func waitForStateBlockNumber(
	t *testing.T,
	ctx context.Context,
	storage interfaces.Storage,
	stateID api.StateID,
	timeout time.Duration,
) *api.BigInt {
	t.Helper()

	var blockNumber *api.BigInt
	require.Eventually(t, func() bool {
		record, err := storage.AggregatorRecordStorage().GetByStateID(ctx, stateID)
		require.NoError(t, err)
		if record == nil {
			return false
		}
		blockNumber = record.BlockNumber
		return blockNumber != nil
	}, timeout, 25*time.Millisecond)

	return blockNumber
}

func getLeafFromCommitment(t *testing.T, commitment *models.CertificationRequest) *smt.Leaf {
	path, err := commitment.StateID.GetPath()
	require.NoError(t, err)
	leafValue, err := commitment.CertificationData.ToAPI().Hash()
	require.NoError(t, err)
	return smt.NewLeaf(path, leafValue)
}

func newTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	l, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	return l
}

func newTestPrecollector(t *testing.T, stream chan *models.CertificationRequest, maxPerRound int) (*childPrecollector, *smt.ThreadSafeSMT) {
	t.Helper()
	log := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	if maxPerRound <= 0 {
		maxPerRound = 10000
	}
	cp := newChildPrecollector(stream, nil, log, maxPerRound, nil)
	return cp, smtInstance
}

func TestProcessMiniBatch_SkipsExistingDuplicateWithoutProofPending(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
			BatchLimit:    1000,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeStandalone,
		},
	}
	testLogger := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, cfg, testLogger, nil, nil, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), smtInstance, nil)
	require.NoError(t, err)

	existing := testutil.CreateTestCertificationRequest(t, "existing_duplicate")
	initialSnapshot := smtInstance.CreateSnapshot()
	_, err = initialSnapshot.AddLeaves([]*smt.Leaf{getLeafFromCommitment(t, existing)})
	require.NoError(t, err)
	initialSnapshot.Commit(smtInstance)

	newCommitment := testutil.CreateTestCertificationRequest(t, "new_commitment")
	duplicate := *existing
	duplicate.StreamID = "duplicate-stream-id"

	rm.currentRound = &Round{
		Number:   api.NewBigInt(big.NewInt(1)),
		State:    RoundStateProcessing,
		Snapshot: testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, []*models.CertificationRequest{&duplicate, newCommitment})
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	require.Len(t, rm.currentRound.PendingCommitments, 1)
	require.Equal(t, newCommitment.StateID, rm.currentRound.PendingCommitments[0].StateID)
	require.Len(t, rm.currentRound.PendingLeaves, 1)

	rm.proofCacheMu.RLock()
	_, duplicatePending := rm.proofPending[existing.StateID.String()]
	_, newPending := rm.proofPending[newCommitment.StateID.String()]
	rm.proofCacheMu.RUnlock()
	require.False(t, duplicatePending)
	require.True(t, newPending)
}

func TestReconcileRecoveredFinalization_CommitsMatchingSnapshotAndClearsProofPending(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
			BatchLimit:    1000,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeStandalone,
		},
	}
	testLogger := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, cfg, testLogger, nil, nil, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), smtInstance, nil)
	require.NoError(t, err)

	commitment := testutil.CreateTestCertificationRequest(t, "recovered_finalization")
	blockNumber := api.NewBigInt(big.NewInt(12))
	rm.currentRound = &Round{
		Number:   blockNumber,
		State:    RoundStateProcessing,
		Snapshot: testRMSnapshot(t, ctx, rm),
	}

	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, []*models.CertificationRequest{commitment})
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	key, err := commitment.StateID.GetTreeKey()
	require.NoError(t, err)
	_, err = smtInstance.GetInclusionCert(key)
	require.Error(t, err)

	rm.proofCacheMu.RLock()
	_, pendingBefore := rm.proofPending[commitment.StateID.String()]
	rm.proofCacheMu.RUnlock()
	require.True(t, pendingBefore)

	rm.reconcileRecoveredFinalization(blockNumber)

	_, err = smtInstance.GetInclusionCert(key)
	require.NoError(t, err)

	rm.proofCacheMu.RLock()
	_, pendingAfter := rm.proofPending[commitment.StateID.String()]
	rm.proofCacheMu.RUnlock()
	require.False(t, pendingAfter)
}

func TestReconcileRecoveredFinalization_MismatchedBlockClearsProofPendingOnly(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: time.Second,
			BatchLimit:    1000,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeStandalone,
		},
	}
	testLogger := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	rm, err := NewRoundManager(ctx, cfg, testLogger, nil, nil, nil, state.NewSyncStateTracker(), nil, events.NewEventBus(testLogger), smtInstance, nil)
	require.NoError(t, err)

	commitment := testutil.CreateTestCertificationRequest(t, "mismatched_recovered_finalization")
	currentBlockNumber := api.NewBigInt(big.NewInt(20))
	recoveredBlockNumber := api.NewBigInt(big.NewInt(19))
	rm.currentRound = &Round{
		Number:   currentBlockNumber,
		State:    RoundStateProcessing,
		Snapshot: testRMSnapshot(t, ctx, rm),
	}

	originalRoot := smtInstance.GetRootHash()
	rm.roundMutex.Lock()
	_, err = rm.processMiniBatch(ctx, []*models.CertificationRequest{commitment})
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	rm.proofCacheMu.RLock()
	_, pendingBefore := rm.proofPending[commitment.StateID.String()]
	rm.proofCacheMu.RUnlock()
	require.True(t, pendingBefore)

	rm.reconcileRecoveredFinalization(recoveredBlockNumber)

	require.Equal(t, originalRoot, smtInstance.GetRootHash())
	key, err := commitment.StateID.GetTreeKey()
	require.NoError(t, err)
	_, err = smtInstance.GetInclusionCert(key)
	require.Error(t, err)

	rm.proofCacheMu.RLock()
	_, pendingAfter := rm.proofPending[commitment.StateID.String()]
	rm.proofCacheMu.RUnlock()
	require.False(t, pendingAfter)
}

func TestDrainBufferedCommitments_StopsAtRoundBoundary(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 8)
	pending := make([]*models.CertificationRequest, 0, miniBatchSize)
	collected := make([]*models.CertificationRequest, 0, 5)
	count := 0
	maxPerRound := 5

	flush := func() error {
		collected = append(collected, pending...)
		count += len(pending)
		pending = pending[:0]
		return nil
	}

	for i := 0; i < maxPerRound+2; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "drain_boundary")
	}

	require.NoError(t, drainBufferedCommitments(stream, maxPerRound, &count, &pending, flush))
	require.NoError(t, flush())

	require.Len(t, collected, maxPerRound)
	require.Len(t, stream, 2)
}

// --- Tests for childPrecollector ---

func TestChildPrecollector_CollectsContinuouslyAcrossRound(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 100)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Send commitments for round 1
	c1 := testutil.CreateTestCertificationRequest(t, "round1_a")
	c2 := testutil.CreateTestCertificationRequest(t, "round1_b")
	stream <- c1
	stream <- c2

	// Small delay to let goroutine process
	time.Sleep(50 * time.Millisecond)

	// Advance round — should return both commitments
	result, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Len(t, result.commitments, 2)
	assert.Len(t, result.leaves, 2)
	assert.NotNil(t, result.snapshot)

	// Send commitments for round 2
	c3 := testutil.CreateTestCertificationRequest(t, "round2_a")
	stream <- c3
	time.Sleep(50 * time.Millisecond)

	result2, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Len(t, result2.commitments, 1)
	assert.Equal(t, c3, result2.commitments[0])

	// Snapshots should be different (round 2 has round 1 data + round 2 data)
	assert.NotEqual(t, testSnapshotRootHex(t, ctx, result.snapshot), testSnapshotRootHex(t, ctx, result2.snapshot))
}

func TestChildPrecollector_AdvanceRound_FlushesPendingBatch(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 200)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Send fewer than miniBatchSize commitments (pending but not flushed)
	count := miniBatchSize - 1
	for i := 0; i < count; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "pending_flush")
	}
	time.Sleep(50 * time.Millisecond)

	result, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Len(t, result.commitments, count, "AdvanceRound must flush pending batch")
	assert.Len(t, result.leaves, count)
}

func TestChildPrecollector_AdvanceRound_NoDropAcrossBoundary(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 200)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	total := 0
	// Round 1: send some commitments
	for i := 0; i < 5; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "boundary_r1")
		total++
	}
	time.Sleep(50 * time.Millisecond)

	r1, err := cp.AdvanceRound()
	require.NoError(t, err)
	r1Count := len(r1.commitments)

	// Round 2: send more commitments
	for i := 0; i < 7; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "boundary_r2")
		total++
	}
	time.Sleep(50 * time.Millisecond)

	r2, err := cp.AdvanceRound()
	require.NoError(t, err)

	// No commitments should be dropped across the boundary
	assert.Equal(t, total, r1Count+len(r2.commitments))
}

func TestChildPrecollector_HonorsMaxCommitmentsWithoutConsumingAndDropping(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 200)
	maxPerRound := 5
	cp, smtInstance := newTestPrecollector(t, stream, maxPerRound)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Send more than max
	for i := 0; i < maxPerRound+3; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "max_test")
	}
	time.Sleep(100 * time.Millisecond)

	result, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Equal(t, maxPerRound, len(result.commitments), "should honor max per round")

	// After advance, the overflow should still be in the stream (not consumed and dropped)
	// The precollector should now collect the remaining 3 for the next round
	time.Sleep(50 * time.Millisecond)
	result2, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Equal(t, 3, len(result2.commitments), "overflow should be collected in next round")
}

func TestChildPrecollector_ControlMessagesProgressUnderBackpressure(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 200)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Fill the stream to create backpressure
	for i := 0; i < 150; i++ {
		stream <- testutil.CreateTestCertificationRequest(t, "backpressure")
	}

	// AdvanceRound should still complete even under backpressure
	done := make(chan struct{})
	go func() {
		_, err := cp.AdvanceRound()
		assert.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("AdvanceRound should complete under backpressure")
	}
}

func TestChildPrecollector_StopCancelsCleanly(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 100)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)

	// Send some data
	stream <- testutil.CreateTestCertificationRequest(t, "stop_test")
	time.Sleep(50 * time.Millisecond)

	// Stop should return promptly
	done := make(chan struct{})
	go func() {
		cp.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop should complete promptly")
	}

	// AdvanceRound after stop should fail
	_, err := cp.AdvanceRound()
	assert.Error(t, err)
}

func TestChildPrecollector_AdvanceRoundWithNoData(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 100)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Advance with no data sent
	result, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Empty(t, result.commitments)
	assert.Empty(t, result.leaves)
	assert.NotNil(t, result.snapshot)
}

func TestChildPrecollector_BatchWithBadLeafFallsBackToOneByOne(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 100)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
	cp.Start(ctx, baseSnapshot)
	defer cp.Stop()

	// Send first commitment
	c1 := testutil.CreateTestCertificationRequest(t, "fallback_1")
	stream <- c1
	time.Sleep(50 * time.Millisecond)

	// Advance to lock in c1
	r1, err := cp.AdvanceRound()
	require.NoError(t, err)
	assert.Len(t, r1.commitments, 1)

	// Send a modified version of c1 (same path, different value) which will conflict,
	// plus a valid commitment c2
	c2 := testutil.CreateTestCertificationRequest(t, "fallback_2")
	modified := *c1
	modified.CertificationData.TransactionHash = c2.CertificationData.TransactionHash
	stream <- &modified
	stream <- c2
	time.Sleep(50 * time.Millisecond)

	r2, err := cp.AdvanceRound()
	require.NoError(t, err)
	// modified should be rejected, c2 should succeed
	assert.Len(t, r2.commitments, 1, "only valid commitment should be collected")
	assert.Equal(t, c2, r2.commitments[0])
}

func TestChildPrecollector_AdvanceRoundFailsOnSnapshotAddError(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 1)
	testLogger := newTestLogger(t)
	addErr := errors.New("snapshot hash mismatch")
	childSnapshot := &precollectorErrorSnapshot{addErr: addErr}
	baseSnapshot := &precollectorErrorSnapshot{forked: childSnapshot}
	cp := newChildPrecollector(stream, nil, testLogger, 10000, nil)
	t.Cleanup(cp.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cp.Start(ctx, baseSnapshot)
	stream <- testutil.CreateTestCertificationRequest(t, "snapshot_add_error")

	result, err := cp.AdvanceRound()
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorContains(t, err, "snapshot hash mismatch")
}

type precollectorErrorSnapshot struct {
	forked smtbackend.Snapshot
	addErr error
}

func (s *precollectorErrorSnapshot) AddLeavesClassified(context.Context, []smtbackend.LeafInput) (smtbackend.BatchApplyResult, error) {
	if s.addErr != nil {
		return smtbackend.BatchApplyResult{}, s.addErr
	}
	return smtbackend.BatchApplyResult{}, nil
}

func (s *precollectorErrorSnapshot) RootHashRaw(context.Context) ([]byte, error) {
	return make([]byte, api.StateTreeKeyLengthBytes), nil
}

func (s *precollectorErrorSnapshot) Fork(context.Context) (smtbackend.Snapshot, error) {
	if s.forked != nil {
		return s.forked, nil
	}
	return s, nil
}

func (s *precollectorErrorSnapshot) SetCommitTarget(context.Context, smtbackend.Backend) error {
	return nil
}

func (s *precollectorErrorSnapshot) Commit(context.Context, smtbackend.CommitMetadata) error {
	return nil
}

func (s *precollectorErrorSnapshot) Discard(context.Context) {}

// --- Snapshot reparenting test (still valid with precollector) ---

func TestPreCollectionReparenting(t *testing.T) {
	t.Run("ReparentedSnapshotCommitsToMainSMT", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testLogger := newTestLogger(t)
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
		backend := testMemoryBackend(smtInstance)
		initialMainRootHash := testSMTRawRootHex(smtInstance)

		// Round N: Create snapshot and add a leaf
		roundNCommitment := testutil.CreateTestCertificationRequest(t, "round_n")
		roundNLeaf := getLeafFromCommitment(t, roundNCommitment)

		roundNSnapshot := testBackendSnapshot(t, ctx, backend)
		testAddLegacyLeavesToBackendSnapshot(t, ctx, roundNSnapshot, []*smt.Leaf{roundNLeaf})
		roundNRootHash := testSnapshotRootHex(t, ctx, roundNSnapshot)

		assert.Equal(t, initialMainRootHash, testSMTRawRootHex(smtInstance))

		// Start precollector from Round N's snapshot
		stream := make(chan *models.CertificationRequest, 100)
		cp := newChildPrecollector(stream, nil, testLogger, 10000, nil)
		cp.Start(ctx, roundNSnapshot)
		defer cp.Stop()

		// Send a pre-collected commitment
		preCollectedCommitment := testutil.CreateTestCertificationRequest(t, "precollected")
		stream <- preCollectedCommitment
		time.Sleep(50 * time.Millisecond)

		result, err := cp.AdvanceRound()
		require.NoError(t, err)
		require.Len(t, result.commitments, 1)

		preCollectedRootHash := testSnapshotRootHex(t, ctx, result.snapshot)
		assert.NotEqual(t, roundNRootHash, preCollectedRootHash)

		// Commit Round N to main SMT (simulating FinalizeBlock)
		testCommitSnapshot(t, ctx, roundNSnapshot)
		assert.Equal(t, roundNRootHash, testSMTRawRootHex(smtInstance))

		// Reparent and commit pre-collection snapshot to main SMT
		testSetSnapshotCommitTarget(t, ctx, result.snapshot, backend)
		testCommitSnapshot(t, ctx, result.snapshot)

		assert.Equal(t, preCollectedRootHash, testSMTRawRootHex(smtInstance))

		// Verify both leaves are in main SMT
		roundNPath, err := roundNCommitment.StateID.GetPath()
		require.NoError(t, err)
		leaf1, err := smtInstance.GetLeaf(roundNPath)
		require.NoError(t, err)
		assert.NotNil(t, leaf1)

		preCollectedPath, err := preCollectedCommitment.StateID.GetPath()
		require.NoError(t, err)
		leaf2, err := smtInstance.GetLeaf(preCollectedPath)
		require.NoError(t, err)
		assert.NotNil(t, leaf2)
	})
}

// TestChildPrecollector_DeactivateDuringInFlightRound deactivates while the
// first child round is blocked waiting for the parent proof. The in-flight
// round should still finish block 1 once the proof is released, but it must
// not start round 2.
func TestChildPrecollector_DeactivateDuringInFlightRound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_child_deactivate_inflight",
		},
		Processing: config.ProcessingConfig{
			RoundDuration:          100 * time.Millisecond,
			MaxCommitmentsPerRound: 1000,
		},
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeChild,
			ShardIDLength: 1,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  5 * time.Second,
				ParentPollInterval: 10 * time.Millisecond,
			},
		},
	}
	store := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	rootClient := newBlockingProofRootAggregatorClient()
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		store.CommitmentQueue(),
		store,
		rootClient,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	// Wait until round 1 is actively polling for the parent proof.
	select {
	case <-rootClient.proofPollStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for round 1 to enter proof polling")
	}

	// Deactivate while round 1 is still in-flight.
	require.NoError(t, rm.Deactivate(ctx))

	// Let the in-flight round finish block 1.
	close(rootClient.releaseProof)

	require.Eventually(t, func() bool {
		block, err := store.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
		return err == nil && block != nil
	}, 3*time.Second, 25*time.Millisecond, "block 1 should be created after proof release")

	// If the bug exists, round 2 would appear shortly after block 1 finalization.
	time.Sleep(500 * time.Millisecond)

	block2, err := store.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	assert.Nil(t, block2, "no block 2 should be created after Deactivate")

	// Wait for goroutines to drain
	rm.wg.Wait()
}

func TestChildRound_ParentProofTimeoutIsRetriable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_child_parent_proof_timeout_retriable",
		},
		Processing: config.ProcessingConfig{
			RoundDuration:          100 * time.Millisecond,
			MaxCommitmentsPerRound: 1000,
		},
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeChild,
			ShardIDLength: 1,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  100 * time.Millisecond,
				ParentPollInterval: 10 * time.Millisecond,
			},
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	rootClient := newBlockingProofRootAggregatorClient()
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		storage.CommitmentQueue(),
		storage,
		rootClient,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	select {
	case <-rootClient.proofPollStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for child round to poll parent proof")
	}

	// Wait past at least one poll timeout window.
	time.Sleep(250 * time.Millisecond)

	block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
	require.NoError(t, err)
	assert.Nil(t, block, "block should not be finalized before proof is released")

	close(rootClient.releaseProof)

	require.Eventually(t, func() bool {
		block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
		return err == nil && block != nil
	}, 3*time.Second, 25*time.Millisecond, "block 1 should be created after proof release")
}

func TestStartNewRoundWithSnapshot(t *testing.T) {
	t.Run("SkipsCollectPhaseWithPreCollectedData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := config.Config{
			Database: config.DatabaseConfig{
				Database: "test_start_round_snapshot",
			},
			Processing: config.ProcessingConfig{
				RoundDuration:          100 * time.Millisecond,
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeStandalone,
			},
		}
		storage := testutil.SetupTestStorage(t, cfg)

		testLogger := newTestLogger(t)
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

		rm, err := NewRoundManager(
			ctx,
			&cfg,
			testLogger,
			storage.CommitmentQueue(),
			storage,
			nil,
			state.NewSyncStateTracker(),
			nil,
			events.NewEventBus(testLogger),
			smtInstance,
			nil,
		)
		require.NoError(t, err)

		preSnapshot := testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance))
		commitment := testutil.CreateTestCertificationRequest(t, "precollected_round")
		leaf := getLeafFromCommitment(t, commitment)
		testAddLegacyLeavesToBackendSnapshot(t, ctx, preSnapshot, []*smt.Leaf{leaf})

		preCommitments := []*models.CertificationRequest{commitment}
		preLeaves := []smtbackend.LeafInput{testLeafInputFromLegacyLeaf(t, leaf)}

		startTime := time.Now()
		err = rm.StartNewRoundWithSnapshot(ctx, api.NewBigInt(big.NewInt(1)), preSnapshot, preCommitments, preLeaves)
		require.NoError(t, err)

		rm.roundMutex.RLock()
		roundState := rm.currentRound.State
		roundCommitments := len(rm.currentRound.Commitments)
		rm.roundMutex.RUnlock()

		assert.Equal(t, RoundStateProcessing, roundState)
		assert.Equal(t, 1, roundCommitments)

		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, 100*time.Millisecond, "Should not wait for collect phase")
	})
}

func TestStandalonePrecollectorGraceIncludesLateCommitment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{Database: "test_standalone_precollector_grace"},
		Processing: config.ProcessingConfig{
			RoundDuration:           100 * time.Millisecond,
			PrecollectorGracePeriod: 150 * time.Millisecond,
			MaxCommitmentsPerRound:  1000,
		},
		Storage:  config.StorageConfig{UseRedisForCommitments: true},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeStandalone},
		BFT: config.BFTConfig{
			Enabled: false,
			// Keep the precollected round in-flight while the test inspects it.
			StubDelay: 5 * time.Second,
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		storage.CommitmentQueue(),
		storage,
		nil,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	cp := newChildPrecollector(
		rm.commitmentStream,
		rm.commitmentQueue,
		rm.logger,
		rm.config.Processing.MaxCommitmentsPerRound,
		rm.markProofsPending,
	)
	rm.roundMutex.Lock()
	rm.precollectorDisabled = false
	rm.precollectorDone = make(chan struct{})
	rm.precollector = cp
	rm.roundMutex.Unlock()
	cp.Start(ctx, testBackendSnapshot(t, ctx, testMemoryBackend(smtInstance)))

	lateCommitment := testutil.CreateTestCertificationRequest(t, "during_grace")
	go func() {
		time.Sleep(50 * time.Millisecond)
		rm.commitmentStream <- lateCommitment
	}()

	start := time.Now()
	require.NoError(t, rm.StartNextRoundFromPrecollector(ctx, api.NewBigInt(big.NewInt(2))))
	assert.GreaterOrEqual(t, time.Since(start), cfg.Processing.PrecollectorGracePeriod)

	rm.roundMutex.RLock()
	currentRound := rm.currentRound
	var commitments []*models.CertificationRequest
	if currentRound != nil {
		commitments = append([]*models.CertificationRequest(nil), currentRound.Commitments...)
	}
	rm.roundMutex.RUnlock()

	require.NotNil(t, currentRound)
	assert.EqualValues(t, 2, currentRound.Number.Int64())
	require.Len(t, commitments, 1)
	assert.Equal(t, lateCommitment.StateID, commitments[0].StateID)
}

func TestStandaloneActivePrecollectorLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{Database: "test_standalone_active_precollector_lifecycle"},
		Processing: config.ProcessingConfig{
			RoundDuration:           100 * time.Millisecond,
			PrecollectorGracePeriod: 50 * time.Millisecond,
			MaxCommitmentsPerRound:  1000,
			CollectPhaseDuration:    500 * time.Millisecond,
		},
		Storage:  config.StorageConfig{UseRedisForCommitments: true},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeStandalone},
		BFT: config.BFTConfig{
			Enabled:   false,
			StubDelay: 1 * time.Second,
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		storage.CommitmentQueue(),
		storage,
		nil,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	require.Eventually(t, func() bool {
		rm.roundMutex.RLock()
		defer rm.roundMutex.RUnlock()
		return rm.precollector != nil
	}, 2*time.Second, 25*time.Millisecond, "round 1 should start the active precollector after fixed collect")

	lateCommitment := testutil.CreateTestCertificationRequest(t, "active_precollector_lifecycle")
	select {
	case rm.commitmentStream <- lateCommitment:
	case <-time.After(time.Second):
		t.Fatal("timed out sending commitment to active precollector")
	}

	require.Eventually(t, func() bool {
		rm.roundMutex.RLock()
		defer rm.roundMutex.RUnlock()
		return rm.currentRound != nil &&
			rm.currentRound.Number.Int64() == 2 &&
			!rm.currentRound.ProposalTime.IsZero()
	}, 4*time.Second, 25*time.Millisecond, "BFT stub finalization should advance to precollected round 2")

	rm.roundMutex.RLock()
	currentRound := rm.currentRound
	roundNumber := currentRound.Number.Int64()
	preCollected := currentRound.PreCollected
	proposalPrep := currentRound.ProposalTime.Sub(currentRound.StartTime)
	commitments := append([]*models.CertificationRequest(nil), currentRound.Commitments...)
	rm.roundMutex.RUnlock()

	assert.EqualValues(t, 2, roundNumber)
	assert.True(t, preCollected, "round 2 should come from the active precollector handoff")
	require.Len(t, commitments, 1)
	assert.Equal(t, lateCommitment.StateID, commitments[0].StateID)
	assert.Less(t, proposalPrep, cfg.Processing.CollectPhaseDuration/2,
		"precollected round should skip the fixed collect phase")

	block1, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
	require.NoError(t, err)
	assert.NotNil(t, block1, "round 2 should be reached through BFT stub finalization of round 1")
}

func TestPipelinedChildModeFlow(t *testing.T) {
	t.Run("SecondRoundUsesPreCollectedData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := config.Config{
			Database: config.DatabaseConfig{
				Database: "test_pipelined_child_flow",
			},
			Processing: config.ProcessingConfig{
				RoundDuration:          100 * time.Millisecond,
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode:          config.ShardingModeChild,
				ShardIDLength: 1,
				Child: config.ChildConfig{
					ShardID:            0b11,
					ParentPollTimeout:  5 * time.Second,
					ParentPollInterval: 50 * time.Millisecond,
				},
			},
		}
		storage := testutil.SetupTestStorage(t, cfg)

		testLogger := newTestLogger(t)
		rootAggregatorClient := testsharding.NewRootAggregatorClientStub()
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

		rm, err := NewRoundManager(
			ctx,
			&cfg,
			testLogger,
			storage.CommitmentQueue(),
			storage,
			rootAggregatorClient,
			state.NewSyncStateTracker(),
			nil,
			events.NewEventBus(testLogger),
			smtInstance,
			nil,
		)
		require.NoError(t, err)

		require.NoError(t, rm.Start(ctx))
		require.NoError(t, rm.Activate(ctx))

		require.Eventually(t, func() bool {
			block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
			return err == nil && block != nil
		}, 3*time.Second, 50*time.Millisecond, "first block should be created")

		require.Eventually(t, func() bool {
			block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
			return err == nil && block != nil
		}, 3*time.Second, 50*time.Millisecond, "second block should be created")

		block1, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
		require.NoError(t, err)
		assert.NotNil(t, block1)

		block2, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
		require.NoError(t, err)
		assert.NotNil(t, block2)

		assert.GreaterOrEqual(t, rootAggregatorClient.SubmissionCount(), 2)
		assert.GreaterOrEqual(t, rootAggregatorClient.ProofCount(), 2)
	})
}

// In proof-driven child mode, once block N is finalized the next round starts immediately.
// A commitment arriving after that point must land in the following block.
func TestChildPreCollection_CommitmentAfterProofBeforeRoundEnd_ShouldBeInNextRound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_child_precollection_tail_gap",
		},
		Processing: config.ProcessingConfig{
			RoundDuration:          500 * time.Millisecond,
			MaxCommitmentsPerRound: 1000,
		},
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeChild,
			ShardIDLength: 1,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  5 * time.Second,
				ParentPollInterval: 200 * time.Millisecond,
			},
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	rootAggregatorClient := testsharding.NewRootAggregatorClientStub()
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		storage.CommitmentQueue(),
		storage,
		rootAggregatorClient,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	require.Eventually(t, func() bool {
		block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
		return err == nil && block != nil
	}, 3*time.Second, 25*time.Millisecond, "block 1 should be finalized before injecting the late commitment")

	lateCommitment := testutil.CreateTestCertificationRequest(t, "after_proof_before_round_end")
	rm.commitmentStream <- lateCommitment

	blockNumber := waitForStateBlockNumber(t, ctx, storage, lateCommitment.StateID, 5*time.Second)

	require.EqualValues(
		t,
		3,
		blockNumber.Int64(),
		"commitment arriving after block 1 finalization should land in block 3 because round 2 has already started",
	)
}

func TestChildMode_RequiresFreshParentProof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_child_requires_fresh_parent_proof",
		},
		Processing: config.ProcessingConfig{
			RoundDuration:          100 * time.Millisecond,
			MaxCommitmentsPerRound: 1000,
		},
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeChild,
			ShardIDLength: 1,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  5 * time.Second,
				ParentPollInterval: 10 * time.Millisecond,
			},
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)

	testLogger := newTestLogger(t)
	rootAggregatorClient := newStaleThenFreshRootAggregatorClient(1, 10, 2, 13)
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))

	rootHash, err := api.NewHexBytesFromString(smtInstance.GetRootHash())
	require.NoError(t, err)
	initialUC, err := testProofUC(1, 10, rootHash)
	require.NoError(t, err)
	initialBlock := models.NewBlock(
		api.NewBigInt(big.NewInt(1)),
		"",
		cfg.Sharding.Child.ShardID,
		"",
		"",
		rootHash,
		api.HexBytes{},
		initialUC,
	)
	initialBlock.Finalized = true
	require.NoError(t, storage.BlockStorage().Store(ctx, initialBlock))

	rm, err := NewRoundManager(
		ctx,
		&cfg,
		testLogger,
		storage.CommitmentQueue(),
		storage,
		rootAggregatorClient,
		state.NewSyncStateTracker(),
		nil,
		events.NewEventBus(testLogger),
		smtInstance,
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = rm.Stop(shutdownCtx)
	})

	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	select {
	case <-rootAggregatorClient.proofPollStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for child round to poll parent proof")
	}

	time.Sleep(250 * time.Millisecond)

	block1, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	assert.Nil(t, block1, "child must not finalize against a stale parent UC")
	assert.Greater(t, rootAggregatorClient.ProofPolls(), 1, "child should keep polling while proof is stale")

	close(rootAggregatorClient.releaseFresh)

	require.Eventually(t, func() bool {
		block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
		return err == nil && block != nil
	}, 3*time.Second, 25*time.Millisecond, "block 2 should be created once a fresh parent proof is available")

	assert.EqualValues(t, 2, rm.lastAcceptedParentUCRound.Load())
}
