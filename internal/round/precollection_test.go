package round

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	testsharding "github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
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
	root := c.submittedRoot.String()
	parentRound := c.staleParentRound
	rootRound := c.staleRootRound

	select {
	case <-c.releaseFresh:
		parentRound = c.freshParentRound
		rootRound = c.freshRootRound
	default:
	}

	uc, err := testProofUC(parentRound, rootRound)
	if err != nil {
		return nil, err
	}

	return &api.RootShardInclusionProof{
		UnicityCertificate: uc,
		MerkleTreePath: &api.MerkleTreePath{
			Steps: []api.MerkleTreeStep{{Data: &root}},
		},
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

func testProofUC(parentRound, rootRound uint64) (api.HexBytes, error) {
	uc := types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			RoundNumber: parentRound,
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
		var err error
		blockNumber, err = storage.BlockRecordsStorage().GetByStateID(ctx, stateID)
		require.NoError(t, err)
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
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
	if maxPerRound <= 0 {
		maxPerRound = 10000
	}
	cp := newChildPrecollector(stream, nil, log, maxPerRound)
	return cp, smtInstance
}

// --- Tests for childPrecollector ---

func TestChildPrecollector_CollectsContinuouslyAcrossRound(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 100)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := smtInstance.CreateSnapshot()
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
	assert.NotEqual(t, result.snapshot.GetRootHash(), result2.snapshot.GetRootHash())
}

func TestChildPrecollector_AdvanceRound_FlushesPendingBatch(t *testing.T) {
	stream := make(chan *models.CertificationRequest, 200)
	cp, smtInstance := newTestPrecollector(t, stream, 10000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

	baseSnapshot := smtInstance.CreateSnapshot()
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

// --- Snapshot reparenting test (still valid with precollector) ---

func TestPreCollectionReparenting(t *testing.T) {
	t.Run("ReparentedSnapshotCommitsToMainSMT", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testLogger := newTestLogger(t)
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
		initialMainRootHash := smtInstance.GetRootHash()

		// Round N: Create snapshot and add a leaf
		roundNCommitment := testutil.CreateTestCertificationRequest(t, "round_n")
		roundNLeaf := getLeafFromCommitment(t, roundNCommitment)

		roundNSnapshot := smtInstance.CreateSnapshot()
		_, err := roundNSnapshot.AddLeaves([]*smt.Leaf{roundNLeaf})
		require.NoError(t, err)
		roundNRootHash := roundNSnapshot.GetRootHash()

		assert.Equal(t, initialMainRootHash, smtInstance.GetRootHash())

		// Start precollector from Round N's snapshot
		stream := make(chan *models.CertificationRequest, 100)
		cp := newChildPrecollector(stream, nil, testLogger, 10000)
		cp.Start(ctx, roundNSnapshot)
		defer cp.Stop()

		// Send a pre-collected commitment
		preCollectedCommitment := testutil.CreateTestCertificationRequest(t, "precollected")
		stream <- preCollectedCommitment
		time.Sleep(50 * time.Millisecond)

		result, err := cp.AdvanceRound()
		require.NoError(t, err)
		require.Len(t, result.commitments, 1)

		preCollectedRootHash := result.snapshot.GetRootHash()
		assert.NotEqual(t, roundNRootHash, preCollectedRootHash)

		// Commit Round N to main SMT (simulating FinalizeBlock)
		roundNSnapshot.Commit(smtInstance)
		assert.Equal(t, roundNRootHash, smtInstance.GetRootHash())

		// Reparent and commit pre-collection snapshot to main SMT
		result.snapshot.SetCommitTarget(smtInstance)
		result.snapshot.Commit(smtInstance)

		assert.Equal(t, preCollectedRootHash, smtInstance.GetRootHash())

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
			Mode: config.ShardingModeChild,
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
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

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
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

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

		preSnapshot := smtInstance.CreateSnapshot()
		commitment := testutil.CreateTestCertificationRequest(t, "precollected_round")
		leaf := getLeafFromCommitment(t, commitment)
		_, err = preSnapshot.AddLeaves([]*smt.Leaf{leaf})
		require.NoError(t, err)

		preCommitments := []*models.CertificationRequest{commitment}
		preLeaves := []*smt.Leaf{leaf}

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
				Mode: config.ShardingModeChild,
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
		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

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
			Mode: config.ShardingModeChild,
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
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

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
			Mode: config.ShardingModeChild,
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
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

	rootHash, err := api.NewHexBytesFromString(smtInstance.GetRootHash())
	require.NoError(t, err)
	initialUC, err := testProofUC(1, 10)
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
		nil,
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
