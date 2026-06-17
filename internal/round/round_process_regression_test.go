package round

import (
	"bytes"
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type recordedBFTProposal struct {
	blockNumber uint64
	rootHash    []byte
}

type recordingBFTClient struct {
	mu        sync.Mutex
	proposals []recordedBFTProposal
	notify    chan struct{}
}

func newRecordingBFTClient() *recordingBFTClient {
	return &recordingBFTClient{notify: make(chan struct{}, 16)}
}

func (c *recordingBFTClient) Start(context.Context) error { return nil }
func (c *recordingBFTClient) Stop()                       {}
func (c *recordingBFTClient) WaitForInitialized(context.Context) error {
	return nil
}

func (c *recordingBFTClient) CertificationRequest(_ context.Context, block *models.Block) error {
	c.mu.Lock()
	c.proposals = append(c.proposals, recordedBFTProposal{
		blockNumber: block.Index.Uint64(),
		rootHash:    append([]byte(nil), block.RootHash...),
	})
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	return nil
}

func (c *recordingBFTClient) snapshot() []recordedBFTProposal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]recordedBFTProposal(nil), c.proposals...)
}

type staleBFTClient struct {
	called chan struct{}
}

func (c *staleBFTClient) Start(context.Context) error { return nil }
func (c *staleBFTClient) Stop()                       {}
func (c *staleBFTClient) WaitForInitialized(context.Context) error {
	return nil
}

func (c *staleBFTClient) CertificationRequest(context.Context, *models.Block) error {
	select {
	case c.called <- struct{}{}:
	default:
	}
	return bft.ErrStaleCertificationRound
}

func TestRoundProcessingUsesScheduledRoundSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_round_processing_uses_scheduled_round",
		},
		Processing: config.ProcessingConfig{
			CollectPhaseDuration:       200 * time.Millisecond,
			CommitmentStreamBufferSize: 16,
			MaxCommitmentsPerRound:     1000,
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	storage := testutil.SetupTestStorage(t, cfg)
	testLogger := newTestLogger(t)
	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
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
		threadSafeSMT,
		nil,
	)
	require.NoError(t, err)
	recorder := newRecordingBFTClient()
	rm.bftClient = recorder

	require.NoError(t, rm.StartNewRound(ctx, api.NewBigInt(big.NewInt(1))))

	roundOneCommitment := testutil.CreateTestCertificationRequest(t, "scheduled_round_one")
	rm.commitmentStream <- roundOneCommitment
	require.Eventually(t, func() bool {
		rm.roundMutex.RLock()
		defer rm.roundMutex.RUnlock()
		return rm.currentRound != nil &&
			rm.currentRound.Number.Int64() == 1 &&
			len(rm.currentRound.Commitments) == 1
	}, time.Second, 10*time.Millisecond)

	roundTwoCommitment := testutil.CreateTestCertificationRequest(t, "scheduled_round_two")
	roundTwoLeaf, err := commitmentLeafInput(roundTwoCommitment)
	require.NoError(t, err)
	roundTwoSnapshot, err := rm.smtBackend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := roundTwoSnapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{roundTwoLeaf})
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(1))
	roundTwoRoot, err := roundTwoSnapshot.RootHashRaw(ctx)
	require.NoError(t, err)

	require.NoError(t, rm.StartNewRoundWithSnapshot(
		ctx,
		api.NewBigInt(big.NewInt(2)),
		roundTwoSnapshot,
		[]*models.CertificationRequest{roundTwoCommitment},
		[]smtbackend.LeafInput{roundTwoLeaf},
		false,
		"",
	))

	require.Eventually(t, func() bool {
		return len(recorder.snapshot()) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	require.Never(t, func() bool {
		for _, proposal := range recorder.snapshot() {
			if proposal.blockNumber == 1 {
				return true
			}
		}
		return false
	}, 300*time.Millisecond, 10*time.Millisecond, "superseded round 1 must not propose")

	proposals := recorder.snapshot()
	require.Len(t, proposals, 1)
	require.EqualValues(t, 2, proposals[0].blockNumber)
	require.True(t, bytes.Equal(proposals[0].rootHash, roundTwoRoot))

	finalizedBlock, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	require.Nil(t, finalizedBlock)

	proposedBlock, err := getBlockAnyFinalization(ctx, storage, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	require.NotNil(t, proposedBlock)
	require.Equal(t, models.FinalityStatusProposed, proposedBlock.Status)

	visibleRecord, err := storage.AggregatorRecordStorage().GetByStateID(ctx, roundTwoCommitment.StateID)
	require.NoError(t, err)
	require.Nil(t, visibleRecord)

	proposedRecord, err := getAggregatorRecordAnyFinalization(ctx, storage, roundTwoCommitment.StateID)
	require.NoError(t, err)
	require.NotNil(t, proposedRecord)
	require.Equal(t, proposedBlock.ProposalID, proposedRecord.ProposalID)

	visibleRecords, err := storage.AggregatorRecordStorage().GetByBlockNumber(ctx, api.NewBigInt(big.NewInt(2)))
	require.NoError(t, err)
	require.Empty(t, visibleRecords)
}

func TestStartNewRoundAbandonsSupersededPendingRound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_start_new_round_abandons_superseded_pending_round",
		},
		Processing: config.ProcessingConfig{
			CollectPhaseDuration:       time.Hour,
			CommitmentStreamBufferSize: 16,
			MaxCommitmentsPerRound:     1000,
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	storage := testutil.SetupTestStorage(t, cfg)
	testLogger := newTestLogger(t)
	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
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
		threadSafeSMT,
		nil,
	)
	require.NoError(t, err)
	rm.bftClient = newRecordingBFTClient()
	defer func() {
		cancel()
		rm.roundWG.Wait()
	}()

	oldCtx, oldCancel := context.WithCancel(ctx)
	commitment := testutil.CreateTestCertificationRequest(t, "abandoned_pending_round")
	discardSpy := &discardCountingSnapshot{Snapshot: testRMSnapshot(t, ctx, rm)}
	rm.currentRound = &Round{
		Number:             api.NewBigInt(big.NewInt(1)),
		StartTime:          time.Now(),
		State:              RoundStateFinalizing,
		Commitments:        []*models.CertificationRequest{commitment},
		Cancel:             oldCancel,
		Snapshot:           discardSpy,
		PendingCommitments: []*models.CertificationRequest{commitment},
		ProposalTime:       time.Now(),
	}
	rm.markProofsPending([]*models.CertificationRequest{commitment})

	require.NoError(t, rm.StartNewRound(ctx, api.NewBigInt(big.NewInt(2))))

	require.Equal(t, 1, discardSpy.discards)
	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("superseded round context was not cancelled")
	}

	_, pending := rm.proofPending[commitment.StateID.String()]
	require.False(t, pending)
}

func TestStaleCertificationRequestAbandonsStoredDurableProposal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Database: config.DatabaseConfig{
			Database: "test_stale_certification_request_abandons_durable_proposal",
		},
		Processing: config.ProcessingConfig{
			CollectPhaseDuration:       time.Hour,
			CommitmentStreamBufferSize: 16,
			MaxCommitmentsPerRound:     1000,
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	storage := testutil.SetupTestStorage(t, cfg)
	testLogger := newTestLogger(t)
	threadSafeSMT := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
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
		threadSafeSMT,
		nil,
	)
	require.NoError(t, err)
	client := &staleBFTClient{called: make(chan struct{}, 1)}
	rm.bftClient = client
	defer func() {
		cancel()
		rm.roundWG.Wait()
	}()

	commitment := testutil.CreateTestCertificationRequest(t, "stale_durable_proposal")
	leaf, err := commitmentLeafInput(commitment)
	require.NoError(t, err)
	snapshot, err := rm.smtBackend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf})
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(1))

	require.NoError(t, rm.StartNewRoundWithSnapshot(
		ctx,
		api.NewBigInt(big.NewInt(1)),
		snapshot,
		[]*models.CertificationRequest{commitment},
		[]smtbackend.LeafInput{leaf},
		false,
		"",
	))

	select {
	case <-client.called:
	case <-time.After(2 * time.Second):
		t.Fatal("expected stale BFT client to be called")
	}
	require.Eventually(t, func() bool {
		block, err := getBlockAnyFinalization(ctx, storage, api.NewBigInt(big.NewInt(1)))
		require.NoError(t, err)
		return block != nil && block.Status == models.FinalityStatusAbandoned
	}, 2*time.Second, 10*time.Millisecond)

	visibleRecord, err := storage.AggregatorRecordStorage().GetByStateID(ctx, commitment.StateID)
	require.NoError(t, err)
	require.Nil(t, visibleRecord)

	_, found, err := rm.LoadDurableProposal(ctx, api.NewBigInt(big.NewInt(1)))
	require.NoError(t, err)
	require.False(t, found)
}
