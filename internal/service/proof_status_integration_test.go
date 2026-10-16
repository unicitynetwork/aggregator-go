package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func memoryBFTShardServiceConfig(name string) config.Config {
	return config.Config{
		Database: config.DatabaseConfig{
			Database: name, ConnectTimeout: 30 * time.Second, ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout: 30 * time.Second, MaxPoolSize: 20, MinPoolSize: 1, MaxConnIdleTime: time.Minute,
		},
		Processing: config.ProcessingConfig{
			BatchLimit: 1000, MaxCommitmentsPerRound: 1000, CollectPhaseDuration: 100 * time.Millisecond,
			CommitmentStreamBufferSize: 1000, SkipDuplicateCheck: true, // worst case for resubmission
		},
		Sharding: config.ShardingConfig{Mode: config.ShardingModeBFTShard},
		Chain:    config.ChainConfig{ID: "unicity", Version: "1.0", ForkID: "test"},
		BFT:      config.BFTConfig{Enabled: false, StubDelay: 750 * time.Millisecond},
	}
}

func proofIncluded(r *api.GetInclusionProofResponseV2) bool {
	return r != nil && r.InclusionProof != nil && len(r.InclusionProof.CertificateBytes) > 0
}

// proofStatusFlow drives the real service, round manager, queue and storage: pending from
// admission, included after certification, harmless resubmission, unknown otherwise.
func proofStatusFlow(t *testing.T, cfg config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage := testutil.SetupTestStorage(t, cfg)
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	rm, err := round.NewRoundManager(ctx, &cfg, log, storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker(), nil,
		events.NewEventBus(log), smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 2*time.Second)
		defer stop()
		_ = rm.Stop(c)
	})
	svc := NewAggregatorService(&cfg, log, rm, storage.CommitmentQueue(), storage, nil)
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	// something to certify first, so the service has a reference time
	warm := testutil.CreateTestCertificationRequest(t, "status_warm").ToAPI()
	resp, err := svc.CertificationRequest(ctx, warm)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Status)
	// Wait for the warm-up proof to be servable, not merely for its record to exist: the record
	// becomes visible when the block is finalized in storage, but the disk backend publishes the
	// proof view (the root proofs are served against) a moment later. Until then the published
	// root is still the initial empty one, no block has it, and the service answers "no block
	// found with root hash 00…0". That is the same before-the-first-block answer the memory
	// backend gives; it is not what this test is about.
	require.Eventually(t, func() bool {
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: warm.StateID})
		return err == nil && proofIncluded(p)
	}, 10*time.Second, 25*time.Millisecond, "warm-up proof servable")

	req := testutil.CreateTestCertificationRequest(t, "status_flow").ToAPI()
	require.Equal(t, api.ProofStatusUnknown, svc.ProofStatusHint(req.StateID), "never seen")
	empty, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
	require.NoError(t, err)
	require.False(t, proofIncluded(empty))

	resp, err = svc.CertificationRequest(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Status)
	require.Equal(t, api.ProofStatusPending, svc.ProofStatusHint(req.StateID), "pending from admission")
	early, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
	require.NoError(t, err)
	require.False(t, proofIncluded(early), "default result for a pending state is the unchanged empty proof")

	// Resubmitting the same signed request while it is still pending is harmless: the Mongo
	// queue rejects the second copy by its unique stateId index (an error to the caller,
	// pre-existing behaviour), the Redis queue accepts it and the round deduplicates it. In
	// both cases the original stays queued and pending, and there is never a second leaf.
	resp, err = svc.CertificationRequest(ctx, req)
	if err != nil {
		require.Contains(t, err.Error(), "duplicate key")
	} else {
		require.Equal(t, "SUCCESS", resp.Status)
	}
	require.Equal(t, api.ProofStatusPending, svc.ProofStatusHint(req.StateID),
		"a failed/duplicate resubmission must not clear the original's pending mark")

	require.Eventually(t, func() bool {
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
		return err == nil && proofIncluded(p)
	}, 15*time.Second, 25*time.Millisecond, "certified")
	require.Eventually(t, func() bool { return svc.ProofStatusHint(req.StateID) != api.ProofStatusPending },
		5*time.Second, 25*time.Millisecond, "the pending mark is cleared on finalization")

	// resubmitted after certification (duplicate check off): a few rounds later still one record
	_, _ = svc.CertificationRequest(ctx, req)
	time.Sleep(2 * time.Second)
	n, err := storage.AggregatorRecordStorage().Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, n, "the warm-up record and exactly one record for the resubmitted request")
	p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
	require.NoError(t, err)
	require.True(t, proofIncluded(p))
}

func TestProofStatusFlowMemoryBackend(t *testing.T) {
	proofStatusFlow(t, memoryBFTShardServiceConfig("test_proof_status_mem"))
}
