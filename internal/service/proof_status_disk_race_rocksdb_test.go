//go:build rocksdb

package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// On the disk backend the block is finalized in storage before the proof view is published, and
// readers take no finalization lock. A hint cleared in that gap would let the gateway (hint first,
// then proof) answer a certified state with `unknown` and the previous, empty proof: not retryable.
// The hook holds the round exactly in the gap and the gateway-order polling must never see
// unknown without a proof; once released the proof is included.
func TestProofStatusDiskNeverUnknownWithoutProofWhileViewUnpublished(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testDiskBFTShardServiceConfig(t)
	cfg.Database.Database = "test_proof_status_disk_race"
	storage := testutil.SetupTestStorage(t, cfg)
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	rm, err := round.NewRoundManager(ctx, &cfg, log, storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker(), nil,
		events.NewEventBus(log), smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)), nil)
	require.NoError(t, err)

	var armed atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	rm.SetTestHookBeforeProofViewPublish(func() {
		if armed.Load() {
			entered <- struct{}{}
			<-release
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = rm.Stop(c)
	})
	svc := NewAggregatorService(&cfg, log, rm, storage.CommitmentQueue(), storage, nil)
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	warm := testutil.CreateTestCertificationRequest(t, "race_warm").ToAPI()
	_, err = svc.CertificationRequest(ctx, warm)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: warm.StateID})
		return err == nil && proofIncluded(p)
	}, 15*time.Second, 25*time.Millisecond)

	armed.Store(true)
	req := testutil.CreateTestCertificationRequest(t, "race_target").ToAPI()
	_, err = svc.CertificationRequest(ctx, req)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the round never reached the publish hook")
	}
	// finalized in storage, proof view not published: poll like the gateway (hint, then proof)
	require.Eventually(t, func() bool {
		r, err := storage.AggregatorRecordStorage().GetByStateID(ctx, req.StateID)
		return err == nil && r != nil
	}, 5*time.Second, 10*time.Millisecond, "the record is visible while the view is unpublished")
	for i := 0; i < 40; i++ {
		hint := svc.ProofStatusHint(req.StateID)
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
		require.NoError(t, err)
		require.False(t, proofIncluded(p), "the view is not published yet")
		require.Equal(t, api.ProofStatusPending, hint, "poll %d: a state without a servable proof stays pending", i)
		time.Sleep(10 * time.Millisecond)
	}

	armed.Store(false)
	close(release)
	require.Eventually(t, func() bool {
		hint := svc.ProofStatusHint(req.StateID)
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
		require.NoError(t, err)
		require.False(t, !proofIncluded(p) && hint == api.ProofStatusUnknown, "never unknown without a proof")
		return proofIncluded(p)
	}, 15*time.Second, 5*time.Millisecond, "included after the publish")
}

// #220 and long-poll (T4): a wake raised while the finalized block's proof view is still
// unpublished would send the waiter into the very gap above. The notifier therefore must not
// fire while the hook holds the round, and must fire only once the view is published and the
// pending hint is cleared.
func TestLongPollNotifyNotBeforeDiskProofViewPublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testDiskBFTShardServiceConfig(t)
	cfg.Database.Database = "test_longpoll_notify_disk"
	storage := testutil.SetupTestStorage(t, cfg)
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	rm, err := round.NewRoundManager(ctx, &cfg, log, storage.CommitmentQueue(), storage, nil, state.NewSyncStateTracker(), nil,
		events.NewEventBus(log), smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)), nil)
	require.NoError(t, err)
	svc := NewAggregatorService(&cfg, log, rm, storage.CommitmentQueue(), storage, nil)

	var holdArmed, checkArmed atomic.Bool
	var notifies atomic.Int32
	var notifiedWithoutProof atomic.Int32
	req := testutil.CreateTestCertificationRequest(t, "notify_target").ToAPI()
	rm.SetWaitNotifier(func() {
		notifies.Add(1)
		if !checkArmed.Load() {
			return
		}
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
		if err != nil || !proofIncluded(p) || svc.ProofStatusHint(req.StateID) == api.ProofStatusPending {
			notifiedWithoutProof.Add(1)
		}
	})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	rm.SetTestHookBeforeProofViewPublish(func() {
		if holdArmed.Load() {
			entered <- struct{}{}
			<-release
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = rm.Stop(c)
	})
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	warm := testutil.CreateTestCertificationRequest(t, "notify_warm").ToAPI()
	_, err = svc.CertificationRequest(ctx, warm)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: warm.StateID})
		return err == nil && proofIncluded(p)
	}, 15*time.Second, 25*time.Millisecond)

	holdArmed.Store(true)
	checkArmed.Store(true)
	_, err = svc.CertificationRequest(ctx, req)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the round never reached the publish hook")
	}
	// Rounds (also empty ones) notify when they complete; the held round has not completed, and
	// no other round can run. Count from the moment the hold engaged.
	before := notifies.Load()
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, before, notifies.Load(), "no wake while the finalized block's view is unpublished")

	holdArmed.Store(false)
	close(release)
	require.Eventually(t, func() bool { return notifies.Load() > before }, 15*time.Second, 5*time.Millisecond, "woken after the publish")
	p, err := svc.GetInclusionProofV2(ctx, &api.GetInclusionProofRequestV2{StateID: req.StateID})
	require.NoError(t, err)
	require.True(t, proofIncluded(p))
	require.Zero(t, notifiedWithoutProof.Load())
}
