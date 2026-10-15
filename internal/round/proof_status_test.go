package round

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Database-free round manager for the proof status tests.
func newStatusRM(t *testing.T) *RoundManager {
	b := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	rm := &RoundManager{
		logger:        newTestLogger(t),
		smtBackend:    b,
		proofPending:  map[string]struct{}{},
		statusTracker: newStatusTracker(),
	}
	rm.referenceTime.Store(1000)
	rm.statusTracker.SetReferenceTime(rm.CurrentReferenceTime)
	return rm
}

func (rm *RoundManager) startStatusRound(t *testing.T, number int64, ref uint64) {
	snap, err := rm.smtBackend.CreateSnapshot(context.Background())
	require.NoError(t, err)
	rm.currentRound = &Round{Number: api.NewBigInt(big.NewInt(number)), ReferenceTime: ref, Snapshot: snap}
}

func admit(rm *RoundManager, c *models.CertificationRequest, deadline uint64) bool {
	c.EffectiveTimeout = deadline
	return rm.MarkRequestAdmitted(c.StateID, deadline)
}

func process(t *testing.T, rm *RoundManager, cs ...*models.CertificationRequest) {
	rm.roundMutex.Lock()
	defer rm.roundMutex.Unlock()
	_, err := rm.processMiniBatch(context.Background(), cs)
	require.NoError(t, err)
}

func TestProofStatus_AdmissionPendingThroughRoundThenCleared(t *testing.T) {
	rm := newStatusRM(t)
	c := testutil.CreateTestCertificationRequest(t, "status_pending")

	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID))
	require.True(t, admit(rm, c, 5000), "newly marked at admission")
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(c.StateID), "queued, not yet in any round")

	rm.startStatusRound(t, 1, 1000)
	process(t, rm, c)
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(c.StateID), "in the current round, not finalized")

	rm.markProofsReady(&models.Block{Index: api.NewBigInt(big.NewInt(1))}, []api.StateID{c.StateID}, nil)
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID), "finalized: the proof answers, the mark is gone")
	require.Zero(t, countPending(rm))
}

func countPending(rm *RoundManager) int { p, _ := rm.statusTracker.Counts(); return p }

// A requests replayed from the shared queue after a restart was never admitted on this node;
// pulling it into a round still marks it pending.
func TestProofStatus_QueueReplayAfterRestartIsPendingOnceInARound(t *testing.T) {
	rm := newStatusRM(t)
	c := testutil.CreateTestCertificationRequest(t, "status_replay")
	c.EffectiveTimeout = 5000
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID), "restart: memory is empty")
	rm.startStatusRound(t, 1, 1000)
	process(t, rm, c)
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(c.StateID))
}

func TestProofStatus_ExpiredDropIsRememberedAndReadmissionIsPendingAgain(t *testing.T) {
	rm := newStatusRM(t)
	c := testutil.CreateTestCertificationRequest(t, "status_expired")
	admit(rm, c, 1000) // deadline == the round's pinned reference time: expired (exclusive)

	rm.startStatusRound(t, 1, 1000)
	process(t, rm, c)
	require.Equal(t, api.ProofStatusExpired, rm.ProofStatus(c.StateID))
	require.Zero(t, countPending(rm))
	require.Empty(t, rm.currentRound.PendingCommitments, "the expired request never became a leaf")

	// the requester signs a fresh deadline and resubmits: pending again
	admit(rm, c, 9000)
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(c.StateID))
}

// Resubmitting the same signed request while it is still pending is harmless: one mark, the
// second copy is dropped as a duplicate of the first, never a second leaf, and the drop does
// not clear the original's pending mark.
func TestProofStatus_ResubmittingAPendingRequestIsHarmless(t *testing.T) {
	rm := newStatusRM(t)
	c := testutil.CreateTestCertificationRequest(t, "status_resubmit")
	dup := *c // the same signed request arriving again (other queue entry)

	require.True(t, admit(rm, c, 5000))
	require.False(t, admit(rm, &dup, 5000), "idempotent: not newly marked")
	require.Equal(t, 1, countPending(rm))

	rm.startStatusRound(t, 1, 1000)
	rm.roundMutex.Lock()
	dropped, err := rm.processMiniBatch(context.Background(), []*models.CertificationRequest{c, &dup})
	rm.roundMutex.Unlock()
	require.NoError(t, err)
	require.Len(t, dropped, 1, "the second copy is deduplicated and acked")
	require.Len(t, rm.currentRound.PendingLeaves, 1, "never a second leaf")
	require.Len(t, rm.currentRound.PendingCommitments, 1)
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(c.StateID), "the duplicate's drop did not clear the original's mark")

	// finalize: one clear, the state is no longer pending
	require.NoError(t, rm.currentRound.Snapshot.Commit(context.Background(), smtbackend.CommitMetadata{BlockNumber: api.NewBigInt(big.NewInt(1))}))
	rm.markProofsReady(&models.Block{Index: api.NewBigInt(big.NewInt(1))}, []api.StateID{c.StateID}, nil)
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID))

	// resubmitted after it is already certified (duplicate check disabled): the leaf exists, the
	// copy is dropped as a duplicate, the tree is unchanged
	rootBefore, err := rm.smtBackend.RootHashRaw(context.Background())
	require.NoError(t, err)
	late := *c
	admit(rm, &late, 7000)
	rm.startStatusRound(t, 2, 1001)
	rm.roundMutex.Lock()
	dropped, err = rm.processMiniBatch(context.Background(), []*models.CertificationRequest{&late})
	rm.roundMutex.Unlock()
	require.NoError(t, err)
	require.Len(t, dropped, 1)
	require.Empty(t, rm.currentRound.PendingLeaves)
	require.Zero(t, countPending(rm), "a copy of an already certified leaf leaves no admission mark behind")
	rootAfter, err := rm.currentRound.Snapshot.RootHashRaw(context.Background())
	require.NoError(t, err)
	require.Equal(t, rootBefore, rootAfter)
}

func TestProofStatus_DeactivationForgetsPendingButNotExpired(t *testing.T) {
	rm := newStatusRM(t)
	a := testutil.CreateTestCertificationRequest(t, "status_a")
	b := testutil.CreateTestCertificationRequest(t, "status_b")
	admit(rm, a, 5000)
	rm.statusTracker.MarkExpired(b.StateID)
	rm.clearProofPending()
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(a.StateID))
	require.Equal(t, api.ProofStatusExpired, rm.ProofStatus(b.StateID))
}

func TestProofStatus_ConcurrentAdmissionAndFinalization(t *testing.T) {
	rm := newStatusRM(t)
	cs := testutil.CreateTestCertificationRequests(t, 50, "status_conc")
	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Add(1)
		go func(c *models.CertificationRequest) {
			defer wg.Done()
			admit(rm, c, 5000)
			_ = rm.ProofStatus(c.StateID)
			rm.markProofsReady(&models.Block{Index: api.NewBigInt(big.NewInt(1))}, []api.StateID{c.StateID}, nil)
		}(c)
	}
	wg.Wait()
	require.Zero(t, countPending(rm))
}

// Review major 2: a request which a round accepted is never called expired just because the
// certified round's UC moved the reference time past its deadline before finalization cleared it.
func TestProofStatus_ReferenceTimeAdvancingDuringCertificationDoesNotExpireAcceptedLeaf(t *testing.T) {
	rm := newStatusRM(t)
	accepted := testutil.CreateTestCertificationRequest(t, "status_accepted")
	queued := testutil.CreateTestCertificationRequest(t, "status_queued")
	admit(rm, accepted, 1500) // deadline D = 1500, round pins 1000 < D: accepted
	admit(rm, queued, 1500)   // same deadline, but never pulled into a round

	rm.startStatusRound(t, 1, 1000)
	process(t, rm, accepted)
	require.Len(t, rm.currentRound.PendingLeaves, 1)

	rm.referenceTime.Store(1600) // the UC of that round (timestamp >= D) arrives; finalization is not done yet

	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(accepted.StateID), "being certified: pending")
	// admission/queries sweep: still not expired
	other := testutil.CreateTestCertificationRequest(t, "status_other")
	admit(rm, other, 9000)
	require.Equal(t, api.ProofStatusPending, rm.ProofStatus(accepted.StateID))
	require.Equal(t, api.ProofStatusExpired, rm.ProofStatus(queued.StateID), "queued past its deadline: the next round must drop it")

	rm.markProofsReady(&models.Block{Index: api.NewBigInt(big.NewInt(1))}, []api.StateID{accepted.StateID}, nil)
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(accepted.StateID), "certified: the proof answers; never expired")
	_, exp := rm.statusTracker.Counts()
	require.Equal(t, 1, exp, "only the genuinely dropped request is remembered as expired")
}

// A finalization also forgets an earlier expired record of the same state.
func TestProofStatus_FinalizationForgetsExpiredRecord(t *testing.T) {
	rm := newStatusRM(t)
	c := testutil.CreateTestCertificationRequest(t, "status_forget")
	rm.statusTracker.MarkExpired(c.StateID)
	rm.markProofsReady(&models.Block{Index: api.NewBigInt(big.NewInt(1))}, []api.StateID{c.StateID}, nil)
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID))
	rm.statusTracker.MarkExpired(c.StateID)
	rm.clearProofStateIDsPending([]api.StateID{c.StateID}) // recovered finalization
	require.Equal(t, api.ProofStatusUnknown, rm.ProofStatus(c.StateID))
}
