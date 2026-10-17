package round

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-go-base/types"
)

type proofPubBase struct {
	suite.Suite
	storage *mongodb.Storage
	cfg     *config.Config
	log     *logger.Logger
}

type ProofPublicationSuite struct{ proofPubBase }

func TestProofPublicationSuite(t *testing.T) { suite.Run(t, new(ProofPublicationSuite)) }

func (s *proofPubBase) SetupSuite() {
	conf := config.Config{Database: config.DatabaseConfig{
		Database: "test_proof_publication_suite", ConnectTimeout: 30 * time.Second,
		ServerSelectionTimeout: 5 * time.Second, SocketTimeout: 30 * time.Second,
		MaxPoolSize: 100, MinPoolSize: 5, MaxConnIdleTime: 5 * time.Minute,
	}}
	s.storage = testutil.SetupTestStorage(s.T(), conf)
	s.cfg = &config.Config{Processing: config.ProcessingConfig{BatchLimit: 1000}}
	var err error
	s.log, err = logger.New("error", "text", "stdout", false)
	s.Require().NoError(err)
}

func (s *proofPubBase) newRM() (*RoundManager, *proofview.Catalog) {
	rm, err := NewRoundManager(context.Background(), s.cfg, s.log, s.storage.CommitmentQueue(), s.storage, nil,
		state.NewSyncStateTracker(), nil, events.NewEventBus(s.log),
		smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)), nil)
	s.Require().NoError(err)
	cat, err := newProofViewCatalog(proofview.DefaultLimits(), rm.smtBackend)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = cat.Close() })
	rm.proofPub = proofpub.New(cat, rm.smtBackend, prooftest.Partition, nil, s.log)
	rm.proofPub.SetSealVerifier(prooftest.New(s.T(), 2, 1, prooftest.Hash("x"), 80).Verifier())
	s.Require().NotNil(rm.proofPub)
	return rm, cat
}

// proposal processes commitments into a round and stores the durable proposal;
// the block carries the UC (and view) of a certification for its root.
func (s *proofPubBase) proposal(rm *RoundManager, number int64, prefix string, withView bool) (*models.Block, *prooftest.Fixture) {
	t := s.T()
	ctx := context.Background()
	commitments := testutil.CreateTestCertificationRequests(t, 3, prefix)
	rm.currentRound = &Round{Number: api.NewBigInt(big.NewInt(number)), State: RoundStateProcessing,
		Commitments: commitments, Snapshot: testRMSnapshot(t, ctx, rm)}
	rm.roundMutex.Lock()
	_, err := rm.processMiniBatch(ctx, commitments)
	rm.roundMutex.Unlock()
	require.NoError(t, err)

	root, err := api.NewHexBytesFromString(testSnapshotRootHex(t, ctx, rm.currentRound.Snapshot))
	require.NoError(t, err)
	f := prooftest.New(t, 2, uint64(number), root, 80+uint64(number))
	uc, err := types.Cbor.Marshal(f.UC)
	require.NoError(t, err)
	block := models.NewBlock(api.NewBigInt(big.NewInt(number)), "unicity", 0, "1.0", "mainnet", root, api.HexBytes{}, api.NewHexBytes(uc), 1755000000)
	if withView {
		block.PartitionView = api.NewHexBytes(f.Raw)
	}
	storeDurableProposalForCurrentRound(t, ctx, rm, block)
	return block, f
}

func (s *ProofPublicationSuite) Test1_FinalizeBlockPublishesAndPersistsView() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, f := s.proposal(rm, 11, "pp1", true)

	require.NoError(t, rm.FinalizeBlock(ctx, block))

	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, []byte(block.RootHash), l.Root())
	require.Equal(t, []byte(block.UnicityCertificate), l.UC())
	require.Equal(t, f.Raw, l.PartitionView())
	require.EqualValues(t, 11, l.Block().Number.Uint64())

	stored, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.True(t, stored.Finalized)
	require.Equal(t, api.HexBytes(f.Raw), stored.PartitionView, "view persisted with the block UC")
}

func (s *ProofPublicationSuite) Test2_NothingPublishedWhenFinalizationFails() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, _ := s.proposal(rm, 12, "pp2", true)
	rm.storage = &finalizationFailStorage{Storage: s.storage,
		blockStorage: &finalizationFailBlockStorage{BlockStorage: s.storage.BlockStorage(), finalizeErr: errors.New("mongo down")}}

	require.Error(t, rm.FinalizeBlock(ctx, block))
	_, err := cat.AcquireLatest(ctx)
	require.ErrorIs(t, err, proofview.ErrNoFinalizedView, "an unfinalized root is never published")
	_, err = cat.AcquireRoot(ctx, block.RootHash)
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
}

func (s *ProofPublicationSuite) Test3_PublicationFailureNeverFailsCertification() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, _ := s.proposal(rm, 13, "pp3", true)
	require.NoError(t, cat.Close()) // every publication now fails

	require.NoError(t, rm.FinalizeBlock(ctx, block), "sidecar/view problems must not fail finalization")
	stored, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.True(t, stored.Finalized)
}

func (s *ProofPublicationSuite) Test4_DurableRecoveryPublishesAndLateViewFills() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, f := s.proposal(rm, 14, "pp4", false)
	uc := append(api.HexBytes(nil), block.UnicityCertificate...)
	// the in-memory round is lost: nothing committed locally yet
	rm.currentRound.Snapshot.Discard(ctx)
	rm.currentRound = nil

	recovered, err := rm.FinalizeCertifiedProposal(ctx, block.Index, block.RootHash, uc)
	require.NoError(t, err)
	require.True(t, recovered)

	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte(uc), l.UC())
	require.Nil(t, l.PartitionView())
	l.Release()

	// the exact UC's sidecar arrives later through the client's sink
	digest, ucBytes, err := proofmaterial.DigestOfUC(f.UC)
	require.NoError(t, err)
	require.NoError(t, rm.proofPub.PublishProofMaterial(ctx, &proofmaterial.Material{
		Kind: proofmaterial.KindFinalized, UC: f.UC, UCBytes: ucBytes, UCDigest: digest,
		PartitionView: f.Raw, BlockNumber: 14, HasBlock: true}))
	l, err = cat.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, f.Raw, l.PartitionView())
}

// notifyProbe records what a wake-up observes at the instant it is raised (T4): the retained
// view, the pending hints and the finalization lock.
type notifyProbe struct {
	rm       *RoundManager
	cat      *proofview.Catalog
	ids      []api.StateID
	wantRoot []byte

	calls           int
	publishedAtCall []bool
	hintsCleared    []bool
	lockFree        []bool
}

func (p *notifyProbe) fn() {
	p.calls++
	l, err := p.cat.AcquireLatest(context.Background())
	pub := err == nil && bytes.Equal(l.Root(), p.wantRoot)
	if err == nil {
		l.Release()
	}
	p.publishedAtCall = append(p.publishedAtCall, pub)
	cleared := true
	for _, id := range p.ids {
		if p.rm.ProofStatus(id) == api.ProofStatusPending {
			cleared = false
		}
	}
	p.hintsCleared = append(p.hintsCleared, cleared)
	free := p.rm.finalizationMu.TryLock()
	if free {
		p.rm.finalizationMu.Unlock()
	}
	p.lockFree = append(p.lockFree, free)
}

// Normal finalization notifies once, after the retained publication, the proof-ready update
// and the clearing of the hints, with finalizationMu released; the duplicate finalization of an
// already finalized block notifies again after it republished; a failed finalization still
// notifies (waiters observe the outcome) but publishes nothing.
func (s *ProofPublicationSuite) Test5_NotifyComesAfterEveryServingBoundary() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, _ := s.proposal(rm, 15, "pp5", true)
	var ids []api.StateID
	for _, c := range rm.currentRound.Commitments {
		ids = append(ids, c.StateID)
	}
	probe := &notifyProbe{rm: rm, cat: cat, ids: ids, wantRoot: block.RootHash}
	rm.SetWaitNotifier(probe.fn)

	require.NoError(t, rm.FinalizeBlock(ctx, block))
	require.Equal(t, 1, probe.calls, "exactly one wake for one finalization (no wake before the serving boundary)")
	require.True(t, probe.publishedAtCall[0])
	require.True(t, probe.hintsCleared[0])
	require.True(t, probe.lockFree[0])

	// duplicate finalization of the same finalized block
	stored, err := s.storage.BlockStorage().GetByNumber(ctx, block.Index)
	require.NoError(t, err)
	require.NoError(t, rm.FinalizeBlock(ctx, stored))
	require.Equal(t, 2, probe.calls)
	require.True(t, probe.publishedAtCall[1])
	require.True(t, probe.hintsCleared[1])
	require.True(t, probe.lockFree[1])

	// failed finalization: a wake, but nothing was published for that block
	rm2, cat2 := s.newRM()
	block2, _ := s.proposal(rm2, 16, "pp5b", true)
	rm2.storage = &finalizationFailStorage{Storage: s.storage,
		blockStorage: &finalizationFailBlockStorage{BlockStorage: s.storage.BlockStorage(), finalizeErr: errors.New("mongo down")}}
	probe2 := &notifyProbe{rm: rm2, cat: cat2, wantRoot: block2.RootHash}
	rm2.SetWaitNotifier(probe2.fn)
	require.Error(t, rm2.FinalizeBlock(ctx, block2))
	require.Equal(t, 1, probe2.calls)
	require.False(t, probe2.publishedAtCall[0], "an unfinalized root is never published, so the wake can only lead to an ordinary read")
	require.True(t, probe2.lockFree[0])
}

// Durable-proposal recovery: the wake follows the retained publication and the proof-ready
// update, with finalizationMu released; a late sidecar wakes through the publisher.
func (s *ProofPublicationSuite) Test6_RecoveryAndLateMaterialNotify() {
	t := s.T()
	ctx := context.Background()
	rm, cat := s.newRM()
	block, f := s.proposal(rm, 17, "pp6", false)
	uc := append(api.HexBytes(nil), block.UnicityCertificate...)
	var ids []api.StateID
	for _, c := range rm.currentRound.Commitments {
		ids = append(ids, c.StateID)
	}
	rm.currentRound.Snapshot.Discard(ctx)
	rm.currentRound = nil
	probe := &notifyProbe{rm: rm, cat: cat, ids: ids, wantRoot: block.RootHash}
	rm.SetWaitNotifier(probe.fn)
	rm.proofPub.SetNotify(rm.notifyWaiters)

	recovered, err := rm.FinalizeCertifiedProposal(ctx, block.Index, block.RootHash, uc)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Equal(t, 1, probe.calls)
	require.True(t, probe.publishedAtCall[0])
	require.True(t, probe.hintsCleared[0])
	require.True(t, probe.lockFree[0])

	digest, ucBytes, err := proofmaterial.DigestOfUC(f.UC)
	require.NoError(t, err)
	require.NoError(t, rm.proofPub.PublishProofMaterial(ctx, &proofmaterial.Material{
		Kind: proofmaterial.KindFinalized, UC: f.UC, UCBytes: ucBytes, UCDigest: digest,
		PartitionView: f.Raw, BlockNumber: 17, HasBlock: true}))
	require.Equal(t, 2, probe.calls, "a late partition view wakes the waiters once attached")
}

// The startup test restores the SMT from storage, so it needs a database holding a
// single consistent chain: its own suite instance (own database).
type ProofPublicationStartupSuite struct{ proofPubBase }

func TestProofPublicationStartupSuite(t *testing.T) { suite.Run(t, new(ProofPublicationStartupSuite)) }

func (s *ProofPublicationStartupSuite) Test1_StartupPublishesValidatedCurrentState() {
	t := s.T()
	ctx := context.Background()
	rm, _ := s.newRM()
	block, f := s.proposal(rm, 15, "pp5", true)
	require.NoError(t, rm.FinalizeBlock(ctx, block))
	// FinalizeBlock persisted the SMT nodes for block 15 (and for earlier tests'
	// blocks); a restarted node restores them from storage.
	latest, err := s.storage.BlockStorage().GetLatest(ctx)
	require.NoError(t, err)

	rm2, cat2 := s.newRM()
	_, err = rm2.restoreSmtFromStorage(ctx)
	require.NoError(t, err)
	rm2.publishStartupView(ctx)

	l, err := cat2.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, []byte(latest.RootHash), l.Root())
	if latest.Index.Cmp(block.Index.Int) == 0 {
		require.Equal(t, f.Raw, l.PartitionView(), "restored view is revalidated and published")
	}
	_, err = cat2.AcquireRoot(ctx, []byte{1, 2, 3})
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "history is not rebuilt from storage")
}
