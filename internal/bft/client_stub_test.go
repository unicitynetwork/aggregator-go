package bft

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmdbft "github.com/unicitynetwork/bft-core/cli/ubft/cmd"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBFTClientStartFailureReturnsToIdleAndCanRetry(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	authSigner, err := abcrypto.NewInMemorySecp256K1Signer()
	require.NoError(t, err)
	authKey, err := authSigner.MarshalPrivateKey()
	require.NoError(t, err)

	client := &BFTClientImpl{
		logger: log,
		conf: &config.BFTConfig{
			Address: "/ip4/127.0.0.1/tcp/0",
			KeyConf: &cmdbft.KeyConf{
				SigKey: cmdbft.Key{
					Algorithm:  cmdbft.KeyAlgorithmSecp256k1,
					PrivateKey: []byte{1},
				},
				AuthKey: cmdbft.Key{
					Algorithm:  cmdbft.KeyAlgorithmSecp256k1,
					PrivateKey: authKey,
				},
			},
		},
	}
	client.status.Store(idle)

	for range 2 {
		err = client.Start(t.Context())
		require.Error(t, err)
		require.Equal(t, idle, client.status.Load().(status))
		require.Nil(t, client.Peer())
	}
}

type stubRoundManager struct {
	finalizedBlocks        []*models.Block
	finalizeBlockCallCnt   int
	startedReferenceTimes  []uint64
	startedRounds          []*api.BigInt
	committedRoot          []byte
	committedBlock         *api.BigInt
	committedRootStarted   chan<- struct{}
	committedRootRelease   <-chan struct{}
	durableRecovered       bool
	durableRecoveryBlock   *api.BigInt
	durableRecoveryRoot    api.HexBytes
	durableRecoveryCert    api.HexBytes
	durableRecoveryCallCnt int
	durableRecoveryErr     error
	durableLoadedBlock     *models.Block
	durableLoadFound       bool
	durableLoadBlock       *api.BigInt
	durableLoadCallCnt     int
	durableAbandonBlock    *api.BigInt
	durableAbandonRoot     api.HexBytes
	durableAbandonCallCnt  int
	durableAbandonErr      error
}

func (m *stubRoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
	m.finalizeBlockCallCnt++
	m.finalizedBlocks = append(m.finalizedBlocks, block)
	return nil
}

func (m *stubRoundManager) FinalizeBlockWithRetry(ctx context.Context, block *models.Block) error {
	return m.FinalizeBlock(ctx, block)
}

func (m *stubRoundManager) StartNewRound(ctx context.Context, roundNumber *api.BigInt, referenceTime uint64) error {
	m.startedRounds = append(m.startedRounds, api.NewBigInt(new(big.Int).Set(roundNumber.Int)))
	m.startedReferenceTimes = append(m.startedReferenceTimes, referenceTime)
	return nil
}

func (m *stubRoundManager) StartNextRoundFromPrecollector(ctx context.Context, roundNumber *api.BigInt, referenceTime uint64) error {
	return m.StartNewRound(ctx, roundNumber, referenceTime)
}

func (m *stubRoundManager) CommittedRoot(context.Context) ([]byte, *api.BigInt, error) {
	if m.committedRootStarted != nil {
		m.committedRootStarted <- struct{}{}
	}
	if m.committedRootRelease != nil {
		<-m.committedRootRelease
	}
	return m.committedRoot, m.committedBlock, nil
}

func (m *stubRoundManager) FinalizeCertifiedProposal(_ context.Context, blockNumber *api.BigInt, rootHash api.HexBytes, unicityCertificate api.HexBytes) (bool, error) {
	m.durableRecoveryCallCnt++
	m.durableRecoveryBlock = api.NewBigInt(new(big.Int).Set(blockNumber.Int))
	m.durableRecoveryRoot = append(api.HexBytes(nil), rootHash...)
	m.durableRecoveryCert = append(api.HexBytes(nil), unicityCertificate...)
	return m.durableRecovered, m.durableRecoveryErr
}

func (m *stubRoundManager) LoadDurableProposal(_ context.Context, blockNumber *api.BigInt) (*models.Block, bool, error) {
	m.durableLoadCallCnt++
	m.durableLoadBlock = api.NewBigInt(new(big.Int).Set(blockNumber.Int))
	return m.durableLoadedBlock, m.durableLoadFound, nil
}

func (m *stubRoundManager) AbandonDurableProposal(_ context.Context, blockNumber *api.BigInt, rootHash api.HexBytes) error {
	m.durableAbandonCallCnt++
	m.durableAbandonBlock = api.NewBigInt(new(big.Int).Set(blockNumber.Int))
	m.durableAbandonRoot = append(api.HexBytes(nil), rootHash...)
	return m.durableAbandonErr
}

func TestBFTClientCertificationRequestDoesNotRewriteBlockNumber(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	client := &BFTClientImpl{logger: log}
	client.status.Store(normal)
	client.nextExpectedRound.Store(55)

	block := models.NewBlock(
		api.NewBigIntFromUint64(52),
		"unicity",
		0,
		"1.0",
		"mainnet",
		api.NewHexBytes(bytes.Repeat([]byte{0x11}, api.SiblingSize)),
		nil,
		nil,
		testSealTimestamp,
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = client.CertificationRequest(ctx, block)

	require.ErrorIs(t, err, ErrStaleCertificationRound)
	require.EqualValues(t, 52, block.Index.Uint64())
}

func TestBFTClientCertificationRequestRejectsLocalRootMismatch(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	localRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	certifiedRoot := bytes.Repeat([]byte{0x20}, api.SiblingSize)
	blockRoot := api.NewHexBytes(bytes.Repeat([]byte{0x30}, api.SiblingSize))
	rm := &stubRoundManager{committedRoot: localRoot}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(normal)
	client.nextExpectedRound.Store(7)
	client.luc.Store(testUnicityCertificate(6, 12, certifiedRoot, nil))

	block := models.NewBlock(
		api.NewBigIntFromUint64(7),
		"unicity",
		0,
		"1.0",
		"mainnet",
		blockRoot,
		nil,
		nil,
		testSealTimestamp,
	)

	err = client.CertificationRequest(t.Context(), block)

	require.ErrorIs(t, err, ErrCertifiedStateMismatch)
	require.Empty(t, rm.finalizedBlocks)
}

func TestBFTClientVerifyLocalRootExtendsLatestUCAllowsMatchingRoot(t *testing.T) {
	root := bytes.Repeat([]byte{0x21}, api.SiblingSize)
	client := &BFTClientImpl{
		roundManager: &stubRoundManager{
			committedRoot:  root,
			committedBlock: api.NewBigIntFromUint64(6),
		},
	}
	luc := testUnicityCertificate(6, 12, root, nil)

	require.NoError(t, client.verifyLocalRootExtendsLatestUC(t.Context(), luc))
}

func TestBFTClientVerifyLocalRootExtendsLatestUCAllowsGenesis(t *testing.T) {
	client := &BFTClientImpl{
		roundManager: &stubRoundManager{
			committedRoot: bytes.Repeat([]byte{0x47}, api.SiblingSize),
		},
	}
	luc := testUnicityCertificate(0, 12, nil, nil)

	require.NoError(t, client.verifyLocalRootExtendsLatestUC(t.Context(), luc))
}

func TestBFTClientCrashAfterProposalBeforeFinalizeWedgesWithoutDurableProposal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	certifiedRoot := bytes.Repeat([]byte{0x20}, api.SiblingSize)
	nextRoot := bytes.Repeat([]byte{0x30}, api.SiblingSize)
	rm := &stubRoundManager{
		committedRoot:  committedRoot,
		committedBlock: api.NewBigIntFromUint64(11),
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(normal)
	client.luc.Store(testUnicityCertificate(11, 20, committedRoot, nil))
	client.lastRootRound.Store(20)
	client.nextExpectedRound.Store(12)

	// Post-crash state: BFT certified round 12, but this node has no in-memory
	// proposal and no durable proposal record to replay.
	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(12, 21, certifiedRoot, committedRoot),
		&certification.TechnicalRecord{Round: 13, Epoch: 1},
	)
	require.NoError(t, err)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 13, rm.startedRounds[0].Uint64())
	require.Empty(t, rm.finalizedBlocks)

	block := models.NewBlock(
		api.NewBigIntFromUint64(13),
		"unicity",
		0,
		"1.0",
		"mainnet",
		api.NewHexBytes(nextRoot),
		nil,
		nil,
		testSealTimestamp,
	)
	err = client.CertificationRequest(t.Context(), block)

	require.ErrorIs(t, err, ErrCertifiedStateMismatch)
	require.Empty(t, rm.finalizedBlocks)
}

func TestBFTClientNoProposedBlockFinalizesDurableProposal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	certifiedRoot := bytes.Repeat([]byte{0x20}, api.SiblingSize)
	rm := &stubRoundManager{
		committedRoot:    committedRoot,
		committedBlock:   api.NewBigIntFromUint64(11),
		durableRecovered: true,
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(normal)
	client.luc.Store(testUnicityCertificate(11, 20, committedRoot, nil))
	client.lastRootRound.Store(20)
	client.nextExpectedRound.Store(12)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(12, 21, certifiedRoot, committedRoot),
		&certification.TechnicalRecord{Round: 13, Epoch: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableRecoveryCallCnt)
	require.EqualValues(t, 12, rm.durableRecoveryBlock.Uint64())
	require.Equal(t, api.HexBytes(certifiedRoot), rm.durableRecoveryRoot)
	require.NotEmpty(t, rm.durableRecoveryCert)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 13, rm.startedRounds[0].Uint64())
	require.Empty(t, rm.finalizedBlocks)
}

func TestBFTClientResumedDurableProposalFinalizesWithoutActiveRound(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	proposalRoot := api.NewHexBytes(bytes.Repeat([]byte{0x20}, api.SiblingSize))
	proposal := models.NewBlock(
		api.NewBigIntFromUint64(12),
		"unicity",
		0,
		"1.0",
		"mainnet",
		proposalRoot,
		api.NewHexBytes(committedRoot),
		nil,
		testSealTimestamp,
	)
	proposal.Status = models.FinalityStatusProposed
	rm := &stubRoundManager{
		committedRoot:    committedRoot,
		committedBlock:   api.NewBigIntFromUint64(11),
		durableRecovered: true,
	}
	client := &BFTClientImpl{
		logger:                 log,
		roundManager:           rm,
		proposedBlock:          proposal,
		resumedDurableProposal: true,
	}
	client.status.Store(normal)
	client.luc.Store(testUnicityCertificate(11, 20, committedRoot, nil))
	client.lastRootRound.Store(20)
	client.nextExpectedRound.Store(12)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(12, 21, proposalRoot, committedRoot),
		&certification.TechnicalRecord{Round: 13, Epoch: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableRecoveryCallCnt)
	require.EqualValues(t, 12, rm.durableRecoveryBlock.Uint64())
	require.Equal(t, proposalRoot, rm.durableRecoveryRoot)
	require.NotEmpty(t, rm.durableRecoveryCert)
	require.Zero(t, rm.finalizeBlockCallCnt)
	require.Empty(t, rm.finalizedBlocks)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 13, rm.startedRounds[0].Uint64())
	require.Nil(t, client.proposedBlock)
	require.False(t, client.resumedDurableProposal)
}

func TestBFTClientInitializationResendsDurableProposal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	proposalRoot := api.NewHexBytes(bytes.Repeat([]byte{0x20}, api.SiblingSize))
	proposal := models.NewBlock(
		api.NewBigIntFromUint64(12),
		"unicity",
		0,
		"1.0",
		"mainnet",
		proposalRoot,
		api.NewHexBytes(committedRoot),
		nil,
		testSealTimestamp,
	)
	proposal.Status = models.FinalityStatusProposed
	rm := &stubRoundManager{
		committedRoot:       committedRoot,
		committedBlock:      api.NewBigIntFromUint64(11),
		durableLoadedBlock:  proposal,
		durableLoadFound:    true,
		durableRecoveryRoot: nil,
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(11, 21, committedRoot, nil),
		&certification.TechnicalRecord{Round: 12, Epoch: 1},
	)

	require.ErrorContains(t, err, "BFT client network is not initialized")
	require.Equal(t, 1, rm.durableLoadCallCnt)
	require.EqualValues(t, 12, rm.durableLoadBlock.Uint64())
	require.Empty(t, rm.startedRounds)
	require.Nil(t, client.proposedBlock)
	require.False(t, client.resumedDurableProposal)
	require.Equal(t, initializing, client.status.Load().(status))
	require.Nil(t, client.luc.Load())
	require.Zero(t, client.nextExpectedRound.Load())
}

func TestBFTClientInitializationFinalizesCertifiedDurableProposalBeforeStartingNextRound(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	certifiedRoot := bytes.Repeat([]byte{0x20}, api.SiblingSize)
	rm := &stubRoundManager{
		committedRoot:    committedRoot,
		committedBlock:   api.NewBigIntFromUint64(120),
		durableRecovered: true,
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(121, 90, certifiedRoot, committedRoot),
		&certification.TechnicalRecord{Round: 123, Epoch: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableRecoveryCallCnt)
	require.EqualValues(t, 121, rm.durableRecoveryBlock.Uint64())
	require.Equal(t, api.HexBytes(certifiedRoot), rm.durableRecoveryRoot)
	require.NotEmpty(t, rm.durableRecoveryCert)
	require.Zero(t, rm.durableLoadCallCnt)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 123, rm.startedRounds[0].Uint64())
	require.Nil(t, client.proposedBlock)
	require.Equal(t, normal, client.status.Load().(status))
}

func TestBFTClientStopClearsSessionProposalState(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	client := &BFTClientImpl{
		logger:                 log,
		proposedBlock:          &models.Block{},
		resumedDurableProposal: true,
	}
	client.status.Store(normal)
	client.certRequestTime.Store(time.Now().UnixNano())

	client.Stop()

	require.Equal(t, idle, client.status.Load().(status))
	require.Nil(t, client.proposedBlock)
	require.False(t, client.resumedDurableProposal)
	require.Zero(t, client.certRequestTime.Load())
}

func TestBFTClientStopWaitsForEventLoopExit(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	loopCanceled := make(chan struct{})
	loopDone := make(chan struct{})
	client := &BFTClientImpl{
		logger:      log,
		msgLoopDone: loopDone,
		msgLoopCancelFn: func() {
			close(loopCanceled)
		},
	}
	client.status.Store(normal)

	stopDone := make(chan struct{})
	go func() {
		client.Stop()
		close(stopDone)
	}()

	select {
	case <-loopCanceled:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the event loop")
	}
	select {
	case <-stopDone:
		t.Fatal("stop returned before the event loop exited")
	default:
	}
	require.Equal(t, stopping, client.status.Load().(status))
	require.ErrorIs(t, client.Start(t.Context()), ErrBFTClientStopping)

	secondStopDone := make(chan struct{})
	go func() {
		client.Stop()
		close(secondStopDone)
	}()
	select {
	case <-secondStopDone:
		t.Fatal("concurrent stop returned before the active stop completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(loopDone)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after the event loop exited")
	}
	select {
	case <-secondStopDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent stop did not join the active stop")
	}
	require.Equal(t, idle, client.status.Load().(status))
}

func TestBFTClientStopCancelsBeforeWaitingForUCProcessing(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	client := &BFTClientImpl{
		logger:          log,
		msgLoopCancelFn: cancelLoop,
	}
	client.status.Store(normal)

	processingStarted := make(chan struct{})
	processingDone := make(chan struct{})
	go func() {
		client.ucProcessingMutex.Lock()
		close(processingStarted)
		<-loopCtx.Done()
		client.ucProcessingMutex.Unlock()
		close(processingDone)
	}()
	<-processingStarted

	stopDone := make(chan struct{})
	go func() {
		client.Stop()
		close(stopDone)
	}()

	select {
	case <-processingDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel UC processing before waiting for it")
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after UC processing exited")
	}
	require.Equal(t, idle, client.status.Load().(status))
}

func TestBFTClientInitializationCannotOverwriteStopping(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	root := bytes.Repeat([]byte{0x31}, api.SiblingSize)
	committedRootStarted := make(chan struct{}, 1)
	committedRootRelease := make(chan struct{})
	rm := &stubRoundManager{
		committedRoot:        root,
		committedBlock:       api.NewBigIntFromUint64(11),
		committedRootStarted: committedRootStarted,
		committedRootRelease: committedRootRelease,
	}
	loopCanceled := make(chan struct{})
	client := &BFTClientImpl{
		logger:          log,
		roundManager:    rm,
		msgLoopCancelFn: func() { close(loopCanceled) },
	}
	client.status.Store(initializing)

	handlerDone := make(chan error, 1)
	go func() {
		handlerDone <- client.handleUnicityCertificate(
			context.Background(),
			testUnicityCertificate(12, 21, root, root),
			&certification.TechnicalRecord{Round: 13, Epoch: 1},
		)
	}()
	<-committedRootStarted

	stopDone := make(chan struct{})
	go func() {
		client.Stop()
		close(stopDone)
	}()
	<-loopCanceled

	// Hold mu after Stop publishes stopping, keeping Stop in cleanup while the
	// initialization handler completes and attempts its status transition.
	client.mu.Lock()
	close(committedRootRelease)
	require.NoError(t, <-handlerDone)
	require.Equal(t, stopping, client.status.Load().(status))
	client.mu.Unlock()

	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not complete after initialization handler exited")
	}
	require.Equal(t, idle, client.status.Load().(status))
}

func TestBFTClientInitializationFailureRemainsRetryable(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	certifiedRoot := bytes.Repeat([]byte{0x20}, api.SiblingSize)
	rm := &stubRoundManager{
		committedRoot:      committedRoot,
		committedBlock:     api.NewBigIntFromUint64(120),
		durableRecoveryErr: errors.New("temporary recovery failure"),
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)
	uc := testUnicityCertificate(121, 90, certifiedRoot, committedRoot)
	tr := &certification.TechnicalRecord{Round: 123, Epoch: 1}

	err = client.handleUnicityCertificate(t.Context(), uc, tr)
	require.ErrorContains(t, err, "temporary recovery failure")
	require.Equal(t, initializing, client.status.Load().(status))
	require.Nil(t, client.luc.Load())
	require.Zero(t, client.nextExpectedRound.Load())

	rm.durableRecoveryErr = nil
	rm.durableRecovered = true
	require.NoError(t, client.handleUnicityCertificate(t.Context(), uc, tr))
	require.Equal(t, normal, client.status.Load().(status))
	require.Equal(t, 2, rm.durableRecoveryCallCnt)
	require.Len(t, rm.startedRounds, 1)
}

func TestBFTClientRetainedDuplicateUCCompletesInitialization(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	root := bytes.Repeat([]byte{0x31}, api.SiblingSize)
	retainedUC := testUnicityCertificate(10, 20, root, nil)
	rm := &stubRoundManager{
		committedRoot:  root,
		committedBlock: api.NewBigIntFromUint64(10),
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)
	client.luc.Store(retainedUC)
	client.lastRootRound.Store(retainedUC.GetRootRoundNumber())

	require.NoError(t, client.handleUnicityCertificate(
		t.Context(),
		retainedUC,
		&certification.TechnicalRecord{Round: 11, Epoch: 1},
	))

	require.Equal(t, normal, client.status.Load().(status))
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 11, rm.startedRounds[0].Uint64())
}

func TestBFTClientRepeatUCCompletesInitialization(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	root := bytes.Repeat([]byte{0x32}, api.SiblingSize)
	previousUC := testUnicityCertificate(10, 20, root, nil)
	repeatUC := testUnicityCertificate(10, 21, root, previousUC.InputRecord.PreviousHash)
	rm := &stubRoundManager{}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)
	client.luc.Store(previousUC)
	client.lastRootRound.Store(previousUC.GetRootRoundNumber())

	require.NoError(t, client.handleUnicityCertificate(
		t.Context(),
		repeatUC,
		&certification.TechnicalRecord{Round: 12, Epoch: 1},
	))

	require.Equal(t, normal, client.status.Load().(status))
	require.Same(t, repeatUC, client.luc.Load())
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 12, rm.startedRounds[0].Uint64())
}

func TestBFTClientCanceledFinalizationDoesNotPublishFatal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)
	eventBus := events.NewEventBus(log)
	fatalEvents := eventBus.Subscribe(events.TopicFatalError)
	client := &BFTClientImpl{logger: log, eventBus: eventBus}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client.publishFatalError(ctx, context.Canceled)

	select {
	case event := <-fatalEvents:
		t.Fatalf("unexpected fatal event during intentional cancellation: %#v", event)
	default:
	}
}

func TestBFTClientCancellationDoesNotHideUnrelatedFatalError(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)
	eventBus := events.NewEventBus(log)
	fatalEvents := eventBus.Subscribe(events.TopicFatalError)
	client := &BFTClientImpl{logger: log, eventBus: eventBus}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	corruptionErr := errors.New("certified root mismatch")
	client.publishFatalError(ctx, corruptionErr)

	select {
	case event := <-fatalEvents:
		fatal, ok := event.(events.FatalErrorEvent)
		require.True(t, ok)
		require.Equal(t, corruptionErr.Error(), fatal.Error)
	default:
		t.Fatal("expected unrelated fatal error to remain visible after cancellation")
	}
}

func TestBFTClientCanceledSessionDoesNotProcessBufferedUC(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	previousUC := testUnicityCertificate(10, 20, bytes.Repeat([]byte{0x10}, api.SiblingSize), nil)
	client := &BFTClientImpl{
		logger:       log,
		roundManager: &stubRoundManager{},
	}
	client.luc.Store(previousUC)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.handleUnicityCertificate(
		ctx,
		testUnicityCertificate(11, 21, bytes.Repeat([]byte{0x11}, api.SiblingSize), previousUC.InputRecord.Hash),
		&certification.TechnicalRecord{Round: 12, Epoch: 1},
	)

	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, previousUC, client.luc.Load())
}

func TestBFTClientRetainedUCDoesNotBypassInitialization(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	client := &BFTClientImpl{logger: log}
	client.status.Store(initializing)
	client.luc.Store(testUnicityCertificate(10, 20, bytes.Repeat([]byte{0x10}, api.SiblingSize), nil))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = client.ensureInitialized(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBFTClientInitializationStartsRoundWhenNoDurableProposal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	committedRoot := bytes.Repeat([]byte{0x10}, api.SiblingSize)
	rm := &stubRoundManager{
		committedRoot:  committedRoot,
		committedBlock: api.NewBigIntFromUint64(11),
	}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.status.Store(initializing)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(11, 21, committedRoot, nil),
		&certification.TechnicalRecord{Round: 12, Epoch: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableLoadCallCnt)
	require.EqualValues(t, 12, rm.durableLoadBlock.Uint64())
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 12, rm.startedRounds[0].Uint64())
	require.Nil(t, client.proposedBlock)
	require.Equal(t, normal, client.status.Load().(status))
}

func TestBFTClientRejectsUCRootMismatch(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	rm := &stubRoundManager{}
	blockRoot := api.NewHexBytes(bytes.Repeat([]byte{0x11}, api.SiblingSize))
	ucRoot := bytes.Repeat([]byte{0x22}, api.SiblingSize)
	block := models.NewBlock(
		api.NewBigIntFromUint64(7),
		"unicity",
		0,
		"1.0",
		"mainnet",
		blockRoot,
		nil,
		nil,
		testSealTimestamp,
	)
	client := &BFTClientImpl{
		logger:        log,
		roundManager:  rm,
		proposedBlock: block,
	}

	err = client.handleUnicityCertificate(
		t.Context(),
		&types.UnicityCertificate{
			InputRecord: &types.InputRecord{
				Version:     1,
				RoundNumber: 7,
				Hash:        ucRoot,
			},
			UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: 7},
		},
		&certification.TechnicalRecord{Round: 8},
	)

	require.ErrorIs(t, err, ErrCertifiedRootMismatch)
	require.Empty(t, rm.finalizedBlocks)
	require.Nil(t, client.proposedBlock)
	require.Empty(t, block.UnicityCertificate)
	// The durable proposal must be abandoned too (not just cleared in memory),
	// so a stale Proposed block cannot survive restart and re-trigger the fatal.
	require.Equal(t, 1, rm.durableAbandonCallCnt)
	require.True(t, bytes.Equal(blockRoot, rm.durableAbandonRoot))
}

func TestBFTClientUCRootMismatchPublishesFatalWhenAbandonFails(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	abandonErr := errors.New("abandon failed")
	rm := &stubRoundManager{durableAbandonErr: abandonErr}
	eventBus := events.NewEventBus(log)
	fatalEvents := eventBus.Subscribe(events.TopicFatalError)
	blockRoot := api.NewHexBytes(bytes.Repeat([]byte{0x11}, api.SiblingSize))
	ucRoot := bytes.Repeat([]byte{0x22}, api.SiblingSize)
	block := models.NewBlock(
		api.NewBigIntFromUint64(7),
		"unicity",
		0,
		"1.0",
		"mainnet",
		blockRoot,
		nil,
		nil,
		testSealTimestamp,
	)
	client := &BFTClientImpl{
		logger:        log,
		roundManager:  rm,
		eventBus:      eventBus,
		proposedBlock: block,
	}

	err = client.handleUnicityCertificate(
		t.Context(),
		&types.UnicityCertificate{
			InputRecord: &types.InputRecord{
				Version:     1,
				RoundNumber: 7,
				Hash:        ucRoot,
			},
			UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: 7},
		},
		&certification.TechnicalRecord{Round: 8},
	)

	require.ErrorIs(t, err, ErrCertifiedRootMismatch)
	require.Equal(t, 1, rm.durableAbandonCallCnt)
	select {
	case event := <-fatalEvents:
		fatal, ok := event.(events.FatalErrorEvent)
		require.True(t, ok)
		require.Equal(t, "bft", fatal.Source)
		require.Contains(t, fatal.Error, ErrCertifiedRootMismatch.Error())
	default:
		t.Fatal("expected fatal event for certified root mismatch")
	}
}

func TestBFTClientRejectsRegressingUC(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	rm := &stubRoundManager{}
	prevUC := testUnicityCertificate(10, 20, bytes.Repeat([]byte{0x10}, api.SiblingSize), nil)
	oldUC := testUnicityCertificate(9, 19, bytes.Repeat([]byte{0x09}, api.SiblingSize), nil)
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.luc.Store(prevUC)
	client.lastRootRound.Store(prevUC.GetRootRoundNumber())
	client.nextExpectedRound.Store(11)

	err = client.handleUnicityCertificate(t.Context(), oldUC, &certification.TechnicalRecord{Round: 10, Epoch: 1})

	require.ErrorIs(t, err, ErrInvalidUCSequence)
	require.Same(t, prevUC, client.luc.Load())
	require.EqualValues(t, 11, client.nextExpectedRound.Load())
	require.Empty(t, rm.startedRounds)
	require.Empty(t, rm.finalizedBlocks)
}

func TestBFTClientRepeatUCStartsFreshRound(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	root := bytes.Repeat([]byte{0x44}, api.SiblingSize)
	prevUC := testUnicityCertificate(52, 80, root, nil)
	repeatUC := testUnicityCertificate(52, 81, root, prevUC.InputRecord.PreviousHash)
	rm := &stubRoundManager{}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
	}
	client.luc.Store(prevUC)
	client.lastRootRound.Store(prevUC.GetRootRoundNumber())
	client.nextExpectedRound.Store(53)
	proposedRoot := api.NewHexBytes(bytes.Repeat([]byte{0x55}, api.SiblingSize))
	client.proposedBlock = models.NewBlock(
		api.NewBigIntFromUint64(53),
		"unicity",
		0,
		"1.0",
		"mainnet",
		proposedRoot,
		api.NewHexBytes(root),
		nil,
		testSealTimestamp,
	)

	err = client.handleUnicityCertificate(t.Context(), repeatUC, &certification.TechnicalRecord{Round: 55, Epoch: 1})

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableAbandonCallCnt)
	require.EqualValues(t, 53, rm.durableAbandonBlock.Uint64())
	require.Equal(t, proposedRoot, rm.durableAbandonRoot)
	require.Nil(t, client.proposedBlock)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 55, rm.startedRounds[0].Uint64())
	require.Empty(t, rm.finalizedBlocks)
	require.Same(t, repeatUC, client.luc.Load())
	require.EqualValues(t, 55, client.nextExpectedRound.Load())
}

func TestBFTClientNewerUCAbandonsStaleProposalAndRecoversDurableProposal(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	staleRoot := api.NewHexBytes(bytes.Repeat([]byte{0x33}, api.SiblingSize))
	certifiedRoot := bytes.Repeat([]byte{0x44}, api.SiblingSize)
	rm := &stubRoundManager{durableRecovered: true}
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
		proposedBlock: models.NewBlock(
			api.NewBigIntFromUint64(12),
			"unicity",
			0,
			"1.0",
			"mainnet",
			staleRoot,
			nil,
			nil,
			1755000000,
		),
	}
	client.status.Store(normal)
	client.nextExpectedRound.Store(12)

	err = client.handleUnicityCertificate(
		t.Context(),
		testUnicityCertificate(13, 25, certifiedRoot, nil),
		&certification.TechnicalRecord{Round: 14, Epoch: 1},
	)

	require.NoError(t, err)
	require.Equal(t, 1, rm.durableAbandonCallCnt)
	require.EqualValues(t, 12, rm.durableAbandonBlock.Uint64())
	require.Equal(t, staleRoot, rm.durableAbandonRoot)
	require.Equal(t, 1, rm.durableRecoveryCallCnt)
	require.EqualValues(t, 13, rm.durableRecoveryBlock.Uint64())
	require.Equal(t, api.HexBytes(certifiedRoot), rm.durableRecoveryRoot)
	require.Len(t, rm.startedRounds, 1)
	require.EqualValues(t, 14, rm.startedRounds[0].Uint64())
	require.Nil(t, client.proposedBlock)
}

func TestBFTClientCertificationInputRecordUsesTechnicalEpoch(t *testing.T) {
	client := &BFTClientImpl{}
	client.nextExpectedEpoch.Store(3)

	previousRoot := bytes.Repeat([]byte{0x11}, api.SiblingSize)
	newRoot := bytes.Repeat([]byte{0x22}, api.SiblingSize)
	luc := testUnicityCertificate(7, 12, previousRoot, nil)
	luc.InputRecord.Epoch = 2

	ir, err := client.buildCertificationInputRecord(luc, newRoot, 8, luc.UnicitySeal.Timestamp)

	require.NoError(t, err)
	require.EqualValues(t, luc.UnicitySeal.Timestamp, ir.Timestamp)
	require.EqualValues(t, 8, ir.RoundNumber)
	require.EqualValues(t, 3, ir.Epoch)
	require.Equal(t, previousRoot, []byte(ir.PreviousHash))
	require.Equal(t, newRoot, []byte(ir.Hash))
	require.Equal(t, newRoot, []byte(ir.BlockHash))
}

func TestBFTClientStub_CertificationRequest_PopulatesSyntheticUC(t *testing.T) {
	rm := &stubRoundManager{}
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	client := NewBFTClientStub(log, rm, api.NewBigIntFromUint64(1), 0)
	block := models.NewBlock(
		api.NewBigIntFromUint64(7),
		"unicity",
		0,
		"1.0",
		"mainnet",
		api.HexBytes("0123"),
		nil,
		nil,
		testSealTimestamp,
	)

	err = client.CertificationRequest(t.Context(), block)
	require.NoError(t, err)
	require.Len(t, rm.finalizedBlocks, 1)
	require.NotEmpty(t, block.UnicityCertificate)

	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(block.UnicityCertificate, &uc))
	require.EqualValues(t, 7, uc.GetRoundNumber())
	require.EqualValues(t, 7, uc.GetRootRoundNumber())
}

// testSealTimestamp is the seal timestamp every fixture certificate carries;
// a round proposing under it pins the same value as its reference time.
const testSealTimestamp uint64 = 1755000000

func testUnicityCertificate(round, rootRound uint64, root []byte, previous []byte) *types.UnicityCertificate {
	if previous == nil {
		previous = bytes.Repeat([]byte{0x00}, api.SiblingSize)
	}
	return &types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			Version:      1,
			RoundNumber:  round,
			Epoch:        1,
			PreviousHash: previous,
			Hash:         root,
			BlockHash:    root,
		},
		UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: rootRound, Timestamp: testSealTimestamp},
	}
}
