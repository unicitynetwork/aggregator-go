package bft

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type stubRoundManager struct {
	finalizedBlocks        []*models.Block
	finalizeBlockCallCnt   int
	startedRounds          []*api.BigInt
	committedRoot          []byte
	committedBlock         *api.BigInt
	durableRecovered       bool
	durableRecoveryBlock   *api.BigInt
	durableRecoveryRoot    api.HexBytes
	durableRecoveryCert    api.HexBytes
	durableRecoveryCallCnt int
	durableLoadedBlock     *models.Block
	durableLoadFound       bool
	durableLoadBlock       *api.BigInt
	durableLoadCallCnt     int
	durableAbandonBlock    *api.BigInt
	durableAbandonRoot     api.HexBytes
	durableAbandonCallCnt  int
}

func (m *stubRoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
	m.finalizeBlockCallCnt++
	m.finalizedBlocks = append(m.finalizedBlocks, block)
	return nil
}

func (m *stubRoundManager) FinalizeBlockWithRetry(ctx context.Context, block *models.Block) error {
	return m.FinalizeBlock(ctx, block)
}

func (m *stubRoundManager) StartNewRound(ctx context.Context, roundNumber *api.BigInt) error {
	m.startedRounds = append(m.startedRounds, api.NewBigInt(new(big.Int).Set(roundNumber.Int)))
	return nil
}

func (m *stubRoundManager) StartNextRoundFromPrecollector(ctx context.Context, roundNumber *api.BigInt) error {
	return m.StartNewRound(ctx, roundNumber)
}

func (m *stubRoundManager) CommittedRoot(context.Context) ([]byte, *api.BigInt, error) {
	return m.committedRoot, m.committedBlock, nil
}

func (m *stubRoundManager) FinalizeCertifiedProposal(_ context.Context, blockNumber *api.BigInt, rootHash api.HexBytes, unicityCertificate api.HexBytes) (bool, error) {
	m.durableRecoveryCallCnt++
	m.durableRecoveryBlock = api.NewBigInt(new(big.Int).Set(blockNumber.Int))
	m.durableRecoveryRoot = append(api.HexBytes(nil), rootHash...)
	m.durableRecoveryCert = append(api.HexBytes(nil), unicityCertificate...)
	return m.durableRecovered, nil
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
	return nil
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
	require.Same(t, proposal, client.proposedBlock)
	require.True(t, client.resumedDurableProposal)
	require.Equal(t, normal, client.status.Load().(status))
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

	ir, err := client.buildCertificationInputRecord(luc, newRoot, 8)

	require.NoError(t, err)
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
		UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: rootRound},
	}
}
