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
	"github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type stubRoundManager struct {
	finalizedBlocks []*models.Block
	startedRounds   []*api.BigInt
}

func (m *stubRoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
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

func TestBFTClientRejectsUCRootMismatch(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	rm := &stubRoundManager{}
	bus := events.NewEventBus(log)
	fatalEvents := bus.Subscribe(events.TopicFatalError)
	client := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
		eventBus:     bus,
	}
	client.status.Store(normal)

	proposedRoot := api.NewHexBytes(bytes.Repeat([]byte{0x11}, api.SiblingSize))
	certifiedRoot := api.NewHexBytes(bytes.Repeat([]byte{0x22}, api.SiblingSize))
	block := models.NewBlock(
		api.NewBigIntFromUint64(12),
		"unicity",
		0,
		"1.0",
		"mainnet",
		proposedRoot,
		nil,
		nil,
	)
	client.proposedBlock = block

	uc := &types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			RoundNumber: 12,
			Hash:        hex.Bytes(certifiedRoot),
		},
		UnicitySeal: &types.UnicitySeal{
			RootChainRoundNumber: 12,
		},
	}
	err = client.handleUnicityCertificate(context.Background(), uc, &certification.TechnicalRecord{Round: 13})

	require.ErrorIs(t, err, ErrCertifiedStateMismatch)
	require.Empty(t, rm.finalizedBlocks)
	require.Nil(t, client.proposedBlock)
	select {
	case event := <-fatalEvents:
		fatal, ok := event.(events.FatalErrorEvent)
		require.True(t, ok)
		require.Equal(t, "bft", fatal.Source)
		require.Contains(t, fatal.Error, ErrCertifiedStateMismatch.Error())
	case <-time.After(time.Second):
		t.Fatal("expected fatal event")
	}
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
