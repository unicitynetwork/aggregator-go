package bft

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

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
