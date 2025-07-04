package bft

import (
	"context"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type (
	BFTClientStub struct {
		logger       *logger.Logger
		roundManager RoundManager
	}
)

func NewBFTClientStub(logger *logger.Logger, roundManager RoundManager) *BFTClientStub {
	logger.Info("Using BFT Client Stub")
	return &BFTClientStub{
		logger:       logger,
		roundManager: roundManager,
	}
}

func (n *BFTClientStub) Start(ctx context.Context, nextRoundNumber *api.BigInt) error {
	return n.roundManager.StartNewRound(ctx, nextRoundNumber)
}

func (n *BFTClientStub) CertificationRequest(ctx context.Context, block *models.Block) error {
	if err := n.roundManager.FinalizeBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to finalize block: %w", err)
	}
	nextRoundNumber := api.NewBigInt(nil)
	nextRoundNumber.Set(block.Index.Int)
	nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
	return n.roundManager.StartNewRound(ctx, nextRoundNumber)
}
