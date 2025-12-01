package bft

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type BFTClientStub struct {
	logger          *logger.Logger
	roundManager    RoundManager
	nextRoundNumber *api.BigInt
	delay           time.Duration
}

func NewBFTClientStub(logger *logger.Logger, roundManager RoundManager, nextRoundNumber *api.BigInt, delay time.Duration) *BFTClientStub {
	logger.Info("Using BFT Client Stub", "delay", delay.String())
	return &BFTClientStub{
		logger:          logger,
		roundManager:    roundManager,
		nextRoundNumber: nextRoundNumber,
		delay:           delay,
	}
}

func (n *BFTClientStub) Start(ctx context.Context) error {
	n.logger.Info("Starting BFT Client Stub")
	return n.roundManager.StartNewRound(ctx, n.nextRoundNumber)
}

func (n *BFTClientStub) Stop() {
	n.logger.Info("Stopping BFT Client Stub")
}

func (n *BFTClientStub) CertificationRequest(ctx context.Context, block *models.Block) error {
	// Simulate BFT certification delay
	if n.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(n.delay):
		}
	}

	if len(block.UnicityCertificate) == 0 {
		blockNumHex := fmt.Sprintf("%032x", block.Index.Int64())
		block.UnicityCertificate = api.HexBytes("1234abcd" + blockNumHex)
	}

	if err := n.roundManager.FinalizeBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to finalize block: %w", err)
	}

	nextRoundNumber := api.NewBigInt(nil)
	nextRoundNumber.Add(block.Index.Int, big.NewInt(1))

	go func() {
		if err := n.roundManager.StartNewRound(ctx, nextRoundNumber); err != nil {
			n.logger.Error("Failed to start next round", "error", err.Error())
		}
	}()

	return nil
}
