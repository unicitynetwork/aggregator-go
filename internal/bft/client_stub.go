package bft

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type BFTClientStub struct {
	logger          *logger.Logger
	roundManager    RoundManager
	nextRoundNumber *api.BigInt
	delay           time.Duration
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
	stopped         bool
	wg              sync.WaitGroup
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
	stubCtx, cancel := context.WithCancel(ctx)
	n.mu.Lock()
	n.ctx = stubCtx
	n.cancel = cancel
	n.stopped = false
	n.mu.Unlock()
	return n.roundManager.StartNewRound(stubCtx, n.nextRoundNumber)
}

func (n *BFTClientStub) Stop() {
	n.logger.Info("Stopping BFT Client Stub")
	n.mu.Lock()
	n.stopped = true
	cancel := n.cancel
	n.cancel = nil
	n.ctx = nil
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	n.wg.Wait()
}

func (n *BFTClientStub) WaitForInitialized(ctx context.Context) error {
	// Stub is always immediately initialized
	return nil
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
		// Emit a monotonic synthetic UC so child-mode freshness checks also work
		// when the parent runs against the local BFT stub.
		roundNumber := block.Index.Uint64()
		uc := types.UnicityCertificate{
			InputRecord: &types.InputRecord{
				RoundNumber: roundNumber,
				Hash:        hex.Bytes(block.RootHash),
			},
			UnicitySeal: &types.UnicitySeal{
				RootChainRoundNumber: roundNumber,
			},
		}
		ucBytes, err := types.Cbor.Marshal(uc)
		if err != nil {
			return err
		}
		block.UnicityCertificate = ucBytes
	}

	if err := n.roundManager.FinalizeBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to finalize block: %w", err)
	}

	nextRoundNumber := api.NewBigInt(nil)
	nextRoundNumber.Add(block.Index.Int, big.NewInt(1))

	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	nextCtx := n.ctx
	if nextCtx == nil {
		nextCtx = ctx
	}
	n.wg.Add(1)
	n.mu.Unlock()

	go func() {
		defer n.wg.Done()
		if err := n.roundManager.StartNextRoundFromPrecollector(nextCtx, nextRoundNumber); err != nil {
			n.logger.Error("Failed to start next round", "error", err.Error())
		}
	}()

	return nil
}
