package bft

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	idle status = iota
	initializing
	normal
)

// BFTRootChainClient handles communication with the BFT root chain via P2P network
type (
	BFTClientImpl struct {
		conf        *config.BFTConfig
		status      atomic.Value
		partitionID types.PartitionID
		shardID     types.ShardID
		logger      *logger.Logger

		// mutex for peer, network, signer, rootNodes
		mu        sync.Mutex
		peer      *network.Peer
		network   *BftNetwork
		rootNodes peer.IDSlice
		signer    crypto.Signer

		// Latest UC this node has seen. Can be ahead of the committed UC during recovery.
		luc           atomic.Pointer[types.UnicityCertificate]
		roundManager  RoundManager
		proposedBlock *models.Block
		// Track the next round number expected by root chain
		nextExpectedRound atomic.Uint64
		// Track the root round number to detect repeat UCs
		lastRootRound atomic.Uint64
		// Mutex to ensure sequential UC processing
		ucProcessingMutex sync.Mutex

		msgLoopCancelFn context.CancelFunc
	}

	BFTClient interface {
		Start(ctx context.Context) error
		Stop()
		CertificationRequest(ctx context.Context, block *models.Block) error
	}

	RoundManager interface {
		FinalizeBlock(ctx context.Context, block *models.Block) error
		StartNewRound(ctx context.Context, roundNumber *api.BigInt) error
	}

	status int
)

func NewBFTClient(ctx context.Context, conf *config.BFTConfig, roundManager RoundManager, logger *logger.Logger) (*BFTClientImpl, error) {
	logger.Info("Creating BFT Client")
	bftClient := &BFTClientImpl{
		logger:       logger,
		partitionID:  conf.ShardConf.PartitionID,
		shardID:      conf.ShardConf.ShardID,
		roundManager: roundManager,
		conf:         conf,
	}
	bftClient.status.Store(idle)
	return bftClient, nil
}

func (c *BFTClientImpl) PartitionID() types.PartitionID {
	return c.partitionID
}

func (c *BFTClientImpl) ShardID() types.ShardID {
	return c.shardID
}

func (c *BFTClientImpl) Peer() *network.Peer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peer
}

func (c *BFTClientImpl) IsValidator() bool {
	return true
}

func (c *BFTClientImpl) Start(ctx context.Context) error {
	c.logger.WithContext(ctx).Info("Starting BFT Client")
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.Store(initializing)

	peerConf, err := c.conf.PeerConf()
	if err != nil {
		return fmt.Errorf("failed to create peer configuration: %w", err)
	}
	self, err := network.NewPeer(ctx, peerConf, c.logger.Logger, nil)
	if err != nil {
		return err
	}
	rootNodes, err := c.conf.GetRootNodes()
	if err != nil {
		return fmt.Errorf("failed to get root nodes: %w", err)
	}
	signer, err := c.conf.KeyConf.Signer()
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}

	networkP2P, err := NewLibP2PNetwork(ctx, self, c.logger, DefaultNetworkOptions)
	if err != nil {
		return fmt.Errorf("failed to create libp2p network: %w", err)
	}

	c.peer = self
	c.network = networkP2P
	c.signer = signer
	c.rootNodes = rootNodes

	if err := c.peer.BootstrapConnect(ctx, c.logger.Logger); err != nil {
		return fmt.Errorf("failed to bootstrap peer: %w", err)
	}

	if err := c.sendHandshake(ctx); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	msgLoopCtx, cancelFn := context.WithCancel(ctx)
	c.msgLoopCancelFn = cancelFn
	go c.loop(msgLoopCtx)

	return nil
}

func (c *BFTClientImpl) Stop() {
	c.logger.Info("Stopping BFT Client")
	c.mu.Lock()
	defer c.mu.Unlock()

	c.status.Store(idle)

	if c.msgLoopCancelFn != nil {
		c.msgLoopCancelFn()
	}
	if c.peer != nil {
		if err := c.peer.Close(); err != nil {
			c.logger.Error("Failed to close peer host", "error", err)
		}
		c.peer = nil
		c.network = nil
		c.signer = nil
		c.rootNodes = nil
	}
}

func (c *BFTClientImpl) sendHandshake(ctx context.Context) error {
	c.logger.WithContext(ctx).Debug("sending handshake to root chain")
	// select some random root nodes
	rootIDs, err := randomNodeSelector(c.rootNodes, defaultHandshakeNodes)
	if err != nil {
		// error should only happen in case the root nodes are not initialized
		return fmt.Errorf("failed to select root nodes for handshake: %w", err)
	}
	if err = c.network.Send(ctx,
		handshake.Handshake{
			PartitionID: c.PartitionID(),
			ShardID:     c.ShardID(),
			NodeID:      c.peer.ID().String(),
		},
		rootIDs...); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}
	return nil
}

func (c *BFTClientImpl) loop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-c.network.ReceivedChannel():
			if !ok {
				return errors.New("network received channel is closed")
			}
			c.logger.WithContext(ctx).Debug("received message", "type", fmt.Sprintf("%T", m))
			c.handleMessage(ctx, m)
		}
	}
}

func (c *BFTClientImpl) handleMessage(ctx context.Context, msg any) {
	switch mt := msg.(type) {
	case *certification.CertificationResponse:
		c.logger.WithContext(ctx).Info("received CertificationResponse")
		c.handleCertificationResponse(ctx, mt)
	default:
		c.logger.WithContext(ctx).Info("received unknown message")
	}
}

func (c *BFTClientImpl) handleCertificationResponse(ctx context.Context, cr *certification.CertificationResponse) error {
	if err := cr.IsValid(); err != nil {
		return fmt.Errorf("invalid CertificationResponse: %w", err)
	}
	c.logger.WithContext(ctx).Info(fmt.Sprintf("handleCertificationResponse: UC round %d, next round %d, next leader %s",
		cr.UC.GetRoundNumber(), cr.Technical.Round, cr.Technical.Leader))

	if cr.Partition != c.PartitionID() || !cr.Shard.Equal(c.ShardID()) {
		return fmt.Errorf("got CertificationResponse for a wrong shard %s - %s", cr.Partition, cr.Shard)
	}

	return c.handleUnicityCertificate(ctx, &cr.UC, &cr.Technical)
}

// isRepeatUC checks if the new UC is a repeat of the previous UC
// A repeat UC has the same InputRecord but higher root round number
func (c *BFTClientImpl) isRepeatUC(prevUC, newUC *types.UnicityCertificate) bool {
	if prevUC == nil || newUC == nil {
		return false
	}

	// Check if InputRecords are equal (same partition round, state hash, etc.)
	if prevUC.InputRecord.RoundNumber != newUC.InputRecord.RoundNumber {
		return false
	}
	if !bytes.Equal(prevUC.InputRecord.Hash, newUC.InputRecord.Hash) {
		return false
	}
	if !bytes.Equal(prevUC.InputRecord.PreviousHash, newUC.InputRecord.PreviousHash) {
		return false
	}
	if !bytes.Equal(prevUC.InputRecord.BlockHash, newUC.InputRecord.BlockHash) {
		return false
	}

	// If InputRecords match but root round is higher, it's a repeat UC
	return prevUC.UnicitySeal.RootChainRoundNumber < newUC.UnicitySeal.RootChainRoundNumber
}

func (c *BFTClientImpl) handleUnicityCertificate(ctx context.Context, uc *types.UnicityCertificate, tr *certification.TechnicalRecord) error {
	// Ensure sequential processing of UCs to prevent race conditions
	c.ucProcessingMutex.Lock()
	defer c.ucProcessingMutex.Unlock()

	c.logger.WithContext(ctx).Debug("Acquired UC processing lock",
		"ucRound", uc.GetRoundNumber(),
		"rootRound", uc.GetRootRoundNumber())

	prevLUC := c.luc.Load()
	// as we can be connected to several root nodes, we can receive the same UC multiple times
	if uc.IsDuplicate(prevLUC) {
		c.logger.WithContext(ctx).Debug(fmt.Sprintf("duplicate UC (same root round %d)", uc.GetRootRoundNumber()))
		return nil
	}

	// Check for repeat UC (same InputRecord but higher root round)
	if c.isRepeatUC(prevLUC, uc) {
		c.logger.WithContext(ctx).Warn("Received repeat UC - root chain timed out waiting for certification",
			"partitionRound", uc.GetRoundNumber(),
			"prevRootRound", prevLUC.GetRootRoundNumber(),
			"newRootRound", uc.GetRootRoundNumber())

		// Store the repeat UC and update root round tracking
		c.luc.Store(uc)
		c.lastRootRound.Store(uc.GetRootRoundNumber())

		// Clear any proposed block as it wasn't accepted in time
		if c.proposedBlock != nil {
			c.logger.WithContext(ctx).Info("Clearing proposed block due to repeat UC",
				"proposedBlockNumber", c.proposedBlock.Index.String())
			c.proposedBlock = nil
		}

		// Start new round immediately with the next expected round
		nextRoundNumber := big.NewInt(0)
		nextRoundNumber.SetUint64(tr.Round)
		c.nextExpectedRound.Store(tr.Round)

		c.logger.WithContext(ctx).Info("Starting new round after repeat UC",
			"nextRoundNumber", nextRoundNumber.String())
		return c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
	}

	// Log both partition and root rounds for better tracking
	c.logger.WithContext(ctx).Info("Handling new unicity certificate",
		"partitionRound", uc.GetRoundNumber(),
		"rootRound", uc.GetRootRoundNumber(),
		"prevPartitionRound", func() uint64 {
			if prevLUC != nil {
				return prevLUC.GetRoundNumber()
			}
			return 0
		}(),
		"prevRootRound", c.lastRootRound.Load())

	c.luc.Store(uc)
	c.lastRootRound.Store(uc.GetRootRoundNumber())

	nextRoundNumber := big.NewInt(0)
	nextRoundNumber.SetUint64(tr.Round)

	// Store the next expected round number
	c.nextExpectedRound.Store(tr.Round)

	wasInitializing := c.status.Load() == initializing
	if wasInitializing {
		c.logger.WithContext(ctx).Info("BFT client initialization finished, starting first round",
			"nextRoundNumber", nextRoundNumber.String())
		// First UC received after an initial handshake with a root node -> initialization finished.
		c.status.Store(normal)
		err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to start first round after initialization",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
		}
		return err
	}

	// Check if we have a proposed block that matches the UC round
	blockNum := "nil"
	expectedRound := uc.GetRoundNumber()
	if c.proposedBlock != nil {
		blockNum = c.proposedBlock.Index.String()
		proposedRound := c.proposedBlock.Index.Uint64()

		// If the UC is for a different round than our proposed block, we need to handle it
		if proposedRound != expectedRound {
			c.logger.WithContext(ctx).Warn("UC round does not match proposed block round",
				"ucRound", expectedRound,
				"proposedBlockRound", proposedRound,
				"nextRound", tr.Round)

			// If root chain has moved ahead, we need to skip to catch up
			if expectedRound < proposedRound {
				c.logger.WithContext(ctx).Warn("Root chain is behind our proposed round - likely timing issue",
					"ucRound", expectedRound,
					"ourProposedRound", proposedRound,
					"nextRoundRequired", tr.Round)
				// This UC is for an older round, but we still need to process it
				// to stay in sync with root chain. Clear our proposed block and
				// start fresh with the root chain's expected round
				c.proposedBlock = nil

				// Start new round immediately with root chain's next round
				c.logger.WithContext(ctx).Info("Starting new round to sync with root chain",
					"nextRoundNumber", nextRoundNumber.String())
				err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
				if err != nil {
					c.logger.WithContext(ctx).Error("Failed to start new round for sync",
						"nextRoundNumber", nextRoundNumber.String(),
						"error", err.Error())
				}
				return err
			}

			// If UC is for an older round than proposed, it's a stale UC
			c.logger.WithContext(ctx).Debug("Received UC for older round than proposed block, ignoring")
			return nil
		}
	}

	// Check if we have a proposed block to finalize
	if c.proposedBlock == nil {
		c.logger.WithContext(ctx).Warn("No proposed block to finalize, starting next round",
			"ucRound", expectedRound,
			"nextRoundNumber", nextRoundNumber.String())
		// This can happen if:
		// 1. We just started and haven't proposed a block yet
		// 2. We cleared the proposed block due to sync issues
		// 3. The root chain advanced without us sending a certification request

		// Start the next round directly
		err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to start next round",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
		}
		return err
	}

	c.logger.WithContext(ctx).Info("Finalizing block with unicity certificate",
		"blockNumber", blockNum,
		"ucRound", expectedRound)

	ucCbor, err := types.Cbor.Marshal(uc)
	if err != nil {
		c.logger.WithContext(ctx).Error("Failed to encode unicity certificate",
			"error", err.Error())
		return fmt.Errorf("failed to encode unicity certificate: %w", err)
	}
	c.proposedBlock.UnicityCertificate = api.NewHexBytes(ucCbor)

	if err := c.roundManager.FinalizeBlock(ctx, c.proposedBlock); err != nil {
		c.logger.WithContext(ctx).Error("Failed to finalize block",
			"blockNumber", c.proposedBlock.Index.String(),
			"error", err.Error())
		return err
	}

	// Clear the proposed block after finalization
	c.proposedBlock = nil

	c.logger.WithContext(ctx).Info("Block finalized, starting new round",
		"nextRoundNumber", nextRoundNumber.String())

	err = c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
	if err != nil {
		c.logger.WithContext(ctx).Error("Failed to start new round",
			"nextRoundNumber", nextRoundNumber.String(),
			"error", err.Error())
	}
	return err
}

func (c *BFTClientImpl) sendCertificationRequest(ctx context.Context, rootHash string, roundNumber uint64) error {
	rootHashBytes, err := hex.DecodeString(rootHash)
	if err != nil {
		return fmt.Errorf("failed to decode root hash: %w", err)
	}

	luc, err := c.waitForLatestUC(ctx)
	if err != nil {
		return fmt.Errorf("failed to prepare certification request: %w", err)
	}

	var blockHash []byte
	if !bytes.Equal(rootHashBytes, luc.InputRecord.Hash) {
		blockHash = rootHashBytes
	}

	// send new input record for certification
	req := &certification.BlockCertificationRequest{
		PartitionID: c.PartitionID(),
		ShardID:     c.ShardID(),
		NodeID:      c.peer.ID().String(),
		InputRecord: &types.InputRecord{
			Version:         1,
			RoundNumber:     roundNumber,
			Epoch:           luc.InputRecord.Epoch,
			PreviousHash:    luc.InputRecord.Hash,
			Hash:            rootHashBytes,
			SummaryValue:    []byte{}, // cant be nil if RoundNumber > 0
			Timestamp:       luc.UnicitySeal.Timestamp,
			BlockHash:       blockHash,
			SumOfEarnedFees: 0,
			ETHash:          nil, // can be nil, not validated
		},
	}

	if err = req.Sign(c.signer); err != nil {
		return fmt.Errorf("failed to sign certification request: %w", err)
	}
	c.logger.WithContext(ctx).Info(fmt.Sprintf("Round %d sending block certification request to root chain, IR hash %X",
		req.InputRecord.RoundNumber, req.InputRecord.Hash))
	rootIDs, err := rootNodesSelector(luc, c.rootNodes, defaultNofRootNodes)
	if err != nil {
		return fmt.Errorf("selecting root nodes: %w", err)
	}
	return c.network.Send(ctx, req, rootIDs...)
}

func (c *BFTClientImpl) CertificationRequest(ctx context.Context, block *models.Block) error {
	c.logger.WithContext(ctx).Info("CertificationRequest called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"lastRootRound", c.lastRootRound.Load())

	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	// Always prefer the expected round number from root chain if available
	expectedRound := c.nextExpectedRound.Load()
	if expectedRound > 0 {
		originalBlockNumber := block.Index.Uint64()
		if expectedRound != originalBlockNumber {
			c.logger.WithContext(ctx).Warn("Adjusting block number to match root chain expectations",
				"originalBlockNumber", originalBlockNumber,
				"adjustedBlockNumber", expectedRound,
				"difference", int64(expectedRound)-int64(originalBlockNumber))
		} else {
			c.logger.WithContext(ctx).Debug("Block number matches root chain expectations",
				"blockNumber", expectedRound)
		}
		// Always use the expected round number when available
		block.Index = api.NewBigInt(new(big.Int).SetUint64(expectedRound))
	} else {
		// No expected round yet - this might be the very first request
		c.logger.WithContext(ctx).Debug("No expected round number from root chain yet, using proposed block number",
			"blockNumber", block.Index.String())
		// If we have a last UC, we can infer the expected round
		if luc := c.luc.Load(); luc != nil {
			// The next round should be the UC round + 1
			inferredRound := luc.GetRoundNumber() + 1
			if inferredRound != block.Index.Uint64() {
				c.logger.WithContext(ctx).Warn("Inferring expected round from last UC",
					"lastUCRound", luc.GetRoundNumber(),
					"inferredRound", inferredRound,
					"proposedRound", block.Index.Uint64())
				block.Index = api.NewBigInt(new(big.Int).SetUint64(inferredRound))
			}
		}
	}

	c.proposedBlock = block

	c.logger.WithContext(ctx).Debug("Sending certification request",
		"blockNumber", c.proposedBlock.Index.String(),
		"roundNumber", c.proposedBlock.Index.Uint64())

	if err := c.sendCertificationRequest(ctx, c.proposedBlock.RootHash.String(), c.proposedBlock.Index.Uint64()); err != nil {
		c.logger.WithContext(ctx).Error("Failed to send certification request",
			"blockNumber", block.Index.String(),
			"error", err.Error())
		return fmt.Errorf("failed to send certification request: %w", err)
	}

	c.logger.WithContext(ctx).Info("Certification request completed",
		"blockNumber", block.Index.String())
	return nil
}

func (c *BFTClientImpl) waitForLatestUC(ctx context.Context) (*types.UnicityCertificate, error) {
	if luc := c.luc.Load(); luc != nil {
		return luc, nil
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if luc := c.luc.Load(); luc != nil {
				return luc, nil
			}
		}
	}
}

func (c *BFTClientImpl) ensureInitialized(ctx context.Context) error {
	if c.status.Load() == normal {
		return nil
	}
	_, err := c.waitForLatestUC(ctx)
	return err
}
