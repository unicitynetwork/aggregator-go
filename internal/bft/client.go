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

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

const (
	initializing status = iota
	normal
)

// BFTRootChainClient handles communication with the BFT root chain via P2P network
type (
	BFTClientImpl struct {
		status      atomic.Value
		partitionID types.PartitionID
		shardID     types.ShardID
		peer        *network.Peer
		network     *BftNetwork
		logger      *logger.Logger
		rootNodes   peer.IDSlice
		signer      crypto.Signer
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
	}

	BFTClient interface {
		Start(ctx context.Context, nextRoundNumber *api.BigInt) error
		CertificationRequest(ctx context.Context, block *models.Block) error
	}

	RoundManager interface {
		FinalizeBlock(ctx context.Context, block *models.Block) error
		StartNewRound(ctx context.Context, roundNumber *api.BigInt) error
	}

	status int
)

func NewBFTClient(ctx context.Context, conf *config.BFTConfig, logger *logger.Logger, roundManager RoundManager) (*BFTClientImpl, error) {
	logger.Info("Using BFT Client")
	peerConf, err := conf.PeerConf()
	if err != nil {
		return nil, fmt.Errorf("failed to create peer configuration: %w", err)
	}
	peer, err := network.NewPeer(ctx, peerConf, logger.Logger, nil)
	if err != nil {
		return nil, err
	}
	rootNodes, err := conf.GetRootNodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get root nodes: %w", err)
	}
	signer, err := conf.KeyConf.Signer()
	if err != nil {
		return nil, err
	}
	bftClient := &BFTClientImpl{
		peer:         peer,
		logger:       logger,
		rootNodes:    rootNodes,
		partitionID:  conf.ShardConf.PartitionID,
		shardID:      conf.ShardConf.ShardID,
		signer:       signer,
		roundManager: roundManager,
	}
	bftClient.status.Store(initializing)
	opts := DefaultNetworkOptions
	bftClient.network, err = NewLibP2PNetwork(ctx, peer, logger, opts)
	if err != nil {
		return nil, err
	}

	if err := peer.BootstrapConnect(ctx, logger.Logger); err != nil {
		return nil, err
	}

	return bftClient, nil
}

func (c *BFTClientImpl) PartitionID() types.PartitionID {
	return c.partitionID
}

func (c *BFTClientImpl) ShardID() types.ShardID {
	return c.shardID
}

func (c *BFTClientImpl) Peer() *network.Peer {
	return c.peer
}

func (c *BFTClientImpl) IsValidator() bool {
	return true
}

func (c *BFTClientImpl) Start(ctx context.Context, _ *api.BigInt) error {
	c.logger.WithContext(ctx).Info("Starting BFT Client")

	if err := c.sendHandshake(ctx); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}
	go c.loop(ctx)

	return nil
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

func (n *BFTClientImpl) loop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-n.network.ReceivedChannel():
			if !ok {
				return errors.New("network received channel is closed")
			}
			n.logger.WithContext(ctx).Debug("received message", "type", fmt.Sprintf("%T", m))
			n.handleMessage(ctx, m)
		}
	}
}

func (n *BFTClientImpl) handleMessage(ctx context.Context, msg any) {
	switch mt := msg.(type) {
	case *certification.CertificationResponse:
		n.logger.WithContext(ctx).Info("received CertificationResponse")
		n.handleCertificationResponse(ctx, mt)
	default:
		n.logger.WithContext(ctx).Info("received unknown message")
	}
}

func (n *BFTClientImpl) handleCertificationResponse(ctx context.Context, cr *certification.CertificationResponse) error {
	if err := cr.IsValid(); err != nil {
		return fmt.Errorf("invalid CertificationResponse: %w", err)
	}
	n.logger.WithContext(ctx).Info(fmt.Sprintf("handleCertificationResponse: UC round %d, next round %d, next leader %s",
		cr.UC.GetRoundNumber(), cr.Technical.Round, cr.Technical.Leader))

	if cr.Partition != n.PartitionID() || !cr.Shard.Equal(n.ShardID()) {
		return fmt.Errorf("got CertificationResponse for a wrong shard %s - %s", cr.Partition, cr.Shard)
	}

	return n.handleUnicityCertificate(ctx, &cr.UC, &cr.Technical)
}

// isRepeatUC checks if the new UC is a repeat of the previous UC
// A repeat UC has the same InputRecord but higher root round number
func (n *BFTClientImpl) isRepeatUC(prevUC, newUC *types.UnicityCertificate) bool {
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

func (n *BFTClientImpl) handleUnicityCertificate(ctx context.Context, uc *types.UnicityCertificate, tr *certification.TechnicalRecord) error {
	// Ensure sequential processing of UCs to prevent race conditions
	n.ucProcessingMutex.Lock()
	defer n.ucProcessingMutex.Unlock()

	n.logger.WithContext(ctx).Debug("Acquired UC processing lock",
		"ucRound", uc.GetRoundNumber(),
		"rootRound", uc.GetRootRoundNumber())

	prevLUC := n.luc.Load()
	// as we can be connected to several root nodes, we can receive the same UC multiple times
	if uc.IsDuplicate(prevLUC) {
		n.logger.WithContext(ctx).Debug(fmt.Sprintf("duplicate UC (same root round %d)", uc.GetRootRoundNumber()))
		return nil
	}

	// Check for repeat UC (same InputRecord but higher root round)
	if n.isRepeatUC(prevLUC, uc) {
		n.logger.WithContext(ctx).Warn("Received repeat UC - root chain timed out waiting for certification",
			"partitionRound", uc.GetRoundNumber(),
			"prevRootRound", prevLUC.GetRootRoundNumber(),
			"newRootRound", uc.GetRootRoundNumber())

		// Store the repeat UC and update root round tracking
		n.luc.Store(uc)
		n.lastRootRound.Store(uc.GetRootRoundNumber())

		// Clear any proposed block as it wasn't accepted in time
		if n.proposedBlock != nil {
			n.logger.WithContext(ctx).Info("Clearing proposed block due to repeat UC",
				"proposedBlockNumber", n.proposedBlock.Index.String())
			n.proposedBlock = nil
		}

		// Start new round immediately with the next expected round
		nextRoundNumber := big.NewInt(0)
		nextRoundNumber.SetUint64(tr.Round)
		n.nextExpectedRound.Store(tr.Round)

		n.logger.WithContext(ctx).Info("Starting new round after repeat UC",
			"nextRoundNumber", nextRoundNumber.String())
		return n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
	}

	// Log both partition and root rounds for better tracking
	n.logger.WithContext(ctx).Info("Handling new unicity certificate",
		"partitionRound", uc.GetRoundNumber(),
		"rootRound", uc.GetRootRoundNumber(),
		"prevPartitionRound", func() uint64 {
			if prevLUC != nil {
				return prevLUC.GetRoundNumber()
			}
			return 0
		}(),
		"prevRootRound", n.lastRootRound.Load())

	n.luc.Store(uc)
	n.lastRootRound.Store(uc.GetRootRoundNumber())

	nextRoundNumber := big.NewInt(0)
	nextRoundNumber.SetUint64(tr.Round)

	// Store the next expected round number
	n.nextExpectedRound.Store(tr.Round)

	wasInitializing := n.status.Load() == initializing
	if wasInitializing {
		n.logger.WithContext(ctx).Info("BFT client initialization finished, starting first round",
			"nextRoundNumber", nextRoundNumber.String())
		// First UC received after an initial handshake with a root node -> initialization finished.
		n.status.Store(normal)
		err := n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
		if err != nil {
			n.logger.WithContext(ctx).Error("Failed to start first round after initialization",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
		}
		return err
	}

	// Check if we have a proposed block that matches the UC round
	blockNum := "nil"
	expectedRound := uc.GetRoundNumber()
	if n.proposedBlock != nil {
		blockNum = n.proposedBlock.Index.String()
		proposedRound := n.proposedBlock.Index.Uint64()

		// If the UC is for a different round than our proposed block, we need to handle it
		if proposedRound != expectedRound {
			n.logger.WithContext(ctx).Warn("UC round does not match proposed block round",
				"ucRound", expectedRound,
				"proposedBlockRound", proposedRound,
				"nextRound", tr.Round)

			// If root chain has moved ahead, we need to skip to catch up
			if expectedRound < proposedRound {
				n.logger.WithContext(ctx).Warn("Root chain is behind our proposed round - likely timing issue",
					"ucRound", expectedRound,
					"ourProposedRound", proposedRound,
					"nextRoundRequired", tr.Round)
				// This UC is for an older round, but we still need to process it
				// to stay in sync with root chain. Clear our proposed block and
				// start fresh with the root chain's expected round
				n.proposedBlock = nil

				// Start new round immediately with root chain's next round
				n.logger.WithContext(ctx).Info("Starting new round to sync with root chain",
					"nextRoundNumber", nextRoundNumber.String())
				err := n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
				if err != nil {
					n.logger.WithContext(ctx).Error("Failed to start new round for sync",
						"nextRoundNumber", nextRoundNumber.String(),
						"error", err.Error())
				}
				return err
			}

			// If UC is for an older round than proposed, it's a stale UC
			n.logger.WithContext(ctx).Debug("Received UC for older round than proposed block, ignoring")
			return nil
		}
	}

	// Check if we have a proposed block to finalize
	if n.proposedBlock == nil {
		n.logger.WithContext(ctx).Warn("No proposed block to finalize, starting next round",
			"ucRound", expectedRound,
			"nextRoundNumber", nextRoundNumber.String())
		// This can happen if:
		// 1. We just started and haven't proposed a block yet
		// 2. We cleared the proposed block due to sync issues
		// 3. The root chain advanced without us sending a certification request

		// Start the next round directly
		err := n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
		if err != nil {
			n.logger.WithContext(ctx).Error("Failed to start next round",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
		}
		return err
	}

	n.logger.WithContext(ctx).Info("Finalizing block with unicity certificate",
		"blockNumber", blockNum,
		"ucRound", expectedRound)

	ucCbor, err := types.Cbor.Marshal(uc)
	if err != nil {
		n.logger.WithContext(ctx).Error("Failed to encode unicity certificate",
			"error", err.Error())
		return fmt.Errorf("failed to encode unicity certificate: %w", err)
	}
	n.proposedBlock.UnicityCertificate = api.NewHexBytes(ucCbor)

	if err := n.roundManager.FinalizeBlock(ctx, n.proposedBlock); err != nil {
		n.logger.WithContext(ctx).Error("Failed to finalize block",
			"blockNumber", n.proposedBlock.Index.String(),
			"error", err.Error())
		return err
	}

	// Clear the proposed block after finalization
	n.proposedBlock = nil

	n.logger.WithContext(ctx).Info("Block finalized, starting new round",
		"nextRoundNumber", nextRoundNumber.String())

	err = n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
	if err != nil {
		n.logger.WithContext(ctx).Error("Failed to start new round",
			"nextRoundNumber", nextRoundNumber.String(),
			"error", err.Error())
	}
	return err
}

func (n *BFTClientImpl) sendCertificationRequest(ctx context.Context, rootHash string, roundNumber uint64) error {
	rootHashBytes, err := hex.DecodeString(rootHash)
	if err != nil {
		return fmt.Errorf("failed to decode root hash: %w", err)
	}

	luc := n.luc.Load()

	var blockHash []byte
	if !bytes.Equal(rootHashBytes, luc.InputRecord.Hash) {
		blockHash = rootHashBytes
	}

	// send new input record for certification
	req := &certification.BlockCertificationRequest{
		PartitionID: n.PartitionID(),
		ShardID:     n.ShardID(),
		NodeID:      n.peer.ID().String(),
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

	if err = req.Sign(n.signer); err != nil {
		return fmt.Errorf("failed to sign certification request: %w", err)
	}
	n.logger.WithContext(ctx).Info(fmt.Sprintf("Round %d sending block certification request to root chain, IR hash %X",
		req.InputRecord.RoundNumber, req.InputRecord.Hash))
	rootIDs, err := rootNodesSelector(luc, n.rootNodes, defaultNofRootNodes)
	if err != nil {
		return fmt.Errorf("selecting root nodes: %w", err)
	}
	return n.network.Send(ctx, req, rootIDs...)
}

func (n *BFTClientImpl) CertificationRequest(ctx context.Context, block *models.Block) error {
	n.logger.WithContext(ctx).Info("CertificationRequest called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"lastRootRound", n.lastRootRound.Load())

	// Always prefer the expected round number from root chain if available
	expectedRound := n.nextExpectedRound.Load()
	if expectedRound > 0 {
		originalBlockNumber := block.Index.Uint64()
		if expectedRound != originalBlockNumber {
			n.logger.WithContext(ctx).Warn("Adjusting block number to match root chain expectations",
				"originalBlockNumber", originalBlockNumber,
				"adjustedBlockNumber", expectedRound,
				"difference", int64(expectedRound)-int64(originalBlockNumber))
		} else {
			n.logger.WithContext(ctx).Debug("Block number matches root chain expectations",
				"blockNumber", expectedRound)
		}
		// Always use the expected round number when available
		block.Index = api.NewBigInt(new(big.Int).SetUint64(expectedRound))
	} else {
		// No expected round yet - this might be the very first request
		n.logger.WithContext(ctx).Debug("No expected round number from root chain yet, using proposed block number",
			"blockNumber", block.Index.String())
		// If we have a last UC, we can infer the expected round
		if luc := n.luc.Load(); luc != nil {
			// The next round should be the UC round + 1
			inferredRound := luc.GetRoundNumber() + 1
			if inferredRound != block.Index.Uint64() {
				n.logger.WithContext(ctx).Warn("Inferring expected round from last UC",
					"lastUCRound", luc.GetRoundNumber(),
					"inferredRound", inferredRound,
					"proposedRound", block.Index.Uint64())
				block.Index = api.NewBigInt(new(big.Int).SetUint64(inferredRound))
			}
		}
	}

	n.proposedBlock = block

	n.logger.WithContext(ctx).Debug("Sending certification request",
		"blockNumber", n.proposedBlock.Index.String(),
		"roundNumber", n.proposedBlock.Index.Uint64())

	if err := n.sendCertificationRequest(ctx, n.proposedBlock.RootHash.String(), n.proposedBlock.Index.Uint64()); err != nil {
		n.logger.WithContext(ctx).Error("Failed to send certification request",
			"blockNumber", block.Index.String(),
			"error", err.Error())
		return fmt.Errorf("failed to send certification request: %w", err)
	}

	n.logger.WithContext(ctx).Info("Certification request completed",
		"blockNumber", block.Index.String())
	return nil
}
