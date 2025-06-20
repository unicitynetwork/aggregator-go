package bft

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
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
			n.logger.WithContext(ctx).Debug("received message %T", m)
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

func (n *BFTClientImpl) handleUnicityCertificate(ctx context.Context, uc *types.UnicityCertificate, tr *certification.TechnicalRecord) error {
	prevLUC := n.luc.Load()
	// as we can be connected to several root nodes, we can receive the same UC multiple times
	if uc.IsDuplicate(prevLUC) {
		n.logger.WithContext(ctx).Debug(fmt.Sprintf("duplicate UC (same root round %d)", uc.GetRootRoundNumber()))
		return nil
	}

	n.logger.WithContext(ctx).Debug(fmt.Sprintf("updated LUC; UC.Round: %d, RootRound: %d",
		uc.GetRoundNumber(), uc.GetRootRoundNumber()))
	n.luc.Store(uc)

	nextRoundNumber := big.NewInt(0)
	nextRoundNumber.SetUint64(tr.Round)

	wasInitializing := n.status.Load() == initializing
	if wasInitializing {
		n.logger.WithContext(ctx).Info("initialization finished, starting new round",
			"nextRoundNumber", nextRoundNumber.String())
		// First UC received after an initial handshake with a root node -> initialization finished.
		n.status.Store(normal)
		return n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
	}

	n.logger.WithContext(ctx).Info("finalize block")

	ucCbor, err := types.Cbor.Marshal(uc)
	if err != nil {
		return fmt.Errorf("failed to encode unicity certificate: %w", err)
	}
	n.proposedBlock.UnicityCertificate = api.NewHexBytes(ucCbor)

	n.roundManager.FinalizeBlock(ctx, n.proposedBlock)
	n.logger.WithContext(ctx).Info("starting new round",
		"nextRoundNumber", nextRoundNumber.String())
	return n.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber))
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
	n.proposedBlock = block
	if err := n.sendCertificationRequest(ctx, n.proposedBlock.RootHash.String(), n.proposedBlock.Index.Uint64()); err != nil {
		return fmt.Errorf("failed to send certification request: %w", err)
	}
	return nil
}
