package bft

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

// BFTRootChainClient handles communication with the BFT root chain via P2P network
type BFTClient struct {
	partitionID types.PartitionID
	shardID     types.ShardID
	peer        *network.Peer
	network     *BftNetwork
	logger      *logger.Logger
	rootNodes   peer.IDSlice
	signer      crypto.Signer
	// Latest UC this node has seen. Can be ahead of the committed UC during recovery.
	luc                       atomic.Pointer[types.UnicityCertificate]
	certificationResponseChan chan struct{}
}

func NewBFTClient(ctx context.Context, conf *config.BFTConfig, logger *logger.Logger) (*BFTClient, error) {
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
	bftClient := &BFTClient{
		peer:                      peer,
		logger:                    logger,
		rootNodes:                 rootNodes,
		partitionID:               conf.ShardConf.PartitionID,
		shardID:                   conf.ShardConf.ShardID,
		signer:                    signer,
		certificationResponseChan: make(chan struct{}),
	}
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

func (c *BFTClient) PartitionID() types.PartitionID {
	return c.partitionID
}

func (c *BFTClient) ShardID() types.ShardID {
	return c.shardID
}

func (c *BFTClient) Peer() *network.Peer {
	return c.peer
}

func (c *BFTClient) IsValidator() bool {
	return true
}

func (c *BFTClient) Start(ctx context.Context) {
	c.logger.WithContext(ctx).Info("Starting BFT Client")

	c.sendHandshake(ctx)
	go c.loop(ctx)
}

func (c *BFTClient) sendHandshake(ctx context.Context) {
	c.logger.WithContext(ctx).Debug("sending handshake to root chain")
	// select some random root nodes
	rootIDs, err := randomNodeSelector(c.rootNodes, defaultHandshakeNodes)
	if err != nil {
		// error should only happen in case the root nodes are not initialized
		c.logger.WarnContext(ctx, "selecting root nodes for handshake", err)
		return
	}
	if err = c.network.Send(ctx,
		handshake.Handshake{
			PartitionID: c.PartitionID(),
			ShardID:     c.ShardID(),
			NodeID:      c.peer.ID().String(),
		},
		rootIDs...); err != nil {
		c.logger.WarnContext(ctx, "error sending handshake", err)
	}
}

func (n *BFTClient) loop(ctx context.Context) error {
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

func (n *BFTClient) handleMessage(ctx context.Context, msg any) {
	switch mt := msg.(type) {
	case *certification.CertificationResponse:
		n.logger.WithContext(ctx).Info("received CertificationResponse")
		n.handleCertificationResponse(ctx, mt)
	default:
		n.logger.WithContext(ctx).Info("received unknown message")
	}
}

func (n *BFTClient) handleCertificationResponse(ctx context.Context, cr *certification.CertificationResponse) error {
	if err := cr.IsValid(); err != nil {
		return fmt.Errorf("invalid CertificationResponse: %w", err)
	}
	n.logger.WithContext(ctx).Info(fmt.Sprintf("handleCertificationResponse: UC round %d, next round %d, next leader %s",
		cr.UC.GetRoundNumber(), cr.Technical.Round, cr.Technical.Leader))

	if cr.Partition != n.PartitionID() || !cr.Shard.Equal(n.ShardID()) {
		return fmt.Errorf("got CertificationResponse for a wrong shard %s - %s", cr.Partition, cr.Shard)
	}

	n.luc.Store(&cr.UC)
	n.logger.WithContext(ctx).Debug(fmt.Sprintf("updated LUC; UC.Round: %d, RootRound: %d",
		cr.UC.GetRoundNumber(), cr.UC.GetRootRoundNumber()))
	n.certificationResponseChan <- struct{}{}
	return nil
}

func (n *BFTClient) sendCertificationRequest(ctx context.Context, rootHash string) error {
	rootHashBytes, err := hex.DecodeString(rootHash)
	if err != nil {
		return fmt.Errorf("failed to decode root hash: %w", err)
	}

	luc := n.luc.Load()

	// send new input record for certification
	req := &certification.BlockCertificationRequest{
		PartitionID: n.PartitionID(),
		ShardID:     n.ShardID(),
		NodeID:      n.peer.ID().String(),
		InputRecord: &types.InputRecord{
			Version:         1,
			RoundNumber:     luc.GetRoundNumber() + 1,
			Epoch:           luc.InputRecord.Epoch,
			PreviousHash:    luc.InputRecord.Hash,
			Hash:            rootHashBytes,
			SummaryValue:    []byte{}, // cant be nil if RoundNumber > 0
			Timestamp:       luc.UnicitySeal.Timestamp,
			BlockHash:       rootHashBytes, // has to have value if Hash != PreviousHash, using root hash for now
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
	for len(n.certificationResponseChan) > 0 {
		<-n.certificationResponseChan
	}
	return n.network.Send(ctx, req, rootIDs...)
}

func (n *BFTClient) CertificationRequest(ctx context.Context, rootHash string) error {
	if err := n.sendCertificationRequest(ctx, rootHash); err != nil {
		return fmt.Errorf("failed to send certification request: %w", err)
	}
	<-n.certificationResponseChan
	return nil
}
