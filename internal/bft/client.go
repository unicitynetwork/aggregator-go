package bft

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/blockproposal"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"github.com/unicitynetwork/bft-core/network/protocol/replication"
	"github.com/unicitynetwork/bft-go-base/types"
)

const defaultHandshakeNodes = 3

// BFTRootChainClient handles communication with the BFT root chain via P2P network
type BFTClient struct {
	partitionID types.PartitionID
	shardID     types.ShardID
	peer        *network.Peer
	network     *BftNetwork
	logger      *logger.Logger
	rootNodes   peer.IDSlice
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
	bftClient := &BFTClient{
		peer:        peer,
		logger:      logger,
		rootNodes:   rootNodes,
		partitionID: conf.ShardConf.PartitionID,
		shardID:     conf.ShardConf.ShardID,
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

func randomNodeSelector(nodes peer.IDSlice, upToNodes int) (peer.IDSlice, error) {
	nodeCnt := len(nodes)
	if nodeCnt < 1 {
		return nil, fmt.Errorf("node list is empty")
	}
	if upToNodes == 0 {
		return nil, fmt.Errorf("invalid parameter, number of nodes to select is 0")
	}
	// optimization for wanting more nodes than in the node list - there is not enough to choose from
	if upToNodes >= nodeCnt {
		return nodes, nil
	}
	rNodes := make(peer.IDSlice, len(nodes))
	// make a copy of available nodes
	copy(rNodes, nodes)
	// randomize
	rand.Shuffle(len(rNodes), func(i, j int) {
		rNodes[i], rNodes[j] = rNodes[j], rNodes[i]
	})
	return rNodes[:upToNodes], nil
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
	switch msg.(type) {
	case *certification.CertificationResponse:
		n.logger.WithContext(ctx).Info("received CertificationResponse")
	case *blockproposal.BlockProposal:
		n.logger.WithContext(ctx).Info("received BlockProposal")
	case *replication.LedgerReplicationRequest:
		n.logger.WithContext(ctx).Info("received LedgerReplicationRequest")
	case *replication.LedgerReplicationResponse:
		n.logger.WithContext(ctx).Info("received LedgerReplicationResponse")
	case *types.Block:
		n.logger.WithContext(ctx).Info("received Block")
	default:
		n.logger.WithContext(ctx).Info("received unknown message")
	}
}
