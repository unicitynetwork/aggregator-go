package bft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	idle status = iota
	initializing
	normal
	stopping
)

var (
	ErrStaleCertificationRound = errors.New("stale certification round")
	ErrCertifiedRootMismatch   = errors.New("certified root does not match proposed block root")
	ErrInvalidUCSequence       = errors.New("invalid unicity certificate sequence")
	ErrCertifiedStateMismatch  = errors.New("local committed state does not match latest certified root")
	ErrBFTClientStopping       = errors.New("BFT client is stopping")
)

// BFTClientImpl handles communication with the BFT root chain via P2P network
type (
	BFTClientImpl struct {
		conf        *config.BFTConfig
		status      atomic.Value
		partitionID types.PartitionID
		shardID     types.ShardID
		logger      *logger.Logger
		eventBus    *events.EventBus

		// Protects peer, network, and signer.
		mu      sync.Mutex
		peer    *network.Peer
		network *BftNetwork
		signer  cryptobft.Signer

		// Latest UC this node has seen. Can be ahead of the committed UC during recovery.
		luc                    atomic.Pointer[types.UnicityCertificate]
		roundManager           RoundManager
		proposedBlock          *models.Block
		resumedDurableProposal bool
		// Track the next round number expected by root chain
		nextExpectedRound atomic.Uint64
		// Track the next epoch expected by root chain technical record
		nextExpectedEpoch atomic.Uint64
		// Track the root round number to detect repeat UCs
		lastRootRound atomic.Uint64
		// Mutex to ensure sequential UC processing
		ucProcessingMutex sync.Mutex

		msgLoopCancelFn context.CancelFunc
		msgLoopDone     chan struct{}
		stopDone        chan struct{}

		// timestamp when last UC was received
		lastCertResponseTime atomic.Int64
		// timestamp when last certification request was sent (unix nano)
		certRequestTime atomic.Int64

		trustBaseStore TrustBaseStore

		// Optional proof material (partition view sidecar) intake. Staged views are
		// keyed by complete UC digest and published only once that exact UC is accepted.
		proofStaging  *proofmaterial.Staging
		proofSink     atomic.Pointer[proofmaterial.Sink]
		proofTopology proofmaterial.TopologyProvider
		// lastResponseRound is the newest shard input record round seen in a certification response.
		lastResponseRound atomic.Uint64
		// authn is the boundary which authenticates received certificates
		authn         *ucAuthenticator
		publishedMu   sync.Mutex
		published     map[proofmaterial.Digest]bool // recent accepted UC digests -> partition view delivered to the sink
		publishedFIFO []proofmaterial.Digest
	}

	BFTClient interface {
		Start(ctx context.Context) error
		Stop()
		WaitForInitialized(ctx context.Context) error
		CertificationRequest(ctx context.Context, block *models.Block) error
	}

	RoundManager interface {
		FinalizeBlock(ctx context.Context, block *models.Block) error
		FinalizeBlockWithRetry(ctx context.Context, block *models.Block) error
		// StartNewRound and StartNextRoundFromPrecollector take the reference
		// time the new round must pin: the seal timestamp of the certificate
		// that ended the previous round. The round builds every leaf value from
		// it and reports it as the input record timestamp, so it cannot be
		// re-read later without risking disagreement with leaves already
		// inserted.
		StartNewRound(ctx context.Context, roundNumber *api.BigInt, referenceTime uint64) error
		StartNextRoundFromPrecollector(ctx context.Context, roundNumber *api.BigInt, referenceTime uint64) error
		CommittedRoot(ctx context.Context) ([]byte, *api.BigInt, error)
	}

	DurableProposalFinalizer interface {
		FinalizeCertifiedProposal(ctx context.Context, blockNumber *api.BigInt, rootHash api.HexBytes, unicityCertificate api.HexBytes) (bool, error)
	}

	DurableProposalLoader interface {
		LoadDurableProposal(ctx context.Context, blockNumber *api.BigInt) (*models.Block, bool, error)
	}

	DurableProposalAbandoner interface {
		AbandonDurableProposal(ctx context.Context, blockNumber *api.BigInt, rootHash api.HexBytes) error
	}

	TrustBaseStore interface {
		GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
	}

	status int
)

func NewBFTClient(
	ctx context.Context,
	conf *config.BFTConfig,
	roundManager RoundManager,
	trustBaseStore TrustBaseStore,
	luc *types.UnicityCertificate,
	logger *logger.Logger,
	eventBus *events.EventBus,
) (*BFTClientImpl, error) {
	logger.Info("Creating BFT Client")
	bftClient := &BFTClientImpl{
		logger:         logger,
		partitionID:    conf.ShardConf.PartitionID,
		shardID:        conf.ShardConf.ShardID,
		roundManager:   roundManager,
		trustBaseStore: trustBaseStore,
		conf:           conf,
		eventBus:       eventBus,
		authn:          newUCAuthenticator(conf.ShardConf.NetworkID, trustBaseStore),
		proofStaging:   proofmaterial.NewStaging(proofmaterial.DefaultStagingEntries),
		published:      map[proofmaterial.Digest]bool{},
	}
	topo, err := proofTopologyFromConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("invalid consolidation topology: %w", err)
	}
	bftClient.proofTopology = topo
	bftClient.status.Store(idle)
	bftClient.luc.Store(luc)
	bftClient.lastCertResponseTime.Store(time.Now().UnixMilli())
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

	clientStatus := c.status.Load().(status)
	if clientStatus == stopping {
		return ErrBFTClientStopping
	}
	if clientStatus != idle {
		c.logger.WithContext(ctx).Warn("BFT Client is not idle, skipping start")
		return nil
	}
	c.status.Store(initializing)
	started := false
	var self *network.Peer
	defer func() {
		if started {
			return
		}
		if self != nil {
			if err := self.Close(); err != nil {
				c.logger.WithContext(ctx).Error("Failed to close partially initialized peer", "error", err)
			}
		}
		c.peer = nil
		c.network = nil
		c.signer = nil
		c.status.Store(idle)
	}()

	peerConf, err := c.conf.PeerConf()
	if err != nil {
		return fmt.Errorf("failed to create peer configuration: %w", err)
	}
	self, err = network.NewPeer(ctx, peerConf, c.logger.Logger, nil)
	if err != nil {
		return err
	}
	signer, err := c.conf.KeyConf.Signer()
	if err != nil {
		return fmt.Errorf("failed to create signer: %w", err)
	}

	networkP2P, err := NewLibP2PNetwork(ctx, self, c.logger, DefaultNetworkOptions)
	if err != nil {
		return fmt.Errorf("failed to create libp2p network: %w", err)
	}

	if err := self.BootstrapConnect(ctx, c.logger.Logger); err != nil {
		return fmt.Errorf("failed to bootstrap peer: %w", err)
	}

	c.peer = self
	c.network = networkP2P
	c.signer = signer

	msgLoopCtx, cancelFn := context.WithCancel(ctx)
	c.msgLoopCancelFn = cancelFn
	msgLoopDone := make(chan struct{})
	c.msgLoopDone = msgLoopDone
	nodeID := self.ID().String()
	go func() {
		defer close(msgLoopDone)
		c.logger.WithContext(ctx).Info("BFT client event loop started")
		if err := c.loop(msgLoopCtx, networkP2P, nodeID); err != nil {
			c.logger.Error("BFT event loop thread exited with error", "error", err.Error())
		} else {
			c.logger.Info("BFT event loop thread finished")
		}
	}()
	started = true

	return nil
}

func (c *BFTClientImpl) Stop() {
	c.logger.Info("Stopping BFT Client")
	c.mu.Lock()

	clientStatus := c.status.Load().(status)
	if clientStatus == idle {
		c.mu.Unlock()
		c.logger.Warn("BFT Client is already idle, skipping stop")
		return
	}
	if clientStatus == stopping {
		stopDone := c.stopDone
		c.mu.Unlock()
		if stopDone != nil {
			<-stopDone
		}
		return
	}

	c.status.Store(stopping)
	stopDone := make(chan struct{})
	c.stopDone = stopDone
	if c.msgLoopCancelFn != nil {
		c.msgLoopCancelFn()
		c.msgLoopCancelFn = nil
	}
	c.mu.Unlock()

	// Cancel first so in-flight UC processing can unblock before Stop waits for it.
	c.ucProcessingMutex.Lock()
	c.mu.Lock()
	if c.peer != nil {
		if err := c.peer.Close(); err != nil {
			c.logger.Error("Failed to close peer host", "error", err)
		}
		c.peer = nil
		c.network = nil
		c.signer = nil
	}

	// Proposal state belongs to this BFT session. The durable proposal in
	// storage is the recovery source after a restart or HA reactivation.
	c.proposedBlock = nil
	c.resumedDurableProposal = false
	c.certRequestTime.Store(0)

	msgLoopDone := c.msgLoopDone
	c.msgLoopDone = nil
	c.mu.Unlock()
	c.ucProcessingMutex.Unlock()

	if msgLoopDone != nil {
		<-msgLoopDone
	}

	c.mu.Lock()
	c.status.Store(idle)
	c.stopDone = nil
	close(stopDone)
	c.mu.Unlock()
}

func (c *BFTClientImpl) sendHandshake(ctx context.Context, bftNetwork *BftNetwork, nodeID string) error {
	metrics.BFTHandshakesTotal.WithLabelValues(c.shardID.String()).Inc()
	c.logger.WithContext(ctx).Debug("sending handshake to root chain")
	if bftNetwork == nil {
		return errors.New("BFT network is not initialized")
	}

	// load trust base
	rootEpoch := c.luc.Load().GetRootEpoch()
	tb, err := c.authn.trustBase(ctx, rootEpoch)
	if err != nil {
		return err
	}
	// select some random root nodes
	rootIDs, err := randomNodeSelector(tb, defaultHandshakeNodes)
	if err != nil {
		return fmt.Errorf("failed to select root nodes for handshake: %w", err)
	}
	if err = bftNetwork.Send(ctx,
		handshake.Handshake{
			PartitionID: c.PartitionID(),
			ShardID:     c.ShardID(),
			NodeID:      nodeID,
		},
		rootIDs...); err != nil {
		return fmt.Errorf("failed to send handshake: %w", err)
	}
	return nil
}

func (c *BFTClientImpl) loop(ctx context.Context, bftNetwork *BftNetwork, nodeID string) error {
	if bftNetwork == nil {
		return errors.New("BFT network is not initialized")
	}
	received := bftNetwork.ReceivedChannel()

	if err := c.sendHandshake(ctx, bftNetwork, nodeID); err != nil {
		return fmt.Errorf("failed to send initial handshake: %w", err)
	}

	heartbeat := time.NewTicker(c.conf.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-received:
			if !ok {
				return errors.New("network received channel is closed")
			}
			c.logger.WithContext(ctx).Debug("received message", "type", fmt.Sprintf("%T", m))
			c.handleMessage(ctx, m)
		case <-heartbeat.C:
			lastCertMillis := c.lastCertResponseTime.Load()
			lastCertTime := time.UnixMilli(lastCertMillis)
			if time.Since(lastCertTime) > c.conf.InactivityTimeout {
				c.logger.Warn("BFT client inactivity timeout exceeded, sending new handshake")
				if err := c.sendHandshake(ctx, bftNetwork, nodeID); err != nil {
					c.logger.Error("failed to send handshake on inactivity timeout", "error", err.Error())
				}
			}
		}
	}
}

func (c *BFTClientImpl) handleMessage(ctx context.Context, msg any) {
	switch mt := msg.(type) {
	case *certification.CertificationResponse:
		c.logger.WithContext(ctx).Info("received CertificationResponse")
		if err := c.handleCertificationResponse(ctx, mt); err != nil {
			c.logger.WithContext(ctx).Error("error processing CertificationResponse message", "error", err.Error())
		}
	case *certification.CertificationResponseV2:
		c.logger.WithContext(ctx).Info("received CertificationResponseV2")
		// The envelope is already structurally valid (version, framing). The ordinary
		// response goes through the unchanged handler; the opaque view is optional.
		if err := c.handleCertificationResponse(ctx, mt.Legacy()); err != nil {
			c.logger.WithContext(ctx).Error("error processing CertificationResponseV2 message", "error", err.Error())
		}
	default:
		c.logger.WithContext(ctx).Info("received unknown message")
	}
}

func (c *BFTClientImpl) handleCertificationResponse(ctx context.Context, cr *certification.CertificationResponse) error {
	metrics.BFTCertificationResponsesTotal.WithLabelValues(c.shardID.String()).Inc()
	c.noteResponseRound(cr)
	c.lastCertResponseTime.Store(time.Now().UnixMilli())

	// malformed nested shapes (null path elements) are rejected before anything evaluates them
	if err := validateUCShape(&cr.UC); err != nil {
		return fmt.Errorf("invalid CertificationResponse: malformed unicity certificate: %w", err)
	}
	if err := cr.IsValid(); err != nil {
		return fmt.Errorf("invalid CertificationResponse: %w", err)
	}

	// verify UC: configured network, exact root epoch, epoch start and quorum; the partition
	// view of the same response is authenticated within the same operation (one resolution of
	// the authority and history, one judgment of the shared seal)
	op := c.authn.begin(ctx)
	if err := op.verifyUC(&cr.UC, c.partitionID, c.shardID); err != nil {
		return fmt.Errorf("failed to verify UC: %w", err)
	}

	c.logger.WithContext(ctx).Info(fmt.Sprintf("handleCertificationResponse: UC round %d, next round %d, next leader %s",
		cr.UC.GetRoundNumber(), cr.Technical.Round, cr.Technical.Leader))

	if cr.Partition != c.PartitionID() || !cr.Shard.Equal(c.ShardID()) {
		return fmt.Errorf("got CertificationResponse for a wrong shard %s - %s", cr.Partition, cr.Shard)
	}

	// Only the response's own UC is processed; the view's other leaves are never dispatched.
	c.stagePartitionViewOp(ctx, op, cr)

	if err := c.handleUnicityCertificate(ctx, &cr.UC, &cr.Technical); err != nil {
		return err
	}
	c.publishSidecarFill(ctx, &cr.UC)
	return nil
}

func (c *BFTClientImpl) handleUnicityCertificate(ctx context.Context, uc *types.UnicityCertificate, tr *certification.TechnicalRecord) error {
	// Ensure sequential processing of UCs to prevent race conditions
	c.ucProcessingMutex.Lock()
	defer c.ucProcessingMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	c.logger.WithContext(ctx).Debug("Acquired UC processing lock",
		"ucRound", uc.GetRoundNumber(),
		"rootRound", uc.GetRootRoundNumber())

	prevLUC := c.luc.Load()
	wasInitializing := c.status.Load() == initializing
	previousRootRound := c.lastRootRound.Load()
	previousExpectedRound := c.nextExpectedRound.Load()
	previousExpectedEpoch := c.nextExpectedEpoch.Load()
	rollbackInitialization := func() {
		if !wasInitializing {
			return
		}
		c.luc.Store(prevLUC)
		c.lastRootRound.Store(previousRootRound)
		c.nextExpectedRound.Store(previousExpectedRound)
		c.nextExpectedEpoch.Store(previousExpectedEpoch)
	}
	completeInitialization := func() {
		if wasInitializing {
			c.status.CompareAndSwap(initializing, normal)
		}
	}

	if prevLUC != nil {
		if err := types.CheckNonEquivocatingCertificates(prevLUC, uc); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidUCSequence, err)
		}
	}
	// as we can be connected to several root nodes, we can receive the same UC multiple times
	if uc.IsDuplicate(prevLUC) {
		if !wasInitializing {
			c.logger.WithContext(ctx).Debug(fmt.Sprintf("duplicate UC (same root round %d)", uc.GetRootRoundNumber()))
			return nil
		}
		c.logger.WithContext(ctx).Info("Using retained UC to complete BFT client initialization",
			"partitionRound", uc.GetRoundNumber(),
			"rootRound", uc.GetRootRoundNumber())
	}

	isRepeat, err := uc.IsRepeat(prevLUC)
	if err != nil {
		return fmt.Errorf("failed to check repeat UC: %w", err)
	}
	if isRepeat {
		c.logger.WithContext(ctx).Warn("Received repeat UC - root chain timed out waiting for certification",
			"partitionRound", uc.GetRoundNumber(),
			"prevRootRound", prevLUC.GetRootRoundNumber(),
			"newRootRound", uc.GetRootRoundNumber())

		if err := c.abandonProposedBlockLocked(ctx, "repeat UC"); err != nil {
			return err
		}

		// Store the repeat UC and update root round tracking after durable
		// cleanup succeeds, so a failed cleanup can be retried by the next UC.
		c.luc.Store(uc)
		c.lastRootRound.Store(uc.GetRootRoundNumber())
		c.publishProofMaterial(ctx, uc, proofmaterial.KindRepeat, 0, false)

		// Start new round immediately with the next expected round
		nextRoundNumber := big.NewInt(0)
		nextRoundNumber.SetUint64(tr.Round)
		c.nextExpectedRound.Store(tr.Round)
		c.nextExpectedEpoch.Store(tr.Epoch)

		c.logger.WithContext(ctx).Info("Starting new round after repeat UC",
			"nextRoundNumber", nextRoundNumber.String())
		if err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc)); err != nil {
			rollbackInitialization()
			return err
		}
		completeInitialization()
		return nil
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
	c.nextExpectedEpoch.Store(tr.Epoch)

	if wasInitializing {
		c.logger.WithContext(ctx).Info("BFT client initialization finished",
			"nextRoundNumber", nextRoundNumber.String())

		recovered, err := c.finalizeCertifiedDurableProposalLocked(ctx, uc)
		if err != nil {
			rollbackInitialization()
			return err
		}
		if recovered {
			c.publishProofMaterial(ctx, uc, proofmaterial.KindFinalized, uc.GetRoundNumber(), true)
			c.logger.WithContext(ctx).Info("Durable proposal finalized from initialization UC",
				"ucRound", uc.GetRoundNumber(),
				"nextRoundNumber", nextRoundNumber.String())
			err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
			if err != nil {
				c.logger.WithContext(ctx).Error("Failed to start first round after durable proposal recovery",
					"nextRoundNumber", nextRoundNumber.String(),
					"error", err.Error())
				rollbackInitialization()
				return err
			}
			completeInitialization()
			return nil
		}
		resumed, err := c.resumeDurableProposalLocked(ctx, api.NewBigInt(nextRoundNumber))
		if err != nil {
			c.proposedBlock = nil
			c.resumedDurableProposal = false
			c.certRequestTime.Store(0)
			rollbackInitialization()
			return err
		}
		if resumed {
			completeInitialization()
			return nil
		}
		err = c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to start first round after initialization",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
			rollbackInitialization()
			return err
		}
		completeInitialization()
		return nil
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
				if err := c.abandonProposedBlockLocked(ctx, "round mismatch"); err != nil {
					return err
				}

				// Start new round immediately with root chain's next round
				c.logger.WithContext(ctx).Info("Starting new round to sync with root chain",
					"nextRoundNumber", nextRoundNumber.String())
				err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
				if err != nil {
					c.logger.WithContext(ctx).Error("Failed to start new round for sync",
						"nextRoundNumber", nextRoundNumber.String(),
						"error", err.Error())
				}
				return err
			}

			c.logger.WithContext(ctx).Warn("UC is newer than proposed block, abandoning stale proposal",
				"ucRound", expectedRound,
				"proposedBlockRound", proposedRound,
				"nextRound", tr.Round)
			if err := c.abandonProposedBlockLocked(ctx, "newer UC"); err != nil {
				return err
			}
			blockNum = "nil"
		}
	}

	// Check if we have a proposed block to finalize
	if c.proposedBlock == nil {
		if uc != nil && uc.InputRecord != nil {
			ucCbor, err := types.Cbor.Marshal(uc)
			if err != nil {
				c.logger.WithContext(ctx).Error("Failed to encode unicity certificate",
					"error", err.Error())
				return fmt.Errorf("failed to encode unicity certificate: %w", err)
			}
			if finalizer, ok := c.roundManager.(DurableProposalFinalizer); ok {
				recovered, err := finalizer.FinalizeCertifiedProposal(ctx,
					api.NewBigInt(new(big.Int).SetUint64(expectedRound)),
					api.HexBytes(append([]byte(nil), uc.InputRecord.Hash...)),
					api.NewHexBytes(ucCbor),
				)
				if err != nil {
					c.logger.WithContext(ctx).Error("Failed to finalize durable proposal",
						"ucRound", expectedRound,
						"error", err.Error())
					metrics.BFTErrorsTotal.Inc()
					c.publishFatalError(ctx, err)
					return fmt.Errorf("failed to finalize durable proposal: %w", err)
				}
				if recovered {
					c.publishProofMaterial(ctx, uc, proofmaterial.KindFinalized, expectedRound, true)
					c.logger.WithContext(ctx).Info("Durable proposal finalized from UC",
						"ucRound", expectedRound,
						"nextRoundNumber", nextRoundNumber.String())
					err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
					if err != nil {
						c.logger.WithContext(ctx).Error("Failed to start next round after durable proposal recovery",
							"nextRoundNumber", nextRoundNumber.String(),
							"error", err.Error())
					}
					return err
				}
			}
		}
		c.logger.WithContext(ctx).Warn("No proposed block to finalize, starting next round",
			"ucRound", expectedRound,
			"nextRoundNumber", nextRoundNumber.String())
		// This can happen if:
		// 1. We just started and haven't proposed a block yet
		// 2. We cleared the proposed block due to sync issues
		// 3. The root chain advanced without us sending a certification request

		// Start the next round directly
		err := c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to start next round",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
		}
		return err
	}

	if uc == nil || uc.InputRecord == nil {
		return fmt.Errorf("cannot finalize block %s: UC input record is missing", c.proposedBlock.Index.String())
	}
	if !bytes.Equal(uc.InputRecord.Hash, c.proposedBlock.RootHash) {
		err := fmt.Errorf("%w: block %s root %s, UC root %s",
			ErrCertifiedRootMismatch,
			c.proposedBlock.Index.String(),
			c.proposedBlock.RootHash.String(),
			api.HexBytes(uc.InputRecord.Hash).String())
		c.logger.WithContext(ctx).Error("UC root does not match proposed block root",
			"blockNumber", c.proposedBlock.Index.String(),
			"blockRoot", c.proposedBlock.RootHash.String(),
			"ucRoot", api.HexBytes(uc.InputRecord.Hash).String())
		if abandonErr := c.abandonProposedBlockLocked(ctx, "certified root mismatch"); abandonErr != nil {
			c.logger.WithContext(ctx).Error("Failed to abandon proposed block after certified root mismatch",
				"error", abandonErr.Error())
		}
		metrics.BFTErrorsTotal.Inc()
		c.publishFatalError(ctx, err)
		return err
	}

	c.logger.WithContext(ctx).Info("Finalizing block with unicity certificate",
		"blockNumber", blockNum,
		"ucRound", expectedRound)

	if certStart := c.certRequestTime.Swap(0); certStart > 0 {
		metrics.BFTCertificationDuration.Observe(float64(time.Now().UnixNano()-certStart) / 1e9)
	}

	ucCbor, err := types.Cbor.Marshal(uc)
	if err != nil {
		c.logger.WithContext(ctx).Error("Failed to encode unicity certificate",
			"error", err.Error())
		rollbackInitialization()
		return fmt.Errorf("failed to encode unicity certificate: %w", err)
	}
	c.proposedBlock.UnicityCertificate = api.NewHexBytes(ucCbor)
	// Persist the view only when it was received for exactly these UC bytes.
	c.proposedBlock.PartitionView = nil
	if view, ok := c.proofStaging.Get(sha256.Sum256(ucCbor)); ok {
		c.proposedBlock.PartitionView = api.NewHexBytes(view)
	}

	if c.resumedDurableProposal {
		finalizer, ok := c.roundManager.(DurableProposalFinalizer)
		if !ok {
			rollbackInitialization()
			return errors.New("round manager does not support durable proposal finalization")
		}
		recovered, err := finalizer.FinalizeCertifiedProposal(ctx,
			api.NewBigInt(new(big.Int).Set(c.proposedBlock.Index.Int)),
			api.HexBytes(append([]byte(nil), uc.InputRecord.Hash...)),
			api.NewHexBytes(ucCbor),
		)
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to finalize resumed durable proposal",
				"blockNumber", c.proposedBlock.Index.String(),
				"error", err.Error())
			metrics.BFTErrorsTotal.Inc()
			c.publishFatalError(ctx, err)
			rollbackInitialization()
			return fmt.Errorf("failed to finalize resumed durable proposal: %w", err)
		}
		if !recovered {
			rollbackInitialization()
			return fmt.Errorf("resumed durable proposal %s was not finalized", c.proposedBlock.Index.String())
		}
		c.publishProofMaterial(ctx, uc, proofmaterial.KindFinalized, c.proposedBlock.Index.Uint64(), true)
		c.proposedBlock = nil
		c.resumedDurableProposal = false

		c.logger.WithContext(ctx).Info("Resumed durable proposal finalized, starting new round",
			"nextRoundNumber", nextRoundNumber.String())
		err = c.roundManager.StartNewRound(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
		if err != nil {
			c.logger.WithContext(ctx).Error("Failed to start new round",
				"nextRoundNumber", nextRoundNumber.String(),
				"error", err.Error())
			rollbackInitialization()
			return err
		}
		completeInitialization()
		return nil
	}

	if err := c.roundManager.FinalizeBlockWithRetry(ctx, c.proposedBlock); err != nil {
		c.logger.WithContext(ctx).Error("Failed to finalize block after retries",
			"blockNumber", c.proposedBlock.Index.String(),
			"error", err.Error())
		metrics.BFTErrorsTotal.Inc()
		c.publishFatalError(ctx, err)
		return fmt.Errorf("failed to finalize block after retries: %w", err)
	}

	c.publishProofMaterial(ctx, uc, proofmaterial.KindFinalized, c.proposedBlock.Index.Uint64(), true)

	// Clear the proposed block after finalization
	c.proposedBlock = nil
	c.resumedDurableProposal = false

	c.logger.WithContext(ctx).Info("Block finalized, starting new round",
		"nextRoundNumber", nextRoundNumber.String())

	err = c.roundManager.StartNextRoundFromPrecollector(ctx, api.NewBigInt(nextRoundNumber), referenceTimeFromUC(uc))
	if err != nil {
		c.logger.WithContext(ctx).Error("Failed to start new round",
			"nextRoundNumber", nextRoundNumber.String(),
			"error", err.Error())
	}
	return err
}

// proofTopologyFromConfig derives the OPTIONAL independently pinned topology used to
// validate partition views more strictly (BFT_CONSOLIDATION_TOPOLOGY_FILE). Without it
// views are authenticated by the seal alone: the scheme and every configuration hash are
// the ones the root quorum signed. Returns nil when no pins are configured.
func proofTopologyFromConfig(conf *config.BFTConfig) (proofmaterial.TopologyProvider, error) {
	if conf == nil || conf.ShardConf == nil || len(conf.ConsolidationShards) == 0 {
		return nil, nil
	}
	scheme := make(types.ShardingScheme, 0, len(conf.ConsolidationShards))
	hashes := make(map[string][]byte, len(conf.ConsolidationShards))
	for _, sh := range conf.ConsolidationShards {
		h, err := hex.DecodeString(strings.TrimPrefix(sh.ConfigHash, "0x"))
		if err != nil {
			return nil, fmt.Errorf("shard %q configuration hash: %w", sh.ShardID, err)
		}
		if _, dup := hashes[sh.ShardID.Key()]; dup {
			return nil, fmt.Errorf("duplicate shard %q", sh.ShardID)
		}
		scheme = append(scheme, sh.ShardID)
		hashes[sh.ShardID.Key()] = h
	}
	if len(scheme) == 1 && scheme[0].Length() == 0 {
		scheme = types.ShardingScheme{}
	}
	t, err := proofmaterial.NewStaticTopology(conf.ShardConf.PartitionID, scheme, hashes)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// noteResponseRound counts responses which certify no new shard block: the shard input
// record round must advance beyond the newest one already received for the response to
// carry new work (new finalized block). Re-deliveries, timeout repeats and extra traffic
// with an advancing root round all count as non-progress.
func (c *BFTClientImpl) noteResponseRound(cr *certification.CertificationResponse) {
	if cr == nil || cr.UC.InputRecord == nil {
		return
	}
	round := cr.UC.InputRecord.RoundNumber
	for {
		last := c.lastResponseRound.Load()
		if round > last {
			if c.lastResponseRound.CompareAndSwap(last, round) {
				return
			}
			continue
		}
		metrics.BFTCertificationResponseNonProgressTotal.WithLabelValues(c.shardID.String()).Inc()
		return
	}
}

// ProofTopology returns the optional independently pinned topology (nil: none configured).
func (c *BFTClientImpl) ProofTopology() proofmaterial.TopologyProvider { return c.proofTopology }

// SealVerifier returns the client's trusted seal verifier (authority resolved from the
// admitted trust history) for re-authenticating stored material.
func (c *BFTClientImpl) SealVerifier(ctx context.Context) types.SealVerifier {
	return c.authn.sealVerifier(ctx)
}

// SetProofMaterialSink sets the consumer of published proof material (A4 wires the
// retained-view catalog here). Safe to call before or after Start; nil resets to discard.
func (c *BFTClientImpl) SetProofMaterialSink(sink proofmaterial.Sink) {
	if sink == nil {
		c.proofSink.Store(nil)
		return
	}
	c.proofSink.Store(&sink)
}

// SetProofTopologyProvider sets the optional trusted topology source used to validate
// partition views. Call before Start.
func (c *BFTClientImpl) SetProofTopologyProvider(p proofmaterial.TopologyProvider) {
	c.proofTopology = p
}

// stagePartitionView validates the sidecar of an already authenticated UC and stages it.
// Every failure only means missing proof material (metric + bounded log).
func (c *BFTClientImpl) stagePartitionView(ctx context.Context, cr *certification.CertificationResponse) {
	if cr.PartitionView == nil {
		return // legacy v1 response, or v2 with null view
	}
	c.stagePartitionViewOp(ctx, c.authn.begin(ctx), cr)
}

// stagePartitionViewOp is stagePartitionView within the intake operation "op".
func (c *BFTClientImpl) stagePartitionViewOp(ctx context.Context, op *authOp, cr *certification.CertificationResponse) {
	if cr.PartitionView == nil {
		return
	}
	if proofmaterial.IsUnshardedUC(&cr.UC) {
		// the UC of an unsharded partition is complete by itself: the sidecar of a one-leaf
		// partition is neither validated, staged, persisted nor served
		metrics.BFTPartitionViewTotal.WithLabelValues("unsharded_skipped").Inc()
		return
	}
	result, view, err := proofmaterial.ValidateView(cr.PartitionView, &cr.UC, c.partitionID, op.sealVerifier(), c.proofTopology)
	metrics.BFTPartitionViewTotal.WithLabelValues(result).Inc()
	if err != nil {
		c.logger.WithContext(ctx).Warn("Ignoring partition view sidecar",
			"result", result, "error", truncate(err.Error(), 200), "rootRound", cr.UC.GetRootRoundNumber())
		return
	}
	digest, _, err := proofmaterial.DigestOfUC(&cr.UC)
	if err != nil {
		return
	}
	c.proofStaging.Put(digest, view)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

const maxPublishedTracked = 16

// publishProofMaterial hands the accepted UC (with its staged view, if any) to the sink.
// Sink failures never affect certification.
func (c *BFTClientImpl) publishProofMaterial(ctx context.Context, uc *types.UnicityCertificate, kind proofmaterial.Kind, blockNumber uint64, hasBlock bool) {
	digest, ucBytes, err := proofmaterial.DigestOfUC(uc)
	if err != nil {
		c.logger.WithContext(ctx).Warn("Not publishing proof material", "error", err.Error())
		return
	}
	view, _ := c.proofStaging.Get(digest)
	c.deliverProofMaterial(ctx, &proofmaterial.Material{
		Kind: kind, UC: uc, UCBytes: ucBytes, UCDigest: digest,
		PartitionView: view, BlockNumber: blockNumber, HasBlock: hasBlock,
	})
}

func (c *BFTClientImpl) deliverProofMaterial(ctx context.Context, m *proofmaterial.Material) {
	// The UC is tracked as accepted from now on; its view counts as delivered only once
	// the sink has accepted it, so a failed fill stays eligible for later duplicates.
	c.trackPublished(m.UCDigest, false)

	sink := c.proofSink.Load()
	if sink == nil {
		return
	}
	if err := (*sink).PublishProofMaterial(ctx, m); err != nil {
		c.logger.WithContext(ctx).Warn("Publishing proof material failed",
			"kind", m.Kind.String(), "error", err.Error())
		return
	}
	if len(m.PartitionView) > 0 {
		c.trackPublished(m.UCDigest, true)
	}
}

// trackPublished records an accepted UC; viewDelivered only ever moves to true.
func (c *BFTClientImpl) trackPublished(d proofmaterial.Digest, viewDelivered bool) {
	c.publishedMu.Lock()
	defer c.publishedMu.Unlock()
	if c.published == nil {
		c.published = map[proofmaterial.Digest]bool{}
	}
	prev, seen := c.published[d]
	if !seen {
		c.publishedFIFO = append(c.publishedFIFO, d)
		if len(c.publishedFIFO) > maxPublishedTracked {
			delete(c.published, c.publishedFIFO[0])
			c.publishedFIFO = c.publishedFIFO[1:]
		}
	}
	c.published[d] = prev || viewDelivered
}

// publishSidecarFill handles a duplicate delivery of an already published UC which
// carried a valid view the earlier publication lacked. The UC must be the one this
// client already accepted and published; nothing is re-executed.
func (c *BFTClientImpl) publishSidecarFill(ctx context.Context, uc *types.UnicityCertificate) {
	digest, ucBytes, err := proofmaterial.DigestOfUC(uc)
	if err != nil {
		return
	}
	view, ok := c.proofStaging.Get(digest)
	if !ok {
		return
	}
	c.publishedMu.Lock()
	hadView, seen := c.published[digest]
	c.publishedMu.Unlock()
	if !seen || hadView {
		return
	}
	c.deliverProofMaterial(ctx, &proofmaterial.Material{
		Kind: proofmaterial.KindSidecarFill, UC: uc, UCBytes: ucBytes, UCDigest: digest, PartitionView: view,
	})
}

func (c *BFTClientImpl) publishFatalError(ctx context.Context, err error) {
	if errors.Is(err, context.Canceled) {
		c.logger.WithContext(ctx).Info("Ignoring finalization cancellation during BFT client shutdown")
		return
	}
	if c.eventBus != nil {
		c.eventBus.Publish(events.TopicFatalError, events.FatalErrorEvent{
			Source: "bft",
			Error:  err.Error(),
		})
	}
}

func (c *BFTClientImpl) finalizeCertifiedDurableProposalLocked(ctx context.Context, uc *types.UnicityCertificate) (bool, error) {
	if uc == nil || uc.InputRecord == nil {
		return false, nil
	}
	if c.roundManager == nil {
		return false, errors.New("round manager is not initialized")
	}
	localRoot, localBlockNumber, err := c.roundManager.CommittedRoot(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to read local committed SMT state: %w", err)
	}
	if uc.GetRoundNumber() == 0 && len(uc.InputRecord.Hash) == 0 && localBlockNumber == nil {
		return false, nil
	}
	if bytes.Equal(localRoot, uc.InputRecord.Hash) {
		return false, nil
	}
	finalizer, ok := c.roundManager.(DurableProposalFinalizer)
	if !ok {
		return false, nil
	}
	ucCbor, err := types.Cbor.Marshal(uc)
	if err != nil {
		c.logger.WithContext(ctx).Error("Failed to encode unicity certificate",
			"error", err.Error())
		return false, fmt.Errorf("failed to encode unicity certificate: %w", err)
	}
	recovered, err := finalizer.FinalizeCertifiedProposal(ctx,
		api.NewBigInt(new(big.Int).SetUint64(uc.GetRoundNumber())),
		api.HexBytes(append([]byte(nil), uc.InputRecord.Hash...)),
		api.NewHexBytes(ucCbor),
	)
	if err != nil {
		return false, err
	}
	return recovered, nil
}

func (c *BFTClientImpl) abandonProposedBlockLocked(ctx context.Context, reason string) error {
	if c.proposedBlock == nil {
		return nil
	}
	block := c.proposedBlock
	c.logger.WithContext(ctx).Info("Abandoning proposed block",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"reason", reason)
	if abandoner, ok := c.roundManager.(DurableProposalAbandoner); ok {
		if err := abandoner.AbandonDurableProposal(ctx, block.Index, block.RootHash); err != nil {
			return fmt.Errorf("failed to abandon durable proposal %s: %w", block.Index.String(), err)
		}
	}
	c.proposedBlock = nil
	c.resumedDurableProposal = false
	return nil
}

func (c *BFTClientImpl) resumeDurableProposalLocked(ctx context.Context, roundNumber *api.BigInt) (bool, error) {
	loader, ok := c.roundManager.(DurableProposalLoader)
	if !ok {
		return false, nil
	}
	block, found, err := loader.LoadDurableProposal(ctx, roundNumber)
	if err != nil {
		return false, fmt.Errorf("failed to load durable proposal for round %s: %w", roundNumber.String(), err)
	}
	if !found {
		return false, nil
	}
	if block == nil {
		return false, fmt.Errorf("durable proposal for round %s is nil", roundNumber.String())
	}
	if block.Index.Cmp(roundNumber.Int) != 0 {
		return false, fmt.Errorf("durable proposal round mismatch: expected %s, got %s", roundNumber.String(), block.Index.String())
	}

	c.logger.WithContext(ctx).Info("Resending durable proposal after BFT initialization",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String())
	c.proposedBlock = block
	c.resumedDurableProposal = true
	c.certRequestTime.Store(time.Now().UnixNano())
	if err := c.sendCertificationRequest(ctx, block.RootHash.String(), block.Index.Uint64(), block.ReferenceTime); err != nil {
		metrics.BFTErrorsTotal.Inc()
		return true, fmt.Errorf("failed to resend durable proposal %s: %w", block.Index.String(), err)
	}
	return true, nil
}

func (c *BFTClientImpl) sendCertificationRequest(ctx context.Context, rootHash string, roundNumber uint64, referenceTime uint64) error {
	rootHashBytes, err := hex.DecodeString(rootHash)
	if err != nil {
		return fmt.Errorf("failed to decode root hash: %w", err)
	}

	luc, err := c.waitForLatestUC(ctx)
	if err != nil {
		return fmt.Errorf("failed to prepare certification request: %w", err)
	}

	inputRecord, err := c.buildCertificationInputRecord(luc, rootHashBytes, roundNumber, referenceTime)
	if err != nil {
		return err
	}
	if err := c.verifyLocalRootExtendsLatestUC(ctx, luc); err != nil {
		return err
	}
	c.mu.Lock()
	bftNetwork := c.network
	peer := c.peer
	signer := c.signer
	c.mu.Unlock()
	if bftNetwork == nil || peer == nil || signer == nil {
		return errors.New("BFT client network is not initialized")
	}

	// send new input record for certification
	req := &certification.BlockCertificationRequest{
		PartitionID: c.PartitionID(),
		ShardID:     c.ShardID(),
		NodeID:      peer.ID().String(),
		InputRecord: inputRecord,
	}

	if err = req.Sign(signer); err != nil {
		return fmt.Errorf("failed to sign certification request: %w", err)
	}
	c.logger.WithContext(ctx).Info(fmt.Sprintf("Round %d sending block certification request to root chain, IR hash %X",
		req.InputRecord.RoundNumber, req.InputRecord.Hash))

	tb, err := c.authn.trustBase(ctx, luc.GetRootEpoch())
	if err != nil {
		return err
	}
	rootIDs, err := rootNodesSelector(luc, tb, defaultNofRootNodes)
	if err != nil {
		return fmt.Errorf("selecting root nodes: %w", err)
	}
	return bftNetwork.Send(ctx, req, rootIDs...)
}

// referenceTimeFromUC returns the reference time a round following uc must pin:
// the timestamp of the seal that certified the previous round.
func referenceTimeFromUC(uc *types.UnicityCertificate) uint64 {
	if uc == nil || uc.UnicitySeal == nil {
		return 0
	}
	return uc.UnicitySeal.Timestamp
}

func (c *BFTClientImpl) buildCertificationInputRecord(luc *types.UnicityCertificate, rootHashBytes []byte, roundNumber uint64, referenceTime uint64) (*types.InputRecord, error) {
	if luc == nil || luc.InputRecord == nil || luc.UnicitySeal == nil {
		return nil, errors.New("latest UC is incomplete")
	}
	// The round built its leaf values from referenceTime. Re-reading the latest
	// seal here instead would certify a root the leaves do not correspond to
	// whenever a repeat certificate arrived mid-round.
	if referenceTime != luc.UnicitySeal.Timestamp {
		return nil, fmt.Errorf("%w: round reference time %d does not match latest seal timestamp %d",
			ErrStaleCertificationRound,
			referenceTime, luc.UnicitySeal.Timestamp)
	}

	var blockHash []byte
	if !bytes.Equal(rootHashBytes, luc.InputRecord.Hash) {
		blockHash = rootHashBytes
	}

	epoch := luc.InputRecord.Epoch
	if expectedEpoch := c.nextExpectedEpoch.Load(); expectedEpoch > 0 {
		epoch = expectedEpoch
	}

	return &types.InputRecord{
		Version:         1,
		RoundNumber:     roundNumber,
		Epoch:           epoch,
		PreviousHash:    luc.InputRecord.Hash,
		Hash:            rootHashBytes,
		SummaryValue:    []byte{}, // cant be nil if RoundNumber > 0
		Timestamp:       referenceTime,
		BlockHash:       blockHash,
		SumOfEarnedFees: 0,
		ETHash:          nil, // can be nil, not validated
	}, nil
}

func (c *BFTClientImpl) verifyLocalRootExtendsLatestUC(ctx context.Context, luc *types.UnicityCertificate) error {
	if luc == nil || luc.InputRecord == nil {
		return errors.New("latest UC input record is missing")
	}
	if c.roundManager == nil {
		return errors.New("round manager is not initialized")
	}
	localRoot, localBlockNumber, err := c.roundManager.CommittedRoot(ctx)
	if err != nil {
		return fmt.Errorf("failed to read local committed SMT state: %w", err)
	}
	// BFT represents the genesis/no-state root as empty bytes; the local SMT
	// represents its empty tree as a real hash. After the first committed block,
	// both sides use the aggregator-computed root and must match exactly.
	if luc.GetRoundNumber() == 0 && len(luc.InputRecord.Hash) == 0 && localBlockNumber == nil {
		return nil
	}
	if !bytes.Equal(localRoot, luc.InputRecord.Hash) {
		return fmt.Errorf("%w: local root %s, latest UC root %s",
			ErrCertifiedStateMismatch,
			api.HexBytes(localRoot).String(),
			api.HexBytes(luc.InputRecord.Hash).String())
	}
	return nil
}

func (c *BFTClientImpl) CertificationRequest(ctx context.Context, block *models.Block) error {
	c.logger.WithContext(ctx).Info("CertificationRequest called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"lastRootRound", c.lastRootRound.Load())

	if err := c.ensureInitialized(ctx); err != nil {
		return err
	}

	c.ucProcessingMutex.Lock()
	defer c.ucProcessingMutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	// The block root is computed for its original block number. If BFT has
	// already moved to another round, this proposal is stale and must be dropped.
	expectedRound := c.nextExpectedRound.Load()
	if expectedRound > 0 {
		blockNumber := block.Index.Uint64()
		if expectedRound != blockNumber {
			c.logger.WithContext(ctx).Warn("Rejecting stale certification request",
				"blockNumber", blockNumber,
				"expectedRound", expectedRound,
				"difference", int64(expectedRound)-int64(blockNumber))
			return fmt.Errorf("%w: expected round %d, got %d", ErrStaleCertificationRound, expectedRound, blockNumber)
		}
		c.logger.WithContext(ctx).Debug("Block number matches root chain expectations",
			"blockNumber", expectedRound)
	} else {
		// No expected round yet - this might be the very first request
		c.logger.WithContext(ctx).Debug("No expected round number from root chain yet, using proposed block number",
			"blockNumber", block.Index.String())
		// If we have a last UC, we can infer the expected round
		if luc := c.luc.Load(); luc != nil {
			// The next round should be the UC round + 1
			inferredRound := luc.GetRoundNumber() + 1
			if inferredRound != block.Index.Uint64() {
				c.logger.WithContext(ctx).Warn("Rejecting stale certification request inferred from last UC",
					"lastUCRound", luc.GetRoundNumber(),
					"inferredRound", inferredRound,
					"proposedRound", block.Index.Uint64())
				return fmt.Errorf("%w: expected round %d, got %d", ErrStaleCertificationRound, inferredRound, block.Index.Uint64())
			}
		}
	}

	c.proposedBlock = block
	c.resumedDurableProposal = false
	c.certRequestTime.Store(time.Now().UnixNano())

	c.logger.WithContext(ctx).Debug("Sending certification request",
		"blockNumber", block.Index.String(),
		"roundNumber", block.Index.Uint64())

	if err := c.sendCertificationRequest(ctx, block.RootHash.String(), block.Index.Uint64(), block.ReferenceTime); err != nil {
		c.logger.WithContext(ctx).Error("Failed to send certification request",
			"blockNumber", block.Index.String(),
			"error", err.Error())
		metrics.BFTErrorsTotal.Inc()
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

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if c.status.Load() == normal {
				return nil
			}
		}
	}
}

// WaitForInitialized blocks until the BFT client has received its first UC and is ready.
// This should be called after Start() and before attempting any certification requests.
func (c *BFTClientImpl) WaitForInitialized(ctx context.Context) error {
	if c.status.Load() == normal {
		return nil
	}

	c.logger.WithContext(ctx).Info("Waiting for BFT client initialization (first UC from root chain)")

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("timeout waiting for BFT client initialization - no UC received from root chain within 30s")
			}
			return ctx.Err()
		case <-ticker.C:
			if c.status.Load() == normal {
				c.logger.WithContext(ctx).Info("BFT client initialized successfully")
				return nil
			}
		}
	}
}
