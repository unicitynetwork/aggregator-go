package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ParentAggregatorService implements the business logic for the parent aggregator
type ParentAggregatorService struct {
	config             *config.Config
	logger             *logger.Logger
	storage            interfaces.Storage
	parentRoundManager *round.ParentRoundManager
	leaderSelector     LeaderSelector
	trustBaseValidator *trustbase.TrustBaseValidator
}

func (pas *ParentAggregatorService) isLeader(ctx context.Context) (bool, error) {
	if pas.leaderSelector == nil {
		return true, nil
	}

	isLeader, err := pas.leaderSelector.IsLeader(ctx)
	if err != nil {
		pas.logger.WithContext(ctx).Error("Failed to determine leadership status", "error", err.Error())
		return false, err
	}

	return isLeader, nil
}

// NewParentAggregatorService creates a new parent aggregator service
func NewParentAggregatorService(
	cfg *config.Config,
	logger *logger.Logger,
	parentRoundManager *round.ParentRoundManager,
	storage interfaces.Storage,
	leaderSelector LeaderSelector,
) *ParentAggregatorService {
	return &ParentAggregatorService{
		config:             cfg,
		logger:             logger,
		storage:            storage,
		parentRoundManager: parentRoundManager,
		leaderSelector:     leaderSelector,
		trustBaseValidator: trustbase.NewTrustBaseValidator(storage.TrustBaseStorage()),
	}
}

// Start starts the parent aggregator service
func (pas *ParentAggregatorService) Start(ctx context.Context) error {
	pas.logger.WithContext(ctx).Info("Starting Parent Aggregator Service")

	if err := pas.parentRoundManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start parent round manager: %w", err)
	}

	pas.logger.WithContext(ctx).Info("Parent Aggregator Service started successfully")
	return nil
}

// Stop stops the parent aggregator service
func (pas *ParentAggregatorService) Stop(ctx context.Context) error {
	pas.logger.WithContext(ctx).Info("Stopping Parent Aggregator Service")

	if err := pas.parentRoundManager.Stop(ctx); err != nil {
		pas.logger.WithContext(ctx).Error("Failed to stop parent round manager", "error", err.Error())
		return fmt.Errorf("failed to stop parent round manager: %w", err)
	}

	pas.logger.WithContext(ctx).Info("Parent Aggregator Service stopped successfully")
	return nil
}

// SubmitShardRoot handles shard root submission from child aggregators
func (pas *ParentAggregatorService) SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error) {
	isLeader, err := pas.isLeader(ctx)
	if err != nil {
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInternalError,
		}, nil
	}

	if !isLeader {
		pas.logger.WithContext(ctx).Warn("Rejecting shard root submission because node is not leader",
			"shardId", req.ShardID)
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusNotLeader,
		}, nil
	}

	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID", "shardId", req.ShardID, "error", err.Error())
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInvalidShardID,
		}, nil
	}

	update := models.NewShardRootUpdate(req.ShardID, req.RootHash)

	err = pas.parentRoundManager.SubmitShardRoot(ctx, update)
	if err != nil {
		pas.logger.WithContext(ctx).Error("Failed to submit shard root to round manager", "error", err.Error())
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInternalError,
		}, nil
	}

	pas.logger.WithContext(ctx).Info("Shard root update accepted",
		"shardId", req.ShardID,
		"rootHash", req.RootHash.String())

	return &api.SubmitShardRootResponse{
		Status: api.ShardRootStatusSuccess,
	}, nil
}

// GetShardProof handles shard proof requests
func (pas *ParentAggregatorService) GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error) {
	if pas.leaderSelector != nil {
		isLeader, err := pas.isLeader(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to determine leadership status: %w", err)
		}
		if !isLeader {
			pas.logger.WithContext(ctx).Debug("Serving shard proof while node is follower")
		}
	}

	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID", "shardId", req.ShardID, "error", err.Error())
		return nil, fmt.Errorf("invalid shard ID: %w", err)
	}

	pas.logger.WithContext(ctx).Debug("Shard proof requested", "shardId", req.ShardID)

	shardPath := new(big.Int).SetInt64(int64(req.ShardID))
	merkleTreePath, err := pas.parentRoundManager.GetSMT().GetPath(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get merkle tree path: %w", err)
	}

	var proofPath *api.MerkleTreePath
	if len(merkleTreePath.Steps) > 0 && merkleTreePath.Steps[0].Data != nil {
		proofPath = merkleTreePath
		pas.logger.WithContext(ctx).Info("Generated shard proof from current state",
			"shardId", req.ShardID,
			"rootHash", merkleTreePath.Root)
	} else {
		proofPath = nil
		pas.logger.WithContext(ctx).Info("Shard has not submitted root yet, returning nil proof",
			"shardId", req.ShardID)
	}

	latestBlock, err := pas.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	var unicityCertificate api.HexBytes
	if latestBlock != nil {
		unicityCertificate = latestBlock.UnicityCertificate
	}

	return &api.GetShardProofResponse{
		MerkleTreePath:     proofPath,
		UnicityCertificate: unicityCertificate,
	}, nil
}

func (pas *ParentAggregatorService) validateShardID(shardID api.ShardID) error {
	if shardID <= 1 {
		return errors.New("shard ID must be positive and have at least 2 bits")
	}

	// ShardID encoding: MSB=1 (prefix bit) + ShardIDLength bits for actual shard identifier
	// For ShardIDLength=1: valid range is [2,3] (0b10, 0b11)
	// For ShardIDLength=2: valid range is [4,7] (0b100-0b111)
	// The prefix bit ensures leading zeros are preserved in the path calculation

	minShardID := int64(1 << pas.config.Sharding.ShardIDLength)
	maxShardID := int64((1 << (pas.config.Sharding.ShardIDLength + 1)) - 1)

	shardValue := int64(shardID)

	if shardValue < minShardID {
		return fmt.Errorf("shard ID %d is below minimum %d for %d-bit shard IDs (MSB prefix bit not set)",
			shardValue, minShardID, pas.config.Sharding.ShardIDLength)
	}

	if shardValue > maxShardID {
		return fmt.Errorf("shard ID %d exceeds maximum %d for %d-bit shard IDs",
			shardValue, maxShardID, pas.config.Sharding.ShardIDLength)
	}

	return nil
}

// SubmitCommitment - not used in parent mode
func (pas *ParentAggregatorService) SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	return nil, fmt.Errorf("submit_commitment is not supported in parent mode - use submit_shard_root instead")
}

// GetInclusionProof - not used in parent mode
func (pas *ParentAggregatorService) GetInclusionProof(ctx context.Context, req *api.GetInclusionProofRequest) (*api.GetInclusionProofResponse, error) {
	return nil, fmt.Errorf("get_inclusion_proof is not supported in parent mode - use get_shard_proof instead")
}

// GetNoDeletionProof - TODO: implement
func (pas *ParentAggregatorService) GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error) {
	return nil, fmt.Errorf("get_no_deletion_proof not implemented yet in parent mode")
}

// GetBlockHeight retrieves the current parent block height
func (pas *ParentAggregatorService) GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error) {
	latestBlockNumber, err := pas.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest parent block number: %w", err)
	}

	return &api.GetBlockHeightResponse{
		BlockNumber: latestBlockNumber,
	}, nil
}

// GetBlock - TODO: implement
func (pas *ParentAggregatorService) GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error) {
	return nil, fmt.Errorf("get_block not implemented yet in parent mode")
}

// GetBlockCommitments - TODO: implement
func (pas *ParentAggregatorService) GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error) {
	return nil, fmt.Errorf("get_block_commitments not implemented yet in parent mode")
}

// GetHealthStatus retrieves the health status of the parent aggregator service
func (pas *ParentAggregatorService) GetHealthStatus(ctx context.Context) (*api.HealthStatus, error) {
	// Check if HA is enabled and determine role
	var role string
	if pas.leaderSelector != nil {
		isLeader, err := pas.leaderSelector.IsLeader(ctx)
		if err != nil {
			pas.logger.WithContext(ctx).Warn("Failed to check leadership status", "error", err.Error())
			// Don't fail health check on leadership query failure
			isLeader = false
		}

		if isLeader {
			role = "parent-leader"
		} else {
			role = "parent-follower"
		}
	} else {
		role = "parent-standalone"
	}

	sharding := api.Sharding{
		Mode:       pas.config.Sharding.Mode.String(),
		ShardIDLen: pas.config.Sharding.ShardIDLength,
	}
	status := models.NewHealthStatus(role, pas.config.HA.ServerID, sharding)

	// Add database connectivity check
	if err := pas.storage.Ping(ctx); err != nil {
		status.Status = "unhealthy"
		status.AddDetail("database", "disconnected")
		pas.logger.WithContext(ctx).Error("Database health check failed", "error", err.Error())
	} else {
		status.AddDetail("database", "connected")
	}

	return modelToAPIHealthStatus(status), nil
}

// PutTrustBase verifies and stores the trust base.
func (pas *ParentAggregatorService) PutTrustBase(ctx context.Context, trustBase *types.RootTrustBaseV1) error {
	if err := pas.trustBaseValidator.Verify(ctx, trustBase); err != nil {
		return fmt.Errorf("failed to verify trust base: %w", err)
	}
	return pas.storage.TrustBaseStorage().Store(ctx, trustBase)
}
