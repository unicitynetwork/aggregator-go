package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ParentAggregatorService implements the business logic for the parent aggregator
type ParentAggregatorService struct {
	config             *config.Config
	logger             *logger.Logger
	storage            interfaces.Storage
	parentRoundManager *round.ParentRoundManager
	leaderSelector     LeaderSelector
}

// NewParentAggregatorService creates a new parent aggregator service
func NewParentAggregatorService(cfg *config.Config, logger *logger.Logger, parentRoundManager *round.ParentRoundManager, storage interfaces.Storage, leaderSelector LeaderSelector) *ParentAggregatorService {
	return &ParentAggregatorService{
		config:             cfg,
		logger:             logger,
		storage:            storage,
		parentRoundManager: parentRoundManager,
		leaderSelector:     leaderSelector,
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
	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID", "shardId", req.ShardID.String(), "error", err.Error())
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInvalidShardID,
		}, nil
	}

	update := models.NewShardRootUpdate(req.ShardID, req.RootHash)

	err := pas.parentRoundManager.SubmitShardRoot(ctx, update)
	if err != nil {
		pas.logger.WithContext(ctx).Error("Failed to submit shard root to round manager", "error", err.Error())
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInternalError,
		}, nil
	}

	pas.logger.WithContext(ctx).Info("Shard root update accepted",
		"shardId", req.ShardID.String(),
		"rootHash", req.RootHash.String())

	return &api.SubmitShardRootResponse{
		Status: api.ShardRootStatusSuccess,
	}, nil
}

// GetShardProof handles shard proof requests
func (pas *ParentAggregatorService) GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error) {
	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID", "shardId", req.ShardID.String(), "error", err.Error())
		return nil, fmt.Errorf("invalid shard ID: %w", err)
	}

	pas.logger.WithContext(ctx).Info("Shard proof requested", "shardId", req.ShardID.String())

	shardPath := new(big.Int).SetBytes(req.ShardID)
	merkleTreePath := pas.parentRoundManager.GetSMT().GetPath(shardPath)

	var proofPath *api.MerkleTreePath
	if len(merkleTreePath.Steps) > 0 && merkleTreePath.Steps[0].Branch != nil {
		proofPath = merkleTreePath
		pas.logger.WithContext(ctx).Info("Generated shard proof from current state",
			"shardId", req.ShardID.String(),
			"rootHash", merkleTreePath.Root)
	} else {
		proofPath = nil
		pas.logger.WithContext(ctx).Info("Shard has not submitted root yet, returning nil proof",
			"shardId", req.ShardID.String())
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

func (pas *ParentAggregatorService) validateShardID(shardID api.HexBytes) error {
	if len(shardID) < 2 {
		return fmt.Errorf("shard ID too short, must have at least 2 bytes")
	}

	if shardID[0] != 0x01 {
		return fmt.Errorf("shard ID must have 0x01 prefix, got: %s", shardID.String())
	}

	shardValueBytes := shardID[1:]
	shardValue := new(big.Int).SetBytes(shardValueBytes)

	maxShardID := int64((1 << pas.config.Sharding.ShardIDLength) - 1)
	maxShardIDBig := big.NewInt(maxShardID)
	if shardValue.Cmp(maxShardIDBig) > 0 {
		return fmt.Errorf("shard ID value %s exceeds maximum %d for %d-bit shard IDs",
			shardValue.String(), maxShardID, pas.config.Sharding.ShardIDLength)
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

	status := models.NewHealthStatus(role, pas.config.HA.ServerID)

	// Add database connectivity check
	if err := pas.storage.Ping(ctx); err != nil {
		status.AddDetail("database", "disconnected")
		pas.logger.WithContext(ctx).Error("Database health check failed", "error", err.Error())
	} else {
		status.AddDetail("database", "connected")
	}

	return modelToAPIHealthStatus(status), nil
}
