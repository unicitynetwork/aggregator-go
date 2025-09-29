package service

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ParentAggregatorService implements the business logic for the parent aggregator
type ParentAggregatorService struct {
	config  *config.Config
	logger  *logger.Logger
	storage interfaces.Storage
	// TODO: Add parent round manager when implemented

	// In-memory tracking of shard root updates for current round
	currentShardUpdates map[string]*models.ShardRootUpdate
}

// NewParentAggregatorService creates a new parent aggregator service
func NewParentAggregatorService(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) (*ParentAggregatorService, error) {
	if !cfg.Sharding.Mode.IsParent() {
		return nil, fmt.Errorf("parent aggregator service can only be created in parent mode, got: %s", cfg.Sharding.Mode)
	}

	// TODO: Create parent round manager when implemented

	return &ParentAggregatorService{
		config:              cfg,
		logger:              logger,
		storage:             storage,
		currentShardUpdates: make(map[string]*models.ShardRootUpdate),
	}, nil
}

// Start starts the parent aggregator service
func (pas *ParentAggregatorService) Start(ctx context.Context) error {
	pas.logger.WithContext(ctx).Info("Starting Parent Aggregator Service")

	// TODO: Start parent round manager when implemented

	pas.logger.WithContext(ctx).Info("Parent Aggregator Service started successfully")
	return nil
}

// Stop stops the parent aggregator service
func (pas *ParentAggregatorService) Stop(ctx context.Context) error {
	pas.logger.WithContext(ctx).Info("Stopping Parent Aggregator Service")

	// TODO: Stop parent round manager when implemented

	pas.logger.WithContext(ctx).Info("Parent Aggregator Service stopped successfully")
	return nil
}

// SubmitShardRoot handles shard root submission from child aggregators
func (pas *ParentAggregatorService) SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error) {
	// Validate shard ID range
	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID", "shardId", req.ShardID, "error", err.Error())
		return &api.SubmitShardRootResponse{
			Status: api.ShardRootStatusInvalidShardID,
		}, nil
	}

	update := models.NewShardRootUpdate(req.ShardID, req.RootHash)
	shardIDStr := fmt.Sprintf("%d", req.ShardID)
	pas.currentShardUpdates[shardIDStr] = update

	pas.logger.WithContext(ctx).Info("Shard root update accepted",
		"shardId", req.ShardID,
		"rootHash", req.RootHash.String(),
		"totalShards", len(pas.currentShardUpdates))

	// TODO: Submit to parent round manager when implemented

	return &api.SubmitShardRootResponse{
		Status: api.ShardRootStatusSuccess,
	}, nil
}

// GetShardProof handles shard proof requests
func (pas *ParentAggregatorService) GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error) {
	// Validate shard ID
	if err := pas.validateShardID(req.ShardID); err != nil {
		pas.logger.WithContext(ctx).Warn("Invalid shard ID for proof request", "shardId", req.ShardID, "error", err.Error())
		return nil, fmt.Errorf("invalid shard ID: %w", err)
	}

	pas.logger.WithContext(ctx).Info("Shard proof requested", "shardId", req.ShardID)

	// TODO: Implement shard proof generation when parent SMT and round manager are ready

	return nil, fmt.Errorf("shard proof generation not implemented yet - requires parent SMT integration")
}

// validateShardID validates that the shard ID is in the correct range
func (pas *ParentAggregatorService) validateShardID(shardID int) error {
	if shardID < 0 {
		return fmt.Errorf("shard ID must be non-negative, got: %d", shardID)
	}

	// Calculate maximum shard ID based on shard_id_length
	maxShardID := (1 << pas.config.Sharding.ShardIDLength) - 1
	if shardID > maxShardID {
		return fmt.Errorf("shard ID %d exceeds maximum %d for %d-bit shard IDs",
			shardID, maxShardID, pas.config.Sharding.ShardIDLength)
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

// GetBlockHeight - TODO: implement
func (pas *ParentAggregatorService) GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error) {
	return nil, fmt.Errorf("get_block_height not implemented yet in parent mode")
}

// GetBlock - TODO: implement
func (pas *ParentAggregatorService) GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error) {
	return nil, fmt.Errorf("get_block not implemented yet in parent mode")
}

// GetBlockCommitments - TODO: implement
func (pas *ParentAggregatorService) GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error) {
	return nil, fmt.Errorf("get_block_commitments not implemented yet in parent mode")
}

// GetHealthStatus - TODO: implement
func (pas *ParentAggregatorService) GetHealthStatus(ctx context.Context) (*api.HealthStatus, error) {
	return nil, fmt.Errorf("get_health_status not implemented yet in parent mode")
}
