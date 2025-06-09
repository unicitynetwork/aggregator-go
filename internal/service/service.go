package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// AggregatorService implements the business logic for the aggregator
type AggregatorService struct {
	config  *config.Config
	logger  *logger.Logger
	storage interfaces.Storage
}

// NewAggregatorService creates a new aggregator service
func NewAggregatorService(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) *AggregatorService {
	return &AggregatorService{
		config:  cfg,
		logger:  logger,
		storage: storage,
	}
}

// SubmitCommitment handles commitment submission
func (as *AggregatorService) SubmitCommitment(ctx context.Context, req *gateway.SubmitCommitmentRequest) (*gateway.SubmitCommitmentResponse, error) {
	// Check if commitment already exists
	existing, err := as.storage.CommitmentStorage().GetByRequestID(ctx, req.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing commitment: %w", err)
	}
	
	if existing != nil {
		return nil, fmt.Errorf("commitment with requestId %s already exists", req.RequestID)
	}

	// Create commitment
	commitment := models.NewCommitment(req.RequestID, req.TransactionHash, req.Authenticator)

	// Store commitment
	if err := as.storage.CommitmentStorage().Store(ctx, commitment); err != nil {
		return nil, fmt.Errorf("failed to store commitment: %w", err)
	}

	as.logger.WithContext(ctx).WithField("requestId", req.RequestID).Info("Commitment submitted successfully")

	response := &gateway.SubmitCommitmentResponse{
		Status: "success",
	}

	// Generate receipt if requested
	if req.Receipt {
		// TODO: Implement receipt generation with actual signing
		receipt := models.NewReceipt(
			commitment,
			"ed25519",
			models.HexBytes("mock_public_key"),
			models.HexBytes("mock_signature"),
		)
		response.Receipt = receipt
	}

	return response, nil
}

// GetInclusionProof retrieves inclusion proof for a commitment
func (as *AggregatorService) GetInclusionProof(ctx context.Context, req *gateway.GetInclusionProofRequest) (*gateway.GetInclusionProofResponse, error) {
	// First check if commitment exists in aggregator records (finalized)
	record, err := as.storage.AggregatorRecordStorage().GetByRequestID(ctx, req.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregator record: %w", err)
	}

	if record == nil {
		// Check if commitment exists but not yet processed
		commitment, err := as.storage.CommitmentStorage().GetByRequestID(ctx, req.RequestID)
		if err != nil {
			return nil, fmt.Errorf("failed to get commitment: %w", err)
		}

		if commitment == nil {
			return nil, fmt.Errorf("commitment not found")
		}

		// Commitment exists but not yet finalized
		proof := models.NewInclusionProof(req.RequestID, nil, nil, []models.ProofNode{}, models.HexBytes{}, false)
		return &gateway.GetInclusionProofResponse{InclusionProof: proof}, nil
	}

	// TODO: Generate actual inclusion proof from SMT
	// For now, return a mock proof indicating inclusion
	proof := models.NewInclusionProof(
		req.RequestID,
		record.BlockNumber,
		record.LeafIndex,
		[]models.ProofNode{}, // TODO: Generate real proof path
		models.HexBytes("mock_root_hash"),
		true,
	)

	return &gateway.GetInclusionProofResponse{InclusionProof: proof}, nil
}

// GetNoDeletionProof retrieves the global no-deletion proof
func (as *AggregatorService) GetNoDeletionProof(ctx context.Context) (*gateway.GetNoDeletionProofResponse, error) {
	// TODO: Implement no-deletion proof generation
	// For now, return a placeholder
	proof := models.NewNoDeletionProof(models.HexBytes("mock_no_deletion_proof"))
	
	return &gateway.GetNoDeletionProofResponse{
		NonDeletionProof: proof,
	}, nil
}

// GetBlockHeight retrieves the current block height
func (as *AggregatorService) GetBlockHeight(ctx context.Context) (*gateway.GetBlockHeightResponse, error) {
	latestBlockNumber, err := as.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %w", err)
	}

	return &gateway.GetBlockHeightResponse{
		BlockNumber: latestBlockNumber,
	}, nil
}

// GetBlock retrieves block information
func (as *AggregatorService) GetBlock(ctx context.Context, req *gateway.GetBlockRequest) (*gateway.GetBlockResponse, error) {
	var block *models.Block
	var err error

	// Handle "latest" or specific block number
	if req.BlockNumber == "latest" {
		block, err = as.storage.BlockStorage().GetLatest(ctx)
	} else {
		// Parse block number
		var blockNum *models.BigInt
		switch v := req.BlockNumber.(type) {
		case float64:
			blockNum = models.NewBigInt(nil)
			blockNum.SetInt64(int64(v))
		case string:
			blockNum, err = models.NewBigIntFromString(v)
			if err != nil {
				return nil, fmt.Errorf("invalid block number: %w", err)
			}
		default:
			return nil, fmt.Errorf("invalid block number type")
		}

		block, err = as.storage.BlockStorage().GetByNumber(ctx, blockNum)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	if block == nil {
		return nil, fmt.Errorf("block not found")
	}

	return &gateway.GetBlockResponse{Block: block}, nil
}

// GetBlockCommitments retrieves all commitments in a block
func (as *AggregatorService) GetBlockCommitments(ctx context.Context, req *gateway.GetBlockCommitmentsRequest) (*gateway.GetBlockCommitmentsResponse, error) {
	records, err := as.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, req.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block commitments: %w", err)
	}

	return &gateway.GetBlockCommitmentsResponse{
		Commitments: records,
	}, nil
}

// GetHealthStatus retrieves the health status of the service
func (as *AggregatorService) GetHealthStatus(ctx context.Context) (*models.HealthStatus, error) {
	// Check if HA is enabled and determine role
	var role string
	var isLeader bool

	if as.config.HA.Enabled {
		var err error
		isLeader, err = as.storage.LeadershipStorage().IsLeader(ctx, as.config.HA.ServerID)
		if err != nil {
			as.logger.WithContext(ctx).WithError(err).Warn("Failed to check leadership status")
			// Don't fail health check on leadership query failure
			isLeader = false
		}

		if isLeader {
			role = "leader"
		} else {
			role = "follower"
		}
	} else {
		role = "standalone"
		isLeader = true // In standalone mode, this server handles everything
	}

	status := models.NewHealthStatus(role, as.config.HA.ServerID)

	// Add database connectivity check
	if err := as.storage.Ping(ctx); err != nil {
		status.AddDetail("database", "disconnected")
		as.logger.WithContext(ctx).WithError(err).Error("Database health check failed")
	} else {
		status.AddDetail("database", "connected")
	}

	// Add commitment queue status
	unprocessedCount, err := as.storage.CommitmentStorage().CountUnprocessed(ctx)
	if err != nil {
		status.AddDetail("commitment_queue", "unknown")
	} else {
		status.AddDetail("commitment_queue", strconv.FormatInt(unprocessedCount, 10))
	}

	return status, nil
}