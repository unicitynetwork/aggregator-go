package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// AggregatorService implements the business logic for the aggregator
type AggregatorService struct {
	config              *config.Config
	logger              *logger.Logger
	storage             interfaces.Storage
	roundManager        *round.RoundManager
	commitmentValidator *signing.CommitmentValidator
}

// Conversion functions between API and internal model types

func modelToAPIBigInt(modelBigInt *api.BigInt) *api.BigInt {
	if modelBigInt == nil {
		return nil
	}
	return &api.BigInt{Int: modelBigInt.Int}
}

func apiToModelBigInt(apiBigInt *api.BigInt) *api.BigInt {
	if apiBigInt == nil {
		return nil
	}
	return &api.BigInt{Int: apiBigInt.Int}
}

func modelToAPIAggregatorRecord(modelRecord *models.AggregatorRecord) *api.AggregatorRecord {
	return &api.AggregatorRecord{
		RequestID:       modelRecord.RequestID,
		TransactionHash: modelRecord.TransactionHash,
		Authenticator: api.Authenticator{
			Algorithm: modelRecord.Authenticator.Algorithm,
			PublicKey: modelRecord.Authenticator.PublicKey,
			Signature: modelRecord.Authenticator.Signature,
			StateHash: api.StateHash(modelRecord.Authenticator.StateHash.String()),
		},
		BlockNumber: modelToAPIBigInt(modelRecord.BlockNumber),
		LeafIndex:   modelToAPIBigInt(modelRecord.LeafIndex),
		CreatedAt:   modelRecord.CreatedAt,
		FinalizedAt: modelRecord.FinalizedAt,
	}
}

func modelToAPIBlock(modelBlock *models.Block) *api.Block {
	return &api.Block{
		Index:               modelToAPIBigInt(modelBlock.Index),
		ChainID:             modelBlock.ChainID,
		Version:             modelBlock.Version,
		ForkID:              modelBlock.ForkID,
		Timestamp:           modelBlock.Timestamp,
		RootHash:            modelBlock.RootHash,
		PreviousBlockHash:   modelBlock.PreviousBlockHash,
		NoDeletionProofHash: modelBlock.NoDeletionProofHash,
		CreatedAt:           modelBlock.CreatedAt,
	}
}

func modelToAPIHealthStatus(modelHealth *models.HealthStatus) *api.HealthStatus {
	return &api.HealthStatus{
		Status:   modelHealth.Status,
		Role:     modelHealth.Role,
		ServerID: modelHealth.ServerID,
		Details:  modelHealth.Details,
	}
}

// NewAggregatorService creates a new aggregator service
func NewAggregatorService(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) (*AggregatorService, error) {
	roundManager, err := round.NewRoundManager(cfg, logger, storage)
	if err != nil {
		return nil, fmt.Errorf("failed to create round manager: %w", err)
	}
	commitmentValidator := signing.NewCommitmentValidator()

	return &AggregatorService{
		config:              cfg,
		logger:              logger,
		storage:             storage,
		roundManager:        roundManager,
		commitmentValidator: commitmentValidator,
	}, nil
}

// Start starts the aggregator service and round manager
func (as *AggregatorService) Start(ctx context.Context) error {
	as.logger.WithContext(ctx).Info("Starting Aggregator Service")

	// Start the round manager
	if err := as.roundManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start round manager: %w", err)
	}

	as.logger.WithContext(ctx).Info("Aggregator Service started successfully")
	return nil
}

// Stop stops the aggregator service and round manager
func (as *AggregatorService) Stop(ctx context.Context) error {
	as.logger.WithContext(ctx).Info("Stopping Aggregator Service")

	// Stop the round manager
	if err := as.roundManager.Stop(ctx); err != nil {
		as.logger.WithContext(ctx).Error("Failed to stop round manager", "error", err.Error())
		return fmt.Errorf("failed to stop round manager: %w", err)
	}

	as.logger.WithContext(ctx).Info("Aggregator Service stopped successfully")
	return nil
}

// SubmitCommitment handles commitment submission
func (as *AggregatorService) SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	// Create commitment for validation
	commitment := models.NewCommitment(req.RequestID, req.TransactionHash, models.Authenticator{
		Algorithm: req.Authenticator.Algorithm,
		PublicKey: req.Authenticator.PublicKey,
		Signature: req.Authenticator.Signature,
		StateHash: req.Authenticator.StateHash,
	})

	// Validate commitment signature and request ID
	validationResult := as.commitmentValidator.ValidateCommitment(commitment)
	if validationResult.Status != signing.ValidationStatusSuccess {
		errorMsg := ""
		if validationResult.Error != nil {
			errorMsg = validationResult.Error.Error()
		}
		as.logger.WithContext(ctx).Warn("Commitment validation failed",
			"requestId", req.RequestID,
			"validationStatus", validationResult.Status.String(),
			"error", errorMsg)

		return &api.SubmitCommitmentResponse{
			Status: validationResult.Status.String(),
		}, nil
	}

	// Check if commitment already exists
	existing, err := as.storage.CommitmentStorage().GetByRequestID(ctx, req.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing commitment: %w", err)
	}

	if existing != nil {
		return &api.SubmitCommitmentResponse{
			Status: "REQUEST_ID_EXISTS",
		}, nil
	}

	// Store commitment
	if err := as.storage.CommitmentStorage().Store(ctx, commitment); err != nil {
		return nil, fmt.Errorf("failed to store commitment: %w", err)
	}

	// Add commitment to current round for processing
	if err := as.roundManager.AddCommitment(ctx, commitment); err != nil {
		as.logger.WithContext(ctx).Warn("Failed to add commitment to round - will be processed in next round",
			"requestId", req.RequestID,
			"error", err.Error())
		// Don't fail the request, the commitment is stored and will be picked up
	}

	as.logger.WithContext(ctx).Info("Commitment submitted successfully", "requestId", req.RequestID)

	response := &api.SubmitCommitmentResponse{
		Status: "SUCCESS",
	}

	// Generate receipt if requested
	if req.Receipt != nil && *req.Receipt {
		// TODO: Implement receipt generation with actual signing
		//receipt := api.NewReceipt(
		//	commitment.,
		//	"secp256k1",
		//	api.HexBytes("mock_public_key"),
		//	api.HexBytes("mock_signature"),
		//)
		// Convert to API receipt
		response.Receipt = &api.Receipt{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes("mock_public_key"),
			Signature: api.HexBytes("mock_signature"),
			Request: api.ReceiptRequest{
				RequestID:       commitment.RequestID,
				TransactionHash: commitment.TransactionHash,
				StateHash:       commitment.Authenticator.StateHash,
			},
		}
	}

	return response, nil
}

// GetInclusionProof retrieves inclusion proof for a commitment
func (as *AggregatorService) GetInclusionProof(ctx context.Context, req *api.GetInclusionProofRequest) (*api.GetInclusionProofResponse, error) {
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
		proof := api.NewInclusionProof(req.RequestID, nil, nil, []api.ProofNode{}, api.HexBytes{}, false)
		return &api.GetInclusionProofResponse{InclusionProof: proof}, nil
	}

	// TODO: Generate actual inclusion proof from SMT when available
	// For now, return a basic proof indicating inclusion
	proof := api.NewInclusionProof(
		req.RequestID,
		modelToAPIBigInt(record.BlockNumber),
		modelToAPIBigInt(record.LeafIndex),
		[]api.ProofNode{}, // TODO: Generate real proof path
		api.NewHexBytes([]byte(as.roundManager.GetSMT().GetRootHash())),
		true,
	)

	return &api.GetInclusionProofResponse{InclusionProof: proof}, nil
}

// GetNoDeletionProof retrieves the global no-deletion proof
func (as *AggregatorService) GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error) {
	// TODO: Implement no-deletion proof generation
	// For now, return a placeholder
	proof := api.NewNoDeletionProof(api.HexBytes("mock_no_deletion_proof"))

	return &api.GetNoDeletionProofResponse{
		NoDeletionProof: proof,
	}, nil
}

// GetBlockHeight retrieves the current block height
func (as *AggregatorService) GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error) {
	latestBlockNumber, err := as.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %w", err)
	}

	return &api.GetBlockHeightResponse{
		BlockNumber: modelToAPIBigInt(latestBlockNumber),
	}, nil
}

// GetBlock retrieves block information
func (as *AggregatorService) GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error) {
	var block *models.Block
	var err error

	// Handle "latest" or specific block number
	if req.BlockNumber == "latest" {
		block, err = as.storage.BlockStorage().GetLatest(ctx)
	} else {
		// Parse block number
		var blockNum *api.BigInt
		switch v := req.BlockNumber.(type) {
		case float64:
			blockNum = api.NewBigInt(nil)
			blockNum.SetInt64(int64(v))
		case string:
			blockNum, err = api.NewBigIntFromString(v)
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

	return &api.GetBlockResponse{Block: modelToAPIBlock(block)}, nil
}

// GetBlockCommitments retrieves all commitments in a block
func (as *AggregatorService) GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error) {
	records, err := as.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, apiToModelBigInt(req.BlockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to get block commitments: %w", err)
	}

	// Convert model records to API records
	apiRecords := make([]*api.AggregatorRecord, len(records))
	for i, record := range records {
		apiRecords[i] = modelToAPIAggregatorRecord(record)
	}

	return &api.GetBlockCommitmentsResponse{
		Commitments: apiRecords,
	}, nil
}

// GetHealthStatus retrieves the health status of the service
func (as *AggregatorService) GetHealthStatus(ctx context.Context) (*api.HealthStatus, error) {
	// Check if HA is enabled and determine role
	var role string
	var isLeader bool

	if as.config.HA.Enabled {
		var err error
		isLeader, err = as.storage.LeadershipStorage().IsLeader(ctx, as.config.HA.ServerID)
		if err != nil {
			as.logger.WithContext(ctx).Warn("Failed to check leadership status", "error", err.Error())
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
		as.logger.WithContext(ctx).Error("Database health check failed", "error", err.Error())
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

	return modelToAPIHealthStatus(status), nil
}
