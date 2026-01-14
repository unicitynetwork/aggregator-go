package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	modelsV1 "github.com/unicitynetwork/aggregator-go/internal/models/v1"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	signingV1 "github.com/unicitynetwork/aggregator-go/internal/signing/v1"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Service defines the common interface that all service implementations must satisfy
type Service interface {
	// JSON-RPC methods
	SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error)
	CertificationRequest(ctx context.Context, req *api.CertificationRequest) (*api.CertificationResponse, error)
	GetInclusionProofV1(ctx context.Context, req *api.GetInclusionProofRequestV1) (*api.GetInclusionProofResponseV1, error)
	GetInclusionProofV2(ctx context.Context, req *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error)
	GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error)
	GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error)
	GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error)
	GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error)
	GetBlockRecords(ctx context.Context, req *api.GetBlockRecords) (*api.GetBlockRecordsResponse, error)
	GetHealthStatus(ctx context.Context) (*api.HealthStatus, error)
	PutTrustBase(ctx context.Context, req *types.RootTrustBaseV1) error

	// Parent mode specific methods
	SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error)
	GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error)
}

// NewService creates the appropriate service based on sharding mode
func NewService(ctx context.Context, cfg *config.Config, logger *logger.Logger, roundManager round.Manager, commitmentQueue interfaces.CommitmentQueue, storage interfaces.Storage, leaderSelector LeaderSelector, receiptSigner *signing.ReceiptSigner) (Service, error) {
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone:
		rm, ok := roundManager.(*round.RoundManager)
		if !ok {
			return nil, fmt.Errorf("invalid round manager type for standalone mode")
		}
		return NewAggregatorService(cfg, logger, rm, commitmentQueue, storage, leaderSelector, receiptSigner), nil
	case config.ShardingModeParent:
		prm, ok := roundManager.(*round.ParentRoundManager)
		if !ok {
			return nil, fmt.Errorf("invalid round manager type for parent mode")
		}
		return NewParentAggregatorService(cfg, logger, prm, storage, leaderSelector), nil
	case config.ShardingModeChild:
		return NewAggregatorService(cfg, logger, roundManager, commitmentQueue, storage, leaderSelector, receiptSigner), nil
	default:
		return nil, fmt.Errorf("unsupported sharding mode: %s", cfg.Sharding.Mode)
	}
}

// AggregatorService implements the business logic for the aggregator
type AggregatorService struct {
	config                        *config.Config
	logger                        *logger.Logger
	commitmentQueue               interfaces.CommitmentQueue
	storage                       interfaces.Storage
	roundManager                  round.Manager
	leaderSelector                LeaderSelector
	certificationRequestValidator *signing.CertificationRequestValidator
	commitmentValidator           *signingV1.CommitmentValidator
	trustBaseValidator            *TrustBaseValidator
	receiptSigner                 *signing.ReceiptSigner
}

type LeaderSelector interface {
	IsLeader(ctx context.Context) (bool, error)
}

// Conversion functions between API and internal model types

func modelToAPIAggregatorRecord(modelRecord *models.AggregatorRecord) *api.AggregatorRecord {
	return &api.AggregatorRecord{
		StateID: modelRecord.StateID,
		CertificationData: api.CertificationData{
			OwnerPredicate:  modelRecord.CertificationData.OwnerPredicate,
			Witness:         modelRecord.CertificationData.Witness,
			SourceStateHash: modelRecord.CertificationData.SourceStateHash,
			TransactionHash: modelRecord.CertificationData.TransactionHash,
		},
		AggregateRequestCount: modelRecord.AggregateRequestCount,
		BlockNumber:           modelRecord.BlockNumber,
		LeafIndex:             modelRecord.LeafIndex,
		CreatedAt:             modelRecord.CreatedAt,
		FinalizedAt:           modelRecord.FinalizedAt,
	}
}

func modelToAPIAggregatorRecordV1(modelRecord *models.AggregatorRecord) *api.AggregatorRecordV1 {
	return &api.AggregatorRecordV1{
		RequestID:       modelRecord.StateID,
		TransactionHash: modelRecord.CertificationData.TransactionHash,
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: modelRecord.CertificationData.OwnerPredicate.Params,
			Signature: modelRecord.CertificationData.Witness,
			StateHash: modelRecord.CertificationData.SourceStateHash,
		},
		AggregateRequestCount: modelRecord.AggregateRequestCount,
		BlockNumber:           modelRecord.BlockNumber,
		LeafIndex:             modelRecord.LeafIndex,
		CreatedAt:             modelRecord.CreatedAt,
		FinalizedAt:           modelRecord.FinalizedAt,
	}
}

func modelToAPIBlock(modelBlock *models.Block) *api.Block {
	return &api.Block{
		Index:                modelBlock.Index,
		ChainID:              modelBlock.ChainID,
		ShardID:              modelBlock.ShardID,
		Version:              modelBlock.Version,
		ForkID:               modelBlock.ForkID,
		RootHash:             modelBlock.RootHash,
		PreviousBlockHash:    modelBlock.PreviousBlockHash,
		NoDeletionProofHash:  modelBlock.NoDeletionProofHash,
		CreatedAt:            modelBlock.CreatedAt,
		UnicityCertificate:   modelBlock.UnicityCertificate,
		ParentMerkleTreePath: modelBlock.ParentMerkleTreePath,
	}
}

func modelToAPIHealthStatus(modelHealth *models.HealthStatus) *api.HealthStatus {
	return &api.HealthStatus{
		Status:   modelHealth.Status,
		Role:     modelHealth.Role,
		ServerID: modelHealth.ServerID,
		Sharding: modelHealth.Sharding,
		Details:  modelHealth.Details,
	}
}

// NewAggregatorService creates a new aggregator service
func NewAggregatorService(cfg *config.Config,
	logger *logger.Logger,
	roundManager round.Manager,
	commitmentQueue interfaces.CommitmentQueue,
	storage interfaces.Storage,
	leaderSelector LeaderSelector,
	receiptSigner *signing.ReceiptSigner,
) *AggregatorService {
	return &AggregatorService{
		config:                        cfg,
		logger:                        logger,
		commitmentQueue:               commitmentQueue,
		storage:                       storage,
		roundManager:                  roundManager,
		leaderSelector:                leaderSelector,
		commitmentValidator:           signingV1.NewCommitmentValidator(cfg.Sharding),
		certificationRequestValidator: signing.NewCertificationRequestValidator(cfg.Sharding),
		trustBaseValidator:            NewTrustBaseValidator(storage.TrustBaseStorage()),
		receiptSigner:                 receiptSigner,
	}
}

// SubmitCommitment handles commitment submission
func (as *AggregatorService) SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	// Create commitment with aggregate count
	aggregateCount := req.AggregateRequestCount
	if aggregateCount == 0 {
		aggregateCount = 1 // Default to 1 if not specified
	}

	commitment := modelsV1.NewCommitmentWithAggregate(
		req.RequestID,
		req.TransactionHash,
		modelsV1.Authenticator{
			Algorithm: req.Authenticator.Algorithm,
			PublicKey: req.Authenticator.PublicKey,
			Signature: req.Authenticator.Signature,
			StateHash: req.Authenticator.StateHash,
		},
		aggregateCount,
	)

	// Validate commitment signature and request ID
	validationResult := as.commitmentValidator.ValidateCommitment(commitment)
	if validationResult.Status != signingV1.ValidationStatusSuccess {
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

	// Check if commitment already processed
	existingRecord, err := as.storage.AggregatorRecordStorage().GetByStateID(ctx, req.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing aggregator record: %w", err)
	}

	if existingRecord != nil {
		return &api.SubmitCommitmentResponse{
			Status: "REQUEST_ID_EXISTS",
		}, nil
	}

	// Store commitment in new format but with version = 1
	certRequest := &models.CertificationRequest{
		Version: 1,
		StateID: commitment.RequestID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.NewPayToPublicKeyPredicate(commitment.Authenticator.PublicKey),
			SourceStateHash: commitment.Authenticator.StateHash,
			TransactionHash: commitment.TransactionHash,
			Witness:         commitment.Authenticator.Signature,
		},
		AggregateRequestCount: commitment.AggregateRequestCount,
		CreatedAt:             commitment.CreatedAt,
		ProcessedAt:           commitment.ProcessedAt,
	}
	if err := as.commitmentQueue.Store(ctx, certRequest); err != nil {
		return nil, fmt.Errorf("failed to store commitment: %w", err)
	}

	as.logger.WithContext(ctx).Info("Commitment submitted successfully", "requestId", req.RequestID)

	response := &api.SubmitCommitmentResponse{
		Status: "SUCCESS",
	}

	// Generate receipt if requested
	if req.Receipt != nil && *req.Receipt {
		if as.receiptSigner == nil {
			as.logger.WithContext(ctx).Warn("Receipt requested but receipt signer not configured", "requestId", req.RequestID)
		} else {
			receipt, err := as.receiptSigner.SignReceiptV1(
				commitment.RequestID,
				commitment.TransactionHash,
				commitment.Authenticator.StateHash,
			)
			if err != nil {
				as.logger.WithContext(ctx).Error("Failed to sign receipt", "requestId", req.RequestID, "error", err.Error())
			} else {
				response.Receipt = receipt
			}
		}
	}

	return response, nil
}

// CertificationRequest handles certification request submission
func (as *AggregatorService) CertificationRequest(ctx context.Context, req *api.CertificationRequest) (*api.CertificationResponse, error) {
	// Create certificationRequest with aggregate count
	aggregateCount := req.AggregateRequestCount
	if aggregateCount == 0 {
		aggregateCount = 1 // Default to 1 if not specified
	}

	certificationRequest := models.NewCertificationRequestWithAggregate(req.StateID, models.CertificationData{
		OwnerPredicate:  req.CertificationData.OwnerPredicate,
		SourceStateHash: req.CertificationData.SourceStateHash,
		TransactionHash: req.CertificationData.TransactionHash,
		Witness:         req.CertificationData.Witness,
	}, aggregateCount)

	// Validate certificationRequest signature and state ID
	validationResult := as.certificationRequestValidator.Validate(certificationRequest)
	if validationResult.Status != signing.ValidationStatusSuccess {
		errorMsg := ""
		if validationResult.Error != nil {
			errorMsg = validationResult.Error.Error()
		}
		as.logger.WithContext(ctx).Warn("CertificationData validation failed",
			"stateId", req.StateID,
			"validationStatus", validationResult.Status.String(),
			"error", errorMsg)

		return &api.CertificationResponse{
			Status: validationResult.Status.String(),
		}, nil
	}

	// Check if certificationRequest already processed
	existingRecord, err := as.storage.AggregatorRecordStorage().GetByStateID(ctx, req.StateID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing aggregator record: %w", err)
	}

	if existingRecord != nil {
		return &api.CertificationResponse{
			Status: "STATE_ID_EXISTS",
		}, nil
	}

	// Store certificationRequest
	if err := as.commitmentQueue.Store(ctx, certificationRequest); err != nil {
		return nil, fmt.Errorf("failed to store certificationRequest: %w", err)
	}

	as.logger.WithContext(ctx).Info("CertificationData submitted successfully", "stateId", req.StateID)

	response := &api.CertificationResponse{
		Status: "SUCCESS",
	}

	// Generate receipt if requested
	if req.Receipt {
		if as.receiptSigner == nil {
			as.logger.WithContext(ctx).Warn("Receipt requested but receipt signer not configured", "stateId", req.StateID)
		} else {
			receipt, err := as.receiptSigner.SignReceiptV2(req.CertificationData)
			if err != nil {
				as.logger.WithContext(ctx).Error("Failed to sign receipt", "requestId", req.StateID, "error", err.Error())
			} else {
				response.Receipt = receipt
			}
		}
	}

	return response, nil
}

// GetInclusionProofV1 retrieves inclusion proof for a commitment
func (as *AggregatorService) GetInclusionProofV1(ctx context.Context, req *api.GetInclusionProofRequestV1) (*api.GetInclusionProofResponseV1, error) {
	// verify that the request ID matches the shard ID of this aggregator
	if err := as.commitmentValidator.ValidateShardID(req.RequestID); err != nil {
		return nil, fmt.Errorf("request ID validation failed: %w", err)
	}

	path, err := req.RequestID.GetPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get path for request ID %s: %w", req.RequestID, err)
	}

	smtInstance := as.roundManager.GetSMT()
	if smtInstance == nil {
		return nil, fmt.Errorf("merkle tree not initialized")
	}
	if keyLen := smtInstance.GetKeyLength(); path.BitLen()-1 != keyLen {
		return nil, fmt.Errorf("request path length %d does not match SMT key length %d", path.BitLen()-1, keyLen)
	}

	merkleTreePath, err := smtInstance.GetPath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get inclusion proof for request ID %s: %w", req.RequestID, err)
	}

	// Find the latest block that matches the current SMT root hash
	rootHash, err := api.NewHexBytesFromString(merkleTreePath.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to parse root hash: %w", err)
	}
	block, err := as.storage.BlockStorage().GetLatestByRootHash(ctx, rootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block by root hash: %w", err)
	}
	if block == nil {
		return nil, fmt.Errorf("no block found with root hash %s", rootHash)
	}

	// Join parent and child SMT paths if sharding mode is enabled
	if as.config.Sharding.Mode == config.ShardingModeChild {
		merkleTreePath, err = smt.JoinPaths(merkleTreePath, block.ParentMerkleTreePath)
		if err != nil {
			return nil, fmt.Errorf("failed to join parent and child aggregator paths: %w", err)
		}
	}

	// First check if commitment exists in aggregator records (finalized)
	record, err := as.storage.AggregatorRecordStorage().GetByStateID(ctx, req.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregator record: %w", err)
	}
	if record == nil {
		// Non-inclusion proof
		return &api.GetInclusionProofResponseV1{
			InclusionProof: &api.InclusionProofV1{
				Authenticator:      nil,
				MerkleTreePath:     merkleTreePath,
				TransactionHash:    nil,
				UnicityCertificate: block.UnicityCertificate,
			},
		}, nil
	}
	if record.Version != 0 && record.Version != 1 {
		return nil, fmt.Errorf("invalid inclusion proof version got %d expected 0 or 1", record.Version)
	}
	return &api.GetInclusionProofResponseV1{
		InclusionProof: &api.InclusionProofV1{
			Authenticator: &api.Authenticator{
				Algorithm: "secp256k1",
				PublicKey: record.CertificationData.OwnerPredicate.Params,
				Signature: record.CertificationData.Witness,
				StateHash: record.CertificationData.SourceStateHash,
			},
			MerkleTreePath:     merkleTreePath,
			TransactionHash:    &record.CertificationData.TransactionHash,
			UnicityCertificate: block.UnicityCertificate,
		},
	}, nil
}

// GetInclusionProofV2 retrieves inclusion proof for a commitment
func (as *AggregatorService) GetInclusionProofV2(ctx context.Context, req *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	// verify that the state ID matches the shard ID of this aggregator
	if err := as.certificationRequestValidator.ValidateShardID(req.StateID); err != nil {
		return nil, fmt.Errorf("state ID validation failed: %w", err)
	}

	path, err := req.StateID.GetPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get path for state ID %s: %w", req.StateID, err)
	}

	smtInstance := as.roundManager.GetSMT()
	if smtInstance == nil {
		return nil, fmt.Errorf("merkle tree not initialized")
	}
	if keyLen := smtInstance.GetKeyLength(); path.BitLen()-1 != keyLen {
		return nil, fmt.Errorf("request path length %d does not match SMT key length %d", path.BitLen()-1, keyLen)
	}

	merkleTreePath, err := as.roundManager.GetSMT().GetPath(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get inclusion proof for state ID %s: %w", req.StateID, err)
	}

	// Find the latest block that matches the current SMT root hash
	rootHash, err := api.NewHexBytesFromString(merkleTreePath.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to parse root hash: %w", err)
	}
	block, err := as.storage.BlockStorage().GetLatestByRootHash(ctx, rootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block by root hash: %w", err)
	}
	if block == nil {
		return nil, fmt.Errorf("no block found with root hash %s", rootHash)
	}

	// Join parent and child SMT paths if sharding mode is enabled
	if as.config.Sharding.Mode == config.ShardingModeChild {
		merkleTreePath, err = smt.JoinPaths(merkleTreePath, block.ParentMerkleTreePath)
		if err != nil {
			return nil, fmt.Errorf("failed to join parent and child aggregator paths: %w", err)
		}
	}

	// First check if certification request exists in aggregator records (finalized)
	record, err := as.storage.AggregatorRecordStorage().GetByStateID(ctx, req.StateID)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregator record: %w", err)
	}
	if record == nil {
		// Non-inclusion proof
		return &api.GetInclusionProofResponseV2{
			InclusionProof: &api.InclusionProofV2{
				CertificationData:  nil,
				MerkleTreePath:     merkleTreePath,
				UnicityCertificate: types.RawCBOR(block.UnicityCertificate),
			},
		}, nil
	}
	if record.Version != 2 {
		return nil, fmt.Errorf("invalid aggregator record version got %d expected 2", record.Version)
	}
	return &api.GetInclusionProofResponseV2{
		BlockNumber: record.BlockNumber.Uint64(),
		InclusionProof: &api.InclusionProofV2{
			CertificationData:  record.CertificationData.ToAPI(),
			MerkleTreePath:     merkleTreePath,
			UnicityCertificate: types.RawCBOR(block.UnicityCertificate),
		},
	}, nil
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
		BlockNumber: latestBlockNumber,
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

	// Calculate total commitments by summing up AggregateRequestCount from all records in this block
	records, err := as.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, block.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to get block commitments: %w", err)
	}

	var totalCommitments uint64
	for _, record := range records {
		totalCommitments += record.AggregateRequestCount
	}

	return &api.GetBlockResponse{
		Block:            modelToAPIBlock(block),
		TotalCommitments: totalCommitments,
	}, nil
}

// GetBlockCommitments retrieves all commitments in a block
func (as *AggregatorService) GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error) {
	records, err := as.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, req.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block commitments: %w", err)
	}

	// Convert model records to API records
	apiRecords := make([]*api.AggregatorRecordV1, len(records))
	for i, record := range records {
		apiRecords[i] = modelToAPIAggregatorRecordV1(record)
	}

	return &api.GetBlockCommitmentsResponse{
		Commitments: apiRecords,
	}, nil
}

// GetBlockRecords retrieves all commitments in a block
func (as *AggregatorService) GetBlockRecords(ctx context.Context, req *api.GetBlockRecords) (*api.GetBlockRecordsResponse, error) {
	records, err := as.storage.AggregatorRecordStorage().GetByBlockNumber(ctx, req.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block commitments: %w", err)
	}

	// Convert model records to API records
	apiRecords := make([]*api.AggregatorRecord, len(records))
	for i, record := range records {
		apiRecords[i] = modelToAPIAggregatorRecord(record)
	}

	return &api.GetBlockRecordsResponse{
		AggregatorRecords: apiRecords,
	}, nil
}

// GetHealthStatus retrieves the health status of the service
func (as *AggregatorService) GetHealthStatus(ctx context.Context) (*api.HealthStatus, error) {
	// Check if HA is enabled and determine role
	var role string
	if as.leaderSelector != nil {
		isLeader, err := as.leaderSelector.IsLeader(ctx)
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
	}

	sharding := api.Sharding{
		Mode:       as.config.Sharding.Mode.String(),
		ShardIDLen: as.config.Sharding.ShardIDLength,
		ShardID:    as.config.Sharding.Child.ShardID,
	}
	status := models.NewHealthStatus(role, as.config.HA.ServerID, sharding)

	// Add database connectivity check
	if err := as.storage.Ping(ctx); err != nil {
		status.Status = "unhealthy"
		status.AddDetail("database", "disconnected")
		as.logger.WithContext(ctx).Error("Database health check failed", "error", err.Error())
	} else {
		status.AddDetail("database", "connected")
	}

	// Add commitment queue status and warning if too high
	unprocessedCount, err := as.commitmentQueue.CountUnprocessed(ctx)
	if err != nil {
		status.Status = "unhealthy"
		status.AddDetail("commitment_queue_status", "error")
		as.logger.WithContext(ctx).Error("Commitment queue health check failed", "error", err.Error())
	} else {
		status.AddDetail("commitment_queue", strconv.FormatInt(unprocessedCount, 10))

		// Add warning if unprocessed count is concerning
		if unprocessedCount > 10000 {
			status.AddDetail("commitment_queue_status", "critical")
			as.logger.WithContext(ctx).Error("Critical: High unprocessed commitment count",
				"count", unprocessedCount)
		} else if unprocessedCount > 5000 {
			status.AddDetail("commitment_queue_status", "warning")
			as.logger.WithContext(ctx).Warn("Warning: Elevated unprocessed commitment count",
				"count", unprocessedCount)
		} else {
			status.AddDetail("commitment_queue_status", "healthy")
		}
	}

	if as.config.Sharding.Mode == config.ShardingModeChild {
		if err := as.roundManager.CheckParentHealth(ctx); err != nil {
			status.Status = "degraded"
			status.AddDetail("parent", "unreachable")
			status.AddDetail("parent_error", err.Error())
			as.logger.WithContext(ctx).Warn("Parent aggregator health check failed", "error", err.Error())
		} else {
			status.AddDetail("parent", "connected")
		}
	}

	return modelToAPIHealthStatus(status), nil
}

// SubmitShardRoot - not supported in standalone mode
func (as *AggregatorService) SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error) {
	return nil, fmt.Errorf("submit_shard_root is not supported in standalone mode")
}

// GetShardProof - not supported in standalone mode
func (as *AggregatorService) GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error) {
	return nil, fmt.Errorf("get_shard_proof is not supported in standalone mode")
}

// PutTrustBase verifies and stores the trust base.
func (as *AggregatorService) PutTrustBase(ctx context.Context, trustBase *types.RootTrustBaseV1) error {
	if err := as.trustBaseValidator.Verify(ctx, trustBase); err != nil {
		return fmt.Errorf("failed to verify trust base: %w", err)
	}
	return as.storage.TrustBaseStorage().Store(ctx, trustBase)
}
