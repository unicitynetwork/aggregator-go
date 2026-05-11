package service

import (
	"context"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Service defines the common interface that all service implementations must satisfy
type Service interface {
	// JSON-RPC methods
	CertificationRequest(ctx context.Context, req *api.CertificationRequest) (*api.CertificationResponse, error)
	GetInclusionProofV2(ctx context.Context, req *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error)
	GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error)
	GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error)
	GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error)
	GetBlockRecords(ctx context.Context, req *api.GetBlockRecords) (*api.GetBlockRecordsResponse, error)
	GetHealthStatus(ctx context.Context) (*api.HealthStatus, error)

	TrustBaseService

	// Parent mode specific methods
	SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error)
	GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error)
}

type TrustBaseService interface {
	PutTrustBase(ctx context.Context, req *types.RootTrustBaseV1) error
	GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error)
	GetLatestTrustBase(ctx context.Context) (*types.RootTrustBaseV1, error)
}

// NewService creates the appropriate service based on sharding mode
func NewService(ctx context.Context, cfg *config.Config, logger *logger.Logger, roundManager round.Manager, commitmentQueue interfaces.CommitmentQueue, storage interfaces.Storage, leaderSelector LeaderSelector) (Service, error) {
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeBFTShard:
		rm, ok := roundManager.(*round.RoundManager)
		if !ok {
			return nil, fmt.Errorf("invalid round manager type for mode %s", cfg.Sharding.Mode)
		}
		return NewAggregatorService(cfg, logger, rm, commitmentQueue, storage, leaderSelector), nil
	case config.ShardingModeParent:
		prm, ok := roundManager.(*round.ParentRoundManager)
		if !ok {
			return nil, fmt.Errorf("invalid round manager type for parent mode")
		}
		return NewParentAggregatorService(cfg, logger, prm, storage, leaderSelector), nil
	case config.ShardingModeChild:
		return NewAggregatorService(cfg, logger, roundManager, commitmentQueue, storage, leaderSelector), nil
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
	trustBaseValidator            *trustbase.TrustBaseValidator
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

func modelToAPIBlock(modelBlock *models.Block) *api.Block {
	return &api.Block{
		Index:               modelBlock.Index,
		ChainID:             modelBlock.ChainID,
		ShardID:             modelBlock.ShardID,
		Version:             modelBlock.Version,
		ForkID:              modelBlock.ForkID,
		RootHash:            modelBlock.RootHash,
		PreviousBlockHash:   modelBlock.PreviousBlockHash,
		NoDeletionProofHash: modelBlock.NoDeletionProofHash,
		CreatedAt:           modelBlock.CreatedAt,
		UnicityCertificate:  modelBlock.UnicityCertificate,
		ParentFragment:      modelBlock.ParentFragment,
		ParentBlockNumber:   modelBlock.ParentBlockNumber,
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
) *AggregatorService {
	var bftShardID types.ShardID
	if cfg.BFT.ShardConf != nil {
		bftShardID = cfg.BFT.ShardConf.ShardID
	}
	return &AggregatorService{
		config:                        cfg,
		logger:                        logger,
		commitmentQueue:               commitmentQueue,
		storage:                       storage,
		roundManager:                  roundManager,
		leaderSelector:                leaderSelector,
		certificationRequestValidator: signing.NewCertificationRequestValidator(cfg.Sharding, bftShardID),
		trustBaseValidator:            trustbase.NewTrustBaseValidator(storage.TrustBaseStorage()),
	}
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

	if !as.config.Processing.SkipDuplicateCheck {
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
	}

	// Store certificationRequest
	if err := as.commitmentQueue.Store(ctx, certificationRequest); err != nil {
		return nil, fmt.Errorf("failed to store certificationRequest: %w", err)
	}

	as.logger.WithContext(ctx).Info("CertificationData submitted successfully", "stateId", req.StateID)

	return &api.CertificationResponse{Status: "SUCCESS"}, nil
}

// GetInclusionProofV2 retrieves a v2 inclusion proof for the given stateId.
// Both standalone and child mode serve proofs against the current certified
// SMT root. In child mode, the local child cert is composed with the stored
// parent fragment and bound to the parent UC.IR.h.
func (as *AggregatorService) GetInclusionProofV2(ctx context.Context, req *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	unlock := as.roundManager.FinalizationReadLock()
	defer unlock()

	if err := as.certificationRequestValidator.ValidateShardID(req.StateID); err != nil {
		return nil, fmt.Errorf("state ID validation failed: %w", err)
	}

	// v2 stateId must be exactly 32 bytes.
	if len(req.StateID) != api.StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid state ID length: expected %d bytes (v2 wire format), got %d",
			api.StateTreeKeyLengthBytes, len(req.StateID))
	}
	key, err := req.StateID.GetTreeKey()
	if err != nil {
		return nil, fmt.Errorf("invalid state ID: %w", err)
	}

	smtInstance := as.roundManager.GetSMT()
	if smtInstance == nil {
		return nil, fmt.Errorf("merkle tree not initialized")
	}
	if keyLen := smtInstance.GetKeyLength(); keyLen != api.StateTreeKeyLengthBits {
		return nil, fmt.Errorf("unexpected SMT key length: got %d bits, want %d", keyLen, api.StateTreeKeyLengthBits)
	}

	if block, ok := as.roundManager.GetKnownNotReadyBlock(req.StateID); ok {
		responseBlockNumber, err := proofBundleBlockNumber(as.config.Sharding.Mode, block)
		if err != nil {
			return nil, err
		}
		return emptyInclusionProofResponse(responseBlockNumber, block), nil
	}

	// Bind the UC via the block whose stored rootHash matches the current
	// raw 32-byte SMT root (which also lives in UC.IR.h).
	rootHashRaw := api.HexBytes(smtInstance.GetRootHashRaw())
	block, err := as.storage.BlockStorage().GetLatestByRootHash(ctx, rootHashRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block by root hash: %w", err)
	}
	if block == nil {
		return nil, fmt.Errorf("no block found with root hash %s", rootHashRaw.String())
	}
	responseBlockNumber, err := proofBundleBlockNumber(as.config.Sharding.Mode, block)
	if err != nil {
		return nil, err
	}

	record, err := as.storage.AggregatorRecordStorage().GetByStateID(ctx, req.StateID)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregator record: %w", err)
	}
	if record == nil || record.BlockNumber.Cmp(block.Index.Int) > 0 {
		// Non-inclusion is not implemented yet. Return an empty v2 proof
		// payload so verifiers short-circuit with ErrExclusionNotImpl.
		return emptyInclusionProofResponse(responseBlockNumber, block), nil
	}
	if record.Version != 2 {
		return nil, fmt.Errorf("invalid aggregator record version got %d expected 2", record.Version)
	}

	childCert, err := smtInstance.GetInclusionCert(key)
	if err != nil {
		return nil, fmt.Errorf("failed to build inclusion cert for state ID %s: %w", req.StateID, err)
	}

	cert := childCert
	if as.config.Sharding.Mode == config.ShardingModeChild {
		if block.ParentFragment == nil {
			return nil, fmt.Errorf("current child block %s is missing parent fragment", block.Index.String())
		}
		childRoot := smtInstance.GetRootHashRaw()
		cert, err = api.ComposeInclusionCert(block.ParentFragment, childCert, childRoot)
		if err != nil {
			return nil, fmt.Errorf("failed to compose child and parent inclusion certs: %w", err)
		}
	}

	certBytes, err := cert.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inclusion cert: %w", err)
	}

	proof := &api.InclusionProofV2{
		CertificationData:  record.CertificationData.ToAPI(),
		CertificateBytes:   certBytes,
		UnicityCertificate: types.RawCBOR(block.UnicityCertificate),
	}

	return &api.GetInclusionProofResponseV2{
		BlockNumber:    responseBlockNumber,
		InclusionProof: proof,
	}, nil
}

func emptyInclusionProofResponse(blockNumber uint64, block *models.Block) *api.GetInclusionProofResponseV2 {
	return &api.GetInclusionProofResponseV2{
		BlockNumber: blockNumber,
		InclusionProof: &api.InclusionProofV2{
			CertificationData:  nil,
			CertificateBytes:   nil,
			UnicityCertificate: types.RawCBOR(block.UnicityCertificate),
		},
	}
}

func proofBundleBlockNumber(mode config.ShardingMode, block *models.Block) (uint64, error) {
	if block == nil {
		return 0, fmt.Errorf("missing block for proof bundle")
	}
	if mode != config.ShardingModeChild {
		return block.Index.Uint64(), nil
	}
	if block.ParentBlockNumber == 0 {
		return 0, fmt.Errorf("current child block %s is missing parent block number", block.Index.String())
	}
	return block.ParentBlockNumber, nil
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

	sharding := buildShardingHealth(as.config)
	status := models.NewHealthStatus(role, as.config.HA.ServerID, sharding)

	// Add database connectivity check
	if err := as.storage.Ping(ctx); err != nil {
		status.Status = api.HealthStatusUnhealthy
		status.AddDetail("database", "disconnected")
		as.logger.WithContext(ctx).Error("Database health check failed", "error", err.Error())
	} else {
		status.AddDetail("database", "connected")
	}

	// Add commitment queue status and warning if too high
	unprocessedCount, err := as.commitmentQueue.CountUnprocessed(ctx)
	if err != nil {
		status.Status = api.HealthStatusUnhealthy
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
			status.Status = api.HealthStatusUnhealthy
			status.AddDetail("parent", "unreachable")
			status.AddDetail("parent_error", err.Error())
			as.logger.WithContext(ctx).Warn("Parent aggregator health check failed", "error", err.Error())
		} else {
			status.AddDetail("parent", "connected")
		}
	}

	return modelToAPIHealthStatus(status), nil
}

// buildShardingHealth builds the api.Sharding health payload. In bft-shard
// mode the shard is reported as a bit-string via BFTShardID and the integer
// shard fields are left at zero.
func buildShardingHealth(cfg *config.Config) api.Sharding {
	sharding := api.Sharding{
		Mode: cfg.Sharding.Mode.String(),
	}
	if cfg.Sharding.Mode == config.ShardingModeBFTShard {
		if cfg.BFT.ShardConf != nil {
			sharding.BFTShardID = cfg.BFT.ShardConf.ShardID.String()
		}
		return sharding
	}
	sharding.ShardIDLen = cfg.Sharding.ShardIDLength
	sharding.ShardID = cfg.Sharding.Child.ShardID
	return sharding
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

func (as *AggregatorService) GetLatestTrustBase(ctx context.Context) (*types.RootTrustBaseV1, error) {
	return as.storage.TrustBaseStorage().GetLatest(ctx)
}

// GetTrustBases retrieves trust bases within the specified range.
func (as *AggregatorService) GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error) {
	return as.storage.TrustBaseStorage().GetTrustBases(ctx, from, to)
}
