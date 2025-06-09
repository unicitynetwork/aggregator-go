package gateway

import (
	"context"
	"encoding/json"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Request and Response types for JSON-RPC methods

// SubmitCommitmentRequest represents the submit_commitment request
type SubmitCommitmentRequest struct {
	RequestID       models.RequestID       `json:"requestId"`
	TransactionHash models.TransactionHash `json:"transactionHash"`
	Authenticator   models.Authenticator   `json:"authenticator"`
	Receipt         *bool                  `json:"receipt,omitempty"`
}

// SubmitCommitmentResponse represents the submit_commitment response
type SubmitCommitmentResponse struct {
	Status  string          `json:"status"`
	Receipt *models.Receipt `json:"receipt,omitempty"`
}

// GetInclusionProofRequest represents the get_inclusion_proof request
type GetInclusionProofRequest struct {
	RequestID models.RequestID `json:"requestId"`
}

// GetInclusionProofResponse represents the get_inclusion_proof response
type GetInclusionProofResponse struct {
	InclusionProof *models.InclusionProof `json:"inclusionProof"`
}

// GetNoDeletionProofResponse represents the get_no_deletion_proof response
type GetNoDeletionProofResponse struct {
	NonDeletionProof *models.NoDeletionProof `json:"nonDeletionProof"`
}

// GetBlockHeightResponse represents the get_block_height response
type GetBlockHeightResponse struct {
	BlockNumber *models.BigInt `json:"blockNumber"`
}

// GetBlockRequest represents the get_block request
type GetBlockRequest struct {
	BlockNumber interface{} `json:"blockNumber"` // Can be number or "latest"
}

// GetBlockResponse represents the get_block response
type GetBlockResponse struct {
	Block *models.Block `json:"block"`
}

// GetBlockCommitmentsRequest represents the get_block_commitments request
type GetBlockCommitmentsRequest struct {
	BlockNumber *models.BigInt `json:"blockNumber"`
}

// GetBlockCommitmentsResponse represents the get_block_commitments response
type GetBlockCommitmentsResponse struct {
	Commitments []*models.AggregatorRecord `json:"commitments"`
}

// JSON-RPC method handlers

// handleSubmitCommitment handles the submit_commitment method
func (s *Server) handleSubmitCommitment(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req SubmitCommitmentRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.RequestID == "" {
		return nil, jsonrpc.NewValidationError("requestId is required")
	}
	if req.TransactionHash == "" {
		return nil, jsonrpc.NewValidationError("transactionHash is required")
	}

	// Call service
	response, err := s.service.SubmitCommitment(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to submit commitment")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to submit commitment", err.Error())
	}

	return response, nil
}

// handleGetInclusionProof handles the get_inclusion_proof method
func (s *Server) handleGetInclusionProof(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req GetInclusionProofRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.RequestID == "" {
		return nil, jsonrpc.NewValidationError("requestId is required")
	}

	// Call service
	response, err := s.service.GetInclusionProof(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get inclusion proof")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get inclusion proof", err.Error())
	}

	return response, nil
}

// handleGetNoDeletionProof handles the get_no_deletion_proof method
func (s *Server) handleGetNoDeletionProof(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	// Call service
	response, err := s.service.GetNoDeletionProof(ctx)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get no-deletion proof")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get no-deletion proof", err.Error())
	}

	return response, nil
}

// handleGetBlockHeight handles the get_block_height method
func (s *Server) handleGetBlockHeight(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	// Call service
	response, err := s.service.GetBlockHeight(ctx)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get block height")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block height", err.Error())
	}

	return response, nil
}

// handleGetBlock handles the get_block method
func (s *Server) handleGetBlock(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req GetBlockRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Call service
	response, err := s.service.GetBlock(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get block")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block", err.Error())
	}

	return response, nil
}

// handleGetBlockCommitments handles the get_block_commitments method
func (s *Server) handleGetBlockCommitments(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req GetBlockCommitmentsRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.BlockNumber == nil {
		return nil, jsonrpc.NewValidationError("blockNumber is required")
	}

	// Call service
	response, err := s.service.GetBlockCommitments(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get block commitments")
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block commitments", err.Error())
	}

	return response, nil
}