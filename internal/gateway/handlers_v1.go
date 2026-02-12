package gateway

import (
	"context"
	"encoding/json"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// JSON-RPC method handlers

// handleSubmitCommitment handles the submit_commitment method
func (s *Server) handleSubmitCommitment(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.SubmitCommitmentRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.RequestID == nil {
		return nil, jsonrpc.NewValidationError("requestId is required")
	}
	if req.TransactionHash == nil {
		return nil, jsonrpc.NewValidationError("transactionHash is required")
	}

	// Call service
	response, err := s.service.SubmitCommitment(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to submit commitment", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to submit commitment", err.Error())
	}

	return response, nil
}

// handleGetInclusionProofV1 handles the get_inclusion_proof method
func (s *Server) handleGetInclusionProofV1(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetInclusionProofRequestV1
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.RequestID == nil {
		return nil, jsonrpc.NewValidationError("requestId is required")
	}

	// Call service
	response, err := s.service.GetInclusionProofV1(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get inclusion proof", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get inclusion proof", err.Error())
	}

	return response, nil
}

// handleGetBlockCommitments handles the get_block_commitments method
func (s *Server) handleGetBlockCommitments(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetBlockCommitmentsRequest
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
		s.logger.WithContext(ctx).Error("Failed to get block commitments", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block commitments", err.Error())
	}

	return response, nil
}
