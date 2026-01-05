package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// JSON-RPC method handlers

// handleCertificationRequest handles the certification_request method
func (s *Server) handleCertificationRequest(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var cborBytes api.HexBytes
	if err := json.Unmarshal(params, &cborBytes); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid hex parameters: " + err.Error())
	}

	var req api.CertificationRequest
	if err := types.Cbor.Unmarshal(cborBytes, &req); err != nil {
		fmt.Println(string(params))
		return nil, jsonrpc.NewValidationError("Invalid cbor parameters: " + err.Error())
	}

	// Validate required fields
	if req.StateID == "" {
		return nil, jsonrpc.NewValidationError("stateId is required")
	}
	if req.CertificationData.TransactionHash == "" {
		return nil, jsonrpc.NewValidationError("transactionHash is required")
	}

	// Call service
	response, err := s.service.CertificationRequest(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to submit certification request", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to submit certification request", err.Error())
	}

	return response, nil
}

// handleGetInclusionProofV2 handles the get_inclusion_proof.v2 method
func (s *Server) handleGetInclusionProofV2(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetInclusionProofRequestV2
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.StateID == "" {
		return nil, jsonrpc.NewValidationError("stateId is required")
	}

	// Call service
	response, err := s.service.GetInclusionProofV2(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get inclusion proof", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get inclusion proof", err.Error())
	}

	return response, nil
}

// handleGetNoDeletionProof handles the get_no_deletion_proof method
func (s *Server) handleGetNoDeletionProof(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	// Call service
	response, err := s.service.GetNoDeletionProof(ctx)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get no-deletion proof", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get no-deletion proof", err.Error())
	}

	return response, nil
}

// handleGetBlockHeight handles the get_block_height method
func (s *Server) handleGetBlockHeight(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	// Call service
	response, err := s.service.GetBlockHeight(ctx)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get block height", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block height", err.Error())
	}

	return response, nil
}

// handleGetBlock handles the get_block method
func (s *Server) handleGetBlock(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetBlockRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Call service
	response, err := s.service.GetBlock(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get block", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block", err.Error())
	}

	return response, nil
}

// handleGetBlockRecords handles the get_block_records method
func (s *Server) handleGetBlockRecords(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetBlockRecords
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	// Validate required fields
	if req.BlockNumber == nil {
		return nil, jsonrpc.NewValidationError("blockNumber is required")
	}

	// Call service
	response, err := s.service.GetBlockRecords(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get block records", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get block records", err.Error())
	}

	return response, nil
}

// Parent mode handlers

// handleSubmitShardRoot handles the submit_shard_root method
func (s *Server) handleSubmitShardRoot(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.SubmitShardRootRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	if req.ShardID <= 1 {
		return nil, jsonrpc.NewValidationError("shard ID must be positive and have at least 2 bits")
	}
	if len(req.RootHash) == 0 {
		return nil, jsonrpc.NewValidationError("rootHash is required")
	}

	response, err := s.service.SubmitShardRoot(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to submit shard root", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to submit shard root", err.Error())
	}

	return response, nil
}

// handleGetShardProof handles the get_shard_proof method
func (s *Server) handleGetShardProof(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	var req api.GetShardProofRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, jsonrpc.NewValidationError("Invalid parameters: " + err.Error())
	}

	if req.ShardID <= 1 {
		return nil, jsonrpc.NewValidationError("shard ID must be positive and have at least 2 bits")
	}

	response, err := s.service.GetShardProof(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get shard proof", "error", err.Error())
		return nil, jsonrpc.NewError(jsonrpc.InternalErrorCode, "Failed to get shard proof", err.Error())
	}

	return response, nil
}
