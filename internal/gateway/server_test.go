package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// MockService is a mock implementation of the Service interface
type MockService struct {
	mock.Mock
}

func (m *MockService) SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	args := m.Called(ctx, req)
	if resp, ok := args.Get(0).(*api.SubmitCommitmentResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetInclusionProof(ctx context.Context, req *api.GetInclusionProofRequest) (*api.GetInclusionProofResponse, error) {
	args := m.Called(ctx, req)
	if resp, ok := args.Get(0).(*api.GetInclusionProofResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error) {
	args := m.Called(ctx)
	if resp, ok := args.Get(0).(*api.GetNoDeletionProofResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error) {
	args := m.Called(ctx)
	if resp, ok := args.Get(0).(*api.GetBlockHeightResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error) {
	args := m.Called(ctx, req)
	if resp, ok := args.Get(0).(*api.GetBlockResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error) {
	args := m.Called(ctx, req)
	if resp, ok := args.Get(0).(*api.GetBlockCommitmentsResponse); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockService) GetHealthStatus(ctx context.Context) (*api.HealthStatus, error) {
	args := m.Called(ctx)
	if resp, ok := args.Get(0).(*api.HealthStatus); ok {
		return resp, args.Error(1)
	}
	return nil, args.Error(1)
}

func setupTestServer() (*Server, *MockService) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: "3000",
			EnableDocs: false,
			EnableCORS: false,
		},
		Logging: config.LoggingConfig{
			Level: "error",
		},
	}

	testLogger, _ := logger.New("error", "json", "stdout", true)
	mockService := new(MockService)

	server := &Server{
		config:  cfg,
		logger:  testLogger,
		router:  gin.New(),
		service: mockService,
	}

	server.setupRoutes()

	return server, mockService
}

func TestHandleSubmitCommitmentREST_Success(t *testing.T) {
	server, mockService := setupTestServer()

	requestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"
	requestBody := api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(requestID),
		TransactionHash: "0000123456789012345678901234567890123456789012345678901234567890123456",
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes("02a1b2c3d4e5f67890123456789012345678901234567890123456789012345678ab"),
			Signature: api.HexBytes("3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678900220123456789012345678901234567890123456789012345678901234567890123456"),
			StateHash: "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	mockService.On("SubmitCommitment", mock.Anything, &requestBody).Return(
		&api.SubmitCommitmentResponse{Status: "SUCCESS"},
		nil,
	)

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+requestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response api.SubmitCommitmentResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "SUCCESS", response.Status)

	mockService.AssertExpectations(t)
}

func TestHandleSubmitCommitmentREST_RequestIDMismatch(t *testing.T) {
	server, _ := setupTestServer()

	pathRequestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"
	bodyRequestID := "0000b5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"

	requestBody := api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(bodyRequestID),
		TransactionHash: "0000123456789012345678901234567890123456789012345678901234567890123456",
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes("02a1b2c3d4e5f67890123456789012345678901234567890123456789012345678ab"),
			Signature: api.HexBytes("3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678900220123456789012345678901234567890123456789012345678901234567890123456"),
			StateHash: "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+pathRequestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "Request ID in path does not match body", response["error"])
	assert.Equal(t, pathRequestID, response["path_request_id"])
	assert.Equal(t, bodyRequestID, response["body_request_id"])
}

func TestHandleSubmitCommitmentREST_InvalidRequestIDFormat(t *testing.T) {
	server, _ := setupTestServer()

	invalidRequestID := "invalid-id"

	requestBody := map[string]interface{}{
		"requestId":       invalidRequestID,
		"transactionHash": "0000123456789012345678901234567890123456789012345678901234567890123456",
		"authenticator": map[string]interface{}{
			"algorithm": "secp256k1",
			"publicKey": "02a1b2c3d4e5f67890123456789012345678901234567890123456789012345678ab",
			"signature": "3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678900220123456789012345678901234567890123456789012345678901234567890123456",
			"stateHash": "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+invalidRequestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "Invalid request ID format in path")
}

func TestHandleSubmitCommitmentREST_MissingTransactionHash(t *testing.T) {
	server, _ := setupTestServer()

	requestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"

	requestBody := map[string]interface{}{
		"requestId": requestID,
		// Missing transactionHash
		"authenticator": map[string]interface{}{
			"algorithm": "secp256k1",
			"publicKey": "02a1b2c3d4e5f67890123456789012345678901234567890123456789012345678ab",
			"signature": "3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef12345678900220123456789012345678901234567890123456789012345678901234567890123456",
			"stateHash": "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+requestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "transactionHash is required", response["error"])
}

func TestHandleSubmitCommitmentREST_DuplicateRequestID(t *testing.T) {
	server, mockService := setupTestServer()

	requestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"

	requestBody := api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(requestID),
		TransactionHash: "0000123456789012345678901234567890123456789012345678901234567890123456",
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes("02a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			Signature: api.HexBytes("3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890022012345678901234567890123456789012345678901234567890123456789012345601"),
			StateHash: "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	mockService.On("SubmitCommitment", mock.Anything, &requestBody).Return(
		nil,
		errors.New("REQUEST_ID_EXISTS"),
	)

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+requestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)

	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "REQUEST_ID_EXISTS", response["error"])
	assert.Equal(t, "Commitment with this request ID already exists", response["message"])

	mockService.AssertExpectations(t)
}

func TestHandleSubmitCommitmentREST_InvalidJSON(t *testing.T) {
	server, _ := setupTestServer()

	requestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"

	req := httptest.NewRequest("POST", "/commitments/"+requestID, bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "Invalid request body")
}

func TestHandleSubmitCommitmentREST_ServiceError(t *testing.T) {
	server, mockService := setupTestServer()

	requestID := "0000a5c3b2d1e0f1234567890abcdef1234567890abcdef1234567890abcdef12345"

	requestBody := api.SubmitCommitmentRequest{
		RequestID:       api.RequestID(requestID),
		TransactionHash: "0000123456789012345678901234567890123456789012345678901234567890123456",
		Authenticator: api.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: api.HexBytes("02a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"),
			Signature: api.HexBytes("3045022100abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890022012345678901234567890123456789012345678901234567890123456789012345601"),
			StateHash: "0000fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	mockService.On("SubmitCommitment", mock.Anything, &requestBody).Return(
		nil,
		errors.New("internal service error"),
	)

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/commitments/"+requestID, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)

	var response map[string]string
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "Failed to submit commitment", response["error"])

	mockService.AssertExpectations(t)
}