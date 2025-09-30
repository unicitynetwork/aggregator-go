package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Server represents the HTTP gateway server
type Server struct {
	config     *config.Config
	logger     *logger.Logger
	storage    interfaces.Storage
	rpcServer  *jsonrpc.Server
	httpServer *http.Server
	router     *gin.Engine
	service    Service
}

// Service represents the business logic service interface
type Service interface {
	SubmitCommitment(ctx context.Context, req *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error)
	GetInclusionProof(ctx context.Context, req *api.GetInclusionProofRequest) (*api.GetInclusionProofResponse, error)
	GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error)
	GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error)
	GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error)
	GetBlockCommitments(ctx context.Context, req *api.GetBlockCommitmentsRequest) (*api.GetBlockCommitmentsResponse, error)
	GetHealthStatus(ctx context.Context) (*api.HealthStatus, error)
}

// NewServer creates a new gateway server
func NewServer(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage, service Service) *Server {
	// Configure Gin
	if cfg.Logging.Level == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()

	// Add Gin middleware
	router.Use(gin.Recovery())
	if cfg.Logging.Level == "debug" {
		router.Use(gin.Logger())
	}

	// Create JSON-RPC server
	rpcServer := jsonrpc.NewServer(logger, cfg.Server.ConcurrencyLimit)

	server := &Server{
		config:    cfg,
		logger:    logger,
		storage:   storage,
		rpcServer: rpcServer,
		router:    router,
		service:   service,
	}

	// Setup routes
	server.setupRoutes()
	server.setupJSONRPCHandlers()

	// Create HTTP server
	server.httpServer = &http.Server{
		Addr:           fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port),
		Handler:        router,
		ReadTimeout:    cfg.Server.ReadTimeout,
		WriteTimeout:   cfg.Server.WriteTimeout,
		IdleTimeout:    cfg.Server.IdleTimeout,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	return server
}

// setupRoutes sets up HTTP routes
func (s *Server) setupRoutes() {
	// Health endpoint
	s.router.GET("/health", s.handleHealth)

	// JSON-RPC endpoint
	s.router.POST("/", gin.WrapH(s.rpcServer))

	// REST endpoint for commitment submission with request ID in path
	// This allows load balancers to route by request ID without parsing body
	s.router.POST("/commitments/:request_id", s.handleSubmitCommitmentREST)

	// API documentation endpoint
	if s.config.Server.EnableDocs {
		s.router.GET("/docs", s.handleDocs)
	}

	// CORS for all routes
	if s.config.Server.EnableCORS {
		s.router.Use(func(c *gin.Context) {
			c.Header("Access-Control-Allow-Origin", "*")
			c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type")

			if c.Request.Method == "OPTIONS" {
				c.AbortWithStatus(http.StatusOK)
				return
			}

			c.Next()
		})
	}
}

// setupJSONRPCHandlers sets up JSON-RPC method handlers
func (s *Server) setupJSONRPCHandlers() {
	// Add middleware
	s.rpcServer.AddMiddleware(jsonrpc.RequestIDMiddleware())
	s.rpcServer.AddMiddleware(jsonrpc.LoggingMiddleware(s.logger))
	s.rpcServer.AddMiddleware(jsonrpc.TimeoutMiddleware(30 * time.Second))

	// Register handlers
	s.rpcServer.RegisterMethod("submit_commitment", s.handleSubmitCommitment)
	s.rpcServer.RegisterMethod("get_inclusion_proof", s.handleGetInclusionProof)
	s.rpcServer.RegisterMethod("get_no_deletion_proof", s.handleGetNoDeletionProof)
	s.rpcServer.RegisterMethod("get_block_height", s.handleGetBlockHeight)
	s.rpcServer.RegisterMethod("get_block", s.handleGetBlock)
	s.rpcServer.RegisterMethod("get_block_commitments", s.handleGetBlockCommitments)
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.WithComponent("gateway").Info("Starting HTTP server", "addr", s.httpServer.Addr)

	if s.config.Server.EnableTLS {
		return s.httpServer.ListenAndServeTLS(s.config.Server.TLSCertFile, s.config.Server.TLSKeyFile)
	}

	return s.httpServer.ListenAndServe()
}

// Stop stops the HTTP server gracefully
func (s *Server) Stop(ctx context.Context) error {
	s.logger.WithComponent("gateway").Info("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// handleHealth handles the health endpoint
func (s *Server) handleHealth(c *gin.Context) {
	ctx := c.Request.Context()

	status, err := s.service.GetHealthStatus(ctx)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get health status", "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	c.JSON(http.StatusOK, status)
}

// handleDocs handles the API documentation endpoint
func (s *Server) handleDocs(c *gin.Context) {
	html := GenerateDocsHTML()
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, html)
}

// handleSubmitCommitmentREST handles REST commitment submission
// Path parameter allows load balancer to route by request ID without parsing body
func (s *Server) handleSubmitCommitmentREST(c *gin.Context) {
	ctx := c.Request.Context()
	requestIDParam := c.Param("request_id")

	// Convert path parameter to RequestID type
	pathRequestID, err := api.NewImprintHexString(requestIDParam)
	if err != nil {
		s.logger.WithContext(ctx).Error("Invalid request ID in path", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request ID format in path: " + err.Error()})
		return
	}

	// Parse request body
	var req api.SubmitCommitmentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.logger.WithContext(ctx).Error("Invalid request body", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// Validate that path parameter matches body
	if string(req.RequestID) != string(pathRequestID) {
		s.logger.WithContext(ctx).Error("Request ID mismatch",
			"path", pathRequestID,
			"body", req.RequestID)
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Request ID in path does not match body",
			"path_request_id": string(pathRequestID),
			"body_request_id": string(req.RequestID),
		})
		return
	}

	// Validate required fields
	if req.TransactionHash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transactionHash is required"})
		return
	}

	// Call service
	response, err := s.service.SubmitCommitment(ctx, &req)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to submit commitment", "error", err.Error())

		// Check for specific error types to return appropriate status codes
		if err.Error() == "REQUEST_ID_EXISTS" {
			c.JSON(http.StatusConflict, gin.H{
				"error": "REQUEST_ID_EXISTS",
				"message": "Commitment with this request ID already exists",
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to submit commitment"})
		return
	}

	c.JSON(http.StatusOK, response)
}
