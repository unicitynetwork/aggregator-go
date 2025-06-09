package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Server represents the HTTP gateway server
type Server struct {
	config        *config.Config
	logger        *logger.Logger
	storage       interfaces.Storage
	rpcServer     *jsonrpc.Server
	httpServer    *http.Server
	router        *gin.Engine
	service       Service
}

// Service represents the business logic service interface
type Service interface {
	SubmitCommitment(ctx context.Context, req *SubmitCommitmentRequest) (*SubmitCommitmentResponse, error)
	GetInclusionProof(ctx context.Context, req *GetInclusionProofRequest) (*GetInclusionProofResponse, error)
	GetNoDeletionProof(ctx context.Context) (*GetNoDeletionProofResponse, error)
	GetBlockHeight(ctx context.Context) (*GetBlockHeightResponse, error)
	GetBlock(ctx context.Context, req *GetBlockRequest) (*GetBlockResponse, error)
	GetBlockCommitments(ctx context.Context, req *GetBlockCommitmentsRequest) (*GetBlockCommitmentsResponse, error)
	GetHealthStatus(ctx context.Context) (*models.HealthStatus, error)
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
	rpcServer := jsonrpc.NewServer(logger.Logger, cfg.Server.ConcurrencyLimit)

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
	s.rpcServer.AddMiddleware(jsonrpc.LoggingMiddleware(s.logger.Logger))
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
func (s *Server) Start(ctx context.Context) error {
	s.logger.WithComponent("gateway").Infof("Starting HTTP server on %s", s.httpServer.Addr)

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
		s.logger.WithContext(ctx).WithError(err).Error("Failed to get health status")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	c.JSON(http.StatusOK, status)
}

// handleDocs handles the API documentation endpoint
func (s *Server) handleDocs(c *gin.Context) {
	// Return a simple API documentation page
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Unicity Aggregator API Documentation</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .method { margin: 20px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }
        .method-name { font-size: 18px; font-weight: bold; color: #333; }
        .method-desc { margin: 10px 0; color: #666; }
        pre { background: #f5f5f5; padding: 10px; border-radius: 3px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>Unicity Aggregator API Documentation</h1>
    <p>This service implements the JSON-RPC 2.0 protocol for Unicity blockchain aggregation.</p>
    
    <div class="method">
        <div class="method-name">submit_commitment</div>
        <div class="method-desc">Submit a state transition request to the aggregation layer.</div>
        <pre>{"jsonrpc": "2.0", "method": "submit_commitment", "params": {"requestId": "...", "transactionHash": "...", "authenticator": {...}}, "id": 1}</pre>
    </div>
    
    <div class="method">
        <div class="method-name">get_inclusion_proof</div>
        <div class="method-desc">Retrieve the inclusion proof for a specific state transition request.</div>
        <pre>{"jsonrpc": "2.0", "method": "get_inclusion_proof", "params": {"requestId": "..."}, "id": 1}</pre>
    </div>
    
    <div class="method">
        <div class="method-name">get_block_height</div>
        <div class="method-desc">Retrieve the current height of the blockchain.</div>
        <pre>{"jsonrpc": "2.0", "method": "get_block_height", "params": {}, "id": 1}</pre>
    </div>
    
    <div class="method">
        <div class="method-name">get_block</div>
        <div class="method-desc">Retrieve detailed information about a specific block.</div>
        <pre>{"jsonrpc": "2.0", "method": "get_block", "params": {"blockNumber": 123}, "id": 1}</pre>
    </div>
    
    <div class="method">
        <div class="method-name">get_block_commitments</div>
        <div class="method-desc">Retrieve all commitments included in a specific block.</div>
        <pre>{"jsonrpc": "2.0", "method": "get_block_commitments", "params": {"blockNumber": 123}, "id": 1}</pre>
    </div>
    
    <p><strong>Health Endpoint:</strong> <a href="/health">GET /health</a></p>
</body>
</html>`
	
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, html)
}