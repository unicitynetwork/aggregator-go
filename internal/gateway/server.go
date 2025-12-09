package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/unicitynetwork/bft-go-base/types"
	"golang.org/x/net/http2"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Server represents the HTTP gateway server
type Server struct {
	config     *config.Config
	logger     *logger.Logger
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
	PutTrustBase(ctx context.Context, req *types.RootTrustBaseV1) error

	// Parent mode specific methods (will return errors in standalone mode)
	SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error)
	GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error)
}

// NewServer creates a new gateway server
func NewServer(cfg *config.Config, logger *logger.Logger, service Service) *Server {
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

	if cfg.Server.EnableTLS {
		gatewayLogger := logger.WithComponent("gateway")
		h2Config := &http2.Server{
			MaxConcurrentStreams: uint32(cfg.Server.HTTP2MaxConcurrentStreams),
		}
		if err := http2.ConfigureServer(server.httpServer, h2Config); err != nil {
			gatewayLogger.Warn("Failed to configure HTTP/2 server", "error", err.Error())
		} else {
			gatewayLogger.Info("Configured HTTP/2 server", "maxConcurrentStreams", cfg.Server.HTTP2MaxConcurrentStreams)
		}
	}

	return server
}

// setupRoutes sets up HTTP routes
func (s *Server) setupRoutes() {
	// Health endpoint
	s.router.GET("/health", s.handleHealth)
	s.router.PUT("/api/v1/trustbases", s.handlePutTrustBase)

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
	s.rpcServer.AddMiddleware(jsonrpc.LoggingMiddleware(s.logger))
	s.rpcServer.AddMiddleware(jsonrpc.TimeoutMiddleware(30 * time.Second))

	// Register handlers based on mode
	if s.config.Sharding.Mode.IsParent() {
		// Parent mode handlers
		s.rpcServer.RegisterMethod("submit_shard_root", s.handleSubmitShardRoot)
		s.rpcServer.RegisterMethod("get_shard_proof", s.handleGetShardProof)
	} else {
		// Standalone mode handlers (default)
		s.rpcServer.RegisterMethod("submit_commitment", s.handleSubmitCommitment)
		s.rpcServer.RegisterMethod("get_inclusion_proof", s.handleGetInclusionProof)
	}

	// Common handlers for all modes
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

	// Return 503 if unhealthy so load balancers can remove from rotation
	if status.Status == "unhealthy" {
		c.JSON(http.StatusServiceUnavailable, status)
		return
	}

	c.JSON(http.StatusOK, status)
}

func (s *Server) handlePutTrustBase(c *gin.Context) {
	ctx := c.Request.Context()

	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to read request body", "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	var trustBase *types.RootTrustBaseV1
	if err := json.Unmarshal(jsonData, &trustBase); err != nil {
		s.logger.WithContext(ctx).Warn("Failed to parse trust base from request body", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse trust base from request body"})
		return
	}

	if err := s.service.PutTrustBase(ctx, trustBase); err != nil {
		s.logger.WithContext(ctx).Warn("Failed to store trust base", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // make the actual error visible to client
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

// handleDocs handles the API documentation endpoint
func (s *Server) handleDocs(c *gin.Context) {
	html := GenerateDocsHTML()
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, html)
}
