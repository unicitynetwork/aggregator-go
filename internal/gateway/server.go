package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/unicitynetwork/bft-go-base/types"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
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

	longPoll *longpoll.Coordinator
	conns    *connTracker

	// admission gate and in-flight register of the handlers (see trackInflight)
	admitMu    sync.Mutex
	closing    bool
	inflightN  int
	baseCtx    context.Context // cancelled to cut residual work at the shutdown deadline
	baseCancel context.CancelFunc
}

// Service represents the business logic service interface
type Service interface {
	CertificationRequest(ctx context.Context, req *api.CertificationRequest) (*api.CertificationResponse, error)
	GetInclusionProofV2(ctx context.Context, req *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error)
	GetNoDeletionProof(ctx context.Context) (*api.GetNoDeletionProofResponse, error)
	GetBlockHeight(ctx context.Context) (*api.GetBlockHeightResponse, error)
	GetBlock(ctx context.Context, req *api.GetBlockRequest) (*api.GetBlockResponse, error)
	GetBlockRecords(ctx context.Context, req *api.GetBlockRecords) (*api.GetBlockRecordsResponse, error)
	GetHealthStatus(ctx context.Context) (*api.HealthStatus, error)

	TrustBaseService

	// Parent mode specific methods (will return errors in standalone mode)
	SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) (*api.SubmitShardRootResponse, error)
	GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.GetShardProofResponse, error)
}

type TrustBaseService interface {
	PutTrustBase(ctx context.Context, req *types.RootTrustBaseV1) error
	GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error)
	GetLatestTrustBase(ctx context.Context) (*types.RootTrustBaseV1, error)
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
	rpcServer.SetMaxRequestBodyBytes(cfg.Server.MaxRequestBodyBytes)

	server := &Server{
		config:    cfg,
		logger:    logger,
		rpcServer: rpcServer,
		router:    router,
		service:   service,
		conns:     newConnTracker(),
	}
	server.baseCtx, server.baseCancel = context.WithCancel(context.Background())

	// Setup routes
	server.setupRoutes()
	server.setupJSONRPCHandlers()

	// Create HTTP server
	server.httpServer = &http.Server{
		Addr:           fmt.Sprintf("%s:%s", cfg.Server.Host, cfg.Server.Port),
		Handler:        server.trackInflight(router),
		ReadTimeout:    cfg.Server.ReadTimeout,
		WriteTimeout:   cfg.Server.WriteTimeout,
		IdleTimeout:    cfg.Server.IdleTimeout,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	gatewayLogger := logger.WithComponent("gateway")
	h2Config := &http2.Server{
		MaxConcurrentStreams: uint32(cfg.Server.HTTP2MaxConcurrentStreams),
	}

	if cfg.Server.EnableTLS {
		if err := http2.ConfigureServer(server.httpServer, h2Config); err != nil {
			gatewayLogger.Warn("Failed to configure HTTP/2 server", "error", err.Error())
		} else {
			gatewayLogger.Info("Configured HTTP/2 (TLS) server", "maxConcurrentStreams", cfg.Server.HTTP2MaxConcurrentStreams)
		}
	} else if cfg.Server.EnableH2C {
		// ConfigureServer registers h2Config with the http.Server, so that Shutdown sends GOAWAY to
		// the connections h2c serves (h2c hijacks them, Shutdown does not know them otherwise).
		if err := http2.ConfigureServer(server.httpServer, h2Config); err != nil {
			gatewayLogger.Warn("Failed to register h2c graceful shutdown", "error", err.Error())
		}
		server.httpServer.Handler = h2c.NewHandler(server.trackInflight(router), h2Config)
		gatewayLogger.Info("Configured h2c (HTTP/2 cleartext) server", "maxConcurrentStreams", cfg.Server.HTTP2MaxConcurrentStreams)
	} else {
		gatewayLogger.Info("h2c disabled, using HTTP/1.1 only")
	}

	return server
}

// setupRoutes sets up HTTP routes
func (s *Server) setupRoutes() {
	// CORS for all routes. gin applies a middleware only to the routes registered after it, so it is installed first
	// (it used to be added after the routes and covered none of them).
	if s.config.Server.EnableCORS {
		s.router.Use(func(c *gin.Context) {
			jsonrpc.SetCORSHeaders(c.Writer.Header(), "GET, POST, PUT, OPTIONS")

			if c.Request.Method == "OPTIONS" {
				c.AbortWithStatus(http.StatusOK)
				return
			}

			c.Next()
		})
	}

	// Health and metrics endpoints
	s.router.GET("/health", s.handleHealth)
	s.router.GET("/health/leader", s.handleLeaderHealth)
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	s.router.PUT("/api/v1/trustbases", s.handlePutTrustBase)
	s.router.GET("/api/v1/trustbases", getTrustBaseHandler(s.logger, s.service))

	// JSON-RPC endpoint
	s.router.POST("/", gin.WrapH(s.rpcServer))

	// API documentation endpoint
	if s.config.Server.EnableDocs {
		s.router.GET("/docs", s.handleDocs)
	}
}

// setupJSONRPCHandlers sets up JSON-RPC method handlers
func (s *Server) setupJSONRPCHandlers() {
	// Add middleware
	s.rpcServer.AddMiddleware(jsonrpc.RequestIDMiddleware())
	s.rpcServer.AddMiddleware(jsonrpc.LoggingMiddleware(s.logger))
	s.rpcServer.AddMiddleware(jsonrpc.TimeoutMiddleware(MethodTimeout, s.logger))

	// Register handlers based on mode
	if s.config.Sharding.Mode.IsParent() {
		// Parent mode handlers
		s.rpcServer.RegisterMethod("submit_shard_root", s.handleSubmitShardRoot)
		s.rpcServer.RegisterMethod("get_shard_proof", s.handleGetShardProof)
	} else {
		// Standalone mode handlers (default)
		s.rpcServer.RegisterMethod("certification_request", s.handleCertificationRequest)
		s.rpcServer.RegisterMethod("get_inclusion_proof.v2", s.handleGetInclusionProofV2) // v2
	}

	// Common handlers for all modes
	s.rpcServer.RegisterMethod("get_no_deletion_proof", s.handleGetNoDeletionProof)
	s.rpcServer.RegisterMethod("get_block_height", s.handleGetBlockHeight)
	s.rpcServer.RegisterMethod("get_block", s.handleGetBlock)
	s.rpcServer.RegisterMethod("get_block_records", s.handleGetBlockRecords)
}

// BatchProofsMethod is the JSON-RPC method of the batch inclusion proof API.
const BatchProofsMethod = "get_inclusion_proofs.v1"

// SetBatchProofHandler registers get_inclusion_proofs.v1 with its request and
// response bounds. Until it is called (publication wiring) the method is not
// served. Deployments that cannot serve it (parent/child aggregation) get the
// unsupported_configuration error instead.
func (s *Server) SetBatchProofHandler(h jsonrpc.HandlerFunc) {
	if m := s.config.Sharding.Mode; m.IsParent() || m.IsChild() {
		h = func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
			return nil, jsonrpc.NewDetailedError(jsonrpc.ProofUnavailableCode, "Proof unavailable", "proof_unavailable", "unsupported_configuration", false, nil)
		}
	}
	s.rpcServer.RegisterMethodWithLimits(BatchProofsMethod, h, jsonrpc.MethodLimits{
		MaxRequestBytes:  api.InclusionProofsMaxRequestBytes,
		MaxParamsBytes:   api.InclusionProofsMaxParamsBytes,
		MaxIDBytes:       api.InclusionProofsMaxIDBytes,
		MaxResponseBytes: api.InclusionProofsMaxResponseBytes,
	})
}

// SetNonInclusionHandler registers get_non_inclusion_proof.v1 with its request and response
// bounds (the params are one state ID). Deployments which cannot serve it get the
// unsupported_configuration error instead of a missing method.
func (s *Server) SetNonInclusionHandler(h jsonrpc.HandlerFunc) {
	if m := s.config.Sharding.Mode; m.IsParent() || m.IsChild() {
		h = func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
			return nil, jsonrpc.NewDetailedError(api.NonInclusionCodeNotFound, "No certified snapshot", "proof_unavailable", "unsupported_configuration", false, nil)
		}
	}
	s.rpcServer.RegisterMethodWithLimits(api.GetNonInclusionProofMethod, h, jsonrpc.MethodLimits{
		MaxRequestBytes:  api.NonInclusionMaxRequestBytes,
		MaxParamsBytes:   1 << 10,
		MaxIDBytes:       api.InclusionProofsMaxIDBytes,
		MaxResponseBytes: api.NonInclusionMaxResponseBytes,
	})
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.WithComponent("gateway").Info("Starting HTTP server", "addr", s.httpServer.Addr)

	addr := s.httpServer.Addr
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	ln = trackedListener{Listener: ln, t: s.conns}
	if s.config.Server.EnableTLS {
		return s.httpServer.ServeTLS(ln, s.config.Server.TLSCertFile, s.config.Server.TLSKeyFile)
	}
	return s.httpServer.Serve(ln)
}

// Stop stops the HTTP server gracefully. In order: the admission gate closes (new requests are
// refused without reaching the service, on open h2c connections too) and long-poll waits are
// ended and refused; the listener closes and HTTP/2 connections get GOAWAY; the admitted handlers
// and the workers of opt-in requests are joined while the caller's storage is still open. If that
// has not happened shortly before the ctx deadline, residual work is cut - request contexts are
// cancelled and the tracked connections closed - and joined for the remaining time. When even
// that fails Stop returns ErrShutdownIncomplete and the caller must not close what the handlers
// use. Stop uses only the time of its ctx: pass a derived context to keep time for later steps.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.WithComponent("gateway").Info("Stopping HTTP server")
	s.closeAdmission()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- s.httpServer.Shutdown(ctx) }()

	soft := ctx
	if dl, ok := ctx.Deadline(); ok {
		cut := time.Until(dl) / 5
		if cut > 2*time.Second {
			cut = 2 * time.Second
		}
		var cancel context.CancelFunc
		soft, cancel = context.WithDeadline(ctx, dl.Add(-cut))
		defer cancel()
	}
	var result error
	if err := s.joinWork(soft); err != nil {
		s.logger.WithComponent("gateway").Warn("Shutdown deadline: cutting residual handlers", "error", err.Error())
		s.baseCancel() // request contexts end: cooperative storage calls return
		s.conns.CloseAll()
		if err := s.joinWork(ctx); err != nil {
			return fmt.Errorf("%w: %d handlers, %d workers, %d active-work tokens: %v", ErrShutdownIncomplete, s.Inflight(), s.rpcServer.ActiveWorkers(), s.rpcServer.ActiveRequests(), err)
		}
		result = fmt.Errorf("shutdown deadline reached, residual handlers were cancelled: %w", context.DeadlineExceeded)
	}
	// Handlers are done, but their last bytes may still be on the way: Shutdown waits for the
	// HTTP/1 connections to finish their responses, and the h2c connections (GOAWAY sent) close
	// themselves once their streams ended. Only what is left after a short grace is closed.
	if err := <-shutdownErr; err != nil && result == nil {
		result = err
	}
	grace := time.NewTimer(time.Second)
	defer grace.Stop()
	for s.conns.Open() > 0 {
		select {
		case <-grace.C:
			s.conns.CloseAll()
		case <-time.After(5 * time.Millisecond):
			continue
		}
		break
	}
	s.baseCancel()
	return result
}
