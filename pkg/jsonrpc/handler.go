package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

// HandlerFunc represents a JSON-RPC method handler
type HandlerFunc func(ctx context.Context, params json.RawMessage) (interface{}, *Error)

// Server represents a JSON-RPC server
type Server struct {
	handlers         map[string]HandlerFunc
	middleware       []MiddlewareFunc
	logger           *logger.Logger
	concurrencyLimit int
	activeSemaphore  chan struct{}
	mutex            sync.RWMutex
}

// MiddlewareFunc represents middleware for JSON-RPC requests
type MiddlewareFunc func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response

// NewServer creates a new JSON-RPC server
func NewServer(logger *logger.Logger, concurrencyLimit int) *Server {
	return &Server{
		handlers:         make(map[string]HandlerFunc),
		middleware:       make([]MiddlewareFunc, 0),
		logger:           logger,
		concurrencyLimit: concurrencyLimit,
		activeSemaphore:  make(chan struct{}, concurrencyLimit),
	}
}

// RegisterMethod registers a new JSON-RPC method handler
func (s *Server) RegisterMethod(method string, handler HandlerFunc) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.handlers[method] = handler
}

// AddMiddleware adds middleware to the server
func (s *Server) AddMiddleware(middleware MiddlewareFunc) {
	s.middleware = append(s.middleware, middleware)
}

// ServeHTTP implements http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		s.writeErrorResponse(w, ErrInvalidRequest, nil)
		return
	}

	// Check concurrency limit
	select {
	case s.activeSemaphore <- struct{}{}:
		defer func() { <-s.activeSemaphore }()
	default:
		s.writeErrorResponse(w, ErrConcurrencyLimit, nil)
		return
	}

	// Add state ID to context
	ctx := context.WithValue(r.Context(), "state_id", uuid.New().String())

	// Parse request
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.WithContext(ctx).Error("Failed to parse JSON-RPC request", "error", err.Error())
		s.writeErrorResponse(w, ErrParseError, nil)
		return
	}

	// Validate request
	if err := req.IsValidRequest(); err != nil {
		s.logger.WithContext(ctx).Error("Invalid JSON-RPC request", "error", err.Error())
		s.writeErrorResponse(w, ErrInvalidRequest, req.ID)
		return
	}

	// Process request
	response := s.processRequest(ctx, &req)

	// Write response
	s.writeJSONResponse(w, response)
}

// processRequest processes a JSON-RPC request
func (s *Server) processRequest(ctx context.Context, req *Request) *Response {
	// Apply middleware
	handler := func(ctx context.Context, req *Request) *Response {
		return s.handleRequest(ctx, req)
	}

	// Apply middleware in reverse order
	for i := len(s.middleware) - 1; i >= 0; i-- {
		middleware := s.middleware[i]
		nextHandler := handler
		handler = func(ctx context.Context, req *Request) *Response {
			return middleware(ctx, req, nextHandler)
		}
	}

	return handler(ctx, req)
}

// handleRequest handles the actual JSON-RPC request
func (s *Server) handleRequest(ctx context.Context, req *Request) *Response {
	s.mutex.RLock()
	handler, exists := s.handlers[req.Method]
	s.mutex.RUnlock()

	if !exists {
		return NewErrorResponse(ErrMethodNotFound, req.ID)
	}

	start := time.Now()
	result, rpcErr := handler(ctx, req.Params)
	duration := time.Since(start)

	// Log request
	if rpcErr != nil {
		s.logger.WithContext(ctx).Error("JSON-RPC request failed",
			"method", req.Method,
			"duration_ms", duration.Milliseconds(),
			"state_id", ctx.Value("state_id"),
			"error_code", rpcErr.Code)
		return NewErrorResponse(rpcErr, req.ID)
	}

	s.logger.WithContext(ctx).Debug("JSON-RPC request completed",
		"method", req.Method,
		"duration_ms", duration.Milliseconds(),
		"state_id", ctx.Value("state_id"))
	return NewResponse(result, req.ID)
}

// writeJSONResponse writes a JSON response
func (s *Server) writeJSONResponse(w http.ResponseWriter, response *Response) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.WithError(err).Error("Failed to encode JSON response")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// writeErrorResponse writes an error response
func (s *Server) writeErrorResponse(w http.ResponseWriter, rpcErr *Error, id interface{}) {
	response := NewErrorResponse(rpcErr, id)
	s.writeJSONResponse(w, response)
}

// StateIDMiddleware adds state ID to the context
func StateIDMiddleware() MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		stateID := uuid.New().String()
		ctx = context.WithValue(ctx, "state_id", stateID)
		return next(ctx, req)
	}
}

// LoggingMiddleware logs JSON-RPC requests
func LoggingMiddleware(logger *logger.Logger) MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		start := time.Now()

		logger.WithContext(ctx).Info("Processing JSON-RPC request",
			"method", req.Method,
			"state_id", ctx.Value("state_id"))

		response := next(ctx, req)

		duration := time.Since(start)

		if response.Error != nil {
			logger.WithContext(ctx).Error("JSON-RPC request failed",
				"method", req.Method,
				"state_id", ctx.Value("state_id"),
				"duration_ms", duration.Milliseconds(),
				"error_code", response.Error.Code)
		} else {
			logger.WithContext(ctx).Debug("JSON-RPC request completed",
				"method", req.Method,
				"state_id", ctx.Value("state_id"),
				"duration_ms", duration.Milliseconds())
		}

		return response
	}
}

// TimeoutMiddleware adds timeout to requests
func TimeoutMiddleware(timeout time.Duration) MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		done := make(chan *Response, 1)

		go func() {
			done <- next(ctx, req)
		}()

		select {
		case response := <-done:
			return response
		case <-ctx.Done():
			return NewErrorResponse(NewError(InternalErrorCode, "Request timeout", nil), req.ID)
		}
	}
}
