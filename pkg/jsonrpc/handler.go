package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
)

// HandlerFunc represents a JSON-RPC method handler
type HandlerFunc func(ctx context.Context, params json.RawMessage) (interface{}, *Error)

// Server represents a JSON-RPC server
type Server struct {
	handlers     map[string]HandlerFunc
	limits       map[string]MethodLimits
	middleware   []MiddlewareFunc
	logger       *logger.Logger
	pool         *slotPool
	ownership    map[string]OwnershipPredicate
	maxBodyBytes int64
	mutex        sync.RWMutex
}

// DefaultMaxRequestBodyBytes is the outer ceiling on any JSON-RPC request body,
// applied before the envelope is decoded. It is deliberately generous so no
// existing method is affected; method-specific limits (for example
// get_inclusion_proofs.v1's 64 KiB) are stricter and checked after the decode.
const DefaultMaxRequestBodyBytes = 16 << 20

// SetMaxRequestBodyBytes sets the outer request body ceiling; n <= 0 restores
// the default.
func (s *Server) SetMaxRequestBodyBytes(n int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if n <= 0 {
		n = DefaultMaxRequestBodyBytes
	}
	s.maxBodyBytes = n
}

func (s *Server) bodyCeiling() int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.maxBodyBytes <= 0 {
		return DefaultMaxRequestBodyBytes
	}
	return s.maxBodyBytes
}

func isBodyTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// MiddlewareFunc represents middleware for JSON-RPC requests
type MiddlewareFunc func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response

// NewServer creates a new JSON-RPC server
func NewServer(logger *logger.Logger, concurrencyLimit int) *Server {
	return &Server{
		handlers:   make(map[string]HandlerFunc),
		limits:     make(map[string]MethodLimits),
		middleware: make([]MiddlewareFunc, 0),
		logger:     logger,
		pool:       &slotPool{limit: concurrencyLimit},
	}
}

// RegisterMethod registers a new JSON-RPC method handler
func (s *Server) RegisterMethod(method string, handler HandlerFunc) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.handlers[method] = handler
}

// RegisterMethodWithLimits registers a handler with method-specific request and
// response bounds.
func (s *Server) RegisterMethodWithLimits(method string, handler HandlerFunc, limits MethodLimits) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.handlers[method] = handler
	s.limits[method] = limits
}

func (s *Server) methodLimits(method string) (MethodLimits, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	l, ok := s.limits[method]
	return l, ok
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

	start := time.Now()
	recordMetrics := func(method, result string) {
		metrics.HTTPRequestsTotal.WithLabelValues(method, result).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	}

	if r.Method != "POST" {
		recordMetrics("unknown", "error")
		s.writeErrorResponse(w, ErrInvalidRequest, nil)
		return
	}

	// Check concurrency limit
	permit, ok := s.newPermit()
	if !ok {
		recordMetrics("unknown", "error")
		s.writeErrorResponse(w, ErrConcurrencyLimit, nil)
		return
	}
	defer permit.Close()

	// Add request ID to context
	ctx := context.WithValue(r.Context(), logger.StateIDKey, uuid.New().String())
	ctx = ContextWithPermit(ctx, permit)

	// Parse request. The ID is kept raw so that method-limited requests can
	// validate its original encoding and echo it without number conversion.
	var env rawRequest
	// Outer cap: no more than the ceiling is ever read from the connection, so
	// the envelope decode below cannot allocate an unbounded body.
	ceiling := s.bodyCeiling()
	r.Body = http.MaxBytesReader(w, r.Body, ceiling)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&env); err != nil {
		if isBodyTooLarge(err) {
			recordMetrics("unknown", "error")
			n := int(ceiling)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			s.writeErrorResponse(w, NewDetailedError(LimitExceededCode, "Limit exceeded", "limit_exceeded", "request_bytes", false, &n), nil)
			return
		}
		s.logger.WithContext(ctx).Error("Failed to parse JSON-RPC request", "error", err.Error())
		recordMetrics("unknown", "error")
		s.writeErrorResponse(w, ErrParseError, nil)
		return
	}
	req := Request{JSONRPC: env.JSONRPC, Method: env.Method, Params: env.Params}
	limits, limited := s.methodLimits(req.Method)
	if !limited {
		// Legacy methods: the ID decodes exactly as before.
		if len(env.ID) > 0 {
			if err := json.Unmarshal(env.ID, &req.ID); err != nil {
				s.logger.WithContext(ctx).Error("Failed to parse JSON-RPC request", "error", err.Error())
				recordMetrics("unknown", "error")
				s.writeErrorResponse(w, ErrParseError, nil)
				return
			}
		}
		if err := req.IsValidRequest(); err != nil {
			s.logger.WithContext(ctx).Error("Invalid JSON-RPC request", "error", err.Error())
			recordMetrics("unknown", "error")
			s.writeErrorResponse(w, ErrInvalidRequest, req.ID)
			return
		}
	} else {
		idOK := validRawID(env.ID, limits.MaxIDBytes)
		var id interface{} // an invalid or oversized ID is never echoed
		if idOK {
			id = json.RawMessage(env.ID)
		}
		if err := req.IsValidRequest(); err != nil {
			s.logger.WithContext(ctx).Error("Invalid JSON-RPC request", "error", err.Error())
			recordMetrics(metrics.NormalizeMethod(req.Method), "error")
			s.writeErrorResponse(w, NewDetailedError(InvalidRequestCode, "Invalid Request", "invalid_request", "invalid_envelope", false, nil), id)
			return
		}
		if rpcErr := checkRequestBody(limits, dec, r.Body); rpcErr != nil {
			recordMetrics(metrics.NormalizeMethod(req.Method), "error")
			s.writeErrorResponse(w, rpcErr, id)
			return
		}
		if !idOK {
			recordMetrics(metrics.NormalizeMethod(req.Method), "error")
			s.writeErrorResponse(w, NewDetailedError(InvalidParamsCode, "Invalid params", "invalid_request", "invalid_id", false, nil), nil)
			return
		}
		if rpcErr := checkParamsLimit(limits, &req); rpcErr != nil {
			recordMetrics(metrics.NormalizeMethod(req.Method), "error")
			s.writeErrorResponse(w, rpcErr, id)
			return
		}
		req.ID = id
	}

	// Add client's JSON-RPC ID to context for end-to-end tracing
	var ctxID interface{} = req.ID
	if raw, ok := req.ID.(json.RawMessage); ok {
		ctxID = string(raw)
	}
	ctx = context.WithValue(ctx, logger.JSONRPCIDKey, ctxID)

	s.markOwned(permit, &req)

	// Process request
	response := s.processRequest(ctx, &req)
	method := metrics.NormalizeMethod(req.Method)
	result := "success"
	if response != nil && response.Error != nil {
		result = "error"
	}
	recordMetrics(method, result)

	// Write response
	if limited && limits.MaxResponseBytes > 0 {
		s.writeLimitedResponse(w, response, limits.MaxResponseBytes)
		return
	}
	s.writeJSONResponse(w, response)
}

// rawRequest is the request envelope with the ID left undecoded.
type rawRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
}

// validRawID reports whether the original ID encoding is a JSON string or
// number within maxBytes (0 = unbounded). Absent, null, boolean, object and
// array IDs are invalid for method-limited requests.
func validRawID(raw json.RawMessage, maxBytes int) bool {
	if len(raw) == 0 || (maxBytes > 0 && len(raw) > maxBytes) {
		return false
	}
	switch c := raw[0]; {
	case c == '"':
		return true
	case c == '-' || (c >= '0' && c <= '9'):
		return true
	}
	return false
}

// checkRequestBody counts the bytes of the complete request body (not just the
// first JSON value): the rest is read through a bounded reader, and anything
// but whitespace after the request object is rejected.
func checkRequestBody(l MethodLimits, dec *json.Decoder, body io.Reader) *Error {
	consumed := dec.InputOffset()
	rest := io.MultiReader(dec.Buffered(), body)
	var buf [4096]byte
	total := consumed
	for {
		n, err := rest.Read(buf[:])
		for _, c := range buf[:n] {
			if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
				return NewDetailedError(InvalidRequestCode, "Invalid Request", "invalid_request", "invalid_envelope", false, nil)
			}
		}
		total += int64(n)
		if l.MaxRequestBytes > 0 && total > int64(l.MaxRequestBytes) {
			lim := l.MaxRequestBytes
			return NewDetailedError(LimitExceededCode, "Limit exceeded", "limit_exceeded", "request_bytes", false, &lim)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			if isBodyTooLarge(err) {
				lim := l.MaxRequestBytes
				return NewDetailedError(LimitExceededCode, "Limit exceeded", "limit_exceeded", "request_bytes", false, &lim)
			}
			return NewDetailedError(InvalidRequestCode, "Invalid Request", "invalid_request", "invalid_envelope", false, nil)
		}
	}
}

func checkParamsLimit(l MethodLimits, req *Request) *Error {
	if l.MaxParamsBytes > 0 && len(req.Params) > l.MaxParamsBytes {
		n := l.MaxParamsBytes
		return NewDetailedError(LimitExceededCode, "Limit exceeded", "limit_exceeded", "request_bytes", false, &n)
	}
	return nil
}

// writeLimitedResponse assembles the complete response and writes it only when
// it fits the limit. A success whose result is BoundedJSON is built with the
// output capped, counting the envelope and the ID, so an oversized result is
// rejected before it is fully encoded; any other result is encoded with
// json.Marshal (the existing path) and measured.
func (s *Server) writeLimitedResponse(w http.ResponseWriter, response *Response, max int) {
	data, err := encodeLimited(response, max)
	if tooLarge(err) {
		n := max
		data, err = json.Marshal(NewErrorResponse(
			NewDetailedError(LimitExceededCode, "Limit exceeded", "limit_exceeded", "response_bytes", false, &n), response.ID))
	}
	if err != nil {
		s.logger.WithError(err).Error("Failed to encode JSON response")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(append(data, '\n'))
}

// encodeLimited returns the response JSON without the trailing newline, or
// ErrResponseTooLarge if the response plus the newline exceeds max.
func encodeLimited(response *Response, max int) ([]byte, error) {
	if bj, ok := response.Result.(BoundedJSON); ok && response.Error == nil {
		id, err := json.Marshal(response.ID)
		if err != nil {
			return nil, err
		}
		const prefix = `{"jsonrpc":"2.0","result":`
		suffix := append(append([]byte(`,"id":`), id...), '}')
		// budget for the result: everything but the envelope and the newline
		budget := max - len(prefix) - len(suffix) - 1
		if budget < 0 {
			return nil, ErrResponseTooLarge
		}
		buf := append(make([]byte, 0, 4096), prefix...)
		buf, err = bj.AppendJSONBounded(buf, len(prefix)+budget)
		if err != nil {
			return nil, err
		}
		return append(buf, suffix...), nil
	}
	data, err := json.Marshal(response)
	if err == nil && len(data)+1 > max {
		return nil, ErrResponseTooLarge
	}
	return data, err
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

	result, rpcErr := handler(ctx, req.Params)

	if rpcErr != nil {
		return NewErrorResponse(rpcErr, req.ID)
	}

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

// RequestIDMiddleware adds request ID to the context
func RequestIDMiddleware() MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		requestID := uuid.New().String()
		ctx = context.WithValue(ctx, logger.StateIDKey, requestID)
		return next(ctx, req)
	}
}

// LoggingMiddleware logs JSON-RPC requests.
// Note: WithContext adds request_id and jsonrpc_id automatically
func LoggingMiddleware(log *logger.Logger) MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		start := time.Now()

		log.WithContext(ctx).Log(ctx, logger.LevelTrace, "Processing JSON-RPC request",
			"method", req.Method)

		response := next(ctx, req)

		duration := time.Since(start)

		if response.Error != nil {
			log.WithContext(ctx).Error("JSON-RPC request failed",
				"method", req.Method,
				"duration_ms", duration.Milliseconds(),
				"error_code", response.Error.Code)
		} else {
			log.WithContext(ctx).Log(ctx, logger.LevelTrace, "JSON-RPC request completed",
				"method", req.Method,
				"duration_ms", duration.Milliseconds())
		}

		return response
	}
}

// TimeoutMiddleware adds timeout to requests
func TimeoutMiddleware(timeout time.Duration, log *logger.Logger) MiddlewareFunc {
	return func(ctx context.Context, req *Request, next func(context.Context, *Request) *Response) *Response {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		done := make(chan *Response, 1)

		// The worker may outlive this middleware (it returns on the deadline without waiting).
		// A request's permit must then stay held until the worker really stops (C07).
		permit := PermitFromContext(ctx)
		tracked := permit != nil && permit.Owned() && permit.AddWorker()

		go func() {
			if tracked {
				defer permit.WorkerDone()
			}
			defer func() {
				if r := recover(); r != nil {
					log.WithContext(ctx).Error("panic in JSON-RPC handler", "panic", r, "method", req.Method)
					errResp := NewErrorResponse(NewError(InternalErrorCode, "Internal server error", nil), req.ID)
					select {
					case done <- errResp:
					default:
					}
				}
			}()

			resp := next(ctx, req)
			select {
			case done <- resp:
			default:
			}
		}()

		select {
		case response := <-done:
			return response
		case <-ctx.Done():
			return NewErrorResponse(NewError(InternalErrorCode, "Request timeout", nil), req.ID)
		}
	}
}
