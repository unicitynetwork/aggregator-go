package jsonrpc

import (
	"encoding/json"
	"fmt"
)

// Version represents the JSON-RPC version
const Version = "2.0"

// Request represents a JSON-RPC 2.0 request
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id"`
}

// Response represents a JSON-RPC 2.0 response
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// Error represents a JSON-RPC 2.0 error
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Error implements the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message)
}

// Standard JSON-RPC 2.0 error codes
const (
	ParseErrorCode     = -32700
	InvalidRequestCode = -32600
	MethodNotFoundCode = -32601
	InvalidParamsCode  = -32602
	InternalErrorCode  = -32603

	// Custom application error codes (starting from -32000)
	ValidationErrorCode    = -32000
	CommitmentExistsCode   = -32001
	CommitmentNotFoundCode = -32002
	BlockNotFoundCode      = -32003
	DatabaseErrorCode      = -32004
	ConsensusErrorCode     = -32005
	ConcurrencyLimitCode   = -32006
	// LimitExceededCode is a method-specific size/count limit (get_inclusion_proofs.v1).
	LimitExceededCode = -32007
	// ProofUnavailableCode: the requested proof material cannot be served (get_inclusion_proofs.v1).
	ProofUnavailableCode = -32008
)

// Predefined errors
var (
	ErrParseError       = &Error{Code: ParseErrorCode, Message: "Parse error"}
	ErrInvalidRequest   = &Error{Code: InvalidRequestCode, Message: "Invalid Request"}
	ErrMethodNotFound   = &Error{Code: MethodNotFoundCode, Message: "Method not found"}
	ErrConcurrencyLimit = &Error{Code: ConcurrencyLimitCode, Message: "Concurrency limit exceeded"}
)

// NewError creates a new JSON-RPC error
func NewError(code int, message string, data interface{}) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Data:    data,
	}
}

// NewValidationError creates a validation error
func NewValidationError(message string) *Error {
	return &Error{
		Code:    ValidationErrorCode,
		Message: message,
	}
}

// NewRequest creates a new JSON-RPC request
func NewRequest(method string, params interface{}, id interface{}) (*Request, error) {
	var rawParams json.RawMessage

	if params != nil {
		if raw, ok := params.(json.RawMessage); ok {
			rawParams = raw
		} else {
			bytes, err := json.Marshal(params)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal params: %w", err)
			}
			rawParams = bytes
		}
	}

	return &Request{
		JSONRPC: Version,
		Method:  method,
		Params:  rawParams,
		ID:      id,
	}, nil
}

// NewResponse creates a new JSON-RPC response
func NewResponse(result interface{}, id interface{}) *Response {
	return &Response{
		JSONRPC: Version,
		Result:  result,
		ID:      id,
	}
}

// NewErrorResponse creates a new JSON-RPC error response
func NewErrorResponse(err *Error, id interface{}) *Response {
	return &Response{
		JSONRPC: Version,
		Error:   err,
		ID:      id,
	}
}

// IsValidRequest validates a JSON-RPC request
func (r *Request) IsValidRequest() error {
	if r.JSONRPC != Version {
		return fmt.Errorf("invalid JSON-RPC version: %s", r.JSONRPC)
	}

	if r.Method == "" {
		return fmt.Errorf("method cannot be empty")
	}

	return nil
}

// UnmarshalParams unmarshals request parameters into the given value
func (r *Request) UnmarshalParams(v interface{}) error {
	if r.Params == nil {
		return nil
	}

	return json.Unmarshal(r.Params, v)
}

// ErrorData is the structured data of the get_inclusion_proofs.v1 errors:
// {"reason":"proof_unavailable","detail":"view_not_retained","retryable":true}.
// Limit is a bounded integer only for limit errors.
type ErrorData struct {
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
	Retryable bool   `json:"retryable"`
	Limit     *int   `json:"limit,omitempty"`
}

// NewDetailedError creates an error carrying ErrorData.
func NewDetailedError(code int, message, reason, detail string, retryable bool, limit *int) *Error {
	return &Error{Code: code, Message: message, Data: ErrorData{Reason: reason, Detail: detail, Retryable: retryable, Limit: limit}}
}

// MethodLimits are request/response bounds applied to one method before its
// handler runs (and, for the response, before any byte is written). Zero means
// unlimited. They are checked after the generic envelope decode, so they do not
// replace the deployment's ingress body cap.
type MethodLimits struct {
	// MaxRequestBytes bounds the bytes of the JSON-RPC request object.
	MaxRequestBytes int
	// MaxParamsBytes bounds the raw params.
	MaxParamsBytes int
	// MaxIDBytes bounds the JSON encoding of the request ID.
	MaxIDBytes int
	// MaxResponseBytes bounds the complete serialized response, including the
	// envelope and request ID. An oversized success is replaced by a
	// limit_exceeded/response_bytes error; nothing partial is ever written.
	MaxResponseBytes int
}
