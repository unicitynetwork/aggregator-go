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
	BlockInProgress        = -32007
)

// Predefined errors
var (
	ErrParseError     = &Error{Code: ParseErrorCode, Message: "Parse error"}
	ErrInvalidRequest = &Error{Code: InvalidRequestCode, Message: "Invalid Request"}
	ErrMethodNotFound = &Error{Code: MethodNotFoundCode, Message: "Method not found"}
	ErrInvalidParams  = &Error{Code: InvalidParamsCode, Message: "Invalid params"}
	ErrInternalError  = &Error{Code: InternalErrorCode, Message: "Internal error"}

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

// NewCommitmentExistsError creates a commitment exists error
func NewCommitmentExistsError(requestID string) *Error {
	return &Error{
		Code:    CommitmentExistsCode,
		Message: "Commitment already exists",
		Data:    map[string]string{"requestId": requestID},
	}
}

// NewCommitmentNotFoundError creates a commitment not found error
func NewCommitmentNotFoundError(requestID string) *Error {
	return &Error{
		Code:    CommitmentNotFoundCode,
		Message: "Commitment not found",
		Data:    map[string]string{"requestId": requestID},
	}
}

// NewBlockNotFoundError creates a block not found error
func NewBlockNotFoundError(blockNumber string) *Error {
	return &Error{
		Code:    BlockNotFoundCode,
		Message: "Block not found",
		Data:    map[string]string{"blockNumber": blockNumber},
	}
}

// NewDatabaseError creates a database error
func NewDatabaseError(message string) *Error {
	return &Error{
		Code:    DatabaseErrorCode,
		Message: message,
	}
}

// NewConsensusError creates a consensus error
func NewConsensusError(message string) *Error {
	return &Error{
		Code:    ConsensusErrorCode,
		Message: message,
	}
}

// NewRequest creates a new JSON-RPC request
func NewRequest(method string, params interface{}, id interface{}) (*Request, error) {
	var paramsBytes json.RawMessage
	if params != nil {
		bytes, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal params: %w", err)
		}
		paramsBytes = bytes
	}

	return &Request{
		JSONRPC: Version,
		Method:  method,
		Params:  paramsBytes,
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
