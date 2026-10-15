package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Kind classifies a failed attempt. Operational failures keep their own kinds:
// only the specified material/pressure outcomes are "unavailable".
type Kind string

const (
	// KindUnavailable: the server explicitly reports that the required proof
	// material cannot be served now (state not in the snapshot, view not retained
	// or evicted, no finalized view, no partition view) or that it is under
	// pressure (busy), or R_min is unknown. Inputs are intact; a *new* attempt may
	// succeed, waiting cannot make an excluded leaf appear in a fixed root.
	KindUnavailable Kind = "unavailable"
	// KindUnsupported: the endpoint cannot serve the method or the configuration
	// (method not found, unsupported_configuration). Retrying cannot help.
	KindUnsupported Kind = "unsupported"
	// KindTransport: the HTTP exchange failed (connection error, non-200 status).
	KindTransport Kind = "transport"
	// KindTimeout: the attempt's total deadline expired.
	KindTimeout Kind = "timeout"
	// KindCancelled: the caller cancelled. Never retryable.
	KindCancelled Kind = "cancelled"
	// KindServer: the server reported an operational failure (database, internal
	// or unknown error code); the original code is preserved.
	KindServer Kind = "server"
	// KindRejected: the server refused the request itself (invalid params, limit
	// exceeded); retrying the same input cannot succeed.
	KindRejected Kind = "rejected"
	// KindInvalidResponse: a response is malformed or inconsistent.
	KindInvalidResponse Kind = "invalid_response"
	// KindVerification: the library refused the material (see Outcome); includes
	// every authentication (signature, authority) failure.
	KindVerification Kind = "verification"
	// KindInput: bad caller input or configuration.
	KindInput Kind = "input"
)

// Error is the bounded, explicit failure of an attempt.
type Error struct {
	Kind  Kind
	Shard types.ShardID
	// Code, Reason and Detail echo the JSON-RPC error when there was one.
	Code      int
	Reason    string
	Detail    string
	Retryable bool
	// Outcome is the library outcome of a KindVerification error.
	Outcome consolidation.Outcome
	Err     error
}

func (e *Error) Error() string {
	s := string(e.Kind)
	if e.Outcome != "" {
		s += "/" + string(e.Outcome)
	}
	if e.Detail != "" {
		s += "/" + e.Detail
	}
	if e.Code != 0 {
		s += fmt.Sprintf(" (rpc %d)", e.Code)
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

// IsUnavailable reports whether err is an explicit unavailable outcome.
func IsUnavailable(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Kind == KindUnavailable
}

// IsCancelled reports caller cancellation.
func IsCancelled(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Kind == KindCancelled
}

func libError(err error) error {
	var le *consolidation.Error
	if errors.As(err, &le) {
		kind := KindVerification
		if le.Outcome == consolidation.OutcomeUnavailable {
			kind = KindUnavailable
		}
		return &Error{Kind: kind, Outcome: le.Outcome, Err: err}
	}
	return &Error{Kind: KindInput, Err: err}
}

// ctxKind maps a context error to its typed operational kind.
func ctxError(shard types.ShardID, err error) *Error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindTimeout, Shard: shard, Detail: "deadline", Retryable: true, Err: err}
	}
	return &Error{Kind: KindCancelled, Shard: shard, Detail: "cancelled", Err: err}
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// transportError types a failed HTTP exchange: context errors keep their own
// kinds, everything else is a transport failure.
func transportError(ctx context.Context, shard types.ShardID, err error) error {
	if ctx.Err() != nil && (isContextErr(err) || isContextErr(ctx.Err())) {
		return ctxError(shard, ctx.Err())
	}
	return &Error{Kind: KindTransport, Shard: shard, Detail: "transport", Retryable: true, Err: err}
}

func rpcError(shard types.ShardID, code int, msg string, data json.RawMessage) error {
	e := &Error{Shard: shard, Code: code, Err: errors.New(msg)}
	var d jsonrpc.ErrorData
	_ = json.Unmarshal(data, &d)
	e.Reason, e.Detail, e.Retryable = d.Reason, d.Detail, d.Retryable
	switch code {
	case jsonrpc.ProofUnavailableCode:
		if d.Detail == "unsupported_configuration" {
			e.Kind, e.Retryable = KindUnsupported, false
		} else {
			e.Kind = KindUnavailable
		}
	case jsonrpc.ConcurrencyLimitCode:
		e.Kind, e.Retryable = KindUnavailable, true
	case jsonrpc.MethodNotFoundCode:
		e.Kind, e.Detail, e.Retryable = KindUnsupported, "method_not_found", false
	case jsonrpc.InvalidParamsCode, jsonrpc.InvalidRequestCode, jsonrpc.LimitExceededCode:
		e.Kind, e.Retryable = KindRejected, false
	default: // database, internal and unknown codes are operational failures, not unavailability
		e.Kind = KindServer
	}
	return e
}
