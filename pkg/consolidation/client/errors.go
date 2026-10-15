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

// Kind classifies a failed attempt.
type Kind string

const (
	// KindUnavailable: the required proof material cannot be served right now
	// (snapshot excludes a leaf, view evicted/not retained, busy, deadline,
	// transport failure). The inputs are intact; a new attempt may succeed but
	// waiting cannot make an excluded leaf appear in a fixed root. Nothing is
	// retried and nothing is resubmitted.
	KindUnavailable Kind = "unavailable"
	// KindRejected: the server refused the request itself (invalid params,
	// limit exceeded, wrong shard); retrying the same input cannot succeed.
	KindRejected Kind = "rejected"
	// KindInvalidResponse: a response is malformed or inconsistent.
	KindInvalidResponse Kind = "invalid_response"
	// KindVerification: the library refused the material (see Outcome).
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

func transportError(ctx context.Context, shard types.ShardID, err error) error {
	detail := "transport"
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		detail = "deadline"
	case errors.Is(ctx.Err(), context.Canceled):
		detail = "cancelled"
	}
	return &Error{Kind: KindUnavailable, Shard: shard, Detail: detail, Retryable: true, Err: err}
}

func rpcError(shard types.ShardID, code int, msg string, data json.RawMessage) error {
	e := &Error{Shard: shard, Code: code, Err: errors.New(msg)}
	var d jsonrpc.ErrorData
	_ = json.Unmarshal(data, &d)
	e.Reason, e.Detail, e.Retryable = d.Reason, d.Detail, d.Retryable
	switch code {
	case jsonrpc.ProofUnavailableCode, jsonrpc.ConcurrencyLimitCode, jsonrpc.DatabaseErrorCode:
		e.Kind = KindUnavailable
		if code == jsonrpc.ConcurrencyLimitCode {
			e.Retryable = true
		}
	case jsonrpc.InvalidParamsCode, jsonrpc.InvalidRequestCode, jsonrpc.LimitExceededCode, jsonrpc.MethodNotFoundCode:
		e.Kind = KindRejected
	default:
		e.Kind = KindUnavailable
	}
	return e
}
