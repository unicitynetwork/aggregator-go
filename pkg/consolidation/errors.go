package consolidation

import (
	"errors"
	"fmt"
)

// Outcome classifies why a consolidation was refused.
type Outcome string

const (
	// OutcomeInvalidInput: malformed or inconsistent caller input or responses.
	OutcomeInvalidInput Outcome = "invalid_input"
	// OutcomeUnavailable: required evidence (R_min, master view) is missing;
	// no value is guessed in its place.
	OutcomeUnavailable Outcome = "unavailable"
	// OutcomeUnauthenticated: the master view or seal is not authenticated by
	// the trusted authority.
	OutcomeUnauthenticated Outcome = "unauthenticated"
	// OutcomeBindingMismatch: a response is not bound to the original input
	// (request, certification data, state ID, shard, root).
	OutcomeBindingMismatch Outcome = "binding_mismatch"
	// OutcomeStale: the master seal predates the required root round.
	OutcomeStale Outcome = "stale"
	// OutcomeExpired: the original request expired at its reference time.
	OutcomeExpired Outcome = "expired"
	// OutcomeReferenceTime: the original reference time is later than the
	// timestamp of the target input record.
	OutcomeReferenceTime Outcome = "reference_time_after_target"
	// OutcomeIncompatibleLineage: the evidence or bound belongs to another
	// network, policy or history than the master seal.
	OutcomeIncompatibleLineage Outcome = "incompatible_lineage"
	// OutcomeUnsupported: unsupported encoding or configuration.
	OutcomeUnsupported Outcome = "unsupported"
)

// Error is the typed refusal returned by this package.
type Error struct {
	Outcome Outcome
	// Index is the index of the offending input, or -1.
	Index  int
	Detail string
	Err    error
}

func (e *Error) Error() string {
	s := string(e.Outcome)
	if e.Index >= 0 {
		s += fmt.Sprintf(" [input %d]", e.Index)
	}
	s += ": " + e.Detail
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

func fail(o Outcome, idx int, err error, format string, a ...any) error {
	return &Error{Outcome: o, Index: idx, Detail: fmt.Sprintf(format, a...), Err: err}
}

// OutcomeOf returns the outcome of an error produced by this package.
func OutcomeOf(err error) (Outcome, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Outcome, true
	}
	return "", false
}
