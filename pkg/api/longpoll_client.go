package api

import (
	"encoding/json"
	"time"
)

// Client-side long-poll budget helpers (revision 3 section 10), shared by the Go clients.
const (
	// DefaultLongPollWindow is the preferred wait window P.
	DefaultLongPollWindow = 20 * time.Second
	// DefaultLongPollMargin is the HTTP response margin M.
	DefaultLongPollMargin = 2 * time.Second
	// NoLimit marks an unbounded term of LongPollWindow.
	NoLimit time.Duration = -1
)

// LongPollWindow returns the waitMs to send, as a duration with millisecond granularity:
// floor(max(0, min(P, R-M, T-M))) for the preferred window P, the known remaining total budget R,
// the transport maximum T and the response margin M. R or T may be NoLimit when genuinely
// unbounded (that term is omitted). Zero means "send no wait": use a normal immediate lookup.
func LongPollWindow(preferred, remaining, transportMax, margin time.Duration) time.Duration {
	w := preferred
	if remaining != NoLimit && remaining-margin < w {
		w = remaining - margin
	}
	if transportMax != NoLimit && transportMax-margin < w {
		w = transportMax - margin
	}
	if w <= 0 {
		return 0
	}
	return w.Truncate(time.Millisecond)
}

// ParseEchoMs validates the raw waitedMs of a response (a result member or an error.data
// member). The echo is unauthenticated timing metadata: it must be a plain non-negative integer
// within the interoperable range, and is never used to extend a deadline.
func ParseEchoMs(raw json.RawMessage) (uint64, error) { return ParseWaitMs(raw) }

// FindEchoMs extracts waitedMs from a JSON object (a batch result or an error.data object). The
// bool reports whether the member is present; a present but malformed value is an error.
func FindEchoMs(object json.RawMessage) (ms uint64, present bool, err error) {
	var m map[string]json.RawMessage
	if json.Unmarshal(object, &m) != nil {
		return 0, false, nil // not an object: no echo
	}
	raw, ok := m["waitedMs"]
	if !ok {
		return 0, false, nil
	}
	ms, err = ParseEchoMs(raw)
	return ms, true, err
}

// IsParserNegotiation reports whether a JSON-RPC error of get_inclusion_proofs.v1 is the generic
// rejection of a strict batch parser which does not know waitMs: -32602, reason invalid_request,
// detail malformed_params and NO waitedMs echo. A rejection carrying the echo comes from a server
// which parsed waitMs, so retrying without it would be wrong. The caller retries exactly once,
// only for a locally validated latest request that carried waitMs.
func IsParserNegotiation(code int, data json.RawMessage) bool {
	if code != -32602 {
		return false
	}
	var d struct {
		Reason string          `json:"reason"`
		Detail string          `json:"detail"`
		Waited json.RawMessage `json:"waitedMs"`
	}
	if json.Unmarshal(data, &d) != nil {
		return false
	}
	return d.Reason == "invalid_request" && d.Detail == DetailMalformedParams && len(d.Waited) == 0
}
