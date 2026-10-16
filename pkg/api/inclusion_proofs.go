package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/unicitynetwork/bft-go-base/types"
)

// get_inclusion_proofs.v1 admission limits (UC consolidation plan §2.3).
const (
	// InclusionProofsMaxStateIDs is the maximum number of state IDs per call.
	InclusionProofsMaxStateIDs = 256
	// InclusionProofsMaxParamsBytes bounds the raw params.
	InclusionProofsMaxParamsBytes = 32 << 10
	// InclusionProofsMaxRequestBytes bounds the whole JSON-RPC request body.
	InclusionProofsMaxRequestBytes = 64 << 10
	// InclusionProofsMaxIDBytes bounds the JSON encoding of the request ID.
	InclusionProofsMaxIDBytes = 128
	// InclusionProofsMaxResponseBytes bounds the serialized JSON-RPC response.
	InclusionProofsMaxResponseBytes = 8 << 20
	// InclusionProofsStateRootLatest is the keyword selecting the latest view.
	InclusionProofsStateRootLatest = "latest"
)

// GetInclusionProofsRequestV1 is the params object of get_inclusion_proofs.v1.
// Decode it with ParseGetInclusionProofsRequestV1, which enforces the limits.
type GetInclusionProofsRequestV1 struct {
	StateIDs  []StateID `json:"stateIds"`
	StateRoot string    `json:"stateRoot,omitempty"` // "latest", 64 hex digits, or omitted (latest)

	// Root is the decoded exact root; nil selects the latest view.
	Root HexBytes `json:"-"`

	// WaitMs is the optional long-poll wait, valid only for latest requests. Reserved: it is
	// validated but not yet acted upon.
	WaitMs WaitMs `json:"-"`
}

// GetInclusionProofsResponseV1 is the result of get_inclusion_proofs.v1. Each
// proof serializes exactly like a get_inclusion_proof.v2 result. PartitionView
// (hex CBOR, byte-identical to the accepted transport sidecar) is present only
// for latest requests.
type GetInclusionProofsResponseV1 struct {
	Proofs        []*GetInclusionProofResponseV2 `json:"proofs"`
	PartitionView HexBytes                       `json:"partitionView,omitempty"`
	// WaitedMs is the long-poll echo; nil (omitted) for requests without waitMs, a pointer so
	// that an explicit zero is serialized.
	WaitedMs *uint64 `json:"waitedMs,omitempty"`
}

// WaitedEcho reports the long-poll echo of the response (present iff the request carried waitMs).
func (r *GetInclusionProofsResponseV1) WaitedEcho() (uint64, bool) {
	if r == nil || r.WaitedMs == nil {
		return 0, false
	}
	return *r.WaitedMs, true
}

// ErrBoundedOutputExceeded is returned by AppendJSONBounded when the output would
// exceed the limit. It satisfies the ResponseTooLarge() contract of
// jsonrpc.BoundedJSON without importing that package.
var ErrBoundedOutputExceeded error = boundedOutputExceeded{}

type boundedOutputExceeded struct{}

func (boundedOutputExceeded) Error() string          { return "bounded JSON output exceeds limit" }
func (boundedOutputExceeded) ResponseTooLarge() bool { return true }

// AppendJSONBounded implements jsonrpc.BoundedJSON: it appends the same JSON as
// json.Marshal, element by element, and stops before encoding an element that
// would take the output past max. Each proof is CBOR-encoded once (its size is
// known before its hex is produced), so at most one oversized element is ever
// held beyond max.
func (r *GetInclusionProofsResponseV1) AppendJSONBounded(dst []byte, max int) ([]byte, error) {
	if r == nil {
		return append(dst, "null"...), nil
	}
	dst = append(dst, `{"proofs":`...)
	if r.Proofs == nil {
		dst = append(dst, "null"...)
	} else {
		dst = append(dst, '[')
		for i, p := range r.Proofs {
			if i > 0 {
				dst = append(dst, ',')
			}
			if p == nil {
				dst = append(dst, "null"...)
				continue
			}
			enc, err := types.Cbor.Marshal(p)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal to CBOR: %w", err)
			}
			if len(dst)+2*len(enc)+2 > max {
				return nil, ErrBoundedOutputExceeded
			}
			dst = appendHexString(dst, enc, max)
		}
		dst = append(dst, ']')
	}
	if len(r.PartitionView) > 0 {
		if len(dst)+len(`,"partitionView":`)+2*len(r.PartitionView)+3 > max {
			return nil, ErrBoundedOutputExceeded
		}
		dst = append(dst, `,"partitionView":`...)
		dst = appendHexString(dst, r.PartitionView, max)
	}
	if r.WaitedMs != nil {
		dst = append(dst, `,"waitedMs":`...)
		dst = strconv.AppendUint(dst, *r.WaitedMs, 10)
	}
	dst = append(dst, '}')
	if len(dst) > max {
		return nil, ErrBoundedOutputExceeded
	}
	return dst, nil
}

// ensureCap makes room for extra more bytes, doubling the capacity (never past
// max when that still fits) so the cumulative allocation stays within ~2x the
// final size instead of the append growth schedule's several times.
func ensureCap(dst []byte, extra, max int) []byte {
	need := len(dst) + extra
	if cap(dst) >= need {
		return dst
	}
	nc := 2 * cap(dst)
	if nc > max {
		nc = max
	}
	if nc < need {
		nc = need
	}
	out := make([]byte, len(dst), nc)
	copy(out, dst)
	return out
}

func appendHexString(dst, b []byte, max int) []byte {
	n := hex.EncodedLen(len(b))
	dst = ensureCap(dst, n+2, max)
	dst = append(dst, '"')
	l := len(dst)
	dst = dst[:l+n]
	hex.Encode(dst[l:], b)
	return append(dst, '"')
}

// Request error details of get_inclusion_proofs.v1 (data.detail).
const (
	DetailMalformedParams  = "malformed_params"
	DetailInvalidStateID   = "invalid_state_id"
	DetailDuplicateStateID = "duplicate_state_id"
	DetailWrongShard       = "wrong_shard"
	DetailInvalidStateRoot = "invalid_state_root"
	DetailStateCount       = "state_count"
	DetailRequestBytes     = "request_bytes"
	DetailResponseBytes    = "response_bytes"
)

// InclusionProofsParamsError is a typed rejection of the request params.
type InclusionProofsParamsError struct {
	Detail string
	// Limit is set for the limit errors (state_count, request_bytes).
	Limit int
	Err   error
}

func (e *InclusionProofsParamsError) Error() string {
	if e.Err != nil {
		return e.Detail + ": " + e.Err.Error()
	}
	return e.Detail
}

func (e *InclusionProofsParamsError) Unwrap() error { return e.Err }

// IsLimit reports whether the error is a limit (not a validity) error.
func (e *InclusionProofsParamsError) IsLimit() bool { return e.Limit > 0 }

func paramsErr(detail string, err error) error {
	return &InclusionProofsParamsError{Detail: detail, Err: err}
}

func parseRawStateKey32(s string) (HexBytes, error) {
	b, err := NewHexBytesFromString(s)
	if err != nil {
		return nil, err
	}
	if len(b) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("expected %d bytes, got %d", StateTreeKeyLengthBytes, len(b))
	}
	return b, nil
}

// ParseGetInclusionProofsRequestV1 strictly decodes the params object. The
// params size is checked first and the stateIds array is counted token by token,
// so a request with more than InclusionProofsMaxStateIDs IDs is rejected before
// the ID slice grows past the limit. Null and omission are distinguished:
// stateRoot omitted selects latest, null/empty/other keywords are invalid.
// Duplicate IDs (by decoded bytes) are an error, never silently removed.
func ParseGetInclusionProofsRequestV1(params []byte) (*GetInclusionProofsRequestV1, error) {
	if len(params) > InclusionProofsMaxParamsBytes {
		return nil, &InclusionProofsParamsError{Detail: DetailRequestBytes, Limit: InclusionProofsMaxParamsBytes}
	}
	dec := json.NewDecoder(bytes.NewReader(params))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, paramsErr(DetailMalformedParams, errors.New("params must be an object"))
	}
	req := &GetInclusionProofsRequestV1{}
	seenIDs, seenRoot := false, false
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, paramsErr(DetailMalformedParams, err)
		}
		key, _ := tok.(string)
		switch key {
		case "stateIds":
			if seenIDs {
				return nil, paramsErr(DetailMalformedParams, errors.New("duplicate stateIds"))
			}
			seenIDs = true
			if err := decodeStateIDs(dec, req); err != nil {
				return nil, err
			}
		case "stateRoot":
			if seenRoot {
				return nil, paramsErr(DetailMalformedParams, errors.New("duplicate stateRoot"))
			}
			seenRoot = true
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, paramsErr(DetailMalformedParams, err)
			}
			if err := decodeStateRoot(raw, req); err != nil {
				return nil, err
			}
		case "waitMs":
			if req.WaitMs.Present {
				return nil, paramsErr(DetailMalformedParams, errDuplicateWaitMs)
			}
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, paramsErr(DetailMalformedParams, err)
			}
			v, err := ParseWaitMs(raw)
			if err != nil {
				return nil, paramsErr(DetailMalformedParams, err)
			}
			req.WaitMs = WaitMs{Present: true, Value: v}
		default:
			return nil, paramsErr(DetailMalformedParams, fmt.Errorf("unknown field %q", key))
		}
	}
	if _, err := dec.Token(); err != nil { // closing brace
		return nil, paramsErr(DetailMalformedParams, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, paramsErr(DetailMalformedParams, errors.New("trailing data after params"))
	}
	if req.WaitMs.Present && req.Root != nil {
		return nil, paramsErr(DetailMalformedParams, errors.New("waitMs is not allowed with an exact stateRoot"))
	}
	if len(req.StateIDs) == 0 {
		return nil, paramsErr(DetailMalformedParams, errors.New("stateIds must contain 1.."+fmt.Sprint(InclusionProofsMaxStateIDs)+" IDs"))
	}
	return req, nil
}

func decodeStateIDs(dec *json.Decoder, req *GetInclusionProofsRequestV1) error {
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return paramsErr(DetailMalformedParams, errors.New("stateIds must be an array"))
	}
	seen := map[string]struct{}{}
	for dec.More() {
		if len(req.StateIDs) == InclusionProofsMaxStateIDs {
			return &InclusionProofsParamsError{Detail: DetailStateCount, Limit: InclusionProofsMaxStateIDs}
		}
		var s string
		if err := dec.Decode(&s); err != nil {
			return paramsErr(DetailMalformedParams, errors.New("stateIds entries must be strings"))
		}
		id, err := parseRawStateKey32(s)
		if err != nil {
			return paramsErr(DetailInvalidStateID, err)
		}
		if _, dup := seen[string(id)]; dup {
			return paramsErr(DetailDuplicateStateID, nil)
		}
		seen[string(id)] = struct{}{}
		req.StateIDs = append(req.StateIDs, StateID(id))
	}
	if _, err := dec.Token(); err != nil { // closing bracket
		return paramsErr(DetailMalformedParams, err)
	}
	return nil
}

func decodeStateRoot(raw json.RawMessage, req *GetInclusionProofsRequestV1) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil { // includes null
		return paramsErr(DetailInvalidStateRoot, errors.New("stateRoot must be a string"))
	}
	if s == InclusionProofsStateRootLatest {
		req.StateRoot = s
		return nil
	}
	if s == "" || strings.EqualFold(s, InclusionProofsStateRootLatest) {
		return paramsErr(DetailInvalidStateRoot, errors.New("stateRoot must be \"latest\" or 64 hex digits"))
	}
	root, err := parseRawStateKey32(s)
	if err != nil {
		return paramsErr(DetailInvalidStateRoot, err)
	}
	req.StateRoot = s
	req.Root = root
	return nil
}

// ErrProofNotYetAvailable is returned by get_inclusion_proof.v2 when no certified block binds the
// current tree root yet (before the first block). It is retryable and must never be answered with
// an empty-tree proof: no UC certifies the empty root. The gateway maps it to the typed
// proof_unavailable / no_finalized_view error (-32008, retryable) of the batch method.
var ErrProofNotYetAvailable = errors.New("proof not yet available: no finalized block")
