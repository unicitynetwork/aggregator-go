package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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
}

// GetInclusionProofsResponseV1 is the result of get_inclusion_proofs.v1. Each
// proof serializes exactly like a get_inclusion_proof.v2 result. PartitionView
// (hex CBOR, byte-identical to the accepted transport sidecar) is present only
// for latest requests.
type GetInclusionProofsResponseV1 struct {
	Proofs        []*GetInclusionProofResponseV2 `json:"proofs"`
	PartitionView HexBytes                       `json:"partitionView,omitempty"`
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
