package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// WaitMsMax is the largest accepted waitMs, 2^53-1: the largest integer every JSON
// implementation (JS numbers included) represents exactly. Larger values are overflow and are
// rejected; values above the server's configured maximum are valid and clamped later.
const WaitMsMax = 9007199254740991

// ErrInvalidWaitMs is returned for a present waitMs which is not a plain non-negative integer
// token within [0, WaitMsMax].
var ErrInvalidWaitMs = fmt.Errorf("waitMs must be a non-negative integer no greater than %d", int64(WaitMsMax))

// errDuplicateWaitMs is returned when waitMs occurs more than once in one params object.
var errDuplicateWaitMs = errors.New("duplicate waitMs")

// WaitMs is the optional long-poll wait of a proof read. Presence is tracked separately from
// the value: an absent field keeps the legacy response bytes, a present zero opts into the
// response metadata without waiting.
type WaitMs struct {
	Present bool
	Value   uint64
}

// ParseWaitMs validates the raw JSON token of a waitMs value: decimal digits only, no sign
// (so no "-0"), no fraction or exponent, no leading zeros, not null/boolean/string/array/object,
// and not above WaitMsMax.
func ParseWaitMs(raw []byte) (uint64, error) {
	if len(raw) == 0 || len(raw) > len(strconv.FormatUint(WaitMsMax, 10)) {
		return 0, ErrInvalidWaitMs
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, ErrInvalidWaitMs
		}
	}
	if len(raw) > 1 && raw[0] == '0' {
		return 0, ErrInvalidWaitMs
	}
	v, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil || v > WaitMsMax {
		return 0, ErrInvalidWaitMs
	}
	return v, nil
}

// ScanWaitMs finds and validates waitMs among the top-level members of a params object which has
// already been accepted as JSON by the caller's own decoder. It never fails for params without a
// waitMs member, whatever else they contain, so legacy behaviour is untouched; non-objects have
// none. The member name is matched exactly (after JSON unescaping).
func ScanWaitMs(params []byte) (WaitMs, error) {
	dec := json.NewDecoder(bytes.NewReader(params))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return WaitMs{}, nil
	}
	var w WaitMs
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return WaitMs{}, nil
		}
		key, _ := tok.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return WaitMs{}, nil
		}
		if key != "waitMs" {
			continue
		}
		if w.Present {
			return WaitMs{}, errDuplicateWaitMs
		}
		v, err := ParseWaitMs(raw)
		if err != nil {
			return WaitMs{}, err
		}
		w = WaitMs{Present: true, Value: v}
	}
	return w, nil
}
