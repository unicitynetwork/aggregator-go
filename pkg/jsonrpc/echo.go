package jsonrpc

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// echoData is the error data of an error answering a request which carried waitMs: the original
// data with "waitedMs" added (revision 3 section 3). An object keeps its members, in order, and
// gets waitedMs last; absent data becomes {"waitedMs":n}; any other original value is kept as
// "originalData".
type echoData struct {
	orig     interface{}
	waitedMs uint64
}

func (d echoData) MarshalJSON() ([]byte, error) {
	waited := strconv.AppendUint([]byte(`"waitedMs":`), d.waitedMs, 10)
	if d.orig == nil {
		return append(append([]byte{'{'}, waited...), '}'), nil
	}
	raw, err := json.Marshal(d.orig)
	if err != nil {
		return nil, err
	}
	raw = bytes.TrimSpace(raw)
	switch {
	case len(raw) == 0 || bytes.Equal(raw, []byte("null")):
		return append(append([]byte{'{'}, waited...), '}'), nil
	case raw[0] == '{':
		inner := bytes.TrimSpace(raw[1 : len(raw)-1])
		out := []byte{'{'}
		if len(inner) > 0 {
			out = append(append(out, inner...), ',')
		}
		return append(append(out, waited...), '}'), nil
	default:
		out := append([]byte(`{"originalData":`), raw...)
		return append(append(out, ','), append(waited, '}')...), nil
	}
}

// WithWaited returns a copy of the error that echoes the long-poll wait in its data. The
// original is not modified (predefined errors are shared).
func (e *Error) WithWaited(waitedMs uint64) *Error {
	if e == nil {
		return nil
	}
	if prev, ok := e.Data.(echoData); ok {
		prev.waitedMs = waitedMs
		return &Error{Code: e.Code, Message: e.Message, Data: prev}
	}
	return &Error{Code: e.Code, Message: e.Message, Data: echoData{orig: e.Data, waitedMs: waitedMs}}
}
