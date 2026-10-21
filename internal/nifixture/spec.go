package nifixture

import (
	_ "embed"
	"encoding/json"
)

//go:embed SPEC.md
var specText string

type rpcCase struct {
	Name       string `json:"name"`
	HTTPStatus int    `json:"httpStatus"`
	// RequestID is the id of the request the response answers.
	RequestID int `json:"requestId"`
	// Body is the exact response body (string, not JSON, so HTML and malformed bodies are representable).
	Body     string `json:"body"`
	Expected string `json:"expected"`
	Note     string `json:"note,omitempty"`
}

func jsonOut(v any) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

// rpcErrors are the envelope vectors of get_non_inclusion_proof.v1: how a client classifies the HTTP
// status and body. A valid envelope is classified by its code, whatever the HTTP status (rugregator uses 404
// and 429, aggregator-go uses 200). Anything which is not a complete, matching JSON-RPC 2.0 envelope is a
// transport error and never evidence about the state.
func rpcErrors() []byte {
	const res = `{"jsonrpc":"2.0","id":7,"result":"00"}`
	cases := []rpcCase{
		{"result-200", 200, 7, res, "result", "the result string is decoded by the wrapper rules"},
		{"not-found-404", 404, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32020,"message":"not found"}}`, "unavailable", "rugregator: no certified snapshot"},
		{"not-found-200", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32020,"message":"not found"}}`, "unavailable", "aggregator-go transport: the same code in a 200"},
		{"state-included-200", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32022,"message":"state included"}}`, "state_included", "false at the selected root; not a cryptographic statement"},
		{"state-included-409", 409, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32022,"message":"state included"}}`, "state_included", "the code decides, not the status"},
		{"busy-429", 429, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32023,"message":"busy"}}`, "busy", "retryable"},
		{"busy-200", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32023,"message":"busy"}}`, "busy", ""},
		{"invalid-params", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32602,"message":"invalid params"}}`, "invalid_params", ""},
		{"method-not-found", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32601,"message":"method not found"}}`, "method_not_found", ""},
		{"internal-500", 500, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"internal"}}`, "internal", "message is not trusted diagnostics"},
		{"unknown-code", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32099,"message":"x"}}`, "rpc_error:-32099", "unknown codes are preserved, never mapped to absence"},
		{"string-data", 429, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32023,"message":"busy","data":"limit"}}`, "busy", "data may be a string (rugregator) or an object (aggregator-go)"},
		{"object-data", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":-32023,"message":"busy","data":{"reason":"busy","detail":"active_requests","retryable":true}}}`, "busy", ""},
		{"proxy-html-404", 404, 7, `<html><body>404 Not Found</body></html>`, "transport_error", "a bare proxy 404 is not proof-service unavailability"},
		{"empty-body-429", 429, 7, ``, "transport_error", ""},
		{"wrong-id", 200, 7, `{"jsonrpc":"2.0","id":8,"error":{"code":-32022,"message":"state included"}}`, "transport_error", "the id is validated before the error is interpreted"},
		{"wrong-version", 200, 7, `{"jsonrpc":"1.0","id":7,"result":"00"}`, "transport_error", ""},
		{"result-and-error", 200, 7, `{"jsonrpc":"2.0","id":7,"result":"00","error":{"code":-32020,"message":"x"}}`, "transport_error", "exclusive result/error"},
		{"neither", 200, 7, `{"jsonrpc":"2.0","id":7}`, "transport_error", ""},
		{"non-integer-code", 200, 7, `{"jsonrpc":"2.0","id":7,"error":{"code":"-32020","message":"x"}}`, "transport_error", ""},
		{"result-not-a-string", 200, 7, `{"jsonrpc":"2.0","id":7,"result":{"proof":"00"}}`, "transport_error", "the result of this method is a string"},
	}
	return jsonOut(map[string]any{
		"description": "client classification of the responses of get_non_inclusion_proof.v1 (requests carry id 7); expected values: result, unavailable, state_included, busy, invalid_params, method_not_found, internal, rpc_error:<code>, transport_error",
		"cases":       cases,
	})
}

// statusHints freezes the spelling agreed for #209: unauthenticated, node-local hints.
func statusHints() []byte {
	return jsonOut(map[string]any{
		"description": "latest batch absence details (#209). Precedence unknown > expired > pending selects data.detail; data.indexes lists the zero-based request positions per class, ascending, at most 256 in total. Hints are node-local and unauthenticated. Exact-root absence is state_not_in_root whatever the live pending state.",
		"examples": []any{
			map[string]any{"name": "pending-only", "code": -32008, "data": map[string]any{"reason": "proof_unavailable", "detail": "state_pending", "retryable": true, "indexes": map[string]any{"pending": []int{1, 3}}}},
			map[string]any{"name": "mixed-unknown-wins", "code": -32008, "data": map[string]any{"reason": "proof_unavailable", "detail": "state_unknown", "retryable": false, "indexes": map[string]any{"pending": []int{0}, "expired": []int{2}, "unknown": []int{1, 4}}}},
			map[string]any{"name": "expired-over-pending", "code": -32008, "data": map[string]any{"reason": "proof_unavailable", "detail": "state_expired", "retryable": false, "indexes": map[string]any{"pending": []int{0}, "expired": []int{2}}}},
			map[string]any{"name": "exact-root-absent", "code": -32008, "data": map[string]any{"reason": "proof_unavailable", "detail": "state_not_in_root", "retryable": false, "indexes": map[string]any{}}},
		},
	})
}
