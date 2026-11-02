package service

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The invalid-waitMs echo ruling (2026-11-02): a params object with a waitMs member, valid or not,
// is a recognized wait-bearing request and every validation error of it echoes waitedMs:0; a
// preflight rejection (request bytes, params bytes, RPC ID) never does.

type echoFixtureCase struct {
	Name       string          `json:"name"`
	Request    json.RawMessage `json:"request"`
	RawParams  string          `json:"rawParams"`
	RawRequest string          `json:"rawRequest"`
	Response   json.RawMessage `json:"response"`
}

func echoFixtureErrorCases(t *testing.T) []echoFixtureCase {
	raw, err := os.ReadFile("../../testdata/longpoll/echo.json")
	require.NoError(t, err)
	var f struct {
		Cases []echoFixtureCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	var out []echoFixtureCase
	for _, c := range f.Cases {
		for _, k := range []string{"invalid-wait", "validation-error", "unknown-member", "exact-root", "error-validation"} {
			if strings.Contains(c.Name, k) {
				out = append(out, c)
				break
			}
		}
	}
	require.NotEmpty(t, out)
	return out
}

// Every validation-error example of echo.json is what the production gateway answers, byte for
// byte as JSON (the exact request text is rawRequest, or the request with rawParams).
func TestLongPollEcho_ValidationFixturesReplay(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	for _, c := range echoFixtureErrorCases(t) {
		t.Run(c.Name, func(t *testing.T) {
			body := c.RawRequest
			if body == "" {
				var req map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(c.Request, &req))
				params := string(req["params"])
				if c.RawParams != "" {
					params = c.RawParams
				}
				body = `{"jsonrpc":"2.0","id":` + string(req["id"]) + `,"method":` + string(req["method"]) + `,"params":` + params + `}`
			}
			got := goldenPost(t, r.url, body)
			require.JSONEq(t, string(c.Response), got)
			if strings.Contains(c.Name, "preflight") {
				require.NotContains(t, got, "waitedMs")
			} else {
				require.Contains(t, got, `"waitedMs":0`)
			}
		})
	}
}

func TestLongPollEcho_PreflightBeatsAnInvalidWait(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	sid := strings.Repeat("ab", 32)
	for method, params := range map[string]func(extra string) string{
		"get_inclusion_proof.v2":  func(extra string) string { return `{"stateId":"` + sid + `","waitMs":-1` + extra + `}` },
		"get_inclusion_proofs.v1": func(extra string) string { return `{"stateIds":["` + sid + `"],"waitMs":-1` + extra + `}` },
	} {
		for _, wait := range []string{`-1`, `1,"waitMs":2`, `"5"`} {
			p := func(extra string) string { return strings.Replace(params(extra), `"waitMs":-1`, `"waitMs":`+wait, 1) }
			// params over 32 KiB
			body := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":` + p(`,"p":"`+strings.Repeat("a", api.InclusionProofsMaxParamsBytes)+`"`) + `}`
			b := goldenPost(t, r.url, body)
			require.Contains(t, b, `"code":-32007`, method)
			require.Contains(t, b, `"detail":"request_bytes"`, method)
			require.NotContains(t, b, "waitedMs", method)
			// request over 64 KiB with small params
			body = `{"jsonrpc":"2.0","id":1,"method":"` + method + `",` + strings.Repeat(" ", api.InclusionProofsMaxRequestBytes) + `"params":` + p(``) + `}`
			b = goldenPost(t, r.url, body)
			require.Contains(t, b, `"code":-32007`, method)
			require.NotContains(t, b, "waitedMs", method)
			// encoded ID over 128 bytes
			body = `{"jsonrpc":"2.0","id":"` + strings.Repeat("x", 200) + `","method":"` + method + `","params":` + p(``) + `}`
			b = goldenPost(t, r.url, body)
			require.Contains(t, b, `"detail":"invalid_id"`, method)
			require.NotContains(t, b, "waitedMs", method)
		}
	}
	// the opt-in bounds apply to a wait-bearing v2 request only: without the member the legacy
	// request is not bounded and gets no metadata
	big := strings.Repeat("a", api.InclusionProofsMaxParamsBytes)
	b := goldenPost(t, r.url, `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"abcd","p":"`+big+`"}}`)
	require.NotContains(t, b, `"request_bytes"`)
	require.NotContains(t, b, "waitedMs")
}

// An invalid or duplicate waitMs never reaches the coordinator: no wait is parked or counted.
func TestLongPollEcho_InvalidWaitNeverWaits(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	for _, extra := range []string{`,"waitMs":-1`, `,"waitMs":"5"`, `,"waitMs":1,"waitMs":1`, `,"waitMs":null`} {
		// a pending state would park a valid wait; the invalid one returns at once with the validation error
		r.st.set(r.later[0], api.ProofStatusPending)
		b := r.call(t, r.later[:1], extra)
		require.Contains(t, b, `"detail":"malformed_params"`, extra)
		require.Contains(t, b, `"waitedMs":0}`, extra)
	}
	require.Zero(t, r.reg.Stats().Reserved)
}

// Non-objects and nested look-alikes are not wait-bearing: no echo.
func TestLongPollEcho_NotRecognized(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	sid := strings.Repeat("ab", 32)
	for _, params := range []string{
		`{"stateIds":["abcd"]}`,
		`{"stateIds":["abcd"],"nested":{"waitMs":-1}}`,
		`{"stateIds":["abcd"],"note":"waitMs"}`,
		`{"stateIds":["abcd"],"waitms":-1}`,
		`[1]`, `"waitMs"`, `7`,
	} {
		b := goldenPost(t, r.url, `{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proofs.v1","params":`+params+`}`)
		require.Contains(t, b, `"error"`, params)
		require.NotContains(t, b, "waitedMs", params)
	}
	b := goldenPost(t, r.url, `{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proof.v2","params":{"stateId":"`+sid[:4]+`","nested":{"waitMs":-1}}}`)
	require.Contains(t, b, `"error"`)
	require.NotContains(t, b, "waitedMs")
}
