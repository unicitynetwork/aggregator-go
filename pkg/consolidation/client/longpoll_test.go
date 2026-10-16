package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// fakeAggregator records the params of every request and answers with a scripted response.
type fakeAggregator struct {
	mu      sync.Mutex
	params  []map[string]json.RawMessage
	respond func(n int, p map[string]json.RawMessage) string
}

func (f *fakeAggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var env struct {
		Params map[string]json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(b, &env)
	f.mu.Lock()
	f.params = append(f.params, env.Params)
	n := len(f.params)
	f.mu.Unlock()
	_, _ = io.WriteString(w, f.respond(n, env.Params))
}

func (f *fakeAggregator) wait(i int) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.params[i]["waitMs"]
	return string(v), ok
}

// requireWait asserts waitMs is want or a few ms less: the remaining time shrinks while the test runs.
func requireWait(t *testing.T, got string, want int, msg string) {
	t.Helper()
	var v int
	require.NoError(t, json.Unmarshal([]byte(got), &v), msg)
	require.LessOrEqual(t, v, want, msg)
	require.GreaterOrEqual(t, v, want-100, msg)
}

func (f *fakeAggregator) requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.params)
}

const okProof = `{"blockNumber":1,"inclusionProof":null}`

func successBody(echo string) string {
	proof, _ := json.Marshal(&api.GetInclusionProofResponseV2{BlockNumber: 1, InclusionProof: &api.InclusionProofV2{UnicityCertificate: types.RawCBOR{0x01}}})
	extra := ""
	if echo != "" {
		extra = `,"waitedMs":` + echo
	}
	return `{"jsonrpc":"2.0","id":1,"result":{"proofs":[` + string(proof) + `]` + extra + `}}`
}

func errorBody(code int, data string) string {
	return `{"jsonrpc":"2.0","id":1,"error":{"code":` + itoa(code) + `,"message":"m","data":` + data + `}}`
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

const genericMalformed = `{"reason":"invalid_request","detail":"malformed_params","retryable":false}`

func lpClient(srv *httptest.Server, mutate func(*Config)) (*Client, *shardGroup, []Item) {
	cfg := Config{LongPoll: true, Timeout: 10 * time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	hc := srv.Client()
	return &Client{cfg: cfg, http: hc},
		&shardGroup{ep: Endpoint{URL: srv.URL}, items: []int{0}},
		[]Item{{Request: &api.CertificationRequest{StateID: make(api.StateID, 32)}}}
}

func TestLongPoll_OffByDefaultAndNeverOnPeerRequests(t *testing.T) {
	f := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return successBody("") }}
	srv := httptest.NewServer(f)
	defer srv.Close()

	c, g, items := lpClient(srv, func(c *Config) { c.LongPoll = false })
	var st Stats
	_, err := c.fetch(t.Context(), g, items, nil, &st)
	require.NoError(t, err)
	_, has := f.wait(0)
	require.False(t, has, "an attempt stays a single non-waiting pass unless enabled")

	c, g, items = lpClient(srv, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = c.fetch(ctx, g, items, nil, &st) // master latest
	require.NoError(t, err)
	_, err = c.fetch(ctx, g, items, make([]byte, 32), &st) // exact-root peer
	require.NoError(t, err)
	w0, has0 := f.wait(1)
	require.True(t, has0)
	requireWait(t, w0, 8000, "10s attempt - 2s margin")
	_, has1 := f.wait(2)
	require.False(t, has1, "never on exact-root peer calls")
}

func TestLongPoll_WindowIsClampedByAttemptAndHTTPTimeout(t *testing.T) {
	f := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return successBody("") }}
	srv := httptest.NewServer(f)
	defer srv.Close()
	for name, tc := range map[string]struct {
		ctxTimeout, httpTimeout time.Duration
		mutate                  func(*Config)
		want                    string // "" = no waitMs
	}{
		"preferred 20s fits":     {60 * time.Second, 0, nil, "20000"},
		"attempt bounds":         {5 * time.Second, 0, nil, "3000"},
		"http client bounds":     {60 * time.Second, 15 * time.Second, nil, "13000"},
		"both bound":             {12 * time.Second, 15 * time.Second, nil, "10000"},
		"nothing left":           {2 * time.Second, 0, nil, ""},
		"configured margin":      {10 * time.Second, 0, func(c *Config) { c.LongPollMargin = 5 * time.Second }, "5000"},
		"configured window":      {60 * time.Second, 0, func(c *Config) { c.LongPollWindow = 7 * time.Second }, "7000"},
		"no deadline, unbounded": {0, 0, nil, "20000"},
	} {
		t.Run(name, func(t *testing.T) {
			f.mu.Lock()
			f.params = nil
			f.mu.Unlock()
			c, g, items := lpClient(srv, tc.mutate)
			c.http = &http.Client{Transport: srv.Client().Transport, Timeout: tc.httpTimeout}
			ctx := t.Context()
			if tc.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.ctxTimeout)
				defer cancel()
			}
			var st Stats
			_, err := c.fetch(ctx, g, items, nil, &st)
			require.NoError(t, err)
			got, has := f.wait(0)
			require.Equal(t, tc.want != "", has)
			if tc.want != "" {
				var want int
				require.NoError(t, json.Unmarshal([]byte(tc.want), &want))
				requireWait(t, got, want, name)
			}
		})
	}
}

// The parser-negotiation exception: exactly one retry without waitMs, only for the matched
// generic malformed_params without an echo, with the same deadline; its outcome is final.
func TestLongPoll_ParserNegotiationRetry(t *testing.T) {
	f := &fakeAggregator{respond: func(n int, p map[string]json.RawMessage) string {
		if _, has := p["waitMs"]; has {
			return errorBody(-32602, genericMalformed) // a strict parser which does not know waitMs
		}
		return successBody("")
	}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c, g, items := lpClient(srv, nil)
	var st Stats
	_, err := c.fetch(t.Context(), g, items, nil, &st)
	require.NoError(t, err)
	require.Equal(t, 2, f.requests())
	_, has := f.wait(0)
	require.True(t, has)
	_, has = f.wait(1)
	require.False(t, has, "the retry omits waitMs and keeps every other param")
	var ids []string
	require.NoError(t, json.Unmarshal(f.params[1]["stateIds"], &ids))
	require.Len(t, ids, 1)
	require.Equal(t, 2, st.Requests, "counted in the request stats")
	require.Equal(t, 1, st.ParserRetries)
	require.Zero(t, st.WaitSent)

	// the second error is final: no third request
	f2 := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return errorBody(-32602, genericMalformed) }}
	srv2 := httptest.NewServer(f2)
	defer srv2.Close()
	c, g, items = lpClient(srv2, nil)
	st = Stats{}
	_, err = c.fetch(t.Context(), g, items, nil, &st)
	var ce *Error
	require.ErrorAs(t, err, &ce)
	require.Equal(t, KindRejected, ce.Kind)
	require.Equal(t, 2, f2.requests())
}

func TestLongPoll_NoRetryForOtherOutcomes(t *testing.T) {
	for name, tc := range map[string]struct {
		code int
		data string
	}{
		"echoed malformed_params":  {-32602, `{"reason":"invalid_request","detail":"malformed_params","retryable":false,"waitedMs":0}`},
		"invalid state id":         {-32602, `{"reason":"invalid_request","detail":"invalid_state_id","retryable":false}`},
		"duplicate state id":       {-32602, `{"reason":"invalid_request","detail":"duplicate_state_id","retryable":false}`},
		"limit":                    {-32007, `{"reason":"limit_exceeded","detail":"state_count","retryable":false,"limit":256}`},
		"method not found":         {-32601, `null`},
		"busy":                     {-32006, `{"reason":"busy","detail":"deadline","retryable":true}`},
		"pending (timeout)":        {-32008, `{"reason":"proof_unavailable","detail":"state_pending","retryable":true,"indexes":{"pending":[0]},"waitedMs":8000}`},
		"pending without echo":     {-32008, `{"reason":"proof_unavailable","detail":"state_pending","retryable":true,"indexes":{"pending":[0]}}`},
		"invalid params no detail": {-32602, `null`},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return errorBody(tc.code, tc.data) }}
			srv := httptest.NewServer(f)
			defer srv.Close()
			c, g, items := lpClient(srv, nil)
			var st Stats
			_, err := c.fetch(t.Context(), g, items, nil, &st)
			require.Error(t, err)
			require.Equal(t, 1, f.requests(), "exactly one request: ordinary outcomes go back to the caller")
			require.Zero(t, st.ParserRetries)
		})
	}
	// transport failures and non-JSON-RPC bodies (a proxy) are not capability evidence
	for name, h := range map[string]http.HandlerFunc{
		"proxy html": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(502)
			_, _ = io.WriteString(w, "<html>bad gateway</html>")
		},
		"auth": func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401); _, _ = io.WriteString(w, "no") },
	} {
		var n int
		var mu sync.Mutex
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mu.Lock(); n++; mu.Unlock(); h(w, r) }))
		c, g, items := lpClient(srv, nil)
		var st Stats
		_, err := c.fetch(t.Context(), g, items, nil, &st)
		require.Error(t, err, name)
		require.Equal(t, 1, n, name)
		srv.Close()
	}
}

// The retry is for waitMs requests only: a latest request without it (LongPoll off, no window
// left) never triggers a retry, and exact-root peers never carry waitMs to begin with.
func TestLongPoll_NoRetryWithoutWaitMs(t *testing.T) {
	f := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return errorBody(-32602, genericMalformed) }}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c, g, items := lpClient(srv, func(c *Config) { c.LongPoll = false })
	var st Stats
	_, err := c.fetch(t.Context(), g, items, nil, &st)
	require.Error(t, err)
	_, err = c.fetch(t.Context(), g, items, make([]byte, 32), &st)
	require.Error(t, err)
	require.Equal(t, 2, f.requests())
}

func TestLongPoll_EchoIsValidatedAndRecorded(t *testing.T) {
	f := &fakeAggregator{}
	srv := httptest.NewServer(f)
	defer srv.Close()
	f.respond = func(int, map[string]json.RawMessage) string { return successBody("1200") }
	c, g, items := lpClient(srv, nil)
	var st Stats
	_, err := c.fetch(t.Context(), g, items, nil, &st)
	require.NoError(t, err)
	require.True(t, st.EchoSeen)
	require.EqualValues(t, 1200, st.WaitedMs)

	// echo in an error's data is recorded too
	f.respond = func(int, map[string]json.RawMessage) string {
		return errorBody(-32008, `{"reason":"proof_unavailable","detail":"state_pending","retryable":true,"waitedMs":8000}`)
	}
	st = Stats{}
	_, err = c.fetch(t.Context(), g, items, nil, &st)
	require.Error(t, err)
	require.True(t, st.EchoSeen)
	require.EqualValues(t, 8000, st.WaitedMs)

	for _, bad := range []string{`-1`, `1.5`, `"7"`, `null`, `9007199254740992`, `true`} {
		f.respond = func(int, map[string]json.RawMessage) string { return successBody(bad) }
		_, err = c.fetch(t.Context(), g, items, nil, &Stats{})
		var ce *Error
		require.ErrorAs(t, err, &ce, bad)
		require.Equal(t, KindInvalidResponse, ce.Kind, "a malformed echo is a protocol error: "+bad)
		f.respond = func(int, map[string]json.RawMessage) string {
			return errorBody(-32008, `{"reason":"proof_unavailable","detail":"state_pending","retryable":true,"waitedMs":`+bad+`}`)
		}
		_, err = c.fetch(t.Context(), g, items, nil, &Stats{})
		require.ErrorAs(t, err, &ce, bad)
		require.Equal(t, KindInvalidResponse, ce.Kind, bad)
	}
}

// A zero or missing echo is simply accepted (the attempt is single-shot; nothing loops).
func TestLongPoll_MissingOrZeroEchoIsAccepted(t *testing.T) {
	for _, echo := range []string{"", "0"} {
		f := &fakeAggregator{respond: func(int, map[string]json.RawMessage) string { return successBody(echo) }}
		srv := httptest.NewServer(f)
		c, g, items := lpClient(srv, nil)
		var st Stats
		_, err := c.fetch(t.Context(), g, items, nil, &st)
		require.NoError(t, err)
		require.Equal(t, 1, f.requests())
		require.Equal(t, echo == "0", st.EchoSeen)
		srv.Close()
	}
}

var _ = strings.TrimSpace

// A non-2xx answer carrying the generic rejection but another request's id (a proxy, a cached or
// foreign body) is not capability evidence: no retry.
func TestLongPoll_NoRetryOnNon2xxWithAForeignID(t *testing.T) {
	for name, id := range map[string]string{"wrong id": "99", "string id": `"1"`, "null id": "null"} {
		var n int
		var mu sync.Mutex
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			n++
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+id+`,"error":{"code":-32602,"message":"m","data":`+genericMalformed+`}}`)
		}))
		c, g, items := lpClient(srv, nil)
		var st Stats
		_, err := c.fetch(t.Context(), g, items, nil, &st)
		require.Error(t, err, name)
		require.Equal(t, 1, n, name)
		require.Zero(t, st.ParserRetries, name)
		srv.Close()
	}
	// the same body with our id (an HTTP 400 from a strict parser behind a proxy) does retry
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"m","data":`+genericMalformed+`}}`)
			return
		}
		_, _ = io.WriteString(w, successBody(""))
	}))
	defer srv.Close()
	c, g, items := lpClient(srv, nil)
	_, err := c.fetch(t.Context(), g, items, nil, &Stats{})
	require.NoError(t, err)
	require.Equal(t, 2, n)
}
