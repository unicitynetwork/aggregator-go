package jsonrpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every capacity-overload answer (-32006, all reasons) is HTTP 503 + Retry-After with the JSON-RPC body unchanged;
// everything else keeps its status.
func TestOverloadIsHTTP503WithRetryAfter(t *testing.T) {
	s := testServer(t, 1)
	busy := func(reason, detail string) HandlerFunc {
		return func(context.Context, json.RawMessage) (interface{}, *Error) {
			return nil, NewDetailedError(ConcurrencyLimitCode, "Concurrency limit exceeded", reason, detail, true, nil)
		}
	}
	s.RegisterMethod("busy_active", busy("busy", "active_requests"))
	s.RegisterMethod("busy_pinned", busy("busy", "pinned_views"))
	s.RegisterMethod("busy_retention", busy("busy", "retention_budget"))
	s.RegisterMethod("busy_deadline", busy("busy", "deadline"))
	s.RegisterMethodWithLimits("busy_limited", busy("busy", "active_requests"), MethodLimits{MaxResponseBytes: 4096})
	s.RegisterMethod("plain_error", func(context.Context, json.RawMessage) (interface{}, *Error) {
		return nil, NewError(InternalErrorCode, "boom", nil)
	})
	s.RegisterMethod("ok", func(context.Context, json.RawMessage) (interface{}, *Error) { return "ok", nil })

	call := func(srv http.Handler, body string) (*http.Response, []byte) {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		res := rec.Result()
		data, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res, data
	}
	rpc := func(method string) string {
		return `{"jsonrpc":"2.0","method":"` + method + `","params":{},"id":7}`
	}
	check := func(t *testing.T, res *http.Response, data []byte, status int, retryAfter string) {
		t.Helper()
		require.Equal(t, status, res.StatusCode)
		require.Equal(t, retryAfter, res.Header.Get("Retry-After"))
		require.Equal(t, strconv.Itoa(len(data)), res.Header.Get("Content-Length"))
		require.Equal(t, "application/json", res.Header.Get("Content-Type"))
	}

	for _, method := range []string{"busy_active", "busy_pinned", "busy_retention", "busy_deadline", "busy_limited"} {
		t.Run(method, func(t *testing.T) {
			res, data := call(s, rpc(method))
			check(t, res, data, http.StatusServiceUnavailable, "1")
			var body struct {
				Error struct {
					Code    int
					Message string
					Data    struct {
						Reason    string
						Detail    string
						Retryable bool
					}
				}
				ID int
			}
			require.NoError(t, json.Unmarshal(data, &body))
			require.Equal(t, -32006, body.Error.Code)
			require.Equal(t, "Concurrency limit exceeded", body.Error.Message)
			require.Equal(t, "busy", body.Error.Data.Reason)
			require.True(t, body.Error.Data.Retryable)
			require.NotEmpty(t, body.Error.Data.Detail)
			require.Equal(t, 7, body.ID, "the id is echoed where known")
		})
	}

	t.Run("pre-parse server-wide limit: id null, body unchanged", func(t *testing.T) {
		permit, ok := s.newPermit()
		require.True(t, ok)
		defer permit.Close()
		res, data := call(s, rpc("ok"))
		check(t, res, data, http.StatusServiceUnavailable, "1")
		require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32006,"message":"Concurrency limit exceeded"},"id":null}`, string(data))
	})

	t.Run("everything else keeps its status", func(t *testing.T) {
		res, data := call(s, rpc("ok"))
		check(t, res, data, http.StatusOK, "")
		res, data = call(s, rpc("plain_error"))
		check(t, res, data, http.StatusOK, "")
		res, data = call(s, rpc("nope"))
		check(t, res, data, http.StatusOK, "")
		res, data = call(s, `{invalid`)
		check(t, res, data, http.StatusOK, "")
		// top-level arrays (batches) are not supported: one invalid-request answer, 200, one slot
		res, data = call(s, `[`+rpc("busy_active")+`]`)
		check(t, res, data, http.StatusOK, "")
		s.SetMaxRequestBodyBytes(64)
		defer s.SetMaxRequestBodyBytes(0)
		res, data = call(s, `{"jsonrpc":"2.0","method":"ok","params":{"pad":"`+strings.Repeat("z", 500)+`"},"id":1}`)
		check(t, res, data, http.StatusRequestEntityTooLarge, "")
	})

	t.Run("through a real server, large framing intact", func(t *testing.T) {
		ts := httptest.NewServer(s)
		defer ts.Close()
		resp, err := http.Post(ts.URL, "application/json", strings.NewReader(rpc("busy_pinned")))
		require.NoError(t, err)
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
		require.EqualValues(t, len(data), resp.ContentLength)
	})
}
