package jsonrpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

// post sends body to a real HTTP server around srv, so that the framing the client sees (Content-Length, no chunking)
// is what a gateway sees.
func post(t *testing.T, srv *Server, body string) (*http.Response, []byte) {
	t.Helper()
	ts := httptest.NewServer(srv)
	defer ts.Close()
	resp, err := http.Post(ts.URL, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, data
}

func requireContentLength(t *testing.T, resp *http.Response, data []byte) {
	t.Helper()
	require.Equal(t, strconv.Itoa(len(data)), resp.Header.Get("Content-Length"), "Content-Length header equals the body length")
	require.EqualValues(t, len(data), resp.ContentLength)
	require.Empty(t, resp.TransferEncoding, "not chunked")
	require.True(t, json.Valid(data))
}

func TestContentLengthOnEveryResponsePath(t *testing.T) {
	log, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(log, 10)
	srv.RegisterMethod("small", func(context.Context, json.RawMessage) (interface{}, *Error) {
		return map[string]int{"height": 1}, nil
	})
	srv.RegisterMethod("fails", func(context.Context, json.RawMessage) (interface{}, *Error) {
		return nil, NewError(InternalErrorCode, "boom", nil)
	})
	// a large result (a batch of proofs is of this size)
	big := strings.Repeat("x", 3<<20)
	srv.RegisterMethod("batch", func(context.Context, json.RawMessage) (interface{}, *Error) {
		items := make([]string, 0, 3)
		for range 3 {
			items = append(items, big)
		}
		return map[string]interface{}{"proofs": items}, nil
	})
	// a held (long-poll) result
	srv.RegisterMethod("held", func(context.Context, json.RawMessage) (interface{}, *Error) {
		time.Sleep(60 * time.Millisecond)
		return map[string]interface{}{"proof": "00", "waitedMs": 60}, nil
	})
	srv.RegisterMethodWithLimits("limited", func(context.Context, json.RawMessage) (interface{}, *Error) {
		return map[string]string{"v": strings.Repeat("y", 1000)}, nil
	}, MethodLimits{MaxResponseBytes: 4096})
	srv.RegisterMethodWithLimits("toolarge", func(context.Context, json.RawMessage) (interface{}, *Error) {
		return map[string]string{"v": strings.Repeat("y", 10_000)}, nil
	}, MethodLimits{MaxResponseBytes: 500})

	call := func(method string) string {
		return `{"jsonrpc":"2.0","method":"` + method + `","params":{},"id":1}`
	}
	for name, body := range map[string]string{
		"result":                     call("small"),
		"handler error":              call("fails"),
		"unknown method":             call("nope"),
		"large batch result":         call("batch"),
		"long-poll result":           call("held"),
		"limited result":             call("limited"),
		"limit exceeded replacement": call("toolarge"),
		"parse error":                `{invalid`,
	} {
		t.Run(name, func(t *testing.T) {
			resp, data := post(t, srv, body)
			requireContentLength(t, resp, data)
		})
	}

	t.Run("large result is really large", func(t *testing.T) {
		resp, data := post(t, srv, call("batch"))
		require.Greater(t, len(data), 9<<20)
		requireContentLength(t, resp, data)
	})

	t.Run("request body over the ceiling: 413 with Content-Length", func(t *testing.T) {
		srv.SetMaxRequestBodyBytes(64)
		defer srv.SetMaxRequestBodyBytes(0)
		resp, data := post(t, srv, `{"jsonrpc":"2.0","method":"small","params":{"pad":"`+strings.Repeat("z", 500)+`"},"id":1}`)
		require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
		requireContentLength(t, resp, data)
	})
}

// discardWriter is a ResponseWriter that keeps nothing, so that only the encoder's own allocations are measured.
type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(int)             {}

// A large legacy (unlimited) response is encoded once: the bytes allocated per call stay well below two body sizes
// (a marshal-then-copy approach allocates the body twice or more). Hardware independent: allocation counts and sizes.
func TestLargeResponseIsNotCopied(t *testing.T) {
	log, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(log, 10)
	body := strings.Repeat("x", 4<<20)
	response := NewResponse(map[string]string{"v": body}, 1)
	w := &discardWriter{h: http.Header{}}

	srv.writeJSONResponse(w, response) // warms the encoder's pooled buffer
	const runs = 20
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range runs {
		srv.writeJSONResponse(w, response)
	}
	runtime.ReadMemStats(&after)
	perCall := (after.TotalAlloc - before.TotalAlloc) / runs
	require.Less(t, perCall, uint64(len(body))*3/4, "bytes allocated per call, body is %d bytes", len(body))
	require.Equal(t, strconv.Itoa(len(body)+len(`{"jsonrpc":"2.0","result":{"v":""},"id":1}`)+1), w.h.Get("Content-Length"))
}
