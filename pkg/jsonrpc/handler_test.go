package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
)

func TestTimeoutMiddlewareRecoversFromPanic(t *testing.T) {
	testLogger, _ := logger.New("error", "text", "stdout", false)
	mw := TimeoutMiddleware(50*time.Millisecond, testLogger)

	req := &Request{ID: 1}

	resp := mw(context.Background(), req, func(ctx context.Context, r *Request) *Response {
		panic("boom")
	})

	require.NotNil(t, resp, "middleware should return a response even on panic")
	require.NotNil(t, resp.Error, "response should contain error information")
	assert.Equal(t, InternalErrorCode, resp.Error.Code)
}

func TestServeHTTP_MetricsRecordedOnNonPOST(t *testing.T) {
	testLogger, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(testLogger, 10)

	before := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("unknown", "error"))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	after := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("unknown", "error"))
	require.Equal(t, before+1, after)
}

func TestServeHTTP_MetricsRecordedOnParseError(t *testing.T) {
	testLogger, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(testLogger, 10)

	before := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("unknown", "error"))

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid-json"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	after := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("unknown", "error"))
	require.Equal(t, before+1, after)
}

func TestServeHTTP_MetricsRecordedOnSuccess(t *testing.T) {
	testLogger, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(testLogger, 10)
	srv.RegisterMethod("get_block_height", func(ctx context.Context, params json.RawMessage) (interface{}, *Error) {
		return map[string]int{"height": 1}, nil
	})

	before := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("get_block_height", "success"))

	body := `{"jsonrpc":"2.0","method":"get_block_height","params":{},"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	after := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues("get_block_height", "success"))
	require.Equal(t, before+1, after)
}
