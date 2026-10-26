package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func corsServer(t *testing.T, enabled bool) *Server {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	cfg := &config.Config{}
	cfg.Server = config.ServerConfig{ConcurrencyLimit: 10, EnableCORS: enabled}
	cfg.Sharding.Mode = config.ShardingModeStandalone
	srv := NewServer(cfg, log, nil)
	srv.rpcServer.RegisterMethod("busy", func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
		return nil, jsonrpc.NewDetailedError(jsonrpc.ConcurrencyLimitCode, "Concurrency limit exceeded", "busy", "active_requests", true, nil)
	})
	srv.rpcServer.RegisterMethod("ok", func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) { return "ok", nil })
	return srv
}

func requireCORS(t *testing.T, h http.Header) {
	t.Helper()
	require.Equal(t, "*", h.Get("Access-Control-Allow-Origin"))
	for _, name := range []string{"Content-Type", "Authorization", "X-State-ID", "X-API-Key"} {
		require.Contains(t, h.Get("Access-Control-Allow-Headers"), name)
	}
	require.Equal(t, "Retry-After", h.Get("Access-Control-Expose-Headers"))
}

func TestCORS_JSONRPCRouteOnly(t *testing.T) {
	srv := corsServer(t, true)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		srv.router.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
		return rec
	}

	t.Run("preflight of the JSON-RPC endpoint", func(t *testing.T) {
		rec := do(http.MethodOptions, "/", "")
		require.Equal(t, http.StatusOK, rec.Code)
		requireCORS(t, rec.Header())
		require.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")
	})
	t.Run("the preflight allows GET, POST and OPTIONS, not PUT", func(t *testing.T) {
		rec := do(http.MethodOptions, "/", "")
		require.Equal(t, "GET, POST, OPTIONS", rec.Header().Get("Access-Control-Allow-Methods"))
	})
	t.Run("other routes carry no CORS headers", func(t *testing.T) {
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/health"}, {http.MethodGet, "/metrics"}, {http.MethodGet, "/api/v1/trustbases"},
			{http.MethodPut, "/api/v1/trustbases"}, {http.MethodOptions, "/health"}, {http.MethodOptions, "/api/v1/trustbases"},
		} {
			rec := do(route.method, route.path, "")
			require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"), route.method+" "+route.path)
			require.Empty(t, rec.Header().Get("Access-Control-Allow-Methods"), route.method+" "+route.path)
		}
	})
	t.Run("a normal JSON-RPC response", func(t *testing.T) {
		rec := do(http.MethodPost, "/", `{"jsonrpc":"2.0","method":"ok","params":{},"id":1}`)
		require.Equal(t, http.StatusOK, rec.Code)
		requireCORS(t, rec.Header())
		require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	})
	t.Run("a 503 overload response is readable by a browser", func(t *testing.T) {
		rec := do(http.MethodPost, "/", `{"jsonrpc":"2.0","method":"busy","params":{},"id":1}`)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Equal(t, "1", rec.Header().Get("Retry-After"))
		requireCORS(t, rec.Header())
		require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	})
}
