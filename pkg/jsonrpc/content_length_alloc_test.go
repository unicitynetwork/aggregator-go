//go:build !race

package jsonrpc

import (
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

// Race instrumentation randomly discards sync.Pool entries, so it cannot measure
// the encoder's steady-state allocation cost. Response correctness is tested in both modes.
func TestLargeResponseIsNotCopied(t *testing.T) {
	log, _ := logger.New("error", "text", "stdout", false)
	srv := NewServer(log, 10)
	body := strings.Repeat("x", 4<<20)
	response := NewResponse(map[string]string{"v": body}, 1)
	w := &discardWriter{h: http.Header{}}

	srv.writeJSONResponse(w, response)
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
