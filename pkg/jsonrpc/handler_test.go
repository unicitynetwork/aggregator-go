package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
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
