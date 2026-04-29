package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func TestRemovedV1MethodsReturnMethodNotFound(t *testing.T) {
	serverAddr, cleanup := setupMongoDBAndAggregator(t, context.Background())
	defer cleanup()

	for _, method := range []string{
		"submit_commitment",
		"get_inclusion_proof",
		"get_block_commitments",
	} {
		t.Run(method, func(t *testing.T) {
			request, err := jsonrpc.NewRequest(method, map[string]any{}, method)
			require.NoError(t, err)

			body, err := json.Marshal(request)
			require.NoError(t, err)

			httpResp, err := http.Post(serverAddr, "application/json", bytes.NewReader(body))
			require.NoError(t, err)
			defer httpResp.Body.Close()

			var response jsonrpc.Response
			require.NoError(t, json.NewDecoder(httpResp.Body).Decode(&response))
			require.NotNil(t, response.Error)
			require.Equal(t, jsonrpc.MethodNotFoundCode, response.Error.Code)
			require.Equal(t, jsonrpc.ErrMethodNotFound.Message, response.Error.Message)
		})
	}
}
