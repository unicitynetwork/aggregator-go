package service

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
	"net/http"
	"testing"
)

// TODO: Currently only one instance of docker compose can run
func setupAggregator(t *testing.T, ctx context.Context) func(t *testing.T) {
	stack, err := compose.NewDockerCompose("../../docker-compose.yml")
	if err != nil {
		t.Logf("Failed to create stack: %v", err)
		return nil
	}

	err = stack.Up(ctx, compose.Wait(false))
	if err != nil {
		t.Fatalf("Failed to run docker compose request: %v", err)
	}

	container, err := stack.ServiceContainer(ctx, "aggregator")
	if err != nil {
		t.Fatalf("Failed to find aggregator container: %v", err)
	}
	err = wait.ForHealthCheck().WaitUntilReady(ctx, container)
	if err != nil {
		t.Fatalf("Aggregator failed to run: %v", err)
	}

	return func(t *testing.T) {
		err = stack.Down(
			context.Background(),
			compose.RemoveOrphans(true),
			compose.RemoveVolumes(true),
			compose.RemoveImagesLocal,
		)
		if err != nil {
			t.Logf("Failed to stop stack: %v", err)
		}
	}
}

func TestInclusionProofMissingRecord(t *testing.T) {
	ctx := context.Background()
	cleanup := setupAggregator(t, ctx)
	defer cleanup(t)

	request, err := jsonrpc.NewRequest(
		"get_inclusion_proof",
		&api.GetInclusionProofRequest{RequestID: "00000000"},
		"test-request-id")

	bodyBytes, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	httpResponse, err := http.Post("http://localhost:3000", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("Failed to send POST request: %v", err)
	}

	var response jsonrpc.Response
	err = json.NewDecoder(httpResponse.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to parse JSON RPC response: %v", err)
	}

	resultBytes, err := json.Marshal(response.Result)
	if err != nil {
		t.Fatalf("Failed to marshal result: %v", err)
	}

	var inclusionProof api.GetInclusionProofResponse
	err = json.Unmarshal(resultBytes, &inclusionProof)
	if err != nil {
		t.Fatalf("Failed to unmarshal result as GetInclusionProofResponse: %v", err)
	}

	assert.Nil(t, inclusionProof.TransactionHash)
	assert.Nil(t, inclusionProof.Authenticator)
	assert.NotNil(t, inclusionProof.MerkleTreePath)
}
