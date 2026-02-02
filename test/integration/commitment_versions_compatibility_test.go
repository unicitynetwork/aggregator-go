package integration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestCompatibilityV2 tests compatibility of v1 and v2 commitments and proofs
func TestCompatibilityV2(t *testing.T) {
	// phase 1:
	// submit commitment_v1
	// submit commitment_v2
	//
	// phase 2:
	// verify inclusion_proof_v1
	// verify inclusion_proof_v2
	//
	// phase 3:
	// verify inclusion_proof_v2 for commitment_v1 returns error
	// verify inclusion_proof_v1 for commitment_v2 returns error
	//
	// phase 4:
	// verify block records v1 and v2
	ctx := t.Context()

	// Start containers (shared MongoDB with different databases per aggregator)
	redis, err := redisContainer.Run(ctx, "redis:7")
	require.NoError(t, err)
	defer redis.Terminate(ctx)
	redisURI, _ := redis.ConnectionString(ctx)

	mongo, err := mongoContainer.Run(ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	require.NoError(t, err)
	defer mongo.Terminate(ctx)
	mongoURI, _ := mongo.ConnectionString(ctx)
	mongoURI += "&directConnection=true"

	// Start standalone aggregator
	url := "http://localhost:9100"
	nodeCleanup := startAggregator(t, ctx, "backwards_compatibility_Test", "9100", mongoURI, redisURI, config.ShardingModeStandalone, 0)
	defer nodeCleanup()
	waitForBlock(t, url, 1, 15*time.Second)

	t.Log("Phase 1: Submitting commitments...")
	v1 := testutil.CreateTestCommitment(t, fmt.Sprintf("commitment_v1"))
	v1Resp, err := submitCommitment(url, v1.ToAPI())
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", v1Resp.Status)

	v2 := testutil.CreateTestCertificationRequest(t, fmt.Sprintf("commitment_v2"))
	submitCertificationRequest(t, url, v2.ToAPI())
	t.Logf("Commitments submitted successfully")

	t.Log("Phase 2: Verifying proofs...")
	v1ProofResp := waitForProofAvailableV1(t, url, v1.RequestID.String(), 5*time.Second)
	verifyProofV1(t, v1ProofResp, v1.ToAPI())
	v2ProofResp := waitForProofAvailableV2(t, url, v2.StateID.String(), 5*time.Second)
	verifyProofV2(t, v2ProofResp, v2.ToAPI())
	t.Logf("Proofs verified successfully")

	t.Log("Phase 3: Verifying endpoint compatibility...")
	// try to fetch v2 proof from v1 api
	invalidProofV1, err := getInclusionProofV1(t, url, v2.StateID.String())
	require.Nil(t, invalidProofV1)
	require.ErrorContains(t, err, "Failed to get inclusion proof")

	// try to fetch v1 proof from v2 api
	invalidProofV2, err := getInclusionProofV2(t, url, v1.RequestID.String())
	require.Nil(t, invalidProofV2)
	require.ErrorContains(t, err, "Failed to get inclusion proof")

	t.Log("Phase 4: Verifying block records endpoint...")
	blockRecordsResponse := getBlockRecords(t, err, url, v2ProofResp.BlockNumber)
	require.Len(t, blockRecordsResponse.AggregatorRecords, 2)

	blockCommitmentsResponse := getBlockCommitments(t, err, url, v2ProofResp.BlockNumber)
	require.Len(t, blockCommitmentsResponse.Commitments, 2)
}

func verifyProofV1(t *testing.T, v1ProofResp *api.GetInclusionProofResponseV1, v1 *api.Commitment) {
	require.NotNil(t, v1ProofResp)
	require.NotNil(t, v1ProofResp.InclusionProof)
	require.NotNil(t, v1ProofResp.InclusionProof.MerkleTreePath)
	require.NoError(t, v1ProofResp.InclusionProof.Verify(v1))
}

func verifyProofV2(t *testing.T, v2ProofResp *api.GetInclusionProofResponseV2, v2 *api.CertificationRequest) {
	require.NotNil(t, v2ProofResp)
	require.NotNil(t, v2ProofResp.InclusionProof)
	require.NotNil(t, v2ProofResp.InclusionProof.MerkleTreePath)
	require.NoError(t, v2ProofResp.InclusionProof.Verify(v2))
}

func submitCommitment(url string, commitment *api.Commitment) (*api.SubmitCommitmentResponse, error) {
	result, err := rpcCall(url, "submit_commitment", commitment)
	if err != nil {
		return nil, err
	}

	var response api.SubmitCommitmentResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

// waitForProofAvailableV1 waits for a VALID inclusion proof to become available
func waitForProofAvailableV1(t *testing.T, url, stateIDStr string, timeout time.Duration) *api.GetInclusionProofResponseV1 {
	deadline := time.Now().Add(timeout)
	requestID := api.RequestID(stateIDStr)

	for time.Now().Before(deadline) {
		resp, err := getInclusionProofV1(t, url, stateIDStr)
		require.NoError(t, err)
		if resp.InclusionProof != nil && resp.InclusionProof.Authenticator != nil {
			return resp // only return non inclusion proofs
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for valid proof for requestID %s at %s", requestID, url)
	return nil
}

// waitForProofAvailableV2 waits for a VALID inclusion proof to become available
// This includes waiting for the parent proof to be received and joined
func waitForProofAvailableV2(t *testing.T, url, stateIDStr string, timeout time.Duration) *api.GetInclusionProofResponseV2 {
	deadline := time.Now().Add(timeout)
	stateID := api.StateID(stateIDStr)

	for time.Now().Before(deadline) {
		resp, err := getInclusionProofV2(t, url, stateIDStr)
		require.NoError(t, err)
		if resp.InclusionProof != nil && resp.InclusionProof.CertificationData != nil {
			return resp // only return non inclusion proofs
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for valid proof for stateID %s at %s", stateID, url)
	return nil
}

func getInclusionProofV1(t *testing.T, url string, requestID string) (*api.GetInclusionProofResponseV1, error) {
	params := map[string]string{"requestId": requestID}
	result, err := rpcCall(url, "get_inclusion_proof", params)
	if err != nil {
		return nil, err
	}

	var response api.GetInclusionProofResponseV1
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

func getInclusionProofV2(t *testing.T, url string, stateID string) (*api.GetInclusionProofResponseV2, error) {
	params := map[string]string{"stateId": stateID}
	result, err := rpcCall(url, "get_inclusion_proof.v2", params)
	if err != nil {
		return nil, err
	}

	var response api.GetInclusionProofResponseV2
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

func getBlockCommitments(t *testing.T, err error, url string, blockNumber uint64) api.GetBlockCommitmentsResponse {
	blockCommitmentsResponseJSON, err := rpcCall(
		url,
		"get_block_commitments",
		api.GetBlockCommitmentsRequest{BlockNumber: api.NewBigIntFromUint64(blockNumber)},
	)
	require.NoError(t, err)

	var blockCommitmentsResponse api.GetBlockCommitmentsResponse
	require.NoError(t, json.Unmarshal(blockCommitmentsResponseJSON, &blockCommitmentsResponse))
	return blockCommitmentsResponse
}

func getBlockRecords(t *testing.T, err error, url string, blockNumber uint64) api.GetBlockRecordsResponse {
	blockRecordsResponseJSON, err := rpcCall(
		url,
		"get_block_records",
		api.GetBlockCommitmentsRequest{BlockNumber: api.NewBigIntFromUint64(blockNumber)},
	)
	require.NoError(t, err)

	var blockRecordsResponse api.GetBlockRecordsResponse
	require.NoError(t, json.Unmarshal(blockRecordsResponseJSON, &blockRecordsResponse))

	return blockRecordsResponse
}
