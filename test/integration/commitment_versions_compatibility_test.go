package integration

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	modelsV1 "github.com/unicitynetwork/aggregator-go/internal/models/v1"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func (suite *ShardingE2ETestSuite) TestCompatibilityV2() {
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
	// verify block records

	url := "http://localhost:9100"
	cfg := suite.buildConfig(config.ShardingModeStandalone, "9100", "backwards_compatibility_Test", 0)
	suite.startAggregatorInstance("aggregator", cfg)

	time.Sleep(500 * time.Millisecond)

	suite.T().Log("Phase 1: Submitting commitments...")
	v1 := testutil.CreateTestCommitment(suite.T(), fmt.Sprintf("commitment_v1"))
	v1Resp, err := suite.submitCommitment(url, v1.ToAPI())
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", v1Resp.Status)

	v2 := testutil.CreateTestCertificationRequest(suite.T(), fmt.Sprintf("commitment_v2"))
	v2Resp, err := suite.certificationRequest(url, v2.ToAPI())
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", v2Resp.Status)
	suite.T().Logf("Commitments submitted successfully")

	suite.T().Log("Phase 2: Verifying proofs...")
	v1ProofResp := suite.waitForProofAvailableV1(url, v1.RequestID.String(), 500*time.Millisecond)
	suite.verifyProofV1(v1ProofResp, v1)
	v2ProofResp := suite.waitForProofAvailable(url, v2.StateID.String(), 500*time.Millisecond)
	suite.verifyProofV2(v2ProofResp, v2)
	suite.T().Logf("Proofs verified successfully")

	suite.T().Log("Phase 3: Verifying endpoint compatibility...")
	invalidProofV1, err := suite.getInclusionProofV1(url, v2.StateID.String()) // fetch proof from v1 api
	suite.Require().Nil(invalidProofV1)
	suite.Require().ErrorContains(err, "Failed to get inclusion proof")

	invalidProofV2, err := suite.getInclusionProof(url, v1.RequestID.String()) // fetch proof from v2 api
	suite.Require().Nil(invalidProofV2)
	suite.Require().ErrorContains(err, "Failed to get inclusion proof")

	suite.T().Log("Phase 4: Verifying block records endpoint...")
	blockRecordsResponseJSON, err := suite.rpcCall(url, "get_block_records", api.GetBlockCommitmentsRequest{BlockNumber: v2ProofResp.BlockNumber})
	suite.Require().NoError(err)
	var blockRecordsResponse api.GetBlockRecordsResponse
	suite.Require().NoError(json.Unmarshal(blockRecordsResponseJSON, &blockRecordsResponse))
	suite.Require().Len(blockRecordsResponse.AggregatorRecords, 2)

	blockCommitmentsResponseJSON, err := suite.rpcCall(url, "get_block_commitments", api.GetBlockCommitmentsRequest{BlockNumber: v2ProofResp.BlockNumber})
	suite.Require().NoError(err)
	var blockCommitmentsResponse api.GetBlockCommitmentsResponse
	suite.Require().NoError(json.Unmarshal(blockCommitmentsResponseJSON, &blockCommitmentsResponse))
	suite.Require().Len(blockCommitmentsResponse.Commitments, 2)
}

func (suite *ShardingE2ETestSuite) verifyProofV1(v1ProofResp *api.GetInclusionProofResponseV1, v1 *modelsV1.Commitment) {
	suite.Require().NotNil(v1ProofResp)
	suite.Require().NotNil(v1ProofResp.InclusionProof)
	suite.Require().NotNil(v1ProofResp.InclusionProof.MerkleTreePath)

	merklePath, err := v1.RequestID.GetPath()
	suite.Require().NoError(err)
	verificationResult, err := v1ProofResp.InclusionProof.MerkleTreePath.Verify(merklePath)
	suite.Require().NoError(err)
	suite.Require().True(verificationResult.Result)
}

func (suite *ShardingE2ETestSuite) verifyProofV2(v2ProofResp *api.GetInclusionProofResponseV2, v2 *models.CertificationRequest) {
	suite.Require().NotNil(v2ProofResp)
	suite.Require().NotNil(v2ProofResp.InclusionProof)
	suite.Require().NotNil(v2ProofResp.InclusionProof.MerkleTreePath)

	merklePath, err := v2.StateID.GetPath()
	suite.Require().NoError(err)
	verificationResult, err := v2ProofResp.InclusionProof.MerkleTreePath.Verify(merklePath)
	suite.Require().NoError(err)
	suite.Require().True(verificationResult.Result)
}

func (suite *ShardingE2ETestSuite) submitCommitment(url string, commitment *api.Commitment) (*api.SubmitCommitmentResponse, error) {
	result, err := suite.rpcCall(url, "submit_commitment", commitment)
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
func (suite *ShardingE2ETestSuite) waitForProofAvailableV1(url, stateIDStr string, timeout time.Duration) *api.GetInclusionProofResponseV1 {
	deadline := time.Now().Add(timeout)
	requestID := api.RequestID(stateIDStr)
	stateIDPath, err := requestID.GetPath()
	suite.Require().NoError(err)

	for time.Now().Before(deadline) {
		resp, err := suite.getInclusionProofV1(url, stateIDStr)
		if resp != nil {
			fmt.Println(resp)
		}
		if err == nil && resp.InclusionProof != nil && resp.InclusionProof.MerkleTreePath != nil {
			// Also verify that the proof is valid (includes parent proof)
			result, verifyErr := resp.InclusionProof.MerkleTreePath.Verify(stateIDPath)
			if verifyErr == nil && result != nil && result.Result {
				return resp
			}
			// Proof exists but not valid yet (probably waiting for parent proof), keep retrying
		}
		time.Sleep(50 * time.Millisecond)
	}
	suite.FailNow(fmt.Sprintf("Timeout waiting for valid proof for requestID %s at %s", requestID, url))
	return nil
}

func (suite *ShardingE2ETestSuite) getInclusionProofV1(url string, requestID string) (*api.GetInclusionProofResponseV1, error) {
	params := map[string]string{"requestId": requestID}
	result, err := suite.rpcCall(url, "get_inclusion_proof", params)
	if err != nil {
		return nil, err
	}

	var response api.GetInclusionProofResponseV1
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}
