package sharding

import (
	"context"
	"fmt"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type RootAggregatorClientStub struct {
	mu                 sync.Mutex
	submissionCount    int
	submissionAttempts int
	returnedProofCount int
	submissions        map[int]*api.SubmitShardRootRequest // shardID => last request
	submittedRootHash  api.HexBytes
	submissionError    error
}

func NewRootAggregatorClientStub() *RootAggregatorClientStub {
	return &RootAggregatorClientStub{
		submissions: make(map[int]*api.SubmitShardRootRequest),
	}
}

func (m *RootAggregatorClientStub) SubmitShardRoot(ctx context.Context, request *api.SubmitShardRootRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.submissionAttempts++
	if m.submissionError != nil {
		return m.submissionError
	}
	m.submissionCount++
	m.submissions[request.ShardID] = request
	m.submittedRootHash = request.RootHash
	return nil
}

func (m *RootAggregatorClientStub) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.submissions[request.ShardID] != nil {
		m.returnedProofCount++
		submittedRootHash := m.submittedRootHash.String()
		ucBytes, err := stubProofUC(uint64(m.returnedProofCount), uint64(m.returnedProofCount))
		if err != nil {
			return nil, err
		}
		return &api.RootShardInclusionProof{
			UnicityCertificate: ucBytes,
			MerkleTreePath: &api.MerkleTreePath{
				Steps: []api.MerkleTreeStep{{Data: &submittedRootHash}},
			},
		}, nil
	}
	return nil, nil
}

func (m *RootAggregatorClientStub) CheckHealth(ctx context.Context) error {
	return nil
}

func (m *RootAggregatorClientStub) SubmissionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.submissionCount
}

func (m *RootAggregatorClientStub) SubmissionAttempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.submissionAttempts
}

func (m *RootAggregatorClientStub) ProofCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.returnedProofCount
}

func (m *RootAggregatorClientStub) SetSubmissionError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.submissionError = err
}

func stubProofUC(parentRound, rootRound uint64) (api.HexBytes, error) {
	uc := types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			RoundNumber: parentRound,
		},
		UnicitySeal: &types.UnicitySeal{
			RootChainRoundNumber: rootRound,
		},
	}

	ucBytes, err := types.Cbor.Marshal(uc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal stub proof UC: %w", err)
	}

	return api.NewHexBytes(ucBytes), nil
}
