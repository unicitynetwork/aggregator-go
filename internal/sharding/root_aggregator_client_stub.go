package sharding

import (
	"context"
	"sync"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type RootAggregatorClientStub struct {
	mu                 sync.Mutex
	submissionCount    int
	returnedProofCount int
	submissions        map[int]*api.SubmitShardRootRequest // shardID => last request
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

	if m.submissionError != nil {
		return m.submissionError
	}
	m.submissionCount++
	m.submissions[request.ShardID] = request
	return nil
}

func (m *RootAggregatorClientStub) GetShardProof(ctx context.Context, request *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.submissions[request.ShardID] != nil {
		m.returnedProofCount++
		return &api.RootShardInclusionProof{UnicityCertificate: api.HexBytes("1234")}, nil
	}
	return nil, nil
}

func (m *RootAggregatorClientStub) SubmissionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.submissionCount
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
