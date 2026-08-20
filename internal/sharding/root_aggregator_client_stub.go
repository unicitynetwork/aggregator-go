package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"

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
	// referenceTime stands in for the seal timestamp a real parent returns: it
	// advances by one per returned proof, so a child under this stub pins
	// distinct, increasing reference times as it does against a live parent.
	referenceTime uint64
}

func NewRootAggregatorClientStub() *RootAggregatorClientStub {
	return &RootAggregatorClientStub{
		submissions:   make(map[int]*api.SubmitShardRootRequest),
		referenceTime: uint64(time.Now().Unix()),
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
		m.referenceTime++
		ucBytes, err := stubProofUC(uint64(m.returnedProofCount), uint64(m.returnedProofCount), m.referenceTime, m.submittedRootHash)
		if err != nil {
			return nil, err
		}
		fragment := &api.ParentInclusionFragment{
			CertificateBytes: api.NewHexBytes(make([]byte, api.BitmapSize)),
			ShardLeafValue:   api.NewHexBytes(m.submittedRootHash),
		}
		return &api.RootShardInclusionProof{
			ParentFragment:     fragment,
			BlockNumber:        uint64(m.returnedProofCount),
			UnicityCertificate: ucBytes,
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

// stubProofUC builds the certificate a parent returns with a shard proof. It
// carries the same two timestamps a live parent does: the input record records
// the reference time the certified round was built under, and the seal records
// the time the child's next round will pin. Without them the child has no
// reference time and rejects every request as not ready.
func stubProofUC(parentRound, rootRound, referenceTime uint64, rootHash api.HexBytes) (api.HexBytes, error) {
	uc := types.UnicityCertificate{
		InputRecord: &types.InputRecord{
			RoundNumber: parentRound,
			Hash:        hex.Bytes(rootHash),
			Timestamp:   referenceTime,
		},
		UnicitySeal: &types.UnicitySeal{
			RootChainRoundNumber: rootRound,
			Timestamp:            referenceTime + 1,
		},
	}

	ucBytes, err := types.Cbor.Marshal(uc)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal stub proof UC: %w", err)
	}

	return api.NewHexBytes(ucBytes), nil
}
