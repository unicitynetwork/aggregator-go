package service

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The get_block_records response is documented in README.md. A consumer needs
// referenceTime to rebuild the certified leaf value and expiresAt to check the
// request deadline, so silently dropping either makes the record unverifiable.
// This pins the exact key set the endpoint emits.
func TestBlockRecordWireShape(t *testing.T) {
	expiresAt := uint64(1755003600)
	record := &models.AggregatorRecord{
		StateID: api.RequireNewImprintV2("c7aa6962316c0eeb1469dc3d7793e39e140c005e6eea0e188dcc73035d765937"),
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{0x01}, Params: []byte{0x02, 0x03}},
			SourceStateHash: api.RequireNewImprintV2("539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647"),
			TransactionHash: api.RequireNewImprintV2("c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"),
			ExpiresAt:       &expiresAt,
			Witness:         []byte{0x04, 0x05},
		},
		ReferenceTime: 1755000000,
		BlockNumber:   api.NewBigInt(big.NewInt(123)),
		LeafIndex:     api.NewBigInt(big.NewInt(0)),
		CreatedAt:     api.NewTimestamp(time.UnixMilli(1734435600000).UTC()),
	}

	encoded, err := json.Marshal(modelToAPIAggregatorRecord(record))
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.ElementsMatch(t,
		[]string{"stateId", "certificationData", "referenceTime", "blockNumber", "leafIndex", "createdAt"},
		keysOf(decoded),
		"get_block_records record keys changed; update README.md to match")

	certData, ok := decoded["certificationData"].(map[string]any)
	require.True(t, ok)
	require.ElementsMatch(t,
		[]string{"version", "ownerPredicate", "sourceStateHash", "transactionHash", "expiresAt", "witness"},
		keysOf(certData),
		"certificationData keys changed; update README.md to match")

	require.EqualValues(t, 1755000000, decoded["referenceTime"])
	require.EqualValues(t, 1755003600, certData["expiresAt"])
	require.EqualValues(t, api.CertificationDataVersion, certData["version"])
	// finalizedAt is deliberately not emitted: nothing persists a finalization
	// timestamp, and the block's CreatedAt is proposal time.
	require.NotContains(t, decoded, "finalizedAt")

	// An absent deadline stays absent rather than becoming zero: the service
	// assigns its own, but that value is not part of the certified record.
	record.CertificationData.ExpiresAt = nil
	encoded, err = json.Marshal(modelToAPIAggregatorRecord(record))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	certData, ok = decoded["certificationData"].(map[string]any)
	require.True(t, ok)
	require.Nil(t, certData["expiresAt"])
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The deadline-origin counter is the migration signal for retiring absent
// deadlines, so it must count only requests that were actually accepted. An
// expired request is rejected and must not inflate the backlog.
func TestDeadlineOriginCountsOnlyAcceptedRequests(t *testing.T) {
	ctx := context.Background()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)

	const referenceTime uint64 = 1755000000
	read := func(origin string) float64 {
		return testutil.ToFloat64(metrics.CertificationRequestsByDeadline.WithLabelValues(origin))
	}

	newService := func(queue *recordingCommitmentQueue) *AggregatorService {
		shardingCfg := config.ShardingConfig{Mode: config.ShardingModeBFTShard}
		return &AggregatorService{
			config: &config.Config{
				Processing: config.ProcessingConfig{SkipDuplicateCheck: true, DefaultRequestTTL: time.Hour},
				Sharding:   shardingCfg,
			},
			logger:                        log,
			commitmentQueue:               queue,
			roundManager:                  &stubRoundManager{referenceTime: referenceTime},
			certificationRequestValidator: signing.NewCertificationRequestValidator(shardingCfg, bfttypes.ShardID{}),
		}
	}

	// An accepted request with no deadline counts as service_assigned.
	before := read("service_assigned")
	queue := &recordingCommitmentQueue{}
	accepted := createTestCertificationRequests(t, 1)[0]
	accepted.CertificationData.ExpiresAt = nil
	resp, err := newService(queue).CertificationRequest(ctx, accepted)
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", resp.Status)
	require.Len(t, queue.stored, 1)
	require.Equal(t, before+1, read("service_assigned"))

	// An expired request is rejected and must not be counted at all.
	beforeExplicit := read("explicit")
	beforeAssigned := read("service_assigned")
	queue = &recordingCommitmentQueue{}
	expired := createTestCertificationRequests(t, 1)[0]
	expired.CertificationData.ExpiresAt = api.Uint64Ptr(referenceTime)
	resp, err = newService(queue).CertificationRequest(ctx, expired)
	require.NoError(t, err)
	require.Equal(t, api.CertificationStatusRequestExpired, resp.Status)
	require.Empty(t, queue.stored)
	require.Equal(t, beforeExplicit, read("explicit"), "a rejected request must not be counted")
	require.Equal(t, beforeAssigned, read("service_assigned"))
}
