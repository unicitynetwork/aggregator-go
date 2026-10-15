package bft

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
)

func TestNoteResponseRound_CountsNonProgressOnly(t *testing.T) {
	c := &BFTClientImpl{}
	shard := c.shardID.String()
	count := func() float64 {
		return testutil.ToFloat64(metrics.BFTCertificationResponseNonProgressTotal.WithLabelValues(shard))
	}
	deliver := func(shardRound, rootRound uint64) {
		c.noteResponseRound(&certification.CertificationResponse{UC: types.UnicityCertificate{
			InputRecord: &types.InputRecord{RoundNumber: shardRound},
			UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: rootRound}}})
	}
	before := count()
	for i := uint64(5); i < 9; i++ { // new shard blocks (and new root rounds)
		deliver(i, 100+i)
	}
	require.Equal(t, before, count())
	deliver(8, 109) // same shard block, advancing root round: extra traffic / timeout repeat
	deliver(8, 110)
	deliver(7, 111) // stale
	deliver(9, 112) // progress again
	require.Equal(t, before+3, count())
	c.noteResponseRound(nil) // never panics
}
