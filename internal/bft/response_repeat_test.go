package bft

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
)

func TestNoteResponseRound_CountsRedeliveriesOnly(t *testing.T) {
	c := &BFTClientImpl{}
	shard := c.shardID.String()
	before := testutil.ToFloat64(metrics.BFTCertificationResponseRepeatsTotal.WithLabelValues(shard))
	deliver := func(round uint64) {
		c.noteResponseRound(&certification.CertificationResponse{UC: types.UnicityCertificate{UnicitySeal: &types.UnicitySeal{RootChainRoundNumber: round}}})
	}
	for _, r := range []uint64{5, 6, 7, 8} { // new certifications only
		deliver(r)
	}
	require.Equal(t, before, testutil.ToFloat64(metrics.BFTCertificationResponseRepeatsTotal.WithLabelValues(shard)))
	deliver(8) // the same certification delivered twice
	deliver(7) // a stale re-delivery
	deliver(9)
	require.Equal(t, before+2, testutil.ToFloat64(metrics.BFTCertificationResponseRepeatsTotal.WithLabelValues(shard)))
	c.noteResponseRound(nil) // never panics
}
