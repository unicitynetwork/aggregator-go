package service

import (
	"context"
	"errors"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

const (
	proofPathRequest        = "request"
	proofPathKnownNotReady  = "known_not_ready"
	proofPathPrecomputedHit = "precomputed_hit"
	proofPathMetadataHit    = "metadata_cache_hit"
	proofPathMetadataMiss   = "metadata_cache_miss"
	proofPathMongoBlock     = "mongo_block_lookup"
	proofPathMongoRecord    = "mongo_record_lookup"
	proofPathEmpty          = "empty_response"
	proofPathLiveCert       = "live_cert"
	proofPathError          = "error"
)

const (
	proofBuildSourcePublishedView = "published_view"
	proofBuildSourceLiveTree      = "live_tree"

	proofBuildResultOK          = "ok"
	proofBuildResultNotFound    = "not_found"
	proofBuildResultRootChanged = "root_changed"
	proofBuildResultError       = "error"
)

func (as *AggregatorService) recordProofPath(_ context.Context, path string) {
	if as == nil {
		return
	}

	metrics.InclusionProofPathTotal.WithLabelValues(path).Inc()
}

func (as *AggregatorService) recordProofBuildDuration(_ context.Context, source, result string, duration time.Duration) {
	if as == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	metrics.SMTInclusionCertBuildDuration.WithLabelValues(source, result).Observe(duration.Seconds())
}

func proofBuildResultFromPublishedErr(err error) string {
	if err == nil {
		return proofBuildResultOK
	}
	if errors.Is(err, smtbackend.ErrPublishedProofLeafNotFound) {
		return proofBuildResultNotFound
	}
	if errors.Is(err, smtbackend.ErrPublishedProofRootChanged) {
		return proofBuildResultRootChanged
	}
	return proofBuildResultError
}

func proofBuildResultFromErr(err error) string {
	if err == nil {
		return proofBuildResultOK
	}
	return proofBuildResultError
}
