package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
)

const (
	proofPathRequest        = "request"
	proofPathFinalizing     = "finalizing"
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

type proofDiagnostics struct {
	lastLogSecond atomic.Int64

	requests       atomic.Int64
	finalizing     atomic.Int64
	knownNotReady  atomic.Int64
	precomputedHit atomic.Int64
	metadataHit    atomic.Int64
	metadataMiss   atomic.Int64
	mongoBlock     atomic.Int64
	mongoRecord    atomic.Int64
	emptyResponses atomic.Int64
	liveCert       atomic.Int64
	errors         atomic.Int64
}

func (as *AggregatorService) recordProofPath(ctx context.Context, path string) {
	if as == nil {
		return
	}

	metrics.InclusionProofPathTotal.WithLabelValues(path).Inc()

	switch path {
	case proofPathRequest:
		as.proofDiag.requests.Add(1)
	case proofPathFinalizing:
		as.proofDiag.finalizing.Add(1)
	case proofPathKnownNotReady:
		as.proofDiag.knownNotReady.Add(1)
	case proofPathPrecomputedHit:
		as.proofDiag.precomputedHit.Add(1)
	case proofPathMetadataHit:
		as.proofDiag.metadataHit.Add(1)
	case proofPathMetadataMiss:
		as.proofDiag.metadataMiss.Add(1)
	case proofPathMongoBlock:
		as.proofDiag.mongoBlock.Add(1)
	case proofPathMongoRecord:
		as.proofDiag.mongoRecord.Add(1)
	case proofPathEmpty:
		as.proofDiag.emptyResponses.Add(1)
	case proofPathLiveCert:
		as.proofDiag.liveCert.Add(1)
	case proofPathError:
		as.proofDiag.errors.Add(1)
	}

	now := time.Now().Unix()
	if !as.proofDiag.lastLogSecond.CompareAndSwap(now-1, now) {
		last := as.proofDiag.lastLogSecond.Load()
		if last == now || !as.proofDiag.lastLogSecond.CompareAndSwap(last, now) {
			return
		}
	}
	as.logProofPathSnapshot(ctx)
}

func (as *AggregatorService) logProofPathSnapshot(ctx context.Context) {
	requests := as.proofDiag.requests.Swap(0)
	finalizing := as.proofDiag.finalizing.Swap(0)
	knownNotReady := as.proofDiag.knownNotReady.Swap(0)
	precomputedHit := as.proofDiag.precomputedHit.Swap(0)
	metadataHit := as.proofDiag.metadataHit.Swap(0)
	metadataMiss := as.proofDiag.metadataMiss.Swap(0)
	mongoBlock := as.proofDiag.mongoBlock.Swap(0)
	mongoRecord := as.proofDiag.mongoRecord.Swap(0)
	emptyResponses := as.proofDiag.emptyResponses.Swap(0)
	liveCert := as.proofDiag.liveCert.Swap(0)
	errors := as.proofDiag.errors.Swap(0)

	if requests == 0 && finalizing == 0 && knownNotReady == 0 && metadataHit == 0 && metadataMiss == 0 && mongoBlock == 0 && mongoRecord == 0 && liveCert == 0 && errors == 0 {
		return
	}
	if as.logger == nil {
		return
	}
	pending, records, blocks := as.roundManager.GetProofCacheStats()

	as.logger.WithContext(ctx).Info("PERF: Proof path",
		"requests", requests,
		"finalizing", finalizing,
		"knownNotReady", knownNotReady,
		"precomputedHit", precomputedHit,
		"metadataCacheHit", metadataHit,
		"metadataCacheMiss", metadataMiss,
		"mongoBlockLookup", mongoBlock,
		"mongoRecordLookup", mongoRecord,
		"emptyResponses", emptyResponses,
		"liveCert", liveCert,
		"errors", errors,
		"proofPending", pending,
		"proofCacheRecords", records,
		"proofCacheBlocks", blocks)
}
