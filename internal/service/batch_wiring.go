package service

import (
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// ProofViewProvider is implemented by round managers which retain finalized views.
type ProofViewProvider interface {
	ProofViewCatalog() *proofview.Catalog
}

// NewBatchProofHandler builds the get_inclusion_proofs.v1 handler for the deployment from
// the retained-view catalog, the finalized record storage and the service's shard router (svc). When the
// deployment cannot serve the method it returns the explicit unsupported_configuration
// handler together with the reason (never a silently missing method):
//   - parent/child aggregation (different tree composition),
//   - BFT disabled or no shard configuration (no certified views exist),
//   - a backend without retained read views, or a record storage without the bounded
//     size projection.
func NewBatchProofHandler(cfg *config.Config, rm any, storage interfaces.Storage, svc any) (jsonrpc.HandlerFunc, string) {
	if m := cfg.Sharding.Mode; m.IsParent() || m.IsChild() {
		return UnsupportedRPC, "parent/child aggregation"
	}
	if !cfg.BFT.Enabled || cfg.BFT.ShardConf == nil {
		return UnsupportedRPC, "BFT disabled"
	}
	provider, ok := rm.(ProofViewProvider)
	if !ok || provider.ProofViewCatalog() == nil {
		return UnsupportedRPC, "SMT backend cannot retain read views"
	}
	records, ok := storage.AggregatorRecordStorage().(BatchRecordSource)
	if !ok {
		return UnsupportedRPC, "record storage lacks the bounded size projection"
	}
	routed, ok := svc.(interface{ ShardValidator() BatchShardValidator })
	if !ok {
		return UnsupportedRPC, "service cannot validate shard routing"
	}
	batch := NewBatchProofService(CatalogViewSource(provider.ProofViewCatalog()), records, routed.ShardValidator(), BatchProofConfig{
		MaxConcurrent: cfg.SMT.MaxConcurrentBatches,
		MaxExecution:  cfg.SMT.BatchTimeout,
	})
	return batch.HandleRPC, ""
}
