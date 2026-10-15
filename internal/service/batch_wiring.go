package service

import (
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// ValidateShardID reports whether the state ID routes to this aggregator's shard.
func (as *AggregatorService) ValidateShardID(id api.StateID) error {
	return as.certificationRequestValidator.ValidateShardID(id)
}

// BatchHandlerFor builds the get_inclusion_proofs.v1 handler for the running
// deployment. Parent/child aggregation and deployments whose backend or
// configuration retains no views answer unsupported_configuration; otherwise the
// handler serves from the round manager's retained-view catalog.
func BatchHandlerFor(cfg *config.Config, svc Service, rm round.Manager, st interfaces.Storage) jsonrpc.HandlerFunc {
	mode := cfg.Sharding.Mode
	if mode != config.ShardingModeStandalone && mode != config.ShardingModeBFTShard {
		return UnsupportedRPC
	}
	pp, ok := rm.(interface{ ProofViewPublisher() *proofpub.Publisher })
	validator, vok := svc.(BatchShardValidator)
	if !ok || !vok || pp.ProofViewPublisher() == nil || pp.ProofViewPublisher().Catalog() == nil {
		return UnsupportedRPC
	}
	records, rok := st.AggregatorRecordStorage().(BatchRecordSource)
	if !rok { // storage without the bounded size projection cannot serve the batch API
		return UnsupportedRPC
	}
	batch := NewBatchProofService(CatalogViewSource(pp.ProofViewPublisher().Catalog()), records, validator, BatchProofConfig{})
	return batch.HandleRPC
}
