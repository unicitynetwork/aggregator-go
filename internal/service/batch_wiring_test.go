package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

type wiringRecords struct {
	interfaces.AggregatorRecordStorage
}

func (wiringRecords) GetStoredSizesByStateIDs(context.Context, []api.StateID) (map[string]int64, error) {
	return nil, nil
}

func (wiringRecords) GetFinalizedByStateIDs(context.Context, []api.StateID) (map[string]*models.AggregatorRecord, error) {
	return nil, nil
}

type wiringStorage struct {
	interfaces.Storage
	rec interfaces.AggregatorRecordStorage
}

func (s wiringStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage { return s.rec }

type wiringRM struct{ cat *proofview.Catalog }

func (r wiringRM) ProofViewCatalog() *proofview.Catalog { return r.cat }

type wiringSvc struct{}

func (wiringSvc) ShardValidator() BatchShardValidator { return acceptAllShards{} }

type acceptAllShards struct{}

func (acceptAllShards) ValidateShardID(api.StateID) error { return nil }

func wiringConfig(mode config.ShardingMode) *config.Config {
	return &config.Config{
		Sharding: config.ShardingConfig{Mode: mode},
		BFT:      config.BFTConfig{Enabled: true, ShardConf: &types.PartitionDescriptionRecord{PartitionID: 1}},
	}
}

func stateIDParams(t *testing.T) json.RawMessage {
	id := strings.Repeat("ab", 32)
	return json.RawMessage(`{"stateIds":["` + id + `"]}`)
}

func requireUnsupported(t *testing.T, h jsonrpc.HandlerFunc) {
	_, rerr := h(t.Context(), stateIDParams(t))
	require.NotNil(t, rerr)
	require.Equal(t, jsonrpc.ProofUnavailableCode, rerr.Code)
	data, _ := json.Marshal(rerr.Data)
	require.Contains(t, string(data), "unsupported_configuration")
}

func TestNewBatchProofHandler(t *testing.T) {
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	defer cat.Close()
	storage := wiringStorage{rec: wiringRecords{}}

	t.Run("served from the catalog when supported", func(t *testing.T) {
		h, reason := NewBatchProofHandler(wiringConfig(config.ShardingModeBFTShard), wiringRM{cat}, storage, wiringSvc{})
		require.Empty(t, reason)
		_, rerr := h(t.Context(), stateIDParams(t))
		require.NotNil(t, rerr)
		data, _ := json.Marshal(rerr.Data)
		require.Contains(t, string(data), "no_finalized_view", "the real service answered (empty catalog), not the unsupported stub")
	})
	t.Run("standalone with real BFT is the natural unsharded deployment: served, no sidecar machinery", func(t *testing.T) {
		cfg := wiringConfig(config.ShardingModeStandalone) // empty BFT shard ID: one-leaf partition
		h, reason := NewBatchProofHandler(cfg, wiringRM{cat}, storage, wiringSvc{})
		require.Empty(t, reason)
		_, rerr := h(t.Context(), stateIDParams(t))
		require.NotNil(t, rerr)
		data, _ := json.Marshal(rerr.Data)
		require.Contains(t, string(data), "no_finalized_view", "the real service answered, not the unsupported stub")
		require.NotContains(t, string(data), "unsupported_configuration")
		// the topology file and any consolidation shard configuration are not needed
		require.Empty(t, cfg.BFT.ConsolidationShards)
	})
	t.Run("explicitly unsupported modes", func(t *testing.T) {
		for name, tc := range map[string]struct {
			cfg *config.Config
			rm  any
			svc any
		}{
			"parent":                      {wiringConfig(config.ShardingModeParent), wiringRM{cat}, wiringSvc{}},
			"child":                       {wiringConfig(config.ShardingModeChild), wiringRM{cat}, wiringSvc{}},
			"BFT disabled":                {func() *config.Config { c := wiringConfig(config.ShardingModeBFTShard); c.BFT.Enabled = false; return c }(), wiringRM{cat}, wiringSvc{}},
			"no retaining backend":        {wiringConfig(config.ShardingModeBFTShard), wiringRM{nil}, wiringSvc{}},
			"round manager without views": {wiringConfig(config.ShardingModeBFTShard), struct{}{}, wiringSvc{}},
			"service without routing":     {wiringConfig(config.ShardingModeBFTShard), wiringRM{cat}, struct{}{}},
		} {
			h, reason := NewBatchProofHandler(tc.cfg, tc.rm, storage, tc.svc)
			require.NotEmpty(t, reason, name)
			requireUnsupported(t, h)
		}
		h, reason := NewBatchProofHandler(wiringConfig(config.ShardingModeBFTShard), wiringRM{cat}, wiringStorage{rec: struct {
			interfaces.AggregatorRecordStorage
		}{}}, wiringSvc{})
		require.NotEmpty(t, reason, "record storage without the bulk projection")
		requireUnsupported(t, h)
	})
}
