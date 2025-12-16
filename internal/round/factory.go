package round

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Manager interface for both standalone and parent round managers
type Manager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Activate(ctx context.Context) error
	Deactivate(ctx context.Context) error
	GetSMT() *smt.ThreadSafeSMT
	CheckParentHealth(ctx context.Context) error
}

// NewManager creates the appropriate round manager based on sharding mode
func NewManager(ctx context.Context, cfg *config.Config, logger *logger.Logger, commitmentQueue interfaces.CommitmentQueue, storage interfaces.Storage, stateTracker *state.Tracker, luc *types.UnicityCertificate) (Manager, error) {
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone:
		smtInstance := smt.NewSparseMerkleTree(api.SHA256, 16+256)
		return NewRoundManager(ctx, cfg, logger, smtInstance, commitmentQueue, storage, nil, stateTracker, luc)
	case config.ShardingModeParent:
		return NewParentRoundManager(ctx, cfg, logger, storage, luc)
	case config.ShardingModeChild:
		smtInstance := smt.NewChildSparseMerkleTree(api.SHA256, 16+256, cfg.Sharding.Child.ShardID)
		rootAggregatorClient := sharding.NewRootAggregatorClient(cfg.Sharding.Child.ParentRpcAddr)
		return NewRoundManager(ctx, cfg, logger, smtInstance, commitmentQueue, storage, rootAggregatorClient, stateTracker, luc)
	default:
		return nil, fmt.Errorf("unsupported sharding mode: %s", cfg.Sharding.Mode)
	}
}
