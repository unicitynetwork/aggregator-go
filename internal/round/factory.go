package round

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// Manager interface for both standalone and parent round managers
type Manager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Activate(ctx context.Context) error
	Deactivate(ctx context.Context) error
	GetSMT() *smt.ThreadSafeSMT
}

// NewManager creates the appropriate round manager based on sharding mode
func NewManager(ctx context.Context, cfg *config.Config, logger *logger.Logger, commitmentQueue interfaces.CommitmentQueue, storage interfaces.Storage, stateTracker *state.Tracker) (Manager, error) {
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone:
		return NewRoundManager(ctx, cfg, logger, commitmentQueue, storage, nil, stateTracker)
	case config.ShardingModeParent:
		return NewParentRoundManager(ctx, cfg, logger, storage)
	default:
		return nil, fmt.Errorf("unsupported sharding mode: %s", cfg.Sharding.Mode)
	}
}
