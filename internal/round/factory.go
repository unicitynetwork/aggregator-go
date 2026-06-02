package round

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
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
	GetSMTBackend() smtbackend.Backend
	CheckParentHealth(ctx context.Context) error
	// FinalizationReadLock blocks during the SMT commit+finalize window to keep root/block consistent.
	FinalizationReadLock() func()
	// TryFinalizationReadLock reports false when a proof request should return not-ready instead of waiting.
	TryFinalizationReadLock() (func(), bool)
	GetKnownNotReadyBlock(stateID api.StateID) (*models.Block, bool)
	GetCachedProofMetadata(stateID api.StateID, rootHash api.HexBytes) (*models.Block, *models.AggregatorRecord, bool)
	GetProofCacheStats() (pending int, records int, blocks int)
}

// NewManager creates the appropriate round manager based on sharding mode
func NewManager(
	ctx context.Context,
	cfg *config.Config,
	logger *logger.Logger,
	commitmentQueue interfaces.CommitmentQueue,
	storage interfaces.Storage,
	stateTracker *state.Tracker,
	luc *types.UnicityCertificate,
	eventBus *events.EventBus,
	threadSafeSmt *smt.ThreadSafeSMT,
	trustBaseProvider interfaces.TrustBaseProvider,
) (Manager, error) {
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeBFTShard:
		return NewRoundManager(ctx, cfg, logger, commitmentQueue, storage, nil, stateTracker, luc, eventBus, threadSafeSmt, trustBaseProvider)
	case config.ShardingModeParent:
		return NewParentRoundManager(ctx, cfg, logger, storage, luc, eventBus, threadSafeSmt, trustBaseProvider)
	case config.ShardingModeChild:
		rootAggregatorClient := sharding.NewRootAggregatorClient(cfg.Sharding.Child.ParentRpcAddr)
		return NewRoundManager(ctx, cfg, logger, commitmentQueue, storage, rootAggregatorClient, stateTracker, luc, eventBus, threadSafeSmt, nil)
	default:
		return nil, fmt.Errorf("unsupported sharding mode: %s", cfg.Sharding.Mode)
	}
}
