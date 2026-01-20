package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// gracefulExit flushes async logger and exits with the given code
func gracefulExit(asyncLogger *logger.AsyncLoggerWrapper, code int) {
	if asyncLogger != nil {
		asyncLogger.Stop()
	}
	os.Exit(code)
}

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	baseLogger, err := logger.NewWithConfig(logger.LogConfig{
		Level:           cfg.Logging.Level,
		Format:          cfg.Logging.Format,
		Output:          cfg.Logging.Output,
		EnableJSON:      cfg.Logging.EnableJSON,
		FilePath:        cfg.Logging.FilePath,
		MaxSizeMB:       cfg.Logging.MaxSizeMB,
		MaxBackups:      cfg.Logging.MaxBackups,
		MaxAgeDays:      cfg.Logging.MaxAgeDays,
		CompressBackups: cfg.Logging.CompressBackups,
	})
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Wrap with async logger if enabled
	var log *logger.Logger
	var asyncLogger *logger.AsyncLoggerWrapper
	if cfg.Logging.EnableAsync {
		bufferSize := cfg.Logging.AsyncBufferSize
		if bufferSize <= 0 {
			bufferSize = 10000 // Default buffer size
		}
		asyncLogger = logger.NewAsyncLogger(baseLogger, bufferSize)
		log = asyncLogger.Logger
		log.WithComponent("main").Info("Async logging enabled",
			"bufferSize", bufferSize)
	} else {
		log = baseLogger
	}

	// Log file logging configuration if enabled
	if cfg.Logging.FilePath != "" {
		log.WithComponent("main").Info("File logging enabled",
			"filePath", cfg.Logging.FilePath,
			"maxSizeMB", cfg.Logging.MaxSizeMB,
			"maxBackups", cfg.Logging.MaxBackups,
			"maxAgeDays", cfg.Logging.MaxAgeDays,
			"compress", cfg.Logging.CompressBackups)
	}

	log.WithComponent("main").Info("Starting Unicity Aggregator")

	// create global context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	commitmentQueue, storageInstance, err := storage.NewStorage(ctx, cfg, log)
	if err != nil {
		log.WithComponent("main").Error("Failed to initialize storage", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	if cfg.Storage.UseRedisForCommitments {
		log.WithComponent("main").Info("Using Redis for commitment queue and MongoDB for persistence",
			"redis_host", cfg.Redis.Host,
			"redis_port", cfg.Redis.Port,
			"flush_interval", cfg.Storage.RedisFlushInterval,
			"max_batch_size", cfg.Storage.RedisMaxBatchSize)
	} else {
		log.WithComponent("main").Info("Using MongoDB for all storage")
	}

	connectionTestCtx, connectionTestCancelFn := context.WithTimeout(ctx, 10*time.Second)
	if err := storageInstance.Ping(connectionTestCtx); err != nil {
		connectionTestCancelFn()
		log.WithComponent("main").Error("Failed to connect to database", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}
	connectionTestCancelFn()

	log.WithComponent("main").Info("Database connection established")

	// Store trust bases from config files
	trustBaseValidator := service.NewTrustBaseValidator(storageInstance.TrustBaseStorage())
	for _, tb := range cfg.BFT.TrustBases {
		if err := trustBaseValidator.Verify(ctx, &tb); err != nil {
			log.WithComponent("main").Error("Trust base verification failed", "error", err.Error())
			gracefulExit(asyncLogger, 1)
		}
		if err := storageInstance.TrustBaseStorage().Store(ctx, &tb); err != nil {
			if errors.Is(err, interfaces.ErrTrustBaseAlreadyExists) {
				log.WithComponent("main").Warn("Trust base already exists, not overwriting it", "epoch", tb.GetEpoch())
			} else {
				log.WithComponent("main").Error("Failed to store trust base", "epoch", tb.GetEpoch(), "error", err.Error())
				gracefulExit(asyncLogger, 1)
			}
		} else {
			log.WithComponent("main").Info("Stored trust base", "epoch", tb.GetEpoch())
		}
	}

	if err := commitmentQueue.Initialize(ctx); err != nil {
		log.WithComponent("main").Error("Failed to initialize commitment queue", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Create the shared state tracker for block sync height
	stateTracker := state.NewSyncStateTracker()

	eventBus := events.NewEventBus(log)
	startFatalErrorListener(ctx, log, eventBus, stop)

	// Load last committed unicity certificate (can be nil for genesis)
	var luc *types.UnicityCertificate
	lastBlock, err := storageInstance.BlockStorage().GetLatest(ctx)
	if err != nil {
		log.WithComponent("main").Error("Failed to load last stored block", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}
	if lastBlock != nil {
		if err := types.Cbor.Unmarshal(lastBlock.UnicityCertificate, &luc); err != nil {
			log.WithComponent("main").Error("Failed to decode unicity certificate", "error", err.Error())
			gracefulExit(asyncLogger, 1)
		}
	}

	// Create SMT instance based on sharding mode
	var smtInstance *smt.SparseMerkleTree
	switch cfg.Sharding.Mode {
	case config.ShardingModeStandalone:
		smtInstance = smt.NewSparseMerkleTree(api.SHA256, 16+256)
	case config.ShardingModeChild:
		smtInstance = smt.NewChildSparseMerkleTree(api.SHA256, 16+256, cfg.Sharding.Child.ShardID)
	case config.ShardingModeParent:
		smtInstance = smt.NewParentSparseMerkleTree(api.SHA256, cfg.Sharding.ShardIDLength)
	default:
		log.WithComponent("main").Error("Unsupported sharding mode", "mode", cfg.Sharding.Mode)
		gracefulExit(asyncLogger, 1)
	}
	threadSafeSmt := smt.NewThreadSafeSMT(smtInstance)

	// Create round manager based on sharding mode
	roundManager, err := round.NewManager(ctx, cfg, log, commitmentQueue, storageInstance, stateTracker, luc, eventBus, threadSafeSmt)
	if err != nil {
		log.WithComponent("main").Error("Failed to create round manager", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize round manager (SMT restoration, etc.)
	if err := roundManager.Start(ctx); err != nil {
		log.WithComponent("main").Error("Failed to start round manager", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize leader selector and block syncer if enabled
	var ls leaderSelector
	var bs *ha.BlockSyncer
	if cfg.HA.Enabled {
		log.WithComponent("main").Info("High availability mode enabled")
		ls = ha.NewLeaderElection(log, cfg.HA, storageInstance.LeadershipStorage(), eventBus)
		ls.Start(ctx)

		// Disable block syncing for parent aggregator mode
		// Parent mode uses state-based SMT (current shard roots) rather than history-based (commitment leaves)
		if cfg.Sharding.Mode == config.ShardingModeParent {
			log.WithComponent("main").Info("Block syncing disabled for parent aggregator mode - SMT will be reconstructed on leadership transition")
		} else {
			log.WithComponent("main").Info("Starting block syncer")
			bs = ha.NewBlockSyncer(log, ls, storageInstance, threadSafeSmt, cfg.Sharding.Child.ShardID, cfg.Processing.RoundDuration, stateTracker)
			bs.Start(ctx)
		}

		// In HA mode, listen for leadership changes to activate/deactivate the round manager
		go func() {
			if err := startLeaderChangedEventListener(ctx, log, cfg, roundManager, bs, eventBus); err != nil {
				log.WithComponent("ha-listener").Error("Fatal error on leader changed event listener", "error", err.Error())
			}
		}()
	} else {
		log.WithComponent("main").Info("High availability mode is disabled, running as standalone leader")
		// In non-HA mode, activate the round manager directly
		if err := roundManager.Activate(ctx); err != nil {
			log.WithComponent("main").Error("Failed to activate round manager", "error", err.Error())
			gracefulExit(asyncLogger, 1)
		}
	}

	aggregatorService, err := service.NewService(ctx, cfg, log, roundManager, commitmentQueue, storageInstance, ls)
	if err != nil {
		log.WithComponent("main").Error("Failed to create service", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, aggregatorService)

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil {
			log.WithComponent("main").Error("Server failed to start", "error", err.Error())
		}
	}()

	log.WithComponent("main").Info("Aggregator started successfully",
		"host", cfg.Server.Host,
		"port", cfg.Server.Port)

	// Wait for interrupt signal
	<-ctx.Done()

	log.WithComponent("main").Info("Shutting down gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop server
	if err := server.Stop(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to stop server gracefully", "error", err.Error())
	}

	// Stop block syncer if it was started
	if bs != nil {
		bs.Stop()
	}

	// Stop leader selector if it was started
	if ls != nil {
		ls.Stop(shutdownCtx)
	}

	// Stop round manager
	if err := roundManager.Stop(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to stop round manager gracefully", "error", err.Error())
	}

	// Close storage backends
	if err := commitmentQueue.Close(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to close commitment queue gracefully", "error", err.Error())
	}
	if err := storageInstance.Close(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to close storage gracefully", "error", err.Error())
	}

	log.WithComponent("main").Info("Aggregator shut down successfully")

	// Stop async logger if enabled
	if asyncLogger != nil {
		asyncLogger.Stop()
	}

	// Close file logger if enabled
	if err := baseLogger.Close(); err != nil {
		fmt.Printf("Failed to close file logger: %v\n", err)
	}
}

func startFatalErrorListener(ctx context.Context, log *logger.Logger, eventBus *events.EventBus, stop context.CancelFunc) {
	fatalCh := eventBus.Subscribe(events.TopicFatalError)
	go func() {
		defer func() {
			if err := eventBus.Unsubscribe(events.TopicFatalError, fatalCh); err != nil {
				log.WithComponent("fatal-listener").Error("Failed to unsubscribe from fatal error topic", "error", err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-fatalCh:
				source := "unknown"
				detail := ""
				switch e := evt.(type) {
				case events.FatalErrorEvent:
					source = e.Source
					detail = e.Error
				case *events.FatalErrorEvent:
					source = e.Source
					detail = e.Error
				}
				log.WithComponent("fatal-listener").Error("Fatal error event received, shutting down",
					"source", source,
					"error", detail)
				stop()
				return
			}
		}
	}()
}

func startLeaderChangedEventListener(ctx context.Context, log *logger.Logger, cfg *config.Config, roundManager round.Manager, bs *ha.BlockSyncer, eventBus *events.EventBus) error {
	log.WithComponent("ha-listener").Info("Subscribing to TopicLeaderChanged")
	leaderChangedCh := eventBus.Subscribe(events.TopicLeaderChanged)
	defer func() {
		if err := eventBus.Unsubscribe(events.TopicLeaderChanged, leaderChangedCh); err != nil {
			log.WithComponent("ha-listener").Error("Failed to unsubscribe from TopicLeaderChanged", "error", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-leaderChangedCh:
			evt := e.(*events.LeaderChangedEvent)
			log.WithComponent("ha-listener").Info("Received LeaderChangedEvent", "isLeader", evt.IsLeader)
			if evt.IsLeader {
				// In child and standalone mode, we must sync SMT state before starting to produce blocks
				// In parent mode, the Activate call handles SMT reconstruction, no block sync needed.
				if cfg.Sharding.Mode != config.ShardingModeParent {
					log.WithComponent("ha-listener").Info("Becoming leader, syncing to latest block...")
					if err := bs.SyncToLatestBlock(ctx); err != nil {
						log.WithComponent("ha-listener").Error("failed to sync to latest block on leadership change", "error", err)
						continue
					} else {
						log.WithComponent("ha-listener").Info("Sync complete.")
					}
				}
				if err := roundManager.Activate(ctx); err != nil {
					log.WithComponent("ha-listener").Error("Failed to activate round manager", "error", err)
				}
			} else {
				if err := roundManager.Deactivate(ctx); err != nil {
					log.WithComponent("ha-listener").Error("Failed to deactivate round manager", "error", err)
				}
			}
		}
	}
}

type leaderSelector interface {
	IsLeader(ctx context.Context) (bool, error)
	Start(ctx context.Context)
	Stop(ctx context.Context)
}
