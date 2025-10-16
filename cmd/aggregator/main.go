package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
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
	baseLogger, err := logger.New(
		cfg.Logging.Level,
		cfg.Logging.Format,
		cfg.Logging.Output,
		cfg.Logging.EnableJSON,
	)
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

	log.WithComponent("main").Info("Starting Unicity Aggregator")

	// create global context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	commitmentQueue, storageInstance, err := storage.NewStorage(cfg, log)
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

	if err := commitmentQueue.Initialize(ctx); err != nil {
		log.WithComponent("main").Error("Failed to initialize commitment queue", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Create the shared state tracker for block sync height
	stateTracker := state.NewSyncStateTracker()

	// Create round manager based on sharding mode
	roundManager, err := round.NewManager(ctx, cfg, log, commitmentQueue, storageInstance, stateTracker)
	if err != nil {
		log.WithComponent("main").Error("Failed to create round manager", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize round manager (SMT restoration, etc.)
	if err := roundManager.Start(ctx); err != nil {
		log.WithComponent("main").Error("Failed to start round manager", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize leader selector and HA Manager if enabled
	var haManager *ha.HAManager
	var leaderSelector *ha.LeaderElection
	if cfg.HA.Enabled {
		log.WithComponent("main").Info("High availability mode enabled")
		leaderSelector = ha.NewLeaderElection(log, cfg.HA, storageInstance.LeadershipStorage())
		leaderSelector.Start(ctx)

		haManager = ha.NewHAManager(log, roundManager, leaderSelector, storageInstance, roundManager.GetSMT(), cfg.Sharding.Child.ShardID, stateTracker, cfg.Processing.RoundDuration)
		haManager.Start(ctx)
	} else {
		log.WithComponent("main").Info("High availability mode is disabled, running as standalone leader")
		// In non-HA mode, activate the round manager directly
		if err := roundManager.Activate(ctx); err != nil {
			log.WithComponent("main").Error("Failed to activate round manager", "error", err.Error())
			gracefulExit(asyncLogger, 1)
		}
	}

	// Create service with round manager and leader selector
	aggregatorService, err := service.NewService(ctx, cfg, log, roundManager, commitmentQueue, storageInstance, leaderSelector)
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

	// Stop HA Manager if it was started
	if haManager != nil {
		haManager.Stop()
	}

	// Stop leader selector if it was started
	if leaderSelector != nil {
		leaderSelector.Stop(shutdownCtx)
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
}
