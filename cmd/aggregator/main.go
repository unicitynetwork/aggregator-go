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
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
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

	// Initialize storage
	storage, err := mongodb.NewStorage(*cfg)
	if err != nil {
		log.WithComponent("main").Error("Failed to initialize storage", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Test database connection
	connectionTestCtx, connectionTestCancelFn := context.WithTimeout(ctx, 10*time.Second)
	if err := storage.Ping(connectionTestCtx); err != nil {
		connectionTestCancelFn()
		log.WithComponent("main").Error("Failed to connect to database", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}
	connectionTestCancelFn()

	log.WithComponent("main").Info("Database connection established")

	// start leader election service
	var leaderElection *ha.LeaderElection
	if cfg.HA.Enabled {
		leaderElection = ha.NewLeaderElection(cfg.HA, log, storage.LeadershipStorage())
		leaderElection.Start(ctx)
		log.WithComponent("main").Info("High availability mode leader election started")
	} else {
		log.WithComponent("main").Info("High availability mode is disabled")
	}

	// Initialize service (uses factory to create appropriate service based on sharding mode)
	aggregatorService, err := service.NewService(ctx, cfg, log, storage, leaderElection)
	if err != nil {
		log.WithComponent("main").Error("Failed to create aggregator service", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Start the aggregator service
	if err := aggregatorService.Start(ctx); err != nil {
		log.WithComponent("main").Error("Failed to start aggregator service", "error", err.Error())
		gracefulExit(asyncLogger, 1)
	}

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, storage, aggregatorService)

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

	// Stop aggregator service
	if err := aggregatorService.Stop(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to stop aggregator service gracefully", "error", err.Error())
	}

	// Stop leader election service
	if leaderElection != nil {
		if err := leaderElection.Shutdown(shutdownCtx); err != nil {
			log.WithComponent("main").Error("Failed to stop leader election", "error", err.Error())
		}
	}

	// Close storage
	if err := storage.Close(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to close storage gracefully", "error", err.Error())
	}

	log.WithComponent("main").Info("Aggregator shut down successfully")

	// Stop async logger if enabled
	if asyncLogger != nil {
		asyncLogger.Stop()
	}
}
