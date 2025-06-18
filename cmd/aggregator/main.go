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
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	log, err := logger.New(
		cfg.Logging.Level,
		cfg.Logging.Format,
		cfg.Logging.Output,
		cfg.Logging.EnableJSON,
	)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log.WithComponent("main").Info("Starting Unicity Aggregator")

	// Initialize storage
	storage, err := mongodb.NewStorage(&cfg.Database)
	if err != nil {
		log.WithComponent("main").Error("Failed to initialize storage", "error", err.Error())
		os.Exit(1)
	}

	// Test database connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := storage.Ping(ctx); err != nil {
		cancel()
		log.WithComponent("main").Error("Failed to connect to database", "error", err.Error())
		os.Exit(1)
	}
	cancel()

	log.WithComponent("main").Info("Database connection established")

	// Initialize service
	aggregatorService, err := service.NewAggregatorService(cfg, log, storage)
	if err != nil {
		log.WithComponent("main").Error("Failed to initialize aggregator service", "error", err.Error())
		os.Exit(1)
	}
	// Start the aggregator service
	if err := aggregatorService.Start(context.Background()); err != nil {
		log.WithComponent("main").Error("Failed to start aggregator service", "error", err.Error())
		os.Exit(1)
	}

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, storage, aggregatorService)

	// Setup graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	// Close storage
	if err := storage.Close(shutdownCtx); err != nil {
		log.WithComponent("main").Error("Failed to close storage gracefully", "error", err.Error())
	}

	log.WithComponent("main").Info("Aggregator shut down successfully")
}
