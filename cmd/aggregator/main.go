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
		log.WithComponent("main").WithError(err).Fatal("Failed to initialize storage")
	}

	// Test database connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := storage.Ping(ctx); err != nil {
		cancel()
		log.WithComponent("main").WithError(err).Fatal("Failed to connect to database")
	}
	cancel()

	log.WithComponent("main").Info("Database connection established")

	// Initialize service
	aggregatorService := service.NewAggregatorService(cfg, log, storage)

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, storage, aggregatorService)

	// Setup graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start server in a goroutine
	go func() {
		if err := server.Start(ctx); err != nil {
			log.WithComponent("main").WithError(err).Error("Server failed to start")
		}
	}()

	log.WithComponent("main").Infof("Aggregator started successfully on %s:%s", cfg.Server.Host, cfg.Server.Port)

	// Wait for interrupt signal
	<-ctx.Done()

	log.WithComponent("main").Info("Shutting down gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop server
	if err := server.Stop(shutdownCtx); err != nil {
		log.WithComponent("main").WithError(err).Error("Failed to stop server gracefully")
	}

	// Close storage
	if err := storage.Close(shutdownCtx); err != nil {
		log.WithComponent("main").WithError(err).Error("Failed to close storage gracefully")
	}

	log.WithComponent("main").Info("Aggregator shut down successfully")
}