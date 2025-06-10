package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log.Println("Starting Aggregator...")
	ctx := quitSignalContext()

	configPath := ""
	if len(os.Args) > 1 {
		configPath = os.Args[1]
		log.Printf("reading config from %s\n", configPath)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}
	log.Printf("config: %+v\n", config)

	if err = run(ctx, config); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("failed to run aggregator: %v", err)
	}
}

func quitSignalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)
		sig := <-sigChan
		log.Printf("Caught signal %v: terminating\n", sig)
		cancel()
	}()

	return ctx
}
