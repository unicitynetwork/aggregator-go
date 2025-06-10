package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting Aggregator...")
	ctx := quitSignalContext()

	configPath := ""
	if len(os.Args) > 1 {
		configPath = os.Args[1]
		fmt.Printf("reading config from %s\n", configPath)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		panic(fmt.Errorf("failed to read config: %w", err))
	}
	fmt.Printf("config: %+v\n", config)

	if err = run(ctx, config); err != nil && !errors.Is(err, context.Canceled) {
		panic(err)
	}
}

func quitSignalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)
		sig := <-sigChan
		fmt.Printf("Caught signal %v: terminating\n", sig)
		cancel()
	}()

	return ctx
}
