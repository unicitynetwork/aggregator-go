package main

import (
	"context"
	"fmt"
	"time"
)

func run(ctx context.Context, config *Config) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled, shutting down.")
			return ctx.Err()
		default:
			// Main logic of the aggregator goes here
			fmt.Println("Aggregator is running...")
			// Simulate work
			time.Sleep(time.Second)
		}
	}
}
