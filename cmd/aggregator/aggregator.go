package main

import (
	"context"
	"log"
	"time"
)

func run(ctx context.Context, config *Config) error {
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, shutting down.")
			return ctx.Err()
		default:
			// Main logic of the aggregator goes here
			log.Println("Aggregator is running...")
			// Simulate work
			time.Sleep(time.Second)
		}
	}
}
