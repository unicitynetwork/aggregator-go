#!/bin/bash

echo "Building performance test..."
cd "$(dirname "$0")/.."

# Build the performance test
go build -o bin/performance-test ./cmd/performance-test

if [ $? -ne 0 ]; then
    echo "Failed to build performance test"
    exit 1
fi

echo "Starting performance test..."
echo "Make sure the aggregator service is running on localhost:3000"
echo ""

# Run the performance test
./bin/performance-test