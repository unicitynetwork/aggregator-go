.PHONY: build test clean run lint fmt vet performance-test

# Build variables
BINARY_NAME=aggregator
BUILD_DIR=bin
MAIN_PATH=./cmd/aggregator

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	@./$(BUILD_DIR)/$(BINARY_NAME)

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Run tests with race detection
test-race:
	@echo "Running tests with race detection..."
	@go test -race -v ./...

# Run benchmarks
benchmark:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

# Build and run performance test
performance-test:
	@echo "Building performance test..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/performance-test ./cmd/performance-test
	@echo "Starting performance test (make sure aggregator is running on localhost:3000)..."
	@./$(BUILD_DIR)/performance-test

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	@go vet ./...

# Lint code (requires golangci-lint)
lint:
	@echo "Linting code..."
	@golangci-lint run

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@go clean

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@go mod download
	@go mod tidy

docker-run-clean:
	@echo "Rebuilding services with clean state as current user..."
	@docker compose down
	@rm -rf ./data
	@mkdir -p ./data/genesis ./data/genesis-root ./data/mongodb_data && chmod -R 777 ./data
	@USER_UID=$$(id -u) USER_GID=$$(id -g) docker compose up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-restart-aggregator:
	@echo "Rebuilding and restarting aggregator service..."
	@docker compose stop aggregator
	@docker compose build aggregator
	@docker compose up -d --no-recreate --no-deps aggregator
	@echo "Aggregator service restarted"
