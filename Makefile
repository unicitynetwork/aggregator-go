.PHONY: build test clean run lint fmt vet performance-test docker-setup docker-fix-permissions docker-up docker-logs docker-down docker-clean docker-run-clean

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

# Docker commands
docker-setup:
	@echo "Setting up Docker environment with proper permissions..."
	@./scripts/setup.sh

docker-fix-permissions:
	@echo "Fixing file permissions for Docker containers..."
	@./scripts/fix-permissions.sh

docker-up: docker-setup
	@echo "Starting services..."
	@docker compose up -d --build

docker-logs:
	@echo "Showing service logs..."
	@docker compose logs -f

docker-down:
	@echo "Stopping services..."
	@docker compose down

docker-clean:
	@echo "Cleaning up all data and containers..."
	@./scripts/cleanup.sh

docker-run-clean: docker-clean docker-setup
	@echo "Rebuilding services with clean state..."
	@docker compose up --force-recreate -d --build
	@echo "Services rebuilt with clean state"