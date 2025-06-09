.PHONY: build test clean run lint fmt vet

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

# Development setup
dev-setup: deps
	@echo "Setting up development environment..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Docker commands
docker-build:
	@echo "Building Docker image..."
	@docker build -t unicity-aggregator .

docker-up:
	@echo "Starting services with Docker Compose..."
	@docker compose up -d

docker-down:
	@echo "Stopping services..."
	@docker compose down

docker-logs:
	@echo "Showing logs..."
	@docker compose logs -f

docker-restart:
	@echo "Restarting services..."
	@docker compose restart

# Combined commands
docker-rebuild: docker-down docker-build docker-up
	@echo "Services rebuilt and restarted"

docker-clean:
	@echo "Cleaning up Docker resources..."
	@docker compose down -v
	@docker image rm unicity-aggregator 2>/dev/null || true