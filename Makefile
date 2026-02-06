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

# Run performance test with custom URL and auth header
performance-test-auth:
	@echo "Building performance test..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/performance-test ./cmd/performance-test
	@if [ -z "$(URL)" ]; then \
		echo "Error: URL parameter required. Usage: make performance-test-auth URL=http://localhost:8080 AUTH='Bearer token'"; \
		exit 1; \
	fi
	@echo "Starting performance test against $(URL)..."
	@AGGREGATOR_URL="$(URL)" AUTH_HEADER="$(AUTH)" ./$(BUILD_DIR)/performance-test

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
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_data ./data/redis_data && chmod -R 777 ./data
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-run-clean-keep-tb:
	@echo "Rebuilding services with clean state but preserving BFT config as current user..."
	@docker compose down
	@rm -rf ./data/mongodb_data ./data/redis_data
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_data ./data/redis_data && chmod -R 777 ./data
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-restart-aggregator:
	@echo "Rebuilding and restarting aggregator service..."
	@docker compose stop aggregator
	@docker compose build aggregator
	@LOG_LEVEL=debug docker compose up -d --force-recreate --no-deps aggregator
	@echo "Aggregator service restarted"

docker-run-ha-clean:
	@echo "Rebuilding services with clean state and HA enabled as current user..."
	@docker compose -f ha-compose.yml down
	@rm -rf ./data
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_data/mongo1 ./data/mongodb_data/mongo2 ./data/mongodb_data/mongo3 ./data/redis_data && chmod -R 777 ./data
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose -f ha-compose.yml up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-run-ha-clean-keep-tb:
	@echo "Rebuilding services with clean state but preserving BFT config and HA enabled as current user..."
	@docker compose -f ha-compose.yml down
	@rm -rf ./data/mongodb_data ./data/redis_data
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_data/mongo1 ./data/mongodb_data/mongo2 ./data/mongodb_data/mongo3 ./data/redis_data && chmod -R 777 ./data
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose -f ha-compose.yml up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-restart-ha:
	@echo "Rebuilding and restarting HA aggregator services..."
	@docker compose -f ha-compose.yml stop aggregator-1 aggregator-2
	@docker compose -f ha-compose.yml build aggregator-1 aggregator-2
	@LOG_LEVEL=debug docker compose -f ha-compose.yml up -d --force-recreate --no-deps aggregator-1 aggregator-2
	@echo "HA Aggregator services restarted"


docker-run-sh-clean-keep-tb:
	@echo "Rebuilding services with clean state but preserving BFT config, with sharding enabled as current user..."
	@docker compose -f sharding-compose.yml down
	@rm -rf ./data/mongodb_shard1_data ./data/mongodb_shard2_data ./data/mongodb_root_data ./data/redis_shared_data
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_shard1_data ./data/mongodb_shard2_data ./data/mongodb_root_data ./data/redis_shared_data && chmod -R 777 ./data
	@mkdir -p ./logs/shard1 ./logs/shard2 ./logs/root && chmod -R 777 ./logs
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose -f sharding-compose.yml up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-run-sh-ha-clean-keep-tb:
	@echo "Rebuilding services with clean state but preserving BFT config, with sharding and HA enabled as current user..."
	@docker compose -f sharding-ha-compose.yml down
	@rm -rf ./data/mongodb_shard1_data_1 ./data/mongodb_shard1_data_2 ./data/mongodb_shard1_data_3 ./data/mongodb_shard2_data_1 ./data/mongodb_shard2_data_2 ./data/mongodb_shard2_data_3 ./data/mongodb_root_data_1 ./data/mongodb_root_data_2 ./data/mongodb_root_data_3 ./data/redis_data
	@mkdir -p ./data/genesis/root ./data/genesis-root ./data/mongodb_shard1_data_1 ./data/mongodb_shard1_data_2 ./data/mongodb_shard1_data_3 ./data/mongodb_shard2_data_1 ./data/mongodb_shard2_data_2 ./data/mongodb_shard2_data_3 ./data/mongodb_root_data_1 ./data/mongodb_root_data_2 ./data/mongodb_root_data_3 ./data/redis_data && chmod -R 777 ./data
	@mkdir -p ./logs/shard1-1 ./logs/shard1-2 ./logs/shard2-1 ./logs/shard2-2 ./logs/root-1 && chmod -R 777 ./logs
	@USER_UID=$$(id -u) USER_GID=$$(id -g) LOG_LEVEL=debug docker compose -f sharding-ha-compose.yml up --force-recreate -d --build
	@echo "Services rebuilt with user UID=$$(id -u):$$(id -g)"

docker-restart-sh-ha:
	@echo "Rebuilding and restarting sharding+HA aggregator services..."
	@docker compose -f sharding-ha-compose.yml stop aggregator-shard1-1 aggregator-shard1-2 aggregator-shard2-1 aggregator-shard2-2 aggregator-root-1
	@docker compose -f sharding-ha-compose.yml build aggregator-shard1-1 aggregator-shard1-2 aggregator-shard2-1 aggregator-shard2-2 aggregator-root-1
	@LOG_LEVEL=debug docker compose -f sharding-ha-compose.yml up -d --force-recreate --no-deps aggregator-shard1-1 aggregator-shard1-2 aggregator-shard2-1 aggregator-shard2-2 aggregator-root-1
	@echo "Sharding+HA Aggregator services restarted"
