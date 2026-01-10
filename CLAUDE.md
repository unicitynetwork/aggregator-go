# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance Go aggregator service for the Unicity blockchain platform. It provides JSON-RPC 2.0 API endpoints for state transition aggregation with MongoDB persistence, optional Redis for the commitment queue, and high availability support. The service implements cryptographic validation (secp256k1), Sparse Merkle Tree operations, and supports horizontal scaling via sharding.

## Development Commands

```bash
# Build the binary
make build

# Run tests
make test

# Run tests with race detection
make test-race

# Run benchmarks
make benchmark

# Format code
make fmt

# Run linter (requires golangci-lint)
make lint

# Run the service (requires MongoDB)
MONGODB_URI=mongodb://admin:password@localhost:27017/aggregator?authSource=admin make run

# Run performance test (IMPORTANT: Always use make, not direct binary)
make performance-test

# Docker: Clean rebuild with fresh state
make docker-run-clean

# Docker: Rebuild keeping BFT trust base config
make docker-run-clean-keep-tb

# Docker: HA mode with clean state
make docker-run-ha-clean

# Docker: Sharding mode
make docker-run-sh-clean-keep-tb

# Docker: Sharding + HA mode
make docker-run-sh-ha-clean-keep-tb
```

## Architecture Overview

### Entry Point and Initialization (`cmd/aggregator/main.go`)
The main function orchestrates startup:
1. Load config from environment variables
2. Initialize logger (with optional async wrapper)
3. Connect to storage (MongoDB + optional Redis)
4. Load and validate trust bases
5. Create SMT instance (based on sharding mode)
6. Start RoundManager (handles block creation)
7. Initialize HA leader election (if enabled)
8. Create AggregatorService (business logic)
9. Start HTTP gateway server

### Core Components

**Gateway Layer** (`internal/gateway/`)
- `Server`: Gin-based HTTP server with JSON-RPC 2.0 handler
- Routes: POST `/` (JSON-RPC), GET `/health`, GET `/docs`, PUT `/api/v1/trustbases`
- Uses `pkg/jsonrpc` for request/response handling

**Service Layer** (`internal/service/`)
- `AggregatorService`: Business logic for standalone and child modes
- `ParentAggregatorService`: Handles parent aggregator in sharded setup
- Validates commitments via `CommitmentValidator` (secp256k1 signatures)
- Factory function `NewService()` creates appropriate service based on sharding mode

**Round Management** (`internal/round/`)
- `RoundManager`: Orchestrates block creation for standalone/child modes
- `ParentRoundManager`: Block creation for parent aggregator
- Lifecycle: `Start()` → `Activate()`/`Deactivate()` (HA transitions) → `Stop()`
- Batches commitments every round (default 1 second)
- Updates SMT with commitment leaves, creates blocks

**Sparse Merkle Tree** (`internal/smt/`)
- `SparseMerkleTree`: Core implementation with copy-on-write snapshots
- `ThreadSafeSMT`: Thread-safe wrapper with mutex locking
- Supports three modes: standalone, child (with shard prefix), parent (aggregates shard roots)

**Storage Layer** (`internal/storage/`)
- `interfaces/`: Storage interface definitions
- `mongodb/`: MongoDB implementations for all storage types
- `redis/`: Redis-based commitment queue (optional, higher throughput)
- Key interfaces: `CommitmentQueue`, `AggregatorRecordStorage`, `BlockStorage`, `SmtStorage`

**High Availability** (`internal/ha/`)
- MongoDB-based leader election with TTL locks
- `BlockSyncer`: Syncs SMT state when becoming leader
- `state.SyncStateTracker`: Tracks block sync progress

**BFT Integration** (`internal/bft/`)
- `Client`: Communicates with BFT root nodes for consensus
- Submits root hashes and receives unicity certificates

### Key Data Flow
```
Client → Gateway (JSON-RPC) → Service → CommitmentQueue
                                           ↓
RoundManager ← (batches commitments every 1s)
     ↓
SMT update → BFT submission → Block creation → AggregatorRecordStorage
```

### Sharding Modes
Configured via `SHARDING_MODE` environment variable:
- **standalone**: Single aggregator (default)
- **parent**: Root aggregator that accepts shard roots from children
- **child**: Shard aggregator that submits roots to parent

Shard assignment uses least significant bits of request ID. Child aggregators validate that commitments belong to their shard.

## API Types (`pkg/api/`)

Common types used across the codebase:
- `RequestID`: 68-char hex string with "0000" SHA256 prefix
- `DataHash`: Algorithm imprint format (4-byte prefix + 32-byte hash)
- `BigInt`: JSON-serializable big.Int wrapper
- `ShardID`: Shard identifier with MSB prefix bit
- `Authenticator`: secp256k1 signature with public key

## Testing

Tests use testcontainers for MongoDB/Redis dependencies:
```bash
# Run specific test file
go test -v ./internal/round/round_manager_test.go

# Run tests matching pattern
go test -v ./... -run TestRoundManager

# Integration tests (requires Docker)
go test -v ./test/integration/...
```

## Configuration

All configuration via environment variables. Key settings:
- `PORT`, `HOST`: HTTP server binding
- `MONGODB_URI`, `MONGODB_DATABASE`: Database connection
- `USE_REDIS_FOR_COMMITMENTS`: Enable Redis queue (default false)
- `SHARDING_MODE`: standalone/parent/child
- `BFT_ENABLED`, `BFT_*`: BFT consensus settings
- `DISABLE_HIGH_AVAILABILITY`: Disable HA mode

See README.md for complete configuration reference.

## Important Development Notes

### Performance Testing
Always use `make performance-test` instead of running the binary directly. The make target ensures proper environment setup.

### Documentation Sync
Keep README.md synchronized with code changes. Document new API endpoints, environment variables, and architecture changes immediately.

### Changelog
Append significant changes to `changes.txt` with timestamp: "CHANGELOG ENTRY - Day Month Year at Time CET"

### Code Patterns
- Context propagation for cancellation
- Structured logging with component tags
- Interface-based storage for testability
- Thread-safe SMT operations via snapshots
- Graceful shutdown with proper cleanup order
