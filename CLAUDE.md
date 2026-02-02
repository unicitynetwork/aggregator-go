# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance Go aggregator service for the Unicity blockchain platform. Core features: JSON-RPC 2.0 API, MongoDB persistence, HA with leader election, BFT consensus integration, round-based block creation.

## Development Commands

```bash
# Build and run
make build                    # Build to bin/aggregator
make run                      # Build and run
go test ./...                 # Run all tests
go test -race ./...           # Run tests with race detection
go test -bench=. ./...        # Run benchmarks

# Run single test
go test -v -run TestName ./path/to/package

# Docker development
make docker-run-clean         # Clean rebuild (stops, removes data, rebuilds)
make docker-run-clean-keep-tb # Rebuild but preserve BFT trust base config
make docker-restart-aggregator # Rebuild just the aggregator service

# Sharding setups
make docker-run-sh-clean-keep-tb    # Sharded setup (parent + 2 children)
make docker-run-sh-ha-clean-keep-tb # Sharded + HA setup

# Performance testing
make performance-test                                           # Local standalone test
make performance-test-auth URL=http://host:port AUTH='Bearer x' # Remote with auth

# Sharded performance test (provide shard targets with shardID suffix)
SHARD_TARGETS="http://localhost:3001:3,http://localhost:3002:2" TEST_DURATION=10s REQUESTS_PER_SEC=100 go run ./cmd/performance-test
```

## Architecture Overview

### Core Components
- **gateway.Server** (`internal/gateway/server.go`): HTTP server with Gin, JSON-RPC routing
- **service.AggregatorService** (`internal/service/service.go`): Business logic, commitment validation
- **service.ParentAggregatorService** (`internal/service/parent_service.go`): Parent mode shard aggregation
- **round.RoundManager** (`internal/round/round_manager.go`): Block creation orchestrator, 1-second rounds
- **round.ParentRoundManager** (`internal/round/parent_round_manager.go`): Parent mode round management
- **smt.ThreadSafeSMT** (`internal/smt/thread_safe_smt.go`): Sparse Merkle Tree with snapshots
- **ha.LeaderElection** (`internal/ha/leader_election.go`): MongoDB-based leader election

### Key Data Flow
1. Client submits commitment via JSON-RPC → validation via `signing.CommitmentValidator`
2. Commitment stored in queue (MongoDB or Redis based on config)
3. RoundManager batches commitments every second (up to `BATCH_LIMIT`)
4. SMT updated with new commitments in parallel batches
5. Root hash submitted to BFT consensus
6. Block created with transaction proof and stored
7. Inclusion proofs generated from SMT for queries

### Sharding Architecture

Three operating modes configured via `SHARDING_MODE`:
- **standalone**: Original mode - processes commitments directly from clients
- **parent**: Aggregates child aggregator root hashes into parent SMT
- **child**: Validates commitment belongs to shard, submits roots to parent

Parent mode uses `submit_shard_root` and `get_shard_proof` APIs instead of commitment APIs.

ShardID format: Uses `1` as MSB prefix (e.g., `0b10` for shard 0, `0b11` for shard 1 with `SHARD_ID_LENGTH=1`).

### Storage Layer

Interface-based abstraction in `internal/storage/interfaces/interfaces.go`:
- **CommitmentQueue**: Pending commitments (MongoDB or Redis)
- **AggregatorRecordStorage**: Finalized commitment records
- **BlockStorage**: Blockchain blocks with BFT proofs
- **SmtStorage**: Sparse Merkle Tree leaf nodes
- **BlockRecordsStorage**: Block number to request ID mappings
- **LeadershipStorage**: HA leader election state
- **TrustBaseStorage**: BFT trust base configuration

## Key Dependencies

- `github.com/gin-gonic/gin`: HTTP router
- `go.mongodb.org/mongo-driver`: MongoDB driver
- `github.com/btcsuite/btcd/btcec/v2`: secp256k1 cryptography
- `github.com/unicitynetwork/bft-core`: BFT consensus client
- `github.com/testcontainers/testcontainers-go`: Integration test infrastructure

## Testing

Integration tests use testcontainers for MongoDB/Redis. Tests with `_test.go` suffix alongside source files.

Key test files:
- `internal/round/round_manager_test.go`: Block creation flow
- `internal/service/parent_service_test.go`: Parent mode operations
- `internal/smt/smt_test.go`: SMT operations and TypeScript compatibility
- `internal/storage/mongodb/*_test.go`: Storage layer tests

## Critical Implementation Notes

### SMT Snapshot Pattern
Create snapshot at round start → add leaves → commit only after block storage succeeds. See `internal/smt/thread_safe_smt_snapshot.go`.

### Commitment Validation Order
1. Algorithm check (secp256k1 only)
2. Public key format (33-byte compressed)
3. StateHash DataHash format ("0000" SHA256 prefix)
4. RequestID = SHA256(publicKey || stateHash)
5. Signature format (65 bytes)
6. TransactionHash format
7. Signature cryptographic verification
8. Shard validation (child mode only)

### BFT Integration
BFT client in `internal/bft/client.go` connects to consensus network. Trust bases loaded from files at startup and can be dynamically added via `/api/v1/trustbases` endpoint.

---

## Development Guidelines

### Documentation
- Keep README.md synchronized with code changes
- Append changelog entries to `changes.txt` with format: "CHANGELOG ENTRY - Day Month Year at Time CET"

### Important Reminders
- **Clean rebuild**: `make docker-run-clean` stops containers, removes data, rebuilds fresh
