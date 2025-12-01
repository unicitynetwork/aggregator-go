# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Go rewrite of the TypeScript aggregator service located in the `aggregator-ts/` folder. The goal is to create a more scalable and performant implementation while preserving all functional requirements.

## Development Commands

### TypeScript Reference Implementation
```bash
# Navigate to TypeScript implementation
cd aggregator-ts/

# Install dependencies
npm install

# Build the project
npm run build

# Start the service
npm run start

# Run tests
npm run test

# Run tests without Docker dependencies
npm run test:no-docker

# Run linting
npm run lint

# Run benchmarks
npm run benchmark

# Start MongoDB for testing
npm run docker:mongo

# Start Alphabill for integration tests
npm run docker:alphabill
```

### Go Implementation
```bash
# Build the Go service
go build -o aggregator ./cmd/aggregator

# Run tests
go test ./...

# Run tests with race detection
go test -race ./...

# Run benchmarks
go test -bench=. ./...

# Format code
go fmt ./...

# Run the service and connect to MongoDB running on localhost
MONGODB_URI=mongodb://admin:password@localhost:27017/aggregator?authSource=admin make run

# Run the performance test (IMPORTANT: Always use make, not direct binary)
make performance-test
```

## Architecture Overview

### Core Components
- **AggregatorGateway**: Main service orchestrator handling HTTP server lifecycle and subsystem coordination
- **AggregatorService**: Business logic layer for certification request validation and proof generation
- **RoundManager**: Block creation orchestrator that batches certification requests and coordinates with consensus
- **Storage Layer**: Interface-based abstraction with MongoDB implementations for various data types
- **High Availability**: Leader election system for distributed processing with automatic failover
- **Consensus Integration**: Alphabill blockchain integration for finality

### Key Data Flow
1. Client submits certification request via JSON-RPC API
2. Certification request validated and stored temporarily
3. RoundManager batches certification requests (1000 per batch) every second
4. SMT updated with new certification requests in parallel
5. Root hash submitted to Alphabill consensus
6. Block created with transaction proof and stored
7. Inclusion proofs generated from SMT for queries

### Storage Abstractions
- **IBlockStorage**: Blockchain blocks with transaction proofs
- **IAggregatorRecordStorage**: Finalized commitment records
- **ICommitmentStorage**: Temporary commitments with cursor-based processing
- **ISmtStorage**: Sparse Merkle Tree nodes
- **IBlockRecordsStorage**: Block number to state ID mappings
- **ILeadershipStorage**: High availability leader election state

### High Availability System
- MongoDB-based leader election with TTL locks
- Only leader processes blocks while all servers handle API requests
- Heartbeat mechanism with automatic failover
- Configurable via environment variables (DISABLE_HIGH_AVAILABILITY, LOCK_TTL_SECONDS, etc.)

## Key Dependencies

### TypeScript Implementation
- `@alphabill/alphabill-js-sdk`: Blockchain consensus integration
- `@unicitylabs/commons`: Core cryptographic and data structures
- `mongodb/mongoose`: Primary data persistence
- `express.js`: HTTP API server
- `winston`: Structured logging

### Go Implementation Dependencies (To be selected)
- MongoDB driver (official Go MongoDB driver)
- HTTP router (gin, echo, or standard library)
- Structured logging (logrus, zap, or slog)
- Cryptographic libraries for SMT and signature verification
- gRPC/HTTP client for Alphabill integration

## Critical Implementation Notes

### Concurrency Patterns
- TypeScript uses async/await extensively - translate to goroutines and channels
- SMT operations require locking mechanism for thread safety
- Cursor-based certification request processing needs careful state management
- Block creation process must be atomic and coordinated

### Error Handling
- Comprehensive error wrapping and structured error responses
- JSON-RPC 2.0 error code compliance
- Graceful degradation patterns for consensus failures
- Proper context cancellation for HTTP requests

### Performance Considerations
- Batch operations for database efficiency
- Parallel SMT updates during block creation
- Configurable concurrency limits for API requests
- Memory-efficient cursor-based processing

### Configuration Management
- Environment variable based configuration with validation
- Default values and validation for all settings
- Structured configuration objects
- High availability settings (leader election, heartbeat intervals)

## Testing Strategy

### Test Organization
- Unit tests for individual components with proper mocking
- Integration tests with real MongoDB (use testcontainers)
- High availability tests for leader election scenarios
- Benchmark tests for performance-critical paths
- API tests for JSON-RPC compliance

### Test Infrastructure
- Use testcontainers for MongoDB and Alphabill dependencies
- Mock implementations for external services
- Comprehensive error scenario testing
- Performance regression testing

## JSON-RPC API Specification

All endpoints follow JSON-RPC 2.0 specification:

### Core Methods
- `certification_request`: Submit state transition request
- `get_inclusion_proof`: Retrieve merkle proof for certification request
- `get_no_deletion_proof`: Get global no-deletion proof (not implemented)
- `get_block_height`: Current block number
- `get_block`: Block data by number or "latest"
- `get_block_records`: All certification request in specific block

### Infrastructure Endpoints
- `GET /health`: Service health and leader status
- `GET /docs`: API documentation

## MongoDB Schema Design

### Critical Collections
- **blocks**: Chain metadata with transaction proofs
- **aggregator_records**: Finalized certification request with certification data
- **certification_requests**: Temporary storage with cursor state
- **smt_nodes**: Sparse merkle tree leaf nodes
- **block_records**: Block number to state ID mappings
- **leadership**: High availability leader election state

### Schema Patterns
- Custom BigInt and Uint8Array serialization
- Indexed fields for efficient queries
- Unique constraints for data integrity
- Atomic operations for critical state changes

---

# Development Guidelines & Reminders

## Documentation Best Practices
**CRITICAL REMINDER**: Always keep README.md synchronized with code changes!

### When making ANY changes to the codebase:
1. **Update README immediately** after implementing features
2. **Document new API endpoints** as soon as they're created
3. **Update architecture diagrams** when adding/modifying files
4. **Add new environment variables** to configuration section
5. **Include new dependencies** in setup instructions

### Documentation workflow:
- Add README updates to todo lists when planning features
- Document endpoints, types, and components concurrently with implementation
- Keep usage examples current with actual functionality
- Update limitations and known issues as they're discovered
- **ALWAYS append changelog entries to changes.txt with human-readable timestamps**

### Changelog Requirements:
- After completing any significant changes, append to `changes.txt`
- Include timestamp in format: "CHANGELOG ENTRY - Day Month Year at Time CET"
- Document what was changed, why, and any important technical details
- Keep entries clear and concise for future reference

This ensures documentation stays accurate and useful for future development sessions.

## Important Development Reminders

### Performance Testing
- **ALWAYS use `make performance-test`** to run the performance test, not the direct binary
- The make target ensures proper environment setup and consistent execution
- Performance test tracks only certification requests submitted in the current run for accurate metrics

### Docker Compose Management
- **Clean rebuild**: `make docker-run-clean`
- This stops containers, removes data, and rebuilds everything fresh

This ensures documentation stays accurate and useful for future development sessions.
