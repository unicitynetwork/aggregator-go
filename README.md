# Unicity Aggregator - Go Implementation

This is a high-performance Go rewrite of the TypeScript aggregator service for the Unicity blockchain platform. The service provides JSON-RPC 2.0 API endpoints for state transition aggregation with MongoDB persistence and high availability support.

## Overview

The Unicity Aggregator implements a decentralized Agent-Aggregator communication layer that enables:

1. **State Transition Submission**: Agents submit state transition requests to the aggregation layer
2. **Proof Generation**: Retrieval of timestamped inclusion proofs and global non-deletion proofs
3. **Block Management**: Blockchain block creation and querying capabilities
4. **High Availability**: Distributed processing with automatic leader election

## Features

- ✅ **JSON-RPC 2.0 API** - Complete implementation of all aggregator methods
- ✅ **MongoDB Integration** - Efficient storage with proper indexing
- ✅ **High Availability** - Leader election and distributed processing
- ✅ **Configurable Concurrency** - Request rate limiting and parallel processing
- ✅ **Graceful Shutdown** - Proper resource cleanup on termination
- ✅ **Health Monitoring** - Comprehensive health and status endpoints
- ✅ **TLS Support** - HTTPS/TLS configuration for production
- ✅ **CORS Support** - Cross-origin resource sharing for web clients
- 🚧 **SMT Integration** - Sparse Merkle Tree for inclusion proofs (planned)
- 🚧 **Round Management** - Automated block creation every second (planned)
- 🚧 **Consensus Integration** - Alphabill blockchain submission (planned)

## Quick Start

### Prerequisites

- Go 1.24 or later
- MongoDB 4.4 or later
- Make (optional, for using Makefile)

### Installation

```bash
# Clone the repository
git clone https://github.com/unicitynetwork/aggregator-go.git
cd aggregator-go

# Install dependencies
go mod download

# Build the application
make build
# OR
go build -o bin/aggregator ./cmd/aggregator
```

### Quick Start with Docker

The easiest way to get started is using Docker Compose:

```bash
# Start both MongoDB and Aggregator services
make docker-up

# View logs
make docker-logs

# Stop services
make docker-down
```

This will start:
- **MongoDB** on `localhost:27017` with admin credentials
- **Aggregator** on `localhost:3333` with full functionality

### Basic Usage (Local Development)

```bash
# Start MongoDB (if not using Docker)
mongod --dbpath /your/db/path

# Run with default configuration
./bin/aggregator

# Run with custom configuration
export MONGODB_URI="mongodb://localhost:27017"
export PORT="8080"
export LOG_LEVEL="debug"
./bin/aggregator
```

The service will start on `http://localhost:3000` by default (or `localhost:3333` with Docker).

## Configuration

The service is configured via environment variables:

### Server Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | HTTP server port | `3000` |
| `HOST` | HTTP server host | `0.0.0.0` |
| `READ_TIMEOUT` | HTTP read timeout | `30s` |
| `WRITE_TIMEOUT` | HTTP write timeout | `30s` |
| `IDLE_TIMEOUT` | HTTP idle timeout | `120s` |
| `CONCURRENCY_LIMIT` | Max concurrent requests | `1000` |
| `ENABLE_DOCS` | Enable /docs endpoint | `true` |
| `ENABLE_CORS` | Enable CORS headers | `true` |
| `ENABLE_TLS` | Enable HTTPS/TLS | `false` |
| `TLS_CERT_FILE` | TLS certificate file path | `` |
| `TLS_KEY_FILE` | TLS private key file path | `` |

### Database Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `MONGODB_DATABASE` | Database name | `aggregator` |
| `MONGODB_CONNECT_TIMEOUT` | Connection timeout | `10s` |
| `MONGODB_SERVER_SELECTION_TIMEOUT` | Server selection timeout | `5s` |
| `MONGODB_SOCKET_TIMEOUT` | Socket timeout | `30s` |
| `MONGODB_MAX_POOL_SIZE` | Maximum connection pool size | `100` |
| `MONGODB_MIN_POOL_SIZE` | Minimum connection pool size | `5` |
| `MONGODB_MAX_CONN_IDLE_TIME` | Max connection idle time | `5m` |

### High Availability Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `DISABLE_HIGH_AVAILABILITY` | Disable HA mode | `false` |
| `LOCK_TTL_SECONDS` | Leadership lock TTL | `30` |
| `LEADER_HEARTBEAT_INTERVAL` | Leader heartbeat frequency | `10s` |
| `LEADER_ELECTION_POLLING_INTERVAL` | Follower polling frequency | `5s` |
| `SERVER_ID` | Unique server identifier | `{hostname}-{pid}` |

### Logging Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |
| `LOG_FORMAT` | Log format (json, text) | `json` |
| `LOG_OUTPUT` | Log output (stdout, stderr, file path) | `stdout` |
| `LOG_ENABLE_JSON` | Enable JSON formatted logs | `true` |

## API Endpoints

### JSON-RPC 2.0 Methods

All JSON-RPC requests should be sent as POST to `/` with `Content-Type: application/json`.

#### `submit_commitment`
Submit a state transition request to the aggregation layer.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "submit_commitment",
  "params": {
    "requestId": "64-character-hex-string",
    "transactionHash": "64-character-hex-string",
    "authenticator": {
      "stateHash": "hex-string",
      "publicKey": "hex-string",
      "signature": "hex-string",
      "signAlg": "ed25519",
      "hashAlg": "SHA256"
    },
    "receipt": false
  },
  "id": 1
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "status": "success",
    "receipt": null
  },
  "id": 1
}
```

#### `get_inclusion_proof`
Retrieve the inclusion proof for a specific state transition request.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_inclusion_proof",
  "params": {
    "requestId": "64-character-hex-string"
  },
  "id": 2
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "inclusionProof": {
      "requestId": "64-character-hex-string",
      "blockNumber": "123",
      "leafIndex": "45",
      "proof": [],
      "rootHash": "hex-string",
      "included": true
    }
  },
  "id": 2
}
```

#### `get_block_height`
Retrieve the current blockchain height.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_block_height",
  "params": {},
  "id": 3
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "blockNumber": "123"
  },
  "id": 3
}
```

#### `get_block`
Retrieve detailed information about a specific block.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_block",
  "params": {
    "blockNumber": 123
  },
  "id": 4
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "block": {
      "index": "123",
      "chainId": "unicity",
      "version": "1.0.0",
      "forkId": "main",
      "timestamp": "1640995200000",
      "rootHash": "hex-string",
      "previousBlockHash": "hex-string",
      "noDeletionProofHash": null
    }
  },
  "id": 4
}
```

#### `get_block_commitments`
Retrieve all commitments included in a specific block.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_block_commitments",
  "params": {
    "blockNumber": 123
  },
  "id": 5
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "commitments": [
      {
        "requestId": "64-character-hex-string",
        "transactionHash": "64-character-hex-string",
        "authenticator": { ... },
        "blockNumber": "123",
        "leafIndex": "0"
      }
    ]
  },
  "id": 5
}
```

### HTTP Endpoints

#### `GET /health`
Returns the health status and role of the service.

**Response:**
```json
{
  "status": "ok",
  "role": "leader",
  "serverId": "hostname-1234",
  "details": {
    "database": "connected",
    "commitment_queue": "42"
  }
}
```

#### `GET /docs`
Returns HTML API documentation page (if `ENABLE_DOCS=true`).

## Development

### Building

```bash
# Build binary
make build

# Build and run
make run

# Run tests
make test

# Run with race detection
make test-race

# Format code
make fmt

# Lint code (requires golangci-lint)
make lint

# Clean build artifacts
make clean
```

### Docker Development

```bash
# Build Docker image
make docker-build

# Start services (MongoDB + Aggregator)
make docker-up

# View service logs
make docker-logs

# Restart services
make docker-restart

# Rebuild and restart services
make docker-rebuild

# Stop services
make docker-down

# Clean up Docker resources
make docker-clean
```

### Project Structure

```
aggregator-go/
├── cmd/aggregator/          # Main application entry point
├── internal/                # Private application code
│   ├── config/             # Configuration management
│   ├── gateway/            # HTTP server and JSON-RPC handlers
│   ├── service/            # Business logic layer
│   ├── storage/            # Storage interfaces and implementations
│   │   ├── interfaces/     # Storage interface definitions
│   │   └── mongodb/        # MongoDB implementations
│   ├── models/             # Data models and types
│   └── logger/             # Logging utilities
├── pkg/                    # Public/reusable packages
│   └── jsonrpc/           # JSON-RPC server implementation
├── tests/                  # Test files (planned)
├── deployments/            # Deployment configurations (planned)
└── scripts/               # Build and utility scripts (planned)
```

### Database Collections

The service creates and manages the following MongoDB collections:

- **`commitments`** - Temporary storage for pending commitments
- **`aggregator_records`** - Finalized commitment records with proofs
- **`blocks`** - Blockchain blocks with metadata
- **`smt_nodes`** - Sparse Merkle Tree leaf nodes
- **`block_records`** - Block number to request ID mappings
- **`leadership`** - High availability leader election state

All collections include proper indexes for efficient querying.

## High Availability

The service implements a MongoDB-based leader election system:

- **Distributed Processing**: All servers handle API requests
- **Leader Election**: Only one server creates blocks and manages consensus
- **Automatic Failover**: Leader election with configurable TTL
- **Health Monitoring**: `/health` endpoint reports current role

### Leadership Roles

- **`leader`** - Actively creating blocks and managing consensus
- **`follower`** - Processing API requests, monitoring for leadership
- **`standalone`** - Single server mode (HA disabled)

## Error Handling

The service implements comprehensive JSON-RPC 2.0 error codes:

| Code | Description |
|------|-------------|
| `-32700` | Parse error |
| `-32600` | Invalid request |
| `-32601` | Method not found |
| `-32602` | Invalid params |
| `-32603` | Internal error |
| `-32000` | Validation error |
| `-32001` | Commitment already exists |
| `-32002` | Commitment not found |
| `-32003` | Block not found |
| `-32004` | Database error |
| `-32005` | Consensus error |
| `-32006` | Concurrency limit exceeded |

## Performance Considerations

- **Connection Pooling**: Configurable MongoDB connection pool
- **Concurrency Limiting**: Prevents resource exhaustion
- **Request Correlation**: Efficient logging with request IDs
- **Graceful Shutdown**: Proper cleanup on termination
- **Batch Operations**: Efficient database operations (when available)

## Limitations

- **SMT Integration**: Inclusion proofs return mock data (implementation pending)
- **Round Management**: No automated block creation yet (implementation pending)
- **Consensus Integration**: No Alphabill submission yet (implementation pending)
- **Receipt Signing**: Returns mock receipts (implementation pending)

## Migration from TypeScript

This Go implementation maintains API compatibility with the TypeScript version while providing:

- **Better Performance**: Native compiled binary with efficient concurrency
- **Lower Memory Usage**: More efficient memory management
- **Type Safety**: Compile-time type checking
- **Easier Deployment**: Single binary deployment

## Contributing

1. Follow Go best practices and conventions
2. Write tests for new functionality
3. Update documentation concurrently with code changes
4. Use the provided Makefile for builds and testing
5. Ensure backward compatibility with TypeScript API

## License

[License information to be added based on project requirements]

## Support

For issues and questions:
- Create GitHub issues for bugs and feature requests
- Check the `/health` endpoint for service status
- Review logs for detailed error information