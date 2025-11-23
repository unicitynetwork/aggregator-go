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
- ✅ **Signature Validation** - Full secp256k1 cryptographic validation for certification requests
- ✅ **SMT Integration** - Sparse Merkle Tree for inclusion proofs with TypeScript compatibility
- ✅ **Round Management** - Automated 1-second block creation with batch processing
- ✅ **DataHash Support** - Proper algorithm imprint format for SHA256 hashes
- ✅ **Configurable Concurrency** - Request rate limiting and parallel processing
- ✅ **Graceful Shutdown** - Proper resource cleanup on termination
- ✅ **Health Monitoring** - Comprehensive health and status endpoints
- ✅ **TLS Support** - HTTPS/TLS configuration for production
- ✅ **CORS Support** - Cross-origin resource sharing for web clients
- ✅ **Performance Testing** - Built-in performance test with cryptographically valid data
- 🚧 **Consensus Integration** - Alphabill blockchain submission (planned)

## Quick Start

### Prerequisites

- Go 1.25 or later
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
# Start all services (MongoDB, BFT nodes, and Aggregator)
docker compose up -d

# Start with clean state (removes old data)
make docker-run-clean

# View logs
docker compose logs -f aggregator

# Stop services
docker compose down
```

This will start:
- **MongoDB** on `localhost:27017` with admin credentials
- **BFT Root Node** - Consensus root node  
- **Aggregator** on `localhost:3000` with full BFT integration

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

The service will start on `http://localhost:3000` by default.

> **⚠️ BFT Configuration Required**: For the aggregator to run properly, BFT configuration is required. This includes setting up BFT configuration files and bootstrap node addresses. See [bft-support.md](bft-support.md) for detailed setup instructions.

## Configuration

The service is configured via environment variables:

### Chain Configuration

| Variable        | Description     | Default   |
|-----------------|-----------------|-----------|
| `CHAIN_ID`      | Chain ID        | `unicity` |
| `CHAIN_VERSION` | Chain version   | `1.0`     |
| `CHAIN_FORK_ID` | Chain's Fork ID | `mainnet` |

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
| `LOCK_ID`                          | Unique lock identifier     | `aggregator_leader_lock` |

### Logging Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |
| `LOG_FORMAT` | Log format (json, text) | `json` |
| `LOG_OUTPUT` | Log output (stdout, stderr, file path) | `stdout` |
| `LOG_ENABLE_JSON` | Enable JSON formatted logs | `true` |
| `LOG_ENABLE_ASYNC` | Enable asynchronous logging for better performance | `true` |
| `LOG_ASYNC_BUFFER_SIZE` | Buffer size for async logging | `10000` |

### Processing Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `BATCH_LIMIT` | Maximum number of certification requests to process per batch | `1000` |
| `ROUND_DURATION` | Duration between block creation rounds | `1s` |

## API Endpoints

### JSON-RPC 2.0 Methods

All JSON-RPC requests should be sent as POST to `/` with `Content-Type: application/json`.

#### `certification_request`
Submit a state transition request to the aggregation layer with cryptographic validation.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "certification_request",
  "params": {
    "stateId": "0000981012b1c865f65d3d5523819cb34fa2c6827e792efd4579b4927144eb243122",
    "certificationData": {
      "publicKey": "027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
      "signature": "65ed0261e093aa2df02c0e8fb0aa46144e053ea705ce7053023745b3626c60550b2a5e90eacb93416df116af96872547608a31de1f8ef25dc5a79104e6b69c8d00",
      "sourceStateHash": "0000539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647",
      "transactionHash": "0000c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"
    },
    "receipt": true
  },
  "id": 1
}
```

**Field Specifications:**
- `stateId`: 68-character hex string with "0000" SHA256 algorithm prefix (derived from publicKey + sourceStateHash)
- `certificationData.publicKey`: 66-character hex string (33-byte compressed secp256k1 public key)
- `certificationData.signature`: 130-character hex string (65-byte secp256k1 signature: 64 bytes + 1 recovery byte)
- `certificationData.sourceStateHash`: 68-character hex string with "0000" SHA256 algorithm prefix (DataHash imprint)
- `certificationData.transactionHash`: 68-character hex string with "0000" SHA256 algorithm prefix (DataHash imprint)

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "status": "SUCCESS",
    "receipt": {
      "publicKey": "027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
      "signature": "...",
      "request": {
        "service": "aggregator",
        "method": "certification_request",
        "stateId": "0000981012b1c865f65d3d5523819cb34fa2c6827e792efd4579b4927144eb243122",
        "sourceStateHash": "0000539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647",
        "transactionHash": "0000c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"
      }
    }
  },
  "id": 1
}
```

**Validation Statuses:**
- `SUCCESS` - Commitment accepted and will be included in next block
- `INVALID_PUBLIC_KEY_FORMAT` - Invalid secp256k1 public key
- `INVALID_SIGNATURE_FORMAT` - Invalid signature format or length
- `SIGNATURE_VERIFICATION_FAILED` - Signature doesn't match transaction hash and public key
- `STATE_ID_MISMATCH` - StateID doesn't match SHA256(CBOR[sourceStateHash, publicKey])
- `INVALID_SOURCE_STATE_HASH_FORMAT` - SourceStateHash not in proper DataHash imprint format
- `INVALID_TRANSACTION_HASH_FORMAT` - TransactionHash not in proper DataHash imprint format
- `INVALID_SHARD` - The certification request was sent to the wrong shard

#### `get_inclusion_proof`
Retrieve the Sparse Merkle Tree inclusion proof for a specific state transition request.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_inclusion_proof",
  "params": {
    "stateId": "0000981012b1c865f65d3d5523819cb34fa2c6827e792efd4579b4927144eb243122"
  },
  "id": 2
}
```

**Response:**
```json
{
  "jsonrpc":"2.0",
  "result":{
    "certificationData":{
      "publicKey":"027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
      "signature":"65ed0261e093aa2df02c0e8fb0aa46144e053ea705ce7053023745b3626c60550b2a5e90eacb93416df116af96872547608a31de1f8ef25dc5a79104e6b69c8d00",
      "sourceStateHash":"0000539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647",
      "transactionHash":"0000c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"
    },
    "merkleTreePath":{
      "root":"0000342d44bb4f43b2de5661cf3690254b95b49e46820b90f13fbe2798f428459ba4",
      "steps":[
        {
          "branch":["0000f00a106493f8bee8846b84325fe71411ea01b8a7c5d7cc0853888b1ef9cbf83b"],
          "path":"7588619140208316325429861720569170962648734570557434545963804239233978322458521890",
          "sibling":null
        }
      ]
    }
  },
  "id":2
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
    "blockNumber": "123"
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
      "rootHash": "0000b67ebbbb3a8369f93981b9d8b510a7b8e72fc1e1b8a83b7c0d8a3c9f7e4d",
      "previousBlockHash": "0000a1b2c3d4e5f6789012345678901234567890123456789012345678901234",
      "noDeletionProofHash": "0000c7d8e9f0123456789abcdef0123456789abcdef0123456789abcdef012345",
      "createdAt": "1734435600000",
      "unicityCertificate": "d903ef8701d903f08a01190146005844303030303936613239366432323466323835633637626565393363333066386133303931353766306461613335646335623837653431306237383633306130396366633758443030303039366132393664323234663238356336376265653933633330663861333039313537663064616133356463356238376534313062373836333061303963666337401a68553075f600f65820d4b5491031d8a9365555a01fa4d9805e32a4205c15fa19e53dc7f27ad4c534e058204296135d76b6345cdffaf57b434f6bd5c3579f3843731fab79e1e5a74a6091c982418080d903f683010780d903e9880103190737001a685530a658200b98a86c69c788bc54773d62cfd053ef54cf495bdb9a4b8298ad6c99966de7e058201d2b93c6e36694c316302b9cf9bf3c6ca076b085d6aaeb1d1874cd23301fa3f4a3783531365569753248416d326857486d66794a36484143696476367934686f377655323778504365436f5253515873694443595937654358417661160bc40a6a8722bd025ab49449dec2cee4a4680cc20f9f4fb2e1328c2f2e511a0390678a911b81a26d0171bfc43e813a01da7458c15558abb954bd2a52e501783531365569753248416d3665514d72327351566263575a73505062706332537537416e6e4d5647487043323350557a47544141546e7058410be3d9a494027aaed1d052145f8bd78ec5f909c1eeaa62e4a0aa79de1aef6108483aa8ff9253fb1d1c73407f49f428d246813780ed3648a92efa4c674fb5531401783531365569753248416d424a394c733865333662776b6a4c3574677737327a6b533578346479636a625a665956614e52676e7447317258415bd2c3b0ca0683c39e66129027eee216a66fc35eca1c58b5ba3e5a99dae4e97357893f88f7e91f70a16cccdfc7bfc9fa46757e2e1b1126bd5145af70a39e4bdb00"
    }
  },
  "id": 4
}
```

#### `get_block_records`
Retrieve all certification requests included in a specific block.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_block_records",
  "params": {
    "blockNumber": "123"
  },
  "id": 5
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "aggregatorRecords": [
      {
        "stateId": "0000981012b1c865f65d3d5523819cb34fa2c6827e792efd4579b4927144eb243122",
        "certificationData": {
          "publicKey": "027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
          "signature": "65ed0261e093aa2df02c0e8fb0aa46144e053ea705ce7053023745b3626c60550b2a5e90eacb93416df116af96872547608a31de1f8ef25dc5a79104e6b69c8d00",
          "sourceStateHash": "0000539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647",
          "transactionHash": "0000c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"
        },
        "blockNumber": "123",
        "leafIndex": "0",
        "createdAt": "1734435600000",
        "finalizedAt": "1734435601000"
      }
    ]
  },
  "id": 5
}
```

#### `get_no_deletion_proof`
Retrieve the global no-deletion proof for the aggregator data structure.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_no_deletion_proof",
  "params": {},
  "id": 6
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "noDeletionProof": {
      "proofHash": "0000c7d8e9f0123456789abcdef0123456789abcdef0123456789abcdef012345",
      "blockNumber": "123",
      "timestamp": "1734435600000"
    }
  },
  "id": 6
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
Returns **executable** interactive HTML API documentation page with live testing capabilities (if `ENABLE_DOCS=true`).

The documentation includes:
- **🚀 Live API testing** - Execute requests directly from the browser
- **📋 cURL export** - Copy commands for terminal use
- **⌨️ Keyboard shortcuts** - Ctrl+Enter to send requests
- **🎯 Status indicators** - Response times and success/error status
- **↻ Reset functionality** - Restore original examples
- **📱 Responsive design** - Works on desktop and mobile
- **💾 Real-time responses** - JSON formatted with syntax highlighting

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

### Docker

```bash
# Clean rebuild (stops, removes data, rebuilds)
make docker-run-clean
# This command automatically uses current user's UID/GID on Linux/macOS
```

### Project Structure

```
aggregator-go/
├── cmd/
│   ├── aggregator/         # Main application entry point
│   └── performance-test/   # Built-in performance testing tool
├── internal/               # Private application code
│   ├── config/            # Configuration management
│   ├── gateway/           # HTTP server and JSON-RPC handlers
│   ├── service/           # Business logic layer
│   ├── round/             # Round management and block creation
│   ├── signing/           # Cryptographic validation (secp256k1)
│   ├── smt/               # Sparse Merkle Tree implementation
│   ├── storage/           # Storage interfaces and implementations
│   │   ├── interfaces/    # Storage interface definitions
│   │   └── mongodb/       # MongoDB implementations
│   ├── models/            # Data models and types
│   └── logger/            # Logging utilities
├── pkg/                   # Public/reusable packages
│   └── jsonrpc/          # JSON-RPC server implementation
├── tests/                 # Comprehensive test suites
│   ├── api/              # API compatibility tests
│   ├── benchmarks/       # Performance benchmarks
│   └── integration/      # Integration tests
└── aggregator-ts/        # TypeScript reference implementation
```

### Database Collections

The service creates and manages the following MongoDB collections:

- **`commitments`** - Temporary storage for pending commitments
- **`aggregator_records`** - Finalized certification request records with proofs
- **`blocks`** - Blockchain blocks with metadata
- **`smt_nodes`** - Sparse Merkle Tree leaf nodes
- **`block_records`** - Block number to state ID mappings
- **`leadership`** - High availability leader election state

All collections include proper indexes for efficient querying.

## Performance Testing

The service includes a built-in performance testing tool that generates cryptographically valid commitments:

```bash
# Run performance test (requires aggregator running on localhost:3000)
make performance-test

# Run performance test against a remote endpoint with authentication
make performance-test-auth URL=http://localhost:8080 AUTH='Bearer supersecret'
```

**Performance Test Features:**
- ✅ **Cryptographically Valid Data** - Real secp256k1 key pairs and signatures
- ✅ **Proper DataHash Format** - Correct algorithm imprints with "0000" SHA256 prefix
- ✅ **Deterministic StateIDs** - Calculated as SHA256(publicKey || sourceStateHash)
- ✅ **High Concurrency** - Configurable worker count and request rate
- ✅ **Block Monitoring** - Tracks certification requests per block and throughput
- ✅ **Real-time Metrics** - Success rate, failure rate, and RPS tracking

**Sample Output:**
```
Starting aggregator performance test...
Target: http://localhost:3000
Duration: 10s
Workers: 100
Target RPS: 5000
----------------------------------------
✓ Connected successfully
Starting block monitoring from block 1
[2s] Total: 9847, Success: 9832, Failed: 15, Exists: 0, RPS: 4923.5
Block 1: 1456 commitments
[4s] Total: 19736, Success: 19684, Failed: 52, Exists: 0, RPS: 4934.0
Block 2: 1523 commitments
...
```

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

## Sharding

To support horizontal scaling, the aggregators can be run in a sharded configuration consisting of one parent aggregator 
and multiple child aggregators. In this mode, the global Sparse Merkle Tree (SMT) is split across the child nodes, and 
agents must submit their certification requests to the correct child node.

For a more detailed technical explanation of the sharded SMT structure, please refer to the official specification:
[https://github.com/unicitynetwork/specs/blob/main/smt.md](https://github.com/unicitynetwork/specs/blob/main/smt.md)

### Certification Request Routing

The requests are assigned to a shard based on the least significant bits of their state identifier. 
The number of bits used to determine the shard is defined by the `SHARD_ID_LENGTH` configuration.

For example `SHARD_ID_LENGTH: 1` means that the rightmost `1` bits of state identifier determines 
the correct shard. In this case there would be 2 shards e.g. certification requests ending with bit `0` would go to 
`shard-1`, and certification requests ending with bit `1` would go to the `shard-2`.

In sharded setup only the parent aggregator talks to the BFT node.

### Shard ID Encoding

The `shardID` is a unique identifier for each shard that includes a `1` as its most significant bit (MSB). This prefix 
bit ensures that the leading zeros are preserved for bit manipulations.

Examples
- For `SHARD_ID_LENGTH: 1` the valid `shardID`s are `0b10` (2) and `0b11` (3), for a total of two shards.
- For `SHARD_ID_LENGTH: 2` the valid `shardID`s are `0b100` (4), `0b101` (5), `0b110` (6) and `0b111` (7), for a total of four shards.

A child aggregator validates incoming certification requests to ensure they belong to its shard. If a certification 
request is sent to a wrong shard, the aggregator will reject it.

### Example Sharded Setup

The following diagram illustrates a sharded setup with one parent and two child aggregators for `SHARD_ID_LENGTH: 1`.

```text
          +--------------------+
          | Parent Aggregator  |
          | (2-leaf SMT)       |
          +--------------------+
           /                  \
          /                    \
+----------------+     +----------------+
| Child Agg. #1  |     | Child Agg. #2  |
| ShardID = 0b10 |     | ShardID = 0b11 |
| (handles *...0)|     | (handles *...1)|
+----------------+     +----------------+
        ^                      ^
        |                      |
+----------------+     +----------------+
| Agent sends    |     | Agent sends    |
| certification request     |     | certification request     |
| ID = ...xxx0   |     | ID = ...xxx1   |
+----------------+     +----------------+
```

### Configuration

The sharded setup is configured via environment variables, as seen in `sharding-compose.yml`.

A **parent** aggregator is configured with:
```yaml
environment:
  SHARDING_MODE: "parent"
  SHARD_ID_LENGTH: 1
```

A **child** aggregator is configured with its unique `shardID` and the address of the parent, for example:

Shard-1:
```yaml
environment:
  SHARDING_MODE: "child"
  SHARDING_CHILD_SHARD_ID: 2 # (binary 0b10)
  SHARDING_CHILD_PARENT_RPC_ADDR: http://aggregator-root:3000
```

Shard-2:
```yaml
environment:
  SHARDING_MODE: "child"
  SHARDING_CHILD_SHARD_ID: 3 # (binary 0b11)
  SHARDING_CHILD_PARENT_RPC_ADDR: http://aggregator-root:3000
```

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
- **Request Correlation**: Efficient logging with state IDs
- **Graceful Shutdown**: Proper cleanup on termination
- **Batch Operations**: Efficient database operations (when available)
- **Asynchronous Logging**: Non-blocking log writes with configurable buffer size (enabled by default)
  - Batched log processing reduces I/O overhead
  - 10ms flush interval ensures timely log delivery
  - Graceful shutdown flushes all pending logs

## Cryptographic Implementation

### Signature Validation
The service implements complete secp256k1 signature validation:

- **✅ Public Key Validation** - Compressed 33-byte secp256k1 public keys
- **✅ Signature Verification** - 65-byte signatures (64 bytes + recovery byte)
- **✅ StateID Validation** - Deterministic calculation: SHA256(publicKey || sourceStateHash)
- **✅ DataHash Support** - Algorithm imprint format with "0000" SHA256 prefix
- **✅ Transaction Signing** - Signatures verified against transaction hash data

### Supported Algorithms
- **secp256k1** - Full implementation with btcec library
- **SHA256** - Hash algorithm for all cryptographic operations
- **DataHash Imprints** - Unicity-compatible format with algorithm prefix

### Validation Process
1. **Algorithm Check** - Verify "secp256k1" algorithm support
2. **Public Key Format** - Validate compressed secp256k1 public key (33 bytes)
3. **State Hash Format** - Validate DataHash imprint with "0000" SHA256 prefix
4. **StateID Verification** - Ensure StateID = SHA256(publicKey || sourceStateHash)
5. **Signature Format** - Validate 65-byte signature length
6. **Transaction Hash Format** - Validate DataHash imprint format
7. **Signature Verification** - Cryptographically verify signature against transaction hash

## Architecture Notes

### Round Management
- **1-second rounds** - Automated block creation every second
- **Batch processing** - Multiple certification requests per block
- **Leader-only block creation** - High availability with single leader
- **Graceful shutdown** - Proper cleanup of pending rounds

### SMT Integration
- **TypeScript compatibility** - Exact hash matching with reference implementation
- **Production performance** - 90,000+ leaves/second processing capability
- **Memory efficient** - Optimized for large-scale operations
- **Concurrent safe** - Thread-safe operations with proper locking

## Limitations

- **Consensus Integration**: No Alphabill submission yet (implementation pending)
- **Receipt Signing**: Returns unsigned receipts (cryptographic signing planned)

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
