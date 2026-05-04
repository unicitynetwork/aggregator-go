# Unicity Aggregator

High-performance aggregator service for the Unicity blockchain platform. Provides JSON-RPC 2.0 API endpoints for state transition aggregation with MongoDB persistence, BFT consensus integration, and high availability support.

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
- ✅ **Signature Validation** - Full secp256k1 cryptographic validation for commitments
- ✅ **SMT Integration** - Sparse Merkle Tree for inclusion proofs
- ✅ **Round Management** - Automated block creation with batch processing
- ✅ **Raw v2 Hash Support** - 32-byte SHA256 state and transaction hashes
- ✅ **Configurable Concurrency** - Request rate limiting and parallel processing
- ✅ **Graceful Shutdown** - Proper resource cleanup on termination
- ✅ **Health Monitoring** - Comprehensive health and status endpoints
- ✅ **TLS Support** - HTTPS/TLS configuration for production
- ✅ **CORS Support** - Cross-origin resource sharing for web clients
- ✅ **Performance Testing** - Built-in performance test with cryptographically valid data
- ✅ **BFT Consensus** - Integration with Unicity BFT network for block finalization

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
| `BATCH_LIMIT` | Maximum number of commitments to process per batch | `1000` |
| `ROUND_DURATION` | Duration between block creation rounds | `1s` |

### Storage Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `USE_REDIS_FOR_COMMITMENTS` | Use Redis for commitment queue (instead of MongoDB) | `false` |
| `REDIS_HOST` | Redis server hostname (single-endpoint mode) | `localhost` |
| `REDIS_PORT` | Redis server port (single-endpoint mode) | `6379` |
| `REDIS_PASSWORD` | Redis server password (data nodes) | `` |
| `REDIS_DB` | Redis database number | `0` |
| `REDIS_STREAM_NAME` | Redis stream name for commitments (allows multiple shards to share a Redis instance) | `commitments` |
| `REDIS_FLUSH_INTERVAL` | Interval for flushing pending commitments to Redis | `50ms` |
| `REDIS_MAX_BATCH_SIZE` | Maximum batch size before forcing flush | `2000` |

#### Redis Sentinel (HA)

Set `REDIS_SENTINEL_ADDRS` to switch the client to Sentinel-backed failover. When set, `REDIS_HOST`/`REDIS_PORT` are ignored and `REDIS_MASTER_NAME` is required.

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_SENTINEL_ADDRS` | Comma-separated `host:port` list of Sentinel nodes. Empty = single-endpoint mode. | `` |
| `REDIS_MASTER_NAME` | Sentinel master name to track. Required when `REDIS_SENTINEL_ADDRS` is set. | `` |
| `REDIS_SENTINEL_PASSWORD` | Password for authenticating to Sentinel nodes. | `` |
| `REDIS_SENTINEL_USERNAME` | ACL username for authenticating to Sentinel nodes. | `` |
| `REDIS_ROUTE_BY_LATENCY` | Route read-only commands to the lowest-latency node. | `false` |
| `REDIS_ROUTE_RANDOMLY` | Route read-only commands to a random master/replica. | `false` |
| `REDIS_REPLICA_ONLY` | Route all commands to replica read-only nodes. | `false` |

Example:

```bash
USE_REDIS_FOR_COMMITMENTS=true \
REDIS_SENTINEL_ADDRS=sentinel-1:26379,sentinel-2:26379,sentinel-3:26379 \
REDIS_MASTER_NAME=mymaster \
REDIS_PASSWORD=secret \
make run
```

### BFT Configuration

| Variable                            | Description                                                                         | Default                          |
|-------------------------------------|-------------------------------------------------------------------------------------|----------------------------------|
| `BFT_ENABLED`                       | Enables or disables the BFT client integration.                                     | `true`                           |
| `BFT_ADDRESS`                       | The libp2p multiaddress for the BFT client to listen on.                            | `/ip4/0.0.0.0/tcp/9000`          |
| `BFT_RPC_ADDRESS`                   | The BFT node's RPC API address.                                                     | `http://127.0.0.1:8002`          |
| `BFT_ANNOUNCE_ADDRESSES`            | Comma-separated list of public callback multi-addresses to announce to other peers. | `""`                             |
| `BFT_BOOTSTRAP_ADDRESSES`           | Comma-separated list of bootstrap peer addresses.                                   | `""`                             |
| `BFT_BOOTSTRAP_CONNECT_RETRY`       | Number of retries for connecting to bootstrap peers.                                | `3`                              |
| `BFT_BOOTSTRAP_CONNECT_RETRY_DELAY` | Delay between bootstrap connection retries (in seconds).                            | `5`                              |
| `BFT_HEARTBEAT_INTERVAL`            | How often the BFT client checks for inactivity.                                     | `1s`                             |
| `BFT_INACTIVITY_TIMEOUT`            | Duration of inactivity before the BFT client sends a new handshake.                 | `5s`                             |
| `BFT_KEY_CONF_FILE`                 | Path to the BFT key configuration file.                                             | `bft-config/keys.json`           |
| `BFT_SHARD_CONF_FILE`               | Path to the aggregator shard configuration file.                                    | `bft-config/shard-conf-7_0.json` |
| `BFT_TRUST_BASE_FILES`              | Comma-separated list of paths to trust base files.                                  | `bft-config/trust-base.json`     |

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
  "params": "<hex-encoded CertificationRequest CBOR>",
  "id": 1
}
```

Params is a hex-encoded `api.CertificationRequest` CBOR payload. The payload contains raw 32-byte values for `stateId`, `sourceStateHash`, and `transactionHash`; these fields do not include algorithm prefixes.

Which corresponds to Go data structures:
```
// CertificationRequest represents the certification_request JSON-RPC request,
// sometimes also referred to as StateTransitionCertificationRequest, Commitment or UnicityServiceRequest.
type CertificationRequest struct {
	_ struct{} `cbor:",toarray"`
	Version types.Version

	// StateID is the unique identifier of the certification request, used as a key in the state tree.
	// In v2 it is the raw 32-byte hash of the CBOR array
	// [CertificationData.OwnerPredicate, CertificationData.SourceStateHash].
	StateID StateID

	// CertificationData contains the necessary cryptographic data needed for the CertificationRequest.
	CertificationData CertificationData

	AggregateRequestCount uint64
}

// CertificationData represents the necessary cryptographic data needed for a state transition CertificationRequest.
type CertificationData struct {
	_ struct{} `cbor:",toarray"`
	Version types.Version

	// OwnerPredicate is the owner predicate in format: CBOR[engine: uint, code: byte[], params: byte[]].
	//
	// In case of standard PayToPublicKey predicate the values must be:
	//  - engine = 01 (plain CBOR uint value of 1)
	//  - code = 4101 (byte array of length 1 containing the CBOR encoding of uint value 1)
	//  - params = 5821 000102..20 (byte array of length 33 containing the raw bytes of the public key value)
	OwnerPredicate Predicate `json:"ownerPredicate"`

	// SourceStateHash is the raw 32-byte hash of the source data.
	SourceStateHash SourceStateHash `json:"sourceStateHash"`

	// TransactionHash is the raw 32-byte hash of the transaction data.
	TransactionHash TransactionHash `json:"transactionHash"`

	// Witness is the "unlocking part" of owner predicate. In case of PayToPublicKey owner predicate the witness must be
	// a signature created on the hash of CBOR array[SourceStateHash, TransactionHash],
	// in Unicity's [R || S || V] format (65 bytes).
	Witness HexBytes `json:"witness"`
}
```

**Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "status": "SUCCESS"
  },
  "id": 1
}
```

**Validation Statuses:**
- `SUCCESS` - Certification request accepted and will be included in next block
- `INVALID_PUBLIC_KEY_FORMAT` - Invalid secp256k1 public key
- `INVALID_SIGNATURE_FORMAT` - Invalid signature format or length
- `SIGNATURE_VERIFICATION_FAILED` - Signature doesn't match transaction hash and public key
- `STATE_ID_MISMATCH` - StateID doesn't match SHA256(CBOR[publicKey, sourceStateHash])
- `INVALID_SOURCE_STATE_HASH_FORMAT` - SourceStateHash is not exactly 32 bytes
- `INVALID_TRANSACTION_HASH_FORMAT` - TransactionHash is not exactly 32 bytes
- `INVALID_SHARD` - The certification request was sent to the wrong shard

#### `get_inclusion_proof.v2`
Retrieve the v2 inclusion proof for a submitted certification request.

The `stateId` must be exactly 64 hex characters (32 raw bytes).

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "get_inclusion_proof.v2",
  "params": {
    "stateId": "c7aa6962316c0eeb1469dc3d7793e39e140c005e6eea0e188dcc73035d765937"
  },
  "id": 2
}
```

**Response:**

```json
{
  "jsonrpc": "2.0",
  "result": "<hex-encoded CBOR>",
  "id": 2
}
```

The `result` field is a hex-encoded CBOR array:
```
[blockNumber, [certificationData, certificateBytes, unicityCertificate]]
```

- `certificationData` is the certification data for inclusion proofs, or `null` for non-inclusion proofs.
- For inclusion proofs, `certificateBytes` is the binary inclusion certificate: `bitmap[32] || sibling_1[32] || ... || sibling_n[32]`, where `n = popcount(bitmap)`. Siblings are in root-to-leaf order. For non-inclusion proofs, `certificateBytes` is an exclusion certificate: `k_l[32] || h_l[32] || bitmap[32] || siblings...` (exclusion proof generation is not yet implemented).
- The expected SMT root is always taken from `UC.IR.h` (input record hash of the Unicity Certificate). No root field appears in the certificate itself.

**Hash rules (Yellowpaper-aligned):**
- Leaf: `H(0x00 || key || value)` where value is the raw transaction hash bytes
- Inner node (two children): `H(0x01 || depth_byte || left || right)`
- Inner node (one child): passthrough (child hash unchanged)

**Key encoding:** 32 bytes, LSB-first bit addressing. `bit(key, d) = (key[d/8] >> (d%8)) & 1`.

**Verification pseudocode:**
```
h = H(0x00 || key || value)
j = len(siblings)
for d in 255..=0:
    if bitmap bit d is not set: continue
    j -= 1
    if bit(key, d) == 1:
        h = H(0x01 || d || siblings[j] || h)
    else:
        h = H(0x01 || d || h || siblings[j])
assert j == 0 and h == UC.IR.h
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
        "stateId": "c7aa6962316c0eeb1469dc3d7793e39e140c005e6eea0e188dcc73035d765937",
        "certificationData": {
          "publicKey": "027c4fdf89e8138b360397a7285ca99b863499d26f3c1652251fcf680f4d64882c",
          "signature": "65ed0261e093aa2df02c0e8fb0aa46144e053ea705ce7053023745b3626c60550b2a5e90eacb93416df116af96872547608a31de1f8ef25dc5a79104e6b69c8d00",
          "sourceStateHash": "539cb40d7450fa842ac13f4ea50a17e56c5b1ee544257d46b6ec8bb48a63e647",
          "transactionHash": "c5f9a1f02e6475c599449250bb741b49bd8858afe8a42059ac1522bff47c6297"
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

#### `PUT /api/v1/trustbases`
Adds trust base to the trust base store. The request body must be a valid trust base in json format.

Example curl request
```curl -X PUT -H 'Content-Type: application/json' -d @./test-nodes/trust-base-1.json http://localhost:3000/api/v1/trustbases```

**If trust base was stored successfully then status 200 with empty response body is returned:**
```json
{}
```

**If trust base is invalid error then status 400 with error cause is returned:**
```json
{
  "error":"failed to store trust base: trust base already exists"
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
└── pkg/                   # Public/reusable packages
    ├── api/              # Public API types
    └── jsonrpc/          # JSON-RPC server implementation
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

# Sharded performance test (provide shard targets with shardID suffix)
SHARD_TARGETS="http://localhost:3001:3,http://localhost:3002:2" TEST_DURATION=10s REQUESTS_PER_SEC=100 go run ./cmd/performance-test
```

**Performance Test Features:**
- ✅ **Cryptographically Valid Data** - Real secp256k1 key pairs and signatures
- ✅ **Raw v2 Hash Format** - 32-byte SHA256 state and transaction hashes
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

The aggregator supports two orthogonal sharding strategies, selected by `SHARDING_MODE`:

- **Application-level sharding** (`parent` / `child`) — aggregator-layer split: one parent aggregator aggregates the SMTs of multiple children. Described in the rest of this section.
- **BFT-side sharding** (`bft-shard`) — BFT-layer split: multiple aggregators act as shard validators of a single multi-shard BFT partition, and shard-inclusion is proved directly by the `ShardTreeCertificate` embedded in the `UnicityCertificate`. Described in [BFT-side sharding](#bft-side-sharding-sharding_modebft-shard).

The two modes use different routing inputs and different shard-ID semantics; they are not interchangeable.

### Application-level sharding (`SHARDING_MODE=parent` / `child`)

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
| commitment     |     | commitment     |
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

### BFT-side sharding (`SHARDING_MODE=bft-shard`)

In `bft-shard` mode, multiple aggregators are deployed as shard validators of a single multi-shard BFT partition. Each aggregator owns one shard, talks directly to the BFT rootchain, and the `UnicityCertificate` it receives embeds a `ShardTreeCertificate` that binds its local SMT root into the partition root. There is no parent aggregator — shard-inclusion is proved by the embedded certificate rather than by a per-round polling loop.

This mode is orthogonal to `parent`/`child`: it uses a different routing key (raw 32-byte `stateId`), a different shard-ID encoding (bit-strings, MSB-first), and a different admission rule.

#### Routing semantics

- Routing key: the raw 32-byte `stateId`.
- Shard identifiers are **bit-strings**, e.g. `"0"`, `"1"`, `"101"`. The first N bits of `stateId` (bit 7 of byte 0 first, descending to bit 0 of byte 31) must equal the shard's bit-string. This matches `types.ShardID.Comparator()` in `bft-go-base`.
- A request whose `stateId` does not match the local shard's prefix is rejected with `INVALID_SHARD`.
- Shard prefixes must form a prefix-free covering set: every possible `stateId` maps to exactly one shard.

#### Example: 1-bit split

```text
                        +---------------------+
                        |  BFT rootchain      |
                        |  (partition id 7)   |
                        +---------------------+
                         /                   \
                        /                     \
+---------------------------+    +---------------------------+
| aggregator-bft-shard0     |    | aggregator-bft-shard1     |
| SHARDING_MODE=bft-shard   |    | SHARDING_MODE=bft-shard   |
| shard-id = 0x40 ("0")     |    | shard-id = 0xC0 ("1")     |
| accepts stateId with MSB=0|    | accepts stateId with MSB=1|
| listens on :3001          |    | listens on :3002          |
+---------------------------+    +---------------------------+
```

Note on shard-conf hex values: `0x40` encodes the bit-string `"0"` under the trailing-`1` end-marker convention in `bft-go-base/types/bitstring.go`; `0x80` is the length-0 empty shard. The two children of the empty-shard root split are `0x40` and `0xC0`.

#### Configuration

A bft-shard aggregator is configured with:

```yaml
environment:
  SHARDING_MODE: "bft-shard"
  BFT_ENABLED: "true"
  BFT_SHARD_CONF_FILE: "/app/bft-config/shard_0/shard-conf-7_0.json"
  BFT_TRUST_BASE_FILES: "/app/bft-config/trust-base.json"
  BFT_RPC_ADDRESS: "http://bft-root:8002"
  # plus the standard BFT_ADDRESS / BFT_BOOTSTRAP_ADDRESSES / BFT_ANNOUNCE_ADDRESSES
```

Shard identity (partition ID, shard ID, genesis epoch) is loaded from `BFT_SHARD_CONF_FILE`, produced by `ubft shard-conf generate ... --shard-id 0x40 ...`. All shard confs in the partition must be pre-provisioned to the rootchain; the aggregator does not register dynamically.

A `bft-shard` config must have a non-empty shard ID (length ≥ 1 bit). For a single-shard deployment, use `SHARDING_MODE=standalone`.

#### Quickstart (local Docker)

```bash
# Spin up 1 BFT rootchain + 2 shard aggregators + MongoDB + Redis.
make docker-run-bft-sh-clean

# Tail shard logs:
docker logs -f aggregator-bft-shard0   # serves on :3001 (shard "0")
docker logs -f aggregator-bft-shard1   # serves on :3002 (shard "1")

# Teardown:
docker compose -f bft-sharding-compose.yml down
```

Other make targets:

- `make docker-run-bft-sh-clean-keep-tb` — preserves BFT genesis/trust-base across restarts; reinitializes MongoDB/Redis only.
- `make docker-restart-bft-sh` — rebuilds and restarts only the aggregators, leaving BFT nodes running.

#### Driving test traffic

`cmd/commitment` submits individual v2 certification requests to a chosen endpoint. `cmd/performance-test` with `SHARDING_MODE=bft-shard` and `SHARD_TARGETS` of the form `http://localhost:3001:0,http://localhost:3002:1` drives both shards in parallel and generates stateIds whose leading bits match each shard's prefix. Cross-shard traffic is rejected with `INVALID_SHARD`.

#### Client-side proof verification

Returned inclusion proofs should be verified on the client side. The Go API exposes the strict verifier as `pkg/api.InclusionProofV2.Verify(req, vctx)`, which checks the proof against the trust base and expected shard/partition context.

#### Observability

- `/health` reports the bft-shard identity via `bftShardId` (MSB-first bit-string; empty in non-bft-shard modes). The integer `shardIdLen` / `shardId` fields are zero in bft-shard mode.
- Wrong-shard submissions return `INVALID_SHARD` at admission; the rejecting aggregator's log records the mismatch.

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
- **Asynchronous Logging**: Non-blocking log writes with configurable buffer size (enabled by default)
  - Batched log processing reduces I/O overhead
  - 10ms flush interval ensures timely log delivery
  - Graceful shutdown flushes all pending logs

## Cryptographic Implementation

### Signature Validation
The service implements complete secp256k1 signature validation:

- **✅ Public Key Validation** - Compressed 33-byte secp256k1 public keys
- **✅ Signature Verification** - 65-byte signatures (64 bytes + recovery byte)
- **✅ StateID Validation** - Deterministic calculation over owner predicate and source state hash
- **✅ Raw Hash Support** - 32-byte source state and transaction hashes
- **✅ Transaction Signing** - Signatures verified against transaction hash data

### Supported Algorithms
- **secp256k1** - Full implementation with btcec library
- **SHA256** - Hash algorithm for all cryptographic operations
- **Raw v2 Hashes** - 32-byte SHA256 hashes without per-field algorithm prefixes

### Validation Process
1. **Algorithm Check** - Verify "secp256k1" algorithm support
2. **Public Key Format** - Validate compressed secp256k1 public key (33 bytes)
3. **State Hash Format** - Validate raw 32-byte source state hash
4. **StateID Verification** - Ensure StateID matches the owner predicate and source state hash
5. **Signature Format** - Validate 65-byte signature length
6. **Transaction Hash Format** - Validate raw 32-byte transaction hash
7. **Signature Verification** - Cryptographically verify signature against transaction hash

## Architecture Notes

### Round Management
- **Round-based processing** - Automated block creation
- **Batch processing** - Multiple commitments per block
- **Leader-only block creation** - High availability with single leader
- **Graceful shutdown** - Proper cleanup of pending rounds

### SMT Integration
- **Spec compliant** - Exact hash matching with Unicity SMT specification
- **Memory efficient** - Optimized for large-scale operations
- **Concurrent safe** - Thread-safe operations with proper locking

## Limitations

- **Receipt Signing**: Returns unsigned receipts (cryptographic signing planned)

## Contributing

1. Follow Go best practices and conventions
2. Write tests for new functionality
3. Update documentation concurrently with code changes
4. Use the provided Makefile for builds and testing

## License

[License information to be added based on project requirements]

## Support

For issues and questions:
- Create GitHub issues for bugs and feature requests
- Check the `/health` endpoint for service status
- Review logs for detailed error information
