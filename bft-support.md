# BFT Support for Unicity Aggregator

## Overview

The Unicity Aggregator supports Byzantine Fault Tolerance (BFT) through integration with the BFT core. This enables the aggregator to participate in a distributed consensus network where nodes can continue to operate correctly even if some nodes fail or act maliciously.

## Requirements for BFT Support

### Prerequisites

1. **BFT Core**: The BFT functionality depends on the BFT core library
2. **Go Dependencies**: Ensure all BFT-related dependencies are available:
   ```
   github.com/unicitynetwork/bft-core
   ```

### Bootstrap Node Configuration

**IMPORTANT**: The aggregator requires at least one bootstrap node to be defined in the BFT configuration to connect and participate in the network.

Bootstrap nodes serve as entry points for the aggregator to join the BFT network. Without properly configured bootstrap nodes, the aggregator will fail to initialize with errors like:

#### Configuring Bootstrap Nodes

Bootstrap nodes are configured via the `BFT_BOOTSTRAP_ADDRESSES` environment variable.

**Environment Variable Configuration:**
```bash
export BFT_BOOTSTRAP_ADDRESSES="/ip4/127.0.0.1/tcp/26662/p2p/16Uiu2HAm6eQMr2sQVbcWZsPPbpc2Su7AnnMVGHpC23PUzGTAATnp"
```

For Docker deployments, set this in your `docker-compose.yml`:
```yaml
environment:
  - BFT_BOOTSTRAP_ADDRESSES: "/ip4/127.0.0.1/tcp/26662/p2p/16Uiu2HAm6eQMr2sQVbcWZsPPbpc2Su7AnnMVGHpC23PUzGTAATnp"
```

If running in Docker, ensure bootstrap nodes are accessible from within the container network.

### Configuration Files

For BFT support, you need to provide BFT-specific configuration files generated by the BFT core:

#### Required Configuration Files

1. **Shard Configuration** (`bft-config/shard-conf-7_0.json`)
   - Generated using BFT core for aggregator node (a custom BFT partition)
   - Contains network topology and node information
   - Defines the shard structure and participant nodes

2. **Keys Configuration** (`bft-config/keys.json`)
   - Contains cryptographic keys for the aggregator node
   - Includes public/private key pairs for consensus participation
   - Must be kept secure and not shared publicly

3. **Trust Base Configuration** (`bft-config/trust-base.json`)
   - Defines the initial trust relationships in the network
   - Contains root certificates or trusted node identities
   - Used for initial network bootstrap and validation

### Configuration File Generation

Use the BFT core tools to generate the required configuration files:

# Generate aggregator node info and keys
`build/ubft shard-node init --home "test-nodes/aggregator" --generate`
# Generate shard configuration for aggregator
`build/ubft shard-conf generate --home test-nodes --network-id 3 --partition-id 7 --partition-type-id 7 --epoch-start 10 --node-info test-nodes/aggregator/node-info.json`
                  
# Copy shard configuration for aggregator node
`cp <bftcore location>/test-nodes/shard-conf-7_0.json bft-config/shard-conf-7_0.json`
# Copy keys configuration
`cp <bftcore location>/test-nodes/aggregator/keys.json bft-config/keys.json`
# Copy trust base configuration  
`cp <bftcore location>/test-nodes/trust-base.json bft-config/trust-base.json`

**Important**: The generated shard configuration file (`shard-conf-7_0.json`) must also be added to the root nodes in the BFT network. Root nodes need to be aware of the aggregator's shard configuration to properly support aggregator node.


### Environment Variable Configuration

The aggregator automatically picks up configuration files from the default locations. You can override these paths and configure bootstrap nodes using environment variables:

- `BFT_KEY_CONF_FILE`: Override path to keys.json
- `BFT_SHARD_CONF_FILE`: Override path to shard configuration file
- `BFT_TRUST_BASE_FILE`: Override path to trust-base.json
- `BFT_BOOTSTRAP_ADDRESSES`: **Required** - Comma-separated list of bootstrap node addresses

Example:
```bash
export BFT_KEY_CONF_FILE=/custom/path/keys.json
export BFT_SHARD_CONF_FILE=/custom/path/shard-conf.json
export BFT_TRUST_BASE_FILE=/custom/path/trust-base.json
export BFT_BOOTSTRAP_ADDRESSES="/ip4/127.0.0.1/tcp/26662/p2p/16Uiu2HAm6eQMr2sQVbcWZsPPbpc2Su7AnnMVGHpC23PUzGTAATnp"
```

### Building with BFT Support

To build the aggregator with BFT support, ensure all dependencies are available:

```bash
go mod tidy
go build ./cmd/aggregator
```

**Note**: BFT functionality is currently optional. The aggregator can run without BFT dependencies for development and testing purposes.
