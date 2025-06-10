# Unicity Aggregator Public API

This package provides public JSON-RPC request and response types for the Unicity Aggregator. These types can be imported and used by external clients to interact with the aggregator service.

## Installation

```go
import "github.com/unicitynetwork/aggregator-go/pkg/api"
```

## Types

### Core Types
- `api.RequestID` - 68-character hex request ID with algorithm prefix
- `api.TransactionHash` - 68-character hex transaction hash with algorithm prefix  
- `api.HexBytes` - Byte array that serializes to/from hex strings
- `api.BigInt` - Big integer with JSON string serialization
- `api.Timestamp` - Unix timestamp in milliseconds

### Data Structures
- `api.Authenticator` - Authentication data with secp256k1 signature
- `api.Commitment` - State transition request
- `api.AggregatorRecord` - Finalized commitment with proof data
- `api.Block` - Blockchain block information
- `api.InclusionProof` - Merkle inclusion proof
- `api.NoDeletionProof` - No-deletion proof
- `api.Receipt` - Signed receipt for commitment submission
- `api.HealthStatus` - Service health status

### JSON-RPC Request/Response Types
- `api.SubmitCommitmentRequest` / `api.SubmitCommitmentResponse`
- `api.GetInclusionProofRequest` / `api.GetInclusionProofResponse`
- `api.GetBlockRequest` / `api.GetBlockResponse`
- `api.GetBlockCommitmentsRequest` / `api.GetBlockCommitmentsResponse`
- `api.GetBlockHeightResponse`
- `api.GetNoDeletionProofResponse`

## Example Usage

```go
package main

import (
    "encoding/json"
    "fmt"
    "github.com/unicitynetwork/aggregator-go/pkg/api"
)

func main() {
    // Create a submission request
    req := &api.SubmitCommitmentRequest{
        RequestID:       "0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00",
        TransactionHash: "00008a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d",
        Authenticator: api.Authenticator{
            Algorithm: "secp256k1",
            PublicKey: api.HexBytes{0x03, 0x20, 0x44, 0xf2},
            Signature: api.HexBytes{0x41, 0x67, 0x51, 0xe8},
            StateHash: api.HexBytes{0x00, 0x00, 0xcd, 0x60},
        },
    }
    
    // Serialize to JSON
    data, _ := json.Marshal(req)
    fmt.Printf("JSON: %s\n", data)
    
    // Parse JSON response
    var resp api.SubmitCommitmentResponse
    json.Unmarshal([]byte(`{"status":"SUCCESS"}`), &resp)
    fmt.Printf("Status: %s\n", resp.Status)
}
```

## Features

✅ **No Internal Dependencies** - The API package has no dependencies on internal types  
✅ **JSON Serialization** - All types support proper JSON marshaling/unmarshaling  
✅ **Type Safety** - Strong typing for all request/response structures  
✅ **Validation** - Built-in validation for hex strings, request IDs, etc.  
✅ **Compatibility** - Compatible with TypeScript aggregator implementations  

## Client Implementation

See the [example client](../../examples/client/main.go) for a complete demonstration of how to use these types to build a client that communicates with the Unicity Aggregator service.

## Cryptographic Operations

For cryptographic operations like signature generation and request ID creation, see the internal signing package or implement your own compatible signing logic using secp256k1.