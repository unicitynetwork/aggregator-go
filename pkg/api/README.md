# Unicity Aggregator Public API

This package provides public JSON-RPC request and response types for the Unicity Aggregator. These types can be imported and used by external clients to interact with the aggregator service.

## Installation

```go
import "github.com/unicitynetwork/aggregator-go/pkg/api"
```

## Types

### Core Types
- `api.StateID` - 68-character hex state ID with algorithm prefix
- `api.TransactionHash` - 68-character hex transaction hash with algorithm prefix  
- `api.HexBytes` - Byte array that serializes to/from hex strings
- `api.BigInt` - Big integer with JSON string serialization
- `api.Timestamp` - Unix timestamp in milliseconds

### Data Structures
- `api.CertificationData` - Certification data with secp256k1 signature
- `api.CertificationRequest` - State transition certification request
- `api.AggregatorRecord` - Finalized certification request with proof data
- `api.Block` - Blockchain block information
- `api.InclusionProof` - Merkle inclusion proof
- `api.NoDeletionProof` - No-deletion proof
- `api.Receipt` - Signed receipt for certification request submission
- `api.HealthStatus` - Service health status

### JSON-RPC Request/Response Types
- `api.CertificationRequest` / `api.CertificationResponse`
- `api.GetInclusionProofRequest` / `api.GetInclusionProofResponse`
- `api.GetBlockRequest` / `api.GetBlockResponse`
- `api.GetBlockRecordsRequest` / `api.GetBlockRecordsResponse`
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
	// Create a certification request
	req := &api.CertificationRequest{
		StateID: "0000b1333daf3261d9bfa9d6dd98f170c0e756c26dbe284b5f90b27df900f6a77c04",
		CertificationData: api.CertificationData{
			OwnerPredicate:  api.NewPayToPublicKeyPredicate([]byte{0x03, 0x20, 0x44, 0xf2}),
			SourceStateHash: api.SourceStateHash(hex.EncodeToString([]byte{0x00, 0x00, 0xcd, 0x60})),
			TransactionHash: api.TransactionHash(hex.EncodeToString([]byte{0x00, 0x00, 0xcd, 0x61})),
			Witness:         []byte{0x41, 0x67, 0x51, 0xe8}},
	}

	// Serialize to JSON
	data, _ := json.Marshal(req)
	fmt.Printf("JSON: %s\n", data)

	// Parse JSON response
	var resp api.CertificationResponse
	json.Unmarshal([]byte(`{"status":"SUCCESS"}`), &resp)
	fmt.Printf("Status: %s\n", resp.Status)
}
```

## Features

✅ **No Internal Dependencies** - The API package has no dependencies on internal types  
✅ **JSON Serialization** - All types support proper JSON marshaling/unmarshaling  
✅ **Type Safety** - Strong typing for all request/response structures  
✅ **Validation** - Built-in validation for hex strings, state IDs, etc.  
✅ **Compatibility** - Compatible with TypeScript aggregator implementations  

## Client Implementation

See the [example client](../../examples/client/main.go) for a complete demonstration of how to use these types to build a client that communicates with the Unicity Aggregator service.

## Cryptographic Operations

For cryptographic operations like signature generation and state ID creation, see the internal signing package or implement your own compatible signing logic using secp256k1.