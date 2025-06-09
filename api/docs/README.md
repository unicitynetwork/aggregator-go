# Executable API Documentation

This directory contains documentation for the Unicity Aggregator JSON-RPC API.

## 🚀 Executable Interactive Documentation

The service provides **executable** interactive API documentation accessible at `/docs` when the service is running.

### ✨ Advanced Features

- **🚀 Live API execution** - Send requests directly from the browser
- **📋 cURL command export** - Copy ready-to-use terminal commands
- **⌨️ Keyboard shortcuts** - Press Ctrl+Enter (or Cmd+Enter) to send requests
- **🎯 Status indicators** - Real-time response times and success/error status
- **↻ Reset functionality** - Restore original example parameters
- **💾 Response persistence** - Responses stay visible until cleared
- **📱 Responsive design** - Works perfectly on desktop and mobile
- **🎨 Professional styling** - Clean, modern interface with color-coded sections

### Accessing Documentation

1. **Local development**: `http://localhost:3000/docs`
2. **Docker setup**: `http://localhost:3333/docs`
3. **Production**: `https://your-domain.com/docs`

## Available Methods

### Core Methods

1. **`submit_commitment`** - Submit state transition requests
2. **`get_inclusion_proof`** - Retrieve inclusion proofs for commitments
3. **`get_no_deletion_proof`** - Get global no-deletion proofs
4. **`get_block_height`** - Get current blockchain height
5. **`get_block`** - Retrieve block information
6. **`get_block_commitments`** - Get all commitments in a block

### Infrastructure Endpoints

- **`GET /health`** - Service health and leadership status
- **`GET /docs`** - Interactive API documentation

## JSON-RPC 2.0 Protocol

All API methods follow the JSON-RPC 2.0 specification:

```json
{
  "jsonrpc": "2.0",
  "method": "method_name",
  "params": { ... },
  "id": 1
}
```

### Response Format

```json
{
  "jsonrpc": "2.0",
  "result": { ... },
  "id": 1
}
```

### Error Format

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32000,
    "message": "Error description",
    "data": { ... }
  },
  "id": 1
}
```

## 🧪 Testing the API

The executable documentation at `/docs` provides multiple ways to test the API:

### Browser Testing
1. **Edit parameters** in the text areas (JSON format)
2. **🚀 Send Request** - Execute API calls with real responses
3. **📋 Copy as cURL** - Get terminal-ready commands
4. **↻ Reset Example** - Restore original parameter values
5. **🗑️ Clear Response** - Reset response displays

### Keyboard Shortcuts
- **Ctrl+Enter** (or **Cmd+Enter**) - Send request from any parameter field
- **Tab** - Navigate between parameter fields
- All textareas include tooltip hints for shortcuts

### Status Indicators
- **🟢 Success (123ms)** - Request completed successfully
- **🔴 Error (400)** - Request failed with HTTP status
- **🔵 Sending...** - Request in progress
- **⚠️ Parse Error** - Invalid JSON parameters

### Manual Testing

```bash
# Test submit_commitment
curl -X POST http://localhost:3333/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "submit_commitment",
    "params": {
      "requestId": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "transactionHash": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
      "authenticator": {
        "stateHash": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "publicKey": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        "signature": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "signAlg": "ed25519",
        "hashAlg": "SHA256"
      }
    },
    "id": 1
  }'

# Test get_block_height  
curl -X POST http://localhost:3333/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "get_block_height",
    "params": {},
    "id": 2
  }'
```

## Documentation Generation

The API documentation is generated programmatically from method definitions in `internal/gateway/docs.go`:

- **RpcMethodDoc** structs define each method
- **GenerateDocsHTML()** creates the interactive HTML
- **Real examples** with valid hex strings and proper formatting

This ensures documentation stays synchronized with the actual API implementation.