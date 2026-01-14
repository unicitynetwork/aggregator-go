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

1. **`certification_request`** - Submit state transition requests
2. **`get_inclusion_proof.v2`** - Retrieve inclusion proofs for commitments
3. **`get_no_deletion_proof`** - Get global no-deletion proofs
4. **`get_block_height`** - Get current blockchain height
5. **`get_block`** - Retrieve block information
6. **`get_block_records`** - Get all certification requests in a block

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
# Test certification_request
curl -X POST http://localhost:3333/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "certification_request",
    "params": "8458220000b1333daf3261d9bfa9d6dd98f170c0e756c26dbe284b5f90b27df900f6a77c04848301410158210299de0a2414a39fc981694b40bcb7006c6a3c70da7097a9a02877469fe1d2a62b582200002dc34763859638857585ce6aa30a43d3d7a342b51e6caee408888f3ab1c9e84b582200004c3b2c6fce3a19589cb219a0c18281696fedcbab1f28afd8aecc830cff55dacb584103ce4ef0fe3b4f53f5264daee6930c5e7a3b60f4dfd102b4d8f2420d8bbba17e446b0f855ad402437f14d00c1f27752e9aa802301ca42a57a80cb1f6f57e03eb00f500",
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