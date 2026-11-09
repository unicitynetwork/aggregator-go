# Executable API Documentation

This directory contains documentation for the Unicity Aggregator JSON-RPC API and HTTP infrastructure endpoints.

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
2. **`get_inclusion_proof.v2`** - Retrieve inclusion proofs for certification requests
3. **`get_no_deletion_proof`** - Get global no-deletion proofs
4. **`get_block_height`** - Get current blockchain height
5. **`get_block`** - Retrieve block information
6. **`get_block_records`** - Get all certification requests in a block

### Infrastructure Endpoints

- **`GET /health`** - Service health and leadership status
- **`GET /docs`** - Interactive API documentation
- **`GET /api/v1/trustbases`** - [Trust-base history](#trust-base-history) (optional V2 configuration)

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
    "params": "<hex-encoded CertificationRequest CBOR>",
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

## Trust-base history

By default `GET /api/v1/trustbases` answers with the installed version 1 trust bases. When a trust-base bundle is configured it answers with the signed
Unicity Trust Base history (record version 2) instead; `PUT /api/v1/trustbases` and the aggregator's own trust base are not changed by this.

| Variable | Meaning |
| --- | --- |
| `UTB_BUNDLE_FILE` | the release bundle, `unicity-trustbase.<network>.cbor` (tag 39015, complete history from the genesis) |
| `UTB_GENESIS_ID` | `0x` + the genesis EntryID: the pin. It comes from your authenticated configuration; it is never computed from the file |
| `UTB_EXPECTED_NETWORK` | the network the bundle must belong to. With BFT enabled it defaults to the shard configuration's network (and must equal it if set); without BFT it is required |

Set both file and pin or neither. The startup bundle budget is 64 MiB and 65,536 entries (`utb.LoadConfig` defaults); served pages have the smaller limits below. At start the file is read within a byte budget, decoded strictly, and verified completely (the genesis is the pin, every later entry is signed by a quorum of its predecessor's members), the network must be the configured one, and the held history must agree with the trust base the aggregator holds: every installed V1 epoch the history covers must have the same network, start round, members (id, key, stake) and quorum threshold. Any failure stops the start; an invalid V2 configuration never falls back to the V1 trust bases.

```
GET /api/v1/trustbases?from=E[&to=E][&snapshotEnd=E&snapshotEntryId=0x..][&maxEntries=N]
```

The answer is `200 application/cbor`, the GetTrustBase response (tag 39015, `[version, network, status, fromEpoch, effectiveEnd, endEntryID, entries, nextEpoch]`), the same bytes a root peer sends. `from` defaults to the genesis epoch, an omitted or future `to` is the tip. A page holds at most 64 entries (`maxEntries` 1..64) and 1 MiB, and one entry at most 128 KiB (the shared fixture with 256 members, 128-byte node ids and 256 signatures is 95,365 bytes); `Append`, the start replay and the refresh refuse a larger one; a client continues with `nextEpoch` and echoes `(effectiveEnd, endEntryID)` as `snapshotEnd`/`snapshotEntryId`, so the end does not move. Protocol outcomes are the status inside the response: `NotYetAvailable` (`from` beyond the tip, or a replica behind the snapshot), `HistoryMismatch` (another entry at the snapshot epoch), `InvalidRequest`, `Unavailable` (see below). A malformed query (repeated or non-decimal fields, a half snapshot, `maxEntries` out of range, a query longer than 1 KiB) is `400`. Responses are `Cache-Control: no-store`. The request carries no network and no byte budget: clients compare the response's network with their own, and the page cap is fixed.

The bundle is the **trust anchor and starting point**, not the whole history. The held history is the bundle plus every later entry appended through `History.Append`: each entry must be exactly the next epoch and is verified from the held tip (previous-quorum signatures, hash link, network, start round); held entries are never replaced. Operators do not edit the file every epoch. Every verified entry, the bundle's own included, is persisted (collection `trust_base_entries_v2`, majority and journaled writes, with an acknowledged tip kept in a metadata document) before it is served, so a restart never falls back to a shorter history. The configured storage must offer this store; without it the aggregator refuses to start rather than serve the history from memory. At start the bundle must be a prefix of what the store holds (a stored epoch the bundle covers must be the bundle's entry), every stored entry beyond it is re-verified in order, and a hole, another history, another network or genesis, or a store that holds less than its own acknowledged tip (a missing or corrupt tail) stops the start. An older matching bundle after a longer one changes nothing. The store cannot detect a rollback of the whole database together with its metadata; that needs an external checkpoint.

**Nothing feeds new entries yet.** `Append` is the one seam a later source (the BFT root connection, once bft-core serves V2) will use; this change adds no source, no HTTP write path (`PUT` is V1 only and never updates the history or converts a V1 trust base), and no bft-core change. A 10 s refresh only adopts entries another aggregator sharing the same database has already persisted. Aggregators with separate databases need their own future source.

"Active" authority here is the aggregator's highest admitted V1 trust base; whether that is the authenticated certified epoch is not established by this route. If that epoch is not in the held history yet (ahead of its tip), that is not a fork: the aggregator starts, serves the verified prefix (`NotYetAvailable` beyond it) and `/health` reports `degraded` (details `trustBaseV2`, `trustBaseV2Tip`, and `trustBaseV2Refresh` after a failed refresh) until an append covers it. A contradiction with the aggregator's own authority (another network, or the same epoch with a different start round, members or quorum) refuses startup or makes `GET` answer `Unavailable`; a failure to read the admitted authority also fails closed. A status or a tip is not authenticated freshness: a client verifies every entry from the entry it holds.

With several shards, each should receive the same entries from the same roots and converge. Until then a lagging shard serves a shorter but valid history (a prefix); the gateway sends this GET to any shard, so a client may meet a shorter one and fetches the rest elsewhere or later. All shards must share the trust anchor (bundle and pin), not the same tip.

With `ENABLE_CORS=true` the route and its preflight carry the JSON-RPC route's CORS headers (methods `GET, OPTIONS`), error responses included.
