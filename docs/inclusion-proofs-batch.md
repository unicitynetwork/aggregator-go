# `get_inclusion_proofs.v1`

Batch inclusion proofs for UC consolidation (plan §2.3, §2.4). Served only by real BFT-shard / standalone endpoints
whose finalization publishes retained views; parent/child aggregation answers `-32008 unsupported_configuration`.

```json
{"jsonrpc":"2.0","id":7,"method":"get_inclusion_proofs.v1","params":{"stateIds":["<64 hex>","<64 hex>"],"stateRoot":"latest"}}
{"jsonrpc":"2.0","id":7,"result":{"proofs":["<v2 result hex-CBOR>","<v2 result hex-CBOR>"],"partitionView":"<hex-CBOR>"}}
```

- `params` is an object. `stateIds`: 1–256 distinct raw 32-byte IDs (optional `0x`, case-insensitive hex); duplicates are an error.
  `stateRoot`: omitted or `"latest"` selects the latest published view; 64 hex digits selects the retained view with that
  raw `UC.InputRecord.Hash` (never substituted by a newer one). `null`, `""` and other keywords are invalid.
- `proofs` has exactly the request's count and order; each element is byte-identical to a `get_inclusion_proof.v2` result.
  All elements come from one pinned finalized view. Any absent state fails the whole call.
- `partitionView` is present only for latest requests (hex CBOR, identical to the accepted transport sidecar); exact-root
  results omit the field. A latest response never carries it when the admitted UC is that of an unsharded partition (empty shard
  ID, zero shard-tree siblings), whether or not a view was received for it: that UC is complete by itself and needs no
  view, and the aggregator neither validates, stores nor serves a sidecar for it. For a sharded UC the view is required
  (`partition_view_unavailable`); there is no option to relax this.
- Limits: params 32 KiB, request 64 KiB, JSON-RPC id 128 encoded bytes, response 8 MiB (serialized JSON-RPC), 8 concurrent
  batches / pins, 5 s execution.

| Code | Message | `data.reason` / `data.detail` |
|---|---|---|
| -32602 | Invalid params | `invalid_request`: `malformed_params`, `invalid_state_id`, `duplicate_state_id`, `wrong_shard`, `invalid_state_root`, `invalid_id` |
| -32007 | Limit exceeded | `limit_exceeded`: `state_count`, `request_bytes`, `response_bytes` (+ `limit`) |
| -32006 | Concurrency limit exceeded | `busy`: `active_requests`, `pinned_views`, `retention_budget`, `deadline` (retryable) |
| -32008 | Proof unavailable | `proof_unavailable`: `state_pending` (retryable), `state_expired`, `state_unknown`, `state_not_in_root`, `view_not_retained`, `no_finalized_view`, `partition_view_unavailable`, `unsupported_configuration` |

Database failures stay `-32004` without database text.

`state_pending` / `state_expired` / `state_unknown` (latest requests) and `state_not_in_root` (exact-root requests) carry
`data.indexes` with the offending request indexes per class; see [proof-status.md](proof-status.md) for the contract,
precedence and the limits of the (unauthenticated) hint.
