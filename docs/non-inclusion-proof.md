# get_non_inclusion_proof.v1

Absence of one state key **at the latest certified snapshot**. It is a historical fact about that snapshot, not
current absence or spendability: pending, expired and unknown states are indistinguishable from absent ones at a root.
The shared fixture corpus and the exact verification order are in `testdata/non-inclusion/v1/SPEC.md`.

```json
{"jsonrpc":"2.0","id":7,"method":"get_non_inclusion_proof.v1","params":{"stateId":"<64 hex digits>"}}
{"jsonrpc":"2.0","id":7,"result":"<hex of [blockNumber, 39034([1, certificate, unicityCertificate])]>"}
```

The result is one string (rugregator and Rust SDK compatible). The certificate is `bitmap || siblings || terminalKey ||
terminalValue` (empty for the certified empty tree) and the UC is embedded as a native CBOR value. The block number is
unauthenticated metadata.

Errors (HTTP 200 JSON-RPC errors; clients map by code, rugregator uses 404/429 for the same codes):

| code | meaning |
|---|---|
| `-32020` | no certified snapshot (`data.detail`: `no_finalized_view`, `view_not_retained`, `unsupported_configuration`, `empty_snapshot_not_provable`) |
| `-32022` | the state is included at the selected root |
| `-32023` | busy (`active_requests`, `pinned_views`, `retention_budget`, `deadline`) |
| `-32602` | invalid params (`wrong_shard`, ...) |
| `-32603` | internal error, no detail |

Server: one catalog lease (UC, block and reader together); the certificate is generated from the immutable retained reader
(memory or the RocksDB read snapshot, never the live store) and verified locally against the leased root before it is
returned. Needs real BFT certification, the retained-view catalog and a shard router; parent/child aggregation and BFT
disabled answer `unsupported_configuration`. Limits: 64 KiB request, 8 MiB response, 8 active proofs, 5 s execution.

Certified empty tree: an authenticated native UC whose input record carries the empty state hash, accepted at
initialization (or as a repeat) by a node with no committed block whose backend explicitly reports the empty state, is
retained as the empty snapshot (block identity 0). It is selected as latest only; no exact-root selector reaches it. A UC
certifying 32 zero bytes is an ordinary root (nothing to prove an absence against).

Client verification: `pkg/noninclusion.Verifier` (anchored authority, expected partition/shard, optional configuration pin,
epoch end, minimum root round).
