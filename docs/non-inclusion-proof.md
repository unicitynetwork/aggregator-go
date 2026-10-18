# get_non_inclusion_proof.v1

Absence of one state key **at the latest certified snapshot**. It is a historical fact about that snapshot, not
current absence or spendability: pending, expired and unknown states are indistinguishable from absent ones at a root.
The shared fixture corpus and the exact verification order are in `testdata/non-inclusion/v2/SPEC.md`.

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
certifying 32 zero bytes is an ordinary root (nothing to prove an absence against). This aggregator certifies exactly
that zero root for an empty tree after genesis (empty rounds still finalize blocks), so the certified empty tree serves
only until the first block; from then until the first commitment is certified, requests answer `-32020`
`empty_snapshot_not_provable` with `retryable: true`.

## Client verification

> Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.

`pkg/noninclusion.Verifier` authenticates the proof (anchored authority, the fixed aggregation partition
`api.AggregationPartitionID` = 7, expected shard, optional configuration pin, epoch end) and returns a discriminated
result: `absent_at_snapshot` or `included_at_snapshot` (each with the queried state ID and the authenticated snapshot), or an
`*noninclusion.Error` with one rejection class (`malformed`, `uc_invalid`, `authority_rejected`, `certificate_invalid`)
and no snapshot metadata.

```go
v, err := noninclusion.NewVerifier(noninclusion.Context{Resolver: trustBaseResolver})
res, err := v.Verify(stateID, proof)
if err != nil { /* rejected: err is a *noninclusion.Error, RejectionOf(err) is the class; no snapshot */ }
if res.Outcome() != noninclusion.OutcomeAbsentAtSnapshot { /* included at the snapshot */ }
snap := res.Snapshot() // NetworkID, PartitionID, ShardID, RootRound, SealTimestampSeconds, RootEpoch, ShardRound, ShardEpoch, StateRoot
if snap.NetworkID() != checkpoint.NetworkID || snap.RootRound() < checkpoint.RequiredRootRound { /* policy not met */ }
// absence at snap only; any spend/finality decision needs its own policy
```

The checkpoint comes from an independent source, never from the proof or the server response. All snapshot values are
read from the authenticated unicity certificate.

### Migration (schema 2)

* `Verifier.VerifyFor` is replaced by `Verifier.Verify`, which returns `(*Result, error)`; a state that is present at the
  proof's snapshot is now the successful outcome `included_at_snapshot`, not an error (`VerdictStateIncluded` is gone).
* **`err == nil` no longer means "absent".** `included_at_snapshot` is a successful verification too. A port of `VerifyFor` that keeps
  "no error ⇒ the state is unspent" gets the meaning inverted for a state that is present at the snapshot. Always switch on
  `Result.Outcome()` (see `ExampleVerifier_Verify_outcome` in `pkg/noninclusion`) and only then apply your snapshot policy.
* `Context.MinRootRound` and the `stale` verdict are removed: freshness is the caller's decision after verification.
* `Context.Partition` is removed: the partition is the constant `api.AggregationPartitionID` (7). `Verdict*` constants are renamed
  `Rejection*`, `VerdictOf` is `RejectionOf`, `Result.UC`, `Result.RootRound` and `Result.EmptyTree` are replaced by the immutable `Snapshot`.
* The shared corpus is `testdata/non-inclusion/v2` (schema 2); `v1` is removed.
