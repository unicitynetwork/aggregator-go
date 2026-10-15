# Proof consolidation (reference library, client, CLI)

Consolidation rebuilds inclusion proofs of states certified under *different* unicity seals of one partition so that all
of them are rooted in **one master seal**. It produces an ordinary proof bundle (each proof is a `get_inclusion_proof.v2`
result); it is not a token format and callers keep their own transaction/predicate checks.

| Piece | Path | Role |
|---|---|---|
| Pure library | `pkg/consolidation` | master-view authentication (bft-go-base `PartitionViewV1` helpers), UC assembly under the master seal, full proof semantics, operation-scoped seal reuse, counters. No I/O. |
| HTTP client | `pkg/consolidation/client` | master-first orchestration over `get_inclusion_proofs.v1` ([API](inclusion-proofs-batch.md)). |
| CLI | `cmd/consolidate-proofs` | file-in/file-out wrapper around the client. |
| Example | `examples/consolidate` | `go run ./examples/consolidate -shards 8`: self-contained 1/2/4/8-shard run. |
| Simulator | `internal/consolidationsim` | in-process shard aggregators (real SMTs, signed seal) for tests and the example. |

## Flow (one attempt)

1. **R_min** is derived from the original evidence: every original proof is authenticated (request binding, leaf path,
   shard/partition path, seal quorum against the configured trust base). The bound is an opaque value tied to the network
   and policy; it cannot be passed in as a number. Its shard is the default master (override with `-master`).
2. **Wave 1**: one `get_inclusion_proofs.v1` request with `stateRoot:"latest"` to the master → its proofs and the
   partition view. The view is authenticated (`PartitionViewV1.AssembleCertificates`, one seal quorum check) and the master
   seal round must be ≥ R_min.
3. **Wave 2**: for every other participating shard, one exact-root request (root taken from the view) with bounded
   parallelism (`MaxParallel`, default 4). Their local UCs may carry other seals; only identity/root binding is checked.
4. Every shard UC is assembled from the view under the identical master seal; every proof is checked against its input
   (certification data, state ID, expiry, **original reference time ≤ target IR timestamp**, shard, path to the target
   root). Original data and reference times are preserved; inputs are never modified.

For *m* participating shards that is *m* requests in two waves (one request if *m* = 1). Instrumentation per attempt:
`QuorumChecks = 1`, `BodyChecks = m`, `LeafChecks = L`. Seal checks done to authenticate the original evidence are
reported separately (`EvidenceQuorumChecks`).

One total deadline governs the attempt (`Timeout`, default 10 s) and context cancellation aborts in-flight requests.
**No retry, no chunking, no chasing of newer latest seals, and nothing is ever submitted**: the client only calls
`get_inclusion_proofs.v1`.

## Outcomes

`client.Error.Kind`: `unavailable` (R_min unknown, snapshot excludes a required leaf, view not retained/evicted, busy,
deadline, cancelled, transport failure — inputs intact; a *new* attempt may succeed, waiting cannot make an excluded leaf
appear in a fixed root), `rejected` (server refused the request: invalid/limit), `invalid_response`, `verification`
(`Outcome`: `unauthenticated`, `binding_mismatch`, `stale`, `expired`, `reference_time_after_target`,
`incompatible_lineage`, `invalid_input`, `unsupported`), `input`.

## CLI

```
consolidate-proofs -config config.json -in items.json [-out bundle.json] [-master 01] [-timeout 10s]
```

`config.json` (trusted deployment configuration; trust bases are `RootTrustBaseV1` JSON files, resolved by network+epoch):

```json
{"partitionId": 168496141, "timeoutSeconds": 10, "maxParallel": 4, "trustBases": ["trust-base.json"],
 "shards": [{"shardBits": "0", "url": "http://shard0:3000", "confHash": "<32-byte hex>"},
            {"shardBits": "1", "url": "http://shard1:3000", "confHash": "<32-byte hex>"}]}
```

`items.json`: `{"items":[{"request":"<hex CBOR CertificationRequest>","proof":"<hex CBOR get_inclusion_proof.v2 result>"}]}`.
The output bundle (`version`, `masterShard`, `rootRound`, `stats`, `counters`, `proofs[]` in input order, `unicityCertificates`)
is written atomically (temp file + rename) only on success. Exit codes: 0 ok, 2 input/config, 3 unavailable, 4 refused, 5 the bundle could not be delivered (stdout/file write failure).
A structured error object is printed to stderr.

## Live integration test (real BFT root)

`test/integration/bft_consolidation_e2e_test.go` (build tag `e2e_bft`) consolidates under continuous writes against a real
`ubft` root node, with complete 4- or 8-shard partitions:

```
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock TESTCONTAINERS_RYUK_DISABLED=true
CONSOLIDATION_E2E_SHARDS=4,8 go test -tags e2e_bft ./test/integration -run TestBFTConsolidationE2E -v -timeout 20m
```

- The root is a host process of the `ubft` binary built from the bft-core module version pinned in `go.mod` (it contains the
  sidecar producer); MongoDB and Redis run in Docker. The aggregators run in-process (production wiring: A4 publication,
  `service.BatchHandlerFor`), so the harness fits the 2 CPU / 3 GiB colima VM. `UBFT_BIN` may point at a prebuilt binary.
- The trusted topology (shard IDs and shard configuration hashes) is written in the `BFT_CONSOLIDATION_TOPOLOGY_FILE` format and
  is also the source of the CLI's `confHash` values.
- Writers submit continuously on every shard while the built `cmd/consolidate-proofs` binary runs repeatedly. Original proofs
  come from different seals. Each bundle is checked with the full `InclusionProofV2.Verify` and native `UnicityCertificate.Verify`
  against the real trust base, with one identical master seal, preserved reference times, and the counters
  (1 quorum / m bodies / L leaves, m requests in 2 waves).
- Root-to-shard message count: the new counter `aggregator_bft_certification_responses_total` counts certification response
  messages received. The test asserts responses per shard per root round ≤ 1 and equal (±0.25) with and without consolidation traffic.
