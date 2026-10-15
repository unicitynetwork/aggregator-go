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

`client.Error.Kind` (with `Retryable`, always "as a new attempt"; nothing is retried automatically):

| Kind | Meaning | Retryable | CLI exit |
|---|---|---|---|
| `unavailable` | only the specified material/pressure outcomes: R_min unknown, no partition view, JSON-RPC `-32008` (`state_not_included`, `view_not_retained`, `no_finalized_view`, `partition_view_unavailable`), `-32006` busy. Inputs intact; waiting cannot make an excluded leaf appear in a fixed root | as reported by the server (busy: yes) | 3 |
| `unsupported` | endpoint cannot serve it: `-32601` method not found, `-32008 unsupported_configuration` | no | 4 |
| `transport` | HTTP exchange failed: connection error, any non-200 status (also with a JSON body; a JSON-RPC error envelope in the body is still decoded) | 408/425/429/5xx and connection errors | 6 |
| `timeout` | the attempt's total deadline expired, or the trust resolver's own timeout (`resolver_timeout`) | yes | 6 |
| `cancelled` | the caller cancelled | never | 130 |
| `server` | operational server failure: `-32004` database, `-32603` internal, unknown codes (code preserved); a resolver cancelled inside itself (`resolver_cancelled`) | no | 6 |
| `rejected` | request refused: `-32602`, `-32600`, `-32007` | no | 4 |
| `invalid_response` | malformed/inconsistent response or envelope (version, id, result/error) | no | 4 |
| `verification` | the library refused the material, including **every** signature/authority failure (`Outcome`: `unauthenticated`, `binding_mismatch`, `stale`, `expired`, `reference_time_after_target`, `incompatible_lineage`, `invalid_input`, `unsupported`) | no | 4 |
| `input` | bad caller input or configuration | no | 2 |

Output delivery failures (stdout/file write) exit 5. The cause of context errors stays reachable with `errors.Is`.

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
is written atomically (temp file + rename) only on success. Exit codes: see the table above (0 ok, 2 input/config, 3 unavailable, 4 refusals, 5 bundle not delivered, 6 operational, 130 cancelled).
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
- Topology pins are optional (T2): by default the e2e configures none and the authenticated view (committed by the root quorum's
  seal) supplies the scheme and configuration hashes; `CONSOLIDATION_E2E_PINNED=1` additionally pins them in the
  `BFT_CONSOLIDATION_TOPOLOGY_FILE` format on the aggregators and as `confHash` in the CLI config (all shards or none).
- Writers submit continuously on every shard (progress is asserted: accepted writes and new finalized blocks on every shard in both phases, and the states submitted while consolidations run must get proofs; `CONSOLIDATION_E2E_NEGATIVE=stop-writers` stops the writers first and must fail) while the built `cmd/consolidate-proofs` binary runs repeatedly. Original proofs
  come from different seals. Each bundle is checked with the full `InclusionProofV2.Verify` and native `UnicityCertificate.Verify`
  against the real trust base, with one identical master seal, preserved reference times, and the counters
  (1 quorum / m bodies / L leaves, m requests in 2 waves).
- Epoch end: the client takes `Config.EpochEnd` (the CLI derives it from the installed trust base history, which must form a
  verified chain; a successor epoch bounds its predecessor) and enforces it on every seal authentication: accepted at
  start−1, rejected at/after the successor's start.
- Root-to-shard message count: `aggregator_bft_certification_responses_total{shard}` counts responses and
  `aggregator_bft_certification_response_nonprogress_total{shard}` those that certify no new shard block (shard input record
  round not advancing: re-deliveries, timeout repeats and any extra traffic, whatever its root round);
  `aggregator_bft_handshakes_total{shard}` counts handshakes. Per shard the test requires: responses ≥ (blocks outside the catch-up bursts after stalls) − 1 + one per stall (the root may coalesce a burst, but 0 responses for 9 blocks never passes); a stall (gap ≥ T2) only in the consolidation phase is compared with the baseline's worst gap and rejected;
  progress responses (responses − non-progress) ≤ finalized blocks + 1; non-progress responses ≤ ⌊(longest gap without a
  finalized block + 0.5 s) / T2⌋ timeout repeats + one re-delivery per handshake; handshakes ≤ baseline + 1. So a single
  duplicate per block is detected even at one finalized block, and extra traffic cannot hide behind advancing root rounds.
  The rule is unit-tested with negative controls (advancing-round extra traffic at the minimum workload, duplicate per block,
  second timeout repeat without a second T2 gap, handshake-excused traffic).

### Recorded live run (colima 2 CPU / 3 GiB, shared with other jobs)

Per phase (12 s, ~21 root rounds), per shard: accepted writes ≈ 46–56, finalized blocks 2–6, responses received = blocks ± 1.

| Shards | Items (originals under ≥2 seals) | Consolidations during phase 2 | Success | Requests / waves | Partition view | Response bytes (all shards) | Sidecars ok / missing |
|---|---|---|---|---|---|---|---|
| 4 | 12 | 3 | 3 | 4 / 2 | 756 B | 24 017 | 82 / 0 |
| 8 | 24 | 4 | 4 | 8 / 2 | 1 508 B | 49 833 | 171 / 0 |

(Consolidation counts depend on host load: a run after merging S1/T2 with no pins did 48/48 (4 shards) and 37/37 (8 shards); the checks do not.)

Every bundle: 1 quorum check, m body checks, L leaf checks; every output proof passed the full `InclusionProofV2.Verify` and
native `UnicityCertificate.Verify` against the real trust base under one identical master seal. What the run establishes:
writers kept submitting and every shard kept finalizing during the consolidation phase (the states submitted meanwhile got
proofs), and each shard received one root response per finalized block with consolidation traffic running — no extra
root-to-shard messages (per-shard responses never exceeded blocks + baseline extra + 2). It does not establish throughput or
latency numbers. The stop-writers negative control fails as intended
(`CONSOLIDATION_E2E_NEGATIVE=stop-writers`: "shard 0 accepted only 0 writes"). A topology file is **not required**: by default the aggregators and the e2e configure no
independent pins, and the topology and configuration hashes come from the authenticated partition view (committed by the root quorum's seal). `BFT_CONSOLIDATION_TOPOLOGY_FILE` (and `confHash` in the CLI configuration) remain as an
optional, stricter independent pin, exercised by `CONSOLIDATION_E2E_PINNED=1`.

## Work bounds (asymptotic regression tests)

`Counters` and `Stats` expose hardware-independent work metrics; the tests sweep the parameter and assert the bound
(`pkg/consolidation/client/asymptotic_test.go`, `router_test.go`, `pkg/consolidation/session_internal_test.go`):

| Work | Bound | Metric |
|---|---|---|
| policy topology validation, canonical order, digest | once per `Session` (defensive snapshot), independent of L, S | `TopologyValidations == 1` |
| evidence UC decode + body check | once per distinct complete UC byte string (E distinct certificates), not per leaf | `UCDecodes`, `EvidenceBodyChecks` |
| per-input rules (binding, expiry, reference time, shard membership, leaf path) | O(1) per input, never memoized | `LeafChecks == L` |
| seal quorum | one per distinct complete seal + authority + policy | `QuorumChecks`, `EvidenceQuorumChecks` |
| shard lookups in the authenticated view | one hash probe per lookup (index built once, O(S)) | `ShardLeafProbes` |
| derived shard certificates | only the m participating shards, lazily, through the authenticated view's own tree and leaf index (no rebuild, no scan; bft-go-base#9) | `UCDerivations == m`, `ViewLeafVisits == 2S` independent of m, `ShardPathSteps == Σ depth` |
| endpoint routing | binary trie, O(shard depth) per input (log S balanced, ≤ key width) | `Stats.RoutingVisits == Σ (depth+1)` |

Memoization keys commit to the complete canonical bytes (and, for seals, the resolved authority content and policy
identity); the authority/epoch-end resolution still runs on every seal use. The trie and the shard index are checked
against a linear-scan reference model.
