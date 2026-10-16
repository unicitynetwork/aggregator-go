# Long-poll shared fixtures

Wire-contract fixtures of the optional `waitMs` long-poll on inclusion-proof reads
(aggregator-go#224, revision 3 of `orchestration/arch-long-poll.md`). They are produced and owned
by the Go server (package A) and consumed by the JS SDK (B) and the Rust SDK / rugregator (C),
which copy them byte-for-byte into their own test data and check the checksums below.

Source: aggregator-go `origin/non-inclusion` @ `a352752144ec40ae5ab8b2daf1c324013dca8213`
(the base of the `long-poll` integration branch). JS compatibility baseline: npm
`@unicitylabs/state-transition-sdk@3.0.1`, tag `v3.0.1` @ `f5f0737306901215860aa920a6ab570b699efba8`.

## Files

| File | Checksum (sha256) |
|---|---|
| `params.json` | 4558bd8047b6fb5f0a00c84a167a0fa8f678c4dd325880e3036c3b033b48cb5e |
| `echo.json` | eece0ee9b8839d5f13ecf72be0d19ecb65e737a6aa1514f4949064a5ce228f23 |
| `capability.json` | f11f2341a0e5aadaba504133e8940cb4f0809d5f163a87e9ad4b95240b827578 |
| `golden/v2-nowait.json` | f43007d67de020bf7eb48c11ec1c989a377f15a3a89d850fce7b9fb56ded33b3 |
| `golden/batch-nowait.json` | 5c36869bef6c9298ed9323b3bc606029ba69690936aa0311a0c6b56136fb331e |

`go test ./pkg/api -run TestLongPollFixtures` fails if a file and this table disagree; after an
intentional change, regenerate the file and update its row. Changing a frozen file after B/C
consumed it needs the orchestrator's agreement.

### `params.json` (T1)
`cases[]`: `method` (`get_inclusion_proofs.v1` or `get_inclusion_proof.v2`), `params` (the raw
JSON text, kept as a string so lexical forms such as `-0`, `1e3` and duplicate members survive),
`valid`, and for valid cases `waitMs` (`null` = member absent, else the integer). For invalid cases
`error` names an entry of `errors`. Rules (revision 3 section 2):
- present `waitMs` is a JSON integer token in `[0, 9007199254740991]`: digits only, no sign
  (so no `-0`), fraction or exponent; null, booleans, strings, arrays, objects, duplicates (also
  via an escaped spelling of the key) and overflow are invalid. Zero is valid and distinct from absence;
- values above the server's limit are valid (clamped later);
- batch: any present `waitMs`, zero included, with an exact `stateRoot` is invalid; `"latest"` is fine;
  the batch parser is strict, so other spellings such as `waitms` are unknown members;
- v2: invalid is `-32000` (the code is the contract); unknown members, `waitms` included, are ignored;
- batch invalid is `-32602`, reason `invalid_request`, detail `malformed_params` (exact).

Not covered: envelopes which are not valid JSON (e.g. a bare `01`) fail before any method sees them.

### `echo.json` (T2)
Frozen response examples with the echo: v2 wait-only / wait+status / zero / cap-full / errors
(including `originalData` for non-object error data), batch success, and every batch error class
with `error.data.waitedMs`. A0 only reserves the request field; no server emits these yet (A3).
No-wait requests never carry `waitedMs`.

### `capability.json` (T8)
Client reaction table for the parser-negotiation retry (exactly one retry without `waitMs`,
only on the matched generic `-32602/invalid_request/malformed_params`) and for responses lacking
or carrying a bad echo.

### `golden/` (T3)
`v2-nowait.json`, `batch-nowait.json`: raw JSON-RPC request bodies and exact response bodies
(including the trailing newline) of requests without `waitMs`, recorded from the handlers
before any long-poll change (`LONGPOLL_GOLDEN_UPDATE=1 go test ./internal/gateway ./internal/service -run TestLongPollGolden`
rewrites them; do that only deliberately). The long-poll handlers must reproduce them byte for byte.
The proofs are produced by deterministic in-test fixtures (not a live deployment); compare raw
bodies, not re-serialized JSON.
