# Long-poll on inclusion-proof reads (`waitMs`)

`get_inclusion_proof.v2` and the latest form of `get_inclusion_proofs.v1` accept an optional
`waitMs`. The aggregator then holds the request while every state you asked about is **known
pending** (admitted at this node, not yet in a certified, published view), and answers as soon
as the situation changes - or after at most `waitMs`. It saves polling round trips and the
post-publication polling delay; it does not make certification faster.

Requests **without** `waitMs` get byte-identical responses and behaviour (the released JS SDK
3.0.1 included). There is no server feature flag: capacity is controlled by resource limits only.

## Wire contract

Request: `"waitMs": <integer>` in the params, `0 <= waitMs <= 9007199254740991`. A plain integer
token (no sign, `-0`, fraction, exponent, string, null, duplicate). Zero is valid and different
from absence: it asks for the metadata without waiting. For the batch method `waitMs` is only valid
with `stateRoot` omitted or `"latest"`; with an exact root (any `waitMs`, zero included) the call is
`-32602 invalid_request / malformed_params`. Invalid `waitMs`: v2 `-32000`, batch `-32602`.

Response, only when `waitMs` was present and valid:

| Method | Success | Handled error |
|---|---|---|
| v2 | `{"proof":"<hex>","waitedMs":n}`, plus `"status"` iff `includeStatus:true` | `error.data.waitedMs` |
| batch | `result.waitedMs` next to `proofs` / `partitionView` | `error.data.waitedMs` after the existing members |

- Errors with non-object data keep it as `data.originalData`; errors without data get `{"waitedMs":n}`.
- Validation errors of a request carrying a valid `waitMs` echo `0`. An invalid `waitMs` is not a wait: legacy bytes, no echo.
- `waitedMs` is the integer ceiling of the cumulative time spent waiting (monotonic clock), excluding parsing, proof work, writing. **It is unauthenticated metadata**: never trust it for time, state or absence, and never put it into proof verification. `status` is likewise a hint; a present proof is always verified.
- A timeout is an ordinary final read: v2 returns the empty proof (with `status:"pending"` if asked), batch returns `-32008 state_pending`. Absence is never proof of absence.
- Zero/full capacity, a drain, no remaining budget, `LONG_POLL_MAX_WAITERS=0` and `CONCURRENCY_LIMIT < 2` all give the ordinary immediate read **with** a zero echo. Admission shedding is never an error.
- Batch is all-or-nothing and waits only while at least one state is pending and **every** non-included state is pending. Any unknown or expired state ends the wait at once (error precedence unknown > expired > pending is unchanged). No partial success.
- Frozen examples and the shared test vectors: `testdata/longpoll/` (`README.md` lists checksums).

## Server configuration

| Variable | Default | Meaning |
|---|---|---|
| `LONG_POLL_MAX_WAIT` | `20s` | Upper bound of any wait. |
| `LONG_POLL_MAX_WAITERS` | `1024` | Global cap on admitted waits (parked and resuming). `0` is supported (immediate reads with a zero echo). |
| `CONCURRENCY_LIMIT` | `1000` | Unchanged: bounds *active* HTTP/RPC work. Parked requests do not hold a token. With `CONCURRENCY_LIMIT < 2` waits are not parked. |
| `WRITE_TIMEOUT` | `30s` | Never extended by parking. Startup fails unless `LONG_POLL_MAX_WAIT + 8s` is below it (if finite) and below the 30 s method budget. |

The effective wait is `min(requested, LONG_POLL_MAX_WAIT, time left in the request - 8s)`, where
the time left is the earliest of the 30 s method budget (counted from admission), the middleware
deadline and the transport write deadline. The 8 s reserve pays for resume admission (up to 2 s),
one final read (up to 5 s) and writing the response (1 s). If nothing remains, the request is served
immediately. This is a bound, not a latency guarantee on a congested network.

## What a wait costs

A parked request holds **no** HTTP concurrency token, no batch slot, no pinned view/lease and no
record or proof buffer: only a goroutine and its stack, the decoded request headers/body already
read, a compact copy of the state IDs (at most 256 x 32 bytes), a timer and the connection. Every
re-read is a complete, ordinary read that takes a fresh view, so a wake-up costs one read attempt
(a batch read keeps its two-record-DB-command maximum), not a held snapshot.

Wake-ups are published after finalization has made the proof servable, after follower sync
publishes its local view, and on expiry, sweep, reset and role-change events. A notification only
asks for a recheck; unrelated notifications never extend a wait. Rechecks are coalesced and run
through a FIFO resume queue limited to `max(1, CONCURRENCY_LIMIT/4)` running re-reads, always
leaving one token for fresh requests. A re-read that cannot get capacity within the resume
reserve fails with the existing retryable `-32006 busy` / `deadline` error (with the echo).

### Overload signalling

Every `-32006` answer is sent as **HTTP 503 Service Unavailable with `Retry-After: 1`** (integer seconds); the JSON-RPC body (code, message, `data` including `retryable`, the request id) is unchanged. The server-wide limit that is hit before the request is parsed answers the same way with `"id": null`. Clients and proxies should honour `Retry-After`, and treat 503 or a `-32006` body as an inconclusive overload, not a failure; a client that does not read the body of a non-200 answer still sees a failure, as before. Aggregator-go sends no 429 (there is no per-client rate limit). All other answers keep their status (200 for JSON-RPC errors, 413, 500).

### Sizing guidance

The memory bound is

    W x (retained request + waiter/timer/stack + L x per-ID cost) + active proof work + transport buffers

with W = `LONG_POLL_MAX_WAITERS` and L <= 256 IDs. **The constants have not been measured yet** (the
resource report of the final test matrix does that), so `1024` is a conservative rollout value, not a
validated maximum. Notes for operators:

- The global cap bounds admitted waits, not connections, header/body ingress or the node's
  pending-status tracker. Raise it only with measurements of heap and RSS under your traffic.
- There is no per-client cap: a client holding many pending states can use every slot. Use your API
  front's per-client quotas.
- HTTP/2: `HTTP2_MAX_CONCURRENT_STREAMS` is per connection and a parked wait occupies a stream;
  keep enough connection and stream capacity for submissions and ordinary reads.
- Each wake of a publication rechecks every armed waiter once (coalesced): work is O(W) per round.
  Watch `aggregator_longpoll_rechecks_total` against rounds before raising W.
- Opt-in request bounds (also applied to v2 when it carries `waitMs`): request 64 KiB, params 32 KiB,
  request id 128 bytes; the batch response limit (8 MiB) includes the echo.

## High availability: affinity is required

Pending hints are **node-local** and nothing is replicated. A wait is useful only on the node that
admitted the submission:

1. Route the submission (`certification_request`) and the following proof reads of that state to the
   **same node**, through a real shared affinity key (a cookie or consistent-hash key that both
   requests carry) or a pinned endpoint. Connection reuse alone is not enough, and an API-key header
   sent only on submission is not enough.
2. A node that does not know the state answers `unknown`, which ends a wait immediately (no hidden
   waiting, no wrong answer). Restarts and demotions lose all node-local hints and waits.
3. **Cold followers are not ready for submissions.** A freshly started follower can serve proofs but
   answers `SERVICE_NOT_READY` to submissions until it has a reference time. Do not route submissions
   to a follower before it is ready; this feature changes neither admission nor time semantics.
4. A follower sees finalized blocks through its block sync (nominally every second, longer under
   load): a wait on a follower is woken when the follower publishes the new local view, and
   expiry on a follower may be noticed late - a wait may then time out as `pending` until local
   evidence changes. The aggregator does not invent replicated expiry facts.
5. On a role change (promotion or demotion) all current waits end with the then-current ordinary
   result and new waits are admitted again once the transition settles.
6. **Load balancer idle timeout.** A held request is an idle connection from the LB's point of view.
   Set the LB's idle / response timeout above `LONG_POLL_MAX_WAIT + 8s` (default 28 s), and for
   cookie-based affinity make sure the cookie outlives the polling session.
7. **Do not auto-retry POSTs.** A load balancer or proxy must not retry JSON-RPC POSTs on its own: a failed
   `certification_request` has an uncertain outcome. SDK waiters repeat the known read operations themselves
   (and honour `Retry-After`). HTTP/1.1 is the interoperable baseline between every hop; enable h2c only when the
   next hop supports it, and never discover protocol support by replaying a request.
8. **Do not cache.** Every JSON-RPC response carries `Cache-Control: no-store` (proof, status and error answers
   change over time). Pass `Retry-After` through; for browsers the aggregator exposes it
   (`Access-Control-Expose-Headers`) and allows `Content-Type`, `Authorization`, `X-API-Key` and `X-State-ID`.
   LB-generated errors need the same CORS headers, or browsers cannot read their status.

## Shutdown

On shutdown the server stops admitting requests (also on open h2c connections), wakes every wait to
a final ordinary read, and joins the handlers and request workers **before** the process closes
storage. At the grace deadline it cancels what is left; if work is still running, the process exits
without closing storage instead of closing it beneath running handlers. The shutdown grace (30 s)
is adequate only if measured drain fits it.

## Metrics (bounded labels only)

`aggregator_longpoll_reservations`, `_resume_queued`, `_resume_running` (gauges);
`_requests_total{method,outcome}`, `_wakes_total{cause}`, `_rechecks_total{method}`,
`_notifies_total`; histograms `_wait_seconds`, `_resume_queue_seconds`,
`_publication_to_response_seconds`. Shed cases appear as `outcome=shed_capacity|shed_draining`.

## Go clients

- `pkg/consolidation/client`: `Config.LongPoll` (off by default) lets the **one master "latest"
  request** of an attempt carry `waitMs = min(window, time left - margin, HTTP client timeout -
  margin)`. A still-pending state is returned to the caller as the usual retryable error;
  exact-root peer requests never wait; there is no retry loop inside an attempt. The only extra
  request is one retry **without** `waitMs` when a strict batch parser rejects the unknown member
  (the generic `-32602 / invalid_request / malformed_params` *without* a `waitedMs` echo); it is
  counted in `Stats` (`ParserRetries`) and keeps the attempt's deadline. A response without an echo
  is accepted (older server) and a malformed echo is a protocol error.
- `cmd/commitment`: waits with `waitMs = min(20 s, time left - 2 s, 15 s HTTP timeout - 2 s)`, so a
  15 s client never sends a 20 s wait. `--long-poll=false` disables it. Pacing: it re-calls at once
  only after a *pending* answer with a valid positive echo to a call that already took the polling
  interval; otherwise (no echo, zero echo, fast call) it sleeps the rest of the interval, so the
  loop cannot spin. A server that answers without an echo makes the rest of that run omit `waitMs`.

## Compatibility

Aggregators without long-poll ignore `waitMs` on v2 (bare string result) and reject the unknown
member on the strict batch method; the clients above handle both. Mixed deployments are handled
per response: the absence of an echo is an operational hint, never a cached capability.
