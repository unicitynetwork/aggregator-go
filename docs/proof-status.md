# Proof status hints (pending / expired / unknown)

Issue #209. An **unauthenticated hint** for control flow and UX (when to stop polling, whether to retry next round). It is
not a proof of absence and must never be used to conclude that a state is unspent.

Values: `included` | `pending` | `expired` | `unknown`.

- **pending**: accepted by this aggregator (admitted to the queue, or in the current round) and not yet in a published
  certified state.
- **expired**: admitted, then dropped because its effective timeout passed before inclusion. Best effort: a bounded
  memory of recent drops (default 100 000 IDs, 10 minutes, metric `aggregator_proof_status_expired_evictions_total`).
- **unknown**: none of the above at this node.

## `get_inclusion_proof.v2`, opt-in

Params: `{"stateId": "<hex>", "includeStatus": true}`. With the flag the result is an object

```json
{"proof": "<unchanged v2 hex-CBOR response>", "status": "included|pending|expired|unknown"}
```

`proof` is byte-identical to the result without the flag in every case, including the empty-proof shape of states that are
not included (pinned by golden tests). Without the flag the result is byte-identical to before. Old servers ignore the
unknown param (plain `json.Unmarshal`), so a **string result means "status unsupported"**.

## `get_inclusion_proofs.v1`

Error `-32008` `proof_unavailable` replaces `state_not_included`:

| Request | `data.detail` | retryable |
|---|---|---|
| latest | `state_pending` | yes |
| latest | `state_expired` | no |
| latest | `state_unknown` | no |
| exact `root` (leaf absent from that retained root) | `state_not_in_root` | no |

With mixed outcomes on a latest request, `unknown` takes precedence over `expired`, which takes precedence over `pending`.
`data.indexes` lists the offending request indexes per class (at most the 256-ID request limit), e.g.

```json
{"reason":"proof_unavailable","detail":"state_unknown","retryable":false,
 "indexes":{"pending":[0],"expired":[2],"unknown":[3]}}
```

Exact-root errors use `{"not_in_root":[...]}`. A state with a stored finalized record that the pinned latest view does
not contain yet (finalized after the view) is always classified `pending`, whatever the status memory says. The batch stays at most two database commands:
classification is memory only.

## How it works and where it is weak

- Marked **pending at admission** (before the request is queued, so a round cannot finalize it first), idempotently; also
  when a round pulls a request that was queued before a restart. Cleared (together with any expired record) on
  finalization. A **queued** request (not yet pulled into a round) whose deadline passes is moved to the expired memory:
  the next round pins a reference time at least that large, so the drop is guaranteed. A request a round has pulled is
  never swept by deadline (it is being certified, even if the certified round's reference time already passed its
  deadline); the round reports real drops. All lookups are O(1) in memory; no extra database round trip.
- Abandoned rounds (round timeout, repeat UC, precollector discard) hand their requests back to the queue: they stay
  pending and are swept by their deadline again. Only deactivation (the node stops answering for the queue) forgets the
  pending marks.
- **Bounds:** pending entries are capped (1 048 576; beyond it a request is simply untracked and counted in
  `aggregator_proof_status_pending_rejected_total`), the deadline heap holds only queued requests with a finite deadline
  and loses a node whenever its request is cleared, finalized, expired or pulled into a round, and requests without a
  deadline never cost a heap node. An exact copy of an already certified leaf leaves no admission mark behind.
- Per node and volatile. **HA / multi-instance:** the queue (Redis/Mongo) is shared, the sets are not, so a node that did
  not admit a request may answer `unknown`. **Restart:** the sets are empty, so queued requests answer `unknown` until a
  round pulls them.
- **Resubmission:** re-sending the same signed request while it is pending does not create a second leaf and does not
  change the status: the Mongo queue rejects the copy by its unique `stateId` index (an error to that caller, pre-existing
  behaviour), the Redis queue accepts it and the round drops it as a duplicate leaf without clearing the original's mark.
- Requests rejected by the SMT (invalid leaf) are not tracked separately; they stay pending until their deadline.

Metrics: `aggregator_proof_status_pending`, `aggregator_proof_status_expired_remembered`,
`aggregator_proof_status_expired_evictions_total`, `aggregator_proof_status_served_total{method,status}`.
