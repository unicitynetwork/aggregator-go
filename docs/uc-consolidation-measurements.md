# UC consolidation: aggregator retention and batch proof measurements

Acceptance evidence for the aggregator-go ticket ("report response sizes, proof latency, retention cost").

- Code: `uc-consolidation` @ `1628dc2` (A2/A3/A6 merged) plus the A5 batch API branch (#193, `uc/A5-batch-api` @ `2b27bad`).
- Machine: Intel Core i7-8850H @ 2.60 GHz, 6 cores / 12 threads, 16 GiB, macOS 15.8.1 (darwin/amd64), Go 1.27.2.
  MongoDB 7.0 ran as a Docker container in a colima VM (2 vCPU, 3 GiB, Ubuntu 24.04), same host.
  RocksDB 11.8.1 (MacPorts, `-tags rocksdb`).
- **The machine was shared and heavily loaded while these ran** (load average 36-61 on 12 threads). Sizes and byte counts are exact;
  latencies are inflated and noisy and are for relative comparison and order of magnitude only. Re-run on a quiet host
  with a native Mongo before setting any numeric gate.
- Everything here is opt-in: the tests skip unless `UC_MEASURE=1`.

## Reproduce

```sh
# retention cost, memory backend
UC_MEASURE=1 go test -count=1 -run Test_MeasureRetentionMemory -v ./internal/smt/backend

# retention cost, RocksDB backend (macOS/MacPorts paths; on Linux use the system librocksdb)
export CGO_CFLAGS=-I/opt/local/include
export CGO_LDFLAGS="-L/opt/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd -lbz2"
UC_MEASURE=1 go test -tags rocksdb -count=1 -run Test_MeasureRetentionRocksDB -v ./internal/smt/backend

# get_inclusion_proofs.v1 latency and response size (the Mongo variant needs Docker/testcontainers)
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock TESTCONTAINERS_RYUK_DISABLED=true
UC_MEASURE=1 go test -count=1 -timeout 25m -run 'Test_MeasureBatchProofs' -v ./internal/service
```

Sources: `internal/smt/backend/retention_measure_test.go`, `retention_measure_rocksdb_test.go`, `internal/service/batch_measure_test.go`.

## Retained-view cost under churn

Setup: a base tree of 100,000 leaves; every block adds B new leaves and retains a view; 16 views are kept (the catalog default),
the oldest closed as new ones arrive. "Accounted" is the `RetainedBytes()` the catalog charges against the 256 MiB budget
(per view, the sum over 16 views divided by 16). "Measured" is the real incremental cost per view:

- memory: Go heap (after GC) with all 16 views retained minus heap with only the newest, divided by 15;
- RocksDB: on-disk size after full compaction of an identical churn run with the 16 snapshots pinned minus the run with no views, divided by 16
  (pinned snapshots keep old node versions from being compacted away).

**Memory backend**

| leaves per block | accounted per view | measured per view | accounted / measured | `RetainReadView` call | views that fit 256 MiB |
|---|---|---|---|---|---|
| 100 | 259,490 B | 193,630 B | 1.34x | 0.48 ms | 1,034 |
| 1,000 | 1,998,903 B | 1,408,278 B | 1.42x | 4.0 ms | 134 |
| 10,000 | 14,311,495 B | 9,195,006 B | 1.56x | 20.1 ms | 18 |

**RocksDB backend**

| leaves per block | accounted per view | measured per view | accounted / measured | `RetainReadView` call | views that fit 256 MiB | disk without views | disk with views |
|---|---|---|---|---|---|---|---|
| 100 | 144,815 B | 54,851 B | 2.64x | 29 µs | 1,853 | 14,050,202 B | 14,927,824 B |
| 1,000 | 1,117,446 B | 393,337 B | 2.84x | 37 µs | 240 | 15,975,478 B | 22,268,872 B |
| 10,000 | 8,051,554 B | 2,786,216 B | 2.89x | 37 µs | 33 | 35,256,225 B | 79,835,689 B |

Reading: in both backends the accounting is conservative (never below the measurement): 1.3-1.6x for memory, about 2.6-2.9x for RocksDB
(it counts bytes written, before compression). Retention cost is proportional to churn, not to tree size. At 10,000 new leaves per
block the 256 MiB budget, not the 16-view cap, limits retention (18 memory views / 33 disk views), so deployments with large blocks
should expect fewer than 16 retained views or raise the budget. Taking a view is cheap (memory: freeze plus walking only the new
nodes; RocksDB: a pinned snapshot, tens of microseconds) and does not block certification.

## get_inclusion_proofs.v1

Setup: 4,096 leaves in one retained view with an 8-shard partition view (1,544 B), real memory SMT, real catalog and lease, real
service. Latency is the service call plus JSON serialization of the response, measured in process (no HTTP). Response size is the
exact serialized JSON-RPC response including envelope, hex proofs and partition view.

| IDs | response | per ID |
|---|---|---|
| 1 | 4,576 B | 4,576 B |
| 16 | 25,409 B | 1,588 B |
| 256 | 356,561 B | 1,392 B |

A 256-ID batch is 4.3% of the 8 MiB response limit. The response is about 1.4 KB per ID plus 1.5 KB of partition view
(latest requests only).

**Record source in process (no database latency; isolates SMT proof building, CBOR and JSON)**

| IDs | p50 | p95 | p99 | iterations |
|---|---|---|---|---|
| 1 | 127 µs | 353 µs | 523 µs | 400 |
| 16 | 1.1 ms | 2.8 ms | 3.4 ms | 400 |
| 256 | 23 ms | 34 ms | 45 ms | 400 |

**MongoDB record source** (real `AggregatorRecordStorage` on MongoDB 7.0 in the colima VM, 4,096 finalized records;
two sequential queries per ID: the `$bsonSize` size projection, then the record fetch). In this environment a driver round trip
(`Ping`) is 7.3 ms, the size projection 12.8 ms and the record fetch 19.2 ms per query, i.e. about 32 ms of database time per ID.

| IDs | p50 | p95 | p99 | iterations | calls over the default 5 s deadline |
|---|---|---|---|---|---|
| 1 | 35 ms | 105 ms | 152 ms | 60 | 0 |
| 16 | 718 ms | 1.7 s | 4.6 s | 30 | 1 of 30 |
| 256 | 8.3 s | 16.7 s | 16.7 s | 12 | 11 of 12 |

(The lease and execution deadlines were lifted to 5 minutes for this run so the real latency could be measured.)

### Finding: batch latency is dominated by sequential database queries

The proof work itself is small (23 ms for 256 IDs). Everything else is two sequential Mongo round trips per ID. On this
(loaded, virtualized) setup a 256-ID batch takes 8 s at the median and exceeds the 5 s deadline in 11 of 12 calls, so it would
fail with `busy/deadline`; 16 IDs usually fit. With a much lower round-trip time the same code would be faster in proportion to the database share, but that was not
measured here, so whether the default 5 s deadline is safe for 256 IDs in production is not established.
Suggested follow-up (not done here): fetch sizes and records for the whole batch with one or two `$in` queries (or bounded parallelism),
which removes the per-ID round trips; then re-run this measurement.

## Caveats

- Shared, loaded machine and a virtualized Mongo: treat latencies as upper-bound, order-of-magnitude figures.
- The batch measurement is in process (no HTTP, TLS or client); the memory SMT backend was used for the proof reader, not RocksDB.
- Not measured: HA follower behaviour, restart/recovery, 4/8-shard live load, and certification latency impact (the retention
  hooks are not wired into finalization until A4).
- The 256 MiB budget is accounting, not a hard RSS cap (see plan §2.5).
