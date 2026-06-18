# Disk-Backed SMT E2E Performance

This document records end-to-end BFT-shard performance for the Go aggregator
with RocksDB-backed SMT and durable proposals.

## Scope

The BFT-sharding stack starts two shards because the local setup expects two
shards. The performance client sends traffic only to shard 0, so the rows below
measure one loaded shard:

```text
SHARD_TARGETS=http://localhost:3001:0
```

All checkpoint rows are single-loaded-shard measurements at `1000/s`. They are
short checkpoint probes.

## Config

The runs used the disk-backed BFT-shard stack with RocksDB as the SMT backend.
All components run co-located on a single host.

| Setting | Value |
|---|---:|
| Host CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |
| Disk | local NVMe SSD |
| Active shard load | shard 0 only |
| Required stack shards | `NUM_SHARDS=2` |
| Mongo groups | `MONGO_GROUPS=2` |
| SMT backend | `rocksdb` |
| Go build tags | `rocksdb` |
| RocksDB cache | `SMT_ROCKSDB_CACHE_MB=1024` |
| RocksDB background jobs | `SMT_ROCKSDB_BG_JOBS=8` |
| RocksDB subcompactions | `SMT_ROCKSDB_SUBCOMPACTIONS=4` |
| RocksDB bloom bits | `SMT_ROCKSDB_BLOOM_BITS=10` |
| RocksDB memtable | `SMT_ROCKSDB_MEMTABLE_MB=64` |
| RocksDB library | static RocksDB 8.10 |
| `PRECOLLECTOR_GRACE_PERIOD` | `0s` |
| `MAX_COMMITMENTS_PER_ROUND` | `20000` |
| `COLLECT_MINI_BATCH_SIZE` | `500` |
| Mongo insert chunk size | `200` |
| Mongo insert chunk workers | `8` |
| Redis ack batch size | `10000` |
| Submission workers | `300` |
| Proof workers | `300` |
| HTTP client pool | `48` |

Each checkpoint probe used:

| Setting | Value |
|---|---:|
| Target RPS | `1000` |
| Duration | `5m` |
| `ROOT_BLOCK_RATE` | `350ms` |
| `PROOF_INITIAL_DELAY` | `1s` |
| `PROOF_RETRY_DELAY` | `500ms` |

## Metrics

`Pre-BFT materialize` and `Pre-BFT record stage (Mongo)` happen before sending
the BFT certification request. `Finalization total` is post-UC work, and `SMT
commit` is part of that finalization total.

Client proof latency includes the configured first poll delay
(`PROOF_INITIAL_DELAY=1s`), so client p50 near `1.005s` means most proofs were
ready on the first poll.

## Results

### Checkpoint Matrix

These rows track one loaded shard at fixed `1000/s` as the RocksDB SMT grows.
Each checkpoint is a `5m` probe with full submission and proof verification
success unless noted otherwise.

| Checkpoint | Tree size | Commitments / round | Server proofReady p50 | Server proofReady p95 | Server proofReady p99 | `<=1s` ready | Client p50 | Client p95 | BFT wait | Pre-BFT materialize | Pre-BFT record stage (Mongo) | Finalization total | SMT commit inside finalization |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Clean | ~303k | 532 | 841ms | 1.149s | 1.189s | 80.0% | 1.005s | 1.506s | 490ms | 8ms | 12ms | 17ms | 14ms |
| 5M | 5.06M | 532 | 814ms | 1.130s | 1.186s | 85.5% | 1.005s | 1.507s | 479ms | 13ms | 11ms | 24ms | 20ms |
| 10M | 10.05M | 537 | 848ms | 1.163s | 1.280s | 78.2% | 1.005s | 1.507s | 468ms | 17ms | 18ms | 28ms | 23ms |
| 15M | 15.71M | 532 | 840ms | 1.149s | 1.189s | 80.2% | 1.005s | 1.507s | 474ms | 18ms | 9ms | 26ms | 22ms |
| 20M | 21.97M | 563 | 882ms | 1.172s | 1.247s | 71.1% | 1.006s | 1.508s | 470ms | 24ms | 19ms | 43ms | 35ms |
| 25M | 26.77M | 559 | 877ms | 1.179s | 1.485s | 71.6% | 1.006s | 1.509s | 464ms | 27ms | 22ms | 40ms | 33ms |
| 30M | 30.44M | 558 | 873ms | 1.176s | 1.334s | 72.6% | 1.006s | 1.508s | 465ms | 26ms | 20ms | 40ms | 32ms |
| 40M | 40.37M | 568 | 867ms | 1.183s | 1.505s | 73.5% | 1.006s | 1.509s | 460ms | 32ms | 25ms | 44ms | 36ms |
| 50M | 50.33M | 565 | 889ms | 1.187s | 1.566s | 69.4% | 1.006s | 1.509s | 452ms | 36ms | 27ms | 42ms | 34ms |
| 60M | 60.32M | 567 | 884ms | 1.189s | 1.666s | 70.4% | 1.006s | 1.509s | 453ms | 33ms | 30ms | 44ms | 36ms |

### Single-Shard Scaling Matrix

These runs increase target load on one active shard using the low-latency BFT cadence.

| Tree size | Target RPS | Duration | Achieved RPS | Submitted / verified | Server proofReady p50 | Server proofReady p95 | Server proofReady p99 | `<=1s` ready | Client p50 | Client p95 | Client p99 | BFT wait | Finalization total | SMT commit inside finalization | Commitments / round | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ~606k | 2,000 | 5m | 1,998.99 | 599,700 / 599,700 | 870ms | 1.166s | 1.197s | 73.9% | 1.006s | 1.507s | 1.512s | 484ms | 31ms | 22ms | 1,109 | pass |
| ~1.51M | 3,000 | 5m | 2,999.02 | 899,700 / 899,700 | 888ms | 1.177s | 1.379s | 69.8% | 1.005s | 1.511s | 1.519s | 452ms | 41ms | 31ms | 1,685 | pass |
| ~2.73M | 4,000 | 5m | 3,999.04 | 1,199,700 / 1,199,700 | 1.027s | 1.546s | 1.838s | 46.4% | 1.015s | 1.516s | 2.017s | 477ms | 55ms | 42ms | 2,651 | pass |
| ~4.24M | 5,000 | 5m | 4,997.57 | 1,496,389 / 1,496,389 | 1.250s | 2.141s | 2.996s | 22.1% | 1.506s | 2.513s | 3.150s | 488ms | 94ms | 73ms | 4,095 | degraded |

### Multi-Shard Scaling

Fresh-tree `5m` scaling checks with `ROOT_BLOCK_RATE=350ms`,
`PRECOLLECTOR_GRACE_PERIOD=0s`, `PROOF_INITIAL_DELAY=2s`, and
`PROOF_RETRY_DELAY=1s`.

| Active shards | Mongo groups | Target RPS | Achieved RPS | Submitted / verified | Server proofReady p50 | Server proofReady p95 | Server proofReady p99 | `<=1s` ready | Client p50 | Client p95 | Client p99 | Finalization total | SMT commit inside finalization | Commitments / round / shard | Aggregator CPU avg | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 4 | 2 | 4,000 | 3,999.15 | 1,199,700 / 1,199,700 | 844ms | 1.159s | 1.196s | 77.9% | 2.002s | 2.005s | 2.008s | 24ms | 15ms | 548 | 369% total | pass |
| 8 | 4 | 8,000 | 7,998.26 | 2,399,691 / 2,399,691 | 881ms | 1.257s | 1.816s | 70.1% | 2.003s | 2.014s | 2.038s | 41ms | 25ms | 574 | 782% total | pass |

The 8-shard row doubles the load and keeps per-shard round size close to the
4-shard run. Throughput scales nearly linearly on the same host; the latency
tail grows, but all submissions and proofs still complete successfully.

## Notes

Separate isolated RocksDB-SMT probes, without BFT/Mongo/Redis/HTTP, showed that
the tree engine can still process large batches at high tree sizes. Around
255M-257M leaves (roughly a 50GB+ RocksDB directory), the Go RocksDB-SMT path
with static RocksDB 8.10 processed 10k-leaf batches in roughly 854-888ms, with
RocksDB engine write time around 87-90ms.

This separates raw SMT capacity from end-to-end latency: the E2E rows measure
the full round pipeline, not only RocksDB-SMT insert/commit speed.
