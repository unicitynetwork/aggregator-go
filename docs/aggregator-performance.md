# Aggregator Performance Results

This document records measured aggregator performance for the BFT-shard throughput work.

## Single-Shard Default Configuration

The default-cadence single-shard matrix uses the relevant `bft-sharding-compose.yml` settings below. Local helper scripts may be used to reset the stack and collect resource artifacts.

| Setting | Value |
|---|---:|
| Test duration | `30s` |
| `SUBMISSION_WORKERS` | `300` |
| `HTTP_CLIENT_POOL_SIZE` | `48` |
| `PRECOLLECTOR_GRACE_PERIOD` | `150ms` |
| `MONGODB_FINALIZATION_INSERT_CHUNK_SIZE` | `1000` |
| `MONGODB_FINALIZATION_INSERT_CHUNK_WORKERS` | `16` |
| `SKIP_DUPLICATE_CHECK` | `true` |
| `COMMITMENT_STREAM_BUFFER_SIZE` | `50000` |
| `MAX_COMMITMENTS_PER_ROUND` | `20000` |
| `REDIS_ACK_BATCH_SIZE` | `10000` |
| `CONCURRENCY_LIMIT` | `10000` |
| `ROOT_BLOCK_RATE` | `900` |
| `PROOF_INITIAL_DELAY` | `2s` |
| `PROOF_RETRY_DELAY` | `1s` |
| Host CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |

## Single-Shard Scaling Matrix

These runs increase target load on one active shard using the default BFT cadence.

| Target RPS | Submitted | Proofs verified | Client proof p50 | Client proof p95 | Server proofReady p50 | Server proofReady p95 | BFT wait | Finalization | Commitments / round | Redis pending max | Host CPU busy avg/max | Mongo CPU avg | Aggregator CPU avg | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000 | 29,879 / 29,879 | 29,879 / 29,879 | 2.015s | 3.017s | 1.938s | 2.373s | 1.183s | 24ms | 1,270 | 1,275 | 9.1% / 23.4% | 48% | 56% | pass |
| 2,000 | 59,838 / 59,838 | 59,838 / 59,838 | 2.019s | 3.019s | 1.905s | 2.438s | 1.175s | 32ms | 2,432 | 2,700 | 17.6% / 41.8% | 91% | 119% | pass |
| 4,000 | 119,980 / 119,980 | 119,980 / 119,980 | 2.026s | 3.032s | 1.958s | 2.534s | 1.152s | 55ms | 5,074 | 5,100 | 30.8% / 76.9% | 156% | 237% | pass |
| 6,000 | 179,999 / 179,999 | 179,999 / 179,999 | 2.026s | 3.033s | 1.984s | 2.539s | 1.121s | 75ms | 7,500 | 7,500 | 38.7% / 83.0% | 192% | 376% | pass |
| 7,000 | 209,966 / 209,966 | 209,966 / 209,966 | 2.037s | 3.058s | 1.935s | 2.524s | 1.098s | 100ms | 9,172 | 8,742 | 39.7% / 83.1% | 206% | 408% | pass |
| 8,000 | 239,663 / 239,663 | 239,663 / 239,663 | 2.075s | 3.151s | 1.909s | 2.524s | 1.075s | 117ms | 10,557 | 9,933 | 41.8% / 86.2% | 213% | 453% | pass |
| 9,000 | 268,145 / 268,145 | 268,145 / 268,145 | 2.127s | 3.263s | 1.916s | 2.535s | 1.064s | 143ms | 12,237 | 12,265 | 43.3% / 96.2% | 221% | 490% | pass |
| 10,000 | 293,520 / 293,520 | 293,520 / 293,520 | 3.013s | 3.593s | 2.099s | 2.785s | 1.088s | 257ms | 14,573 | 14,746 | 45.2% / 97.1% | 235% | 539% | borderline |

## Multi-Shard Scaling

These runs use the default BFT cadence and split load across active shard targets.

On one machine, adding shards mostly redistributes work; it does not add CPU, disk, or network capacity. More shards reduce commitments per round per shard and lower finalization/Mongo pressure, but higher throughput scaling should be measured with shards spread across separate machines or with the perf client moved off the aggregator host.

| Active shards | Mongo groups | Target RPS | Achieved RPS | Submitted | Client proof p50 | Client proof p95 | Server proofReady p50 | Server proofReady p95 | BFT wait | Finalization | Commitments / round / shard | Host CPU busy | Aggregator CPU avg | Mongo CPU avg | Perf tool CPU avg | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 2 | 8,000 | 7,974 | 239,245 / 239,245 | 2.048s | 3.127s | 1.884s | 2.454s | 1.123s | 82ms | 4,871 | 37.5% / 89.8% | 317% total | 203% total | 608% | pass |
| 2 | 2 | 9,000 | 8,850 | 265,964 / 265,964 | 2.098s | 3.264s | 1.923s | 2.525s | 1.112s | 91ms | 5,630 | 38.0% / 90.3% | 345% total | 204% total | 626% | pass |
| 2 | 2 | 10,000 | 9,523 | 286,420 / 286,420 | 2.172s | 3.509s | 1.880s | 2.524s | 1.061s | 144ms | 6,459 | 41.6% / 98.1% | 374% total | 204% total | 680% | borderline |
| 4 | 2 | 9,000 | 8,958 | 269,375 / 269,375 | 2.035s | 2.593s | 1.914s | 2.468s | 1.113s | 87ms | 2,796 | 38.5% / 95.0% | 431% total | 187% total | 638% | pass |
| 4 | 2 | 10,000 | 9,685 | 291,580 / 291,580 | 2.142s | 3.038s | 1.946s | 2.519s | 1.089s | 127ms | 3,092 | 38.3% / 98.4% | 454% total | 181% total | 654% | borderline |
| 8 | 2 | 10,000 | 9,646 | 292,200 / 292,200 | 2.099s | 2.784s | 1.922s | 2.485s | 1.094s | 107ms | 1,549 | 37.3% / 98.2% | 437% total | 155% total | 649% | borderline |

## Low-Latency BFT Cadence

These runs test whether lower BFT cadence can move proof latency closer to 1s. Keep this separate from the default `ROOT_BLOCK_RATE=900` throughput matrix because it changes consensus timing.

Current low-latency settings:

| Setting | Value |
|---|---:|
| `ROOT_BLOCK_RATE` | `400` |
| `PRECOLLECTOR_GRACE_PERIOD` | `100ms` |
| `MONGODB_FINALIZATION_INSERT_CHUNK_SIZE` | `1000` |
| `MONGODB_FINALIZATION_INSERT_CHUNK_WORKERS` | `16` |
| `SKIP_DUPLICATE_CHECK` | `true` |
| `COMMITMENT_STREAM_BUFFER_SIZE` | `50000` |
| `MAX_COMMITMENTS_PER_ROUND` | `20000` |
| `REDIS_ACK_BATCH_SIZE` | `10000` |
| `CONCURRENCY_LIMIT` | `10000` |
| Mongo layout | `MONGO_GROUPS=2` |
| `PROOF_INITIAL_DELAY` | `1.1s` |

| Target RPS | Submitted | Proofs verified | Client proof p50 | Client proof p95 | Server proofReady p50 | Server proofReady p95 | BFT wait | Finalization | Commitments / round | Host CPU busy | Aggregator CPU avg | Mongo CPU avg | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000 | 29,701 / 29,701 | 29,701 / 29,701 | 1.113s | 1.118s | 992ms | 1.021s | 489ms | 16ms | 605 | 9.5% | 59% | 46% | pass |
| 4,000 | 119,700 / 119,700 | 119,700 / 119,700 | 1.115s | 1.133s | 868ms | 1.103s | 462ms | 45ms | 2,395 | 30.2% | 238% | 163% | pass |
| 6,000 | 179,701 / 179,701 | 179,701 / 179,701 | 1.115s | 1.611s | 894ms | 1.132s | 438ms | 62ms | 3,327 | 39.7% | 376% | 217% | pass |
| 7,000 | 209,794 / 209,794 | 209,794 / 209,794 | 1.118s | 1.635s | 931ms | 1.228s | 480ms | 70ms | 4,486 | 40.4% | 414% | 205% | pass |
| 8,000 | 239,807 / 239,807 | 239,807 / 239,807 | 1.129s | 1.677s | 975ms | 1.275s | 490ms | 86ms | 5,225 | 40.8% | 453% | 213% | pass |
| 9,000 | 269,241 / 269,241 | 269,241 / 269,241 | 1.197s | 1.779s | 1.050s | 1.382s | 537ms | 103ms | 6,464 | 42.6% | 503% | 210% | boundary |

### Lower BFT Cadence Variant

This variant uses `ROOT_BLOCK_RATE=350`, `PRECOLLECTOR_GRACE_PERIOD=75ms`, `PROOF_INITIAL_DELAY=1s`, and `PROOF_RETRY_DELAY=500ms`. At 8k/s, most proofs are ready on the first 1s poll, while the remaining tail succeeds on the second poll.

| Target RPS | Submitted | Proofs verified | Attempt 1 proofs | Client proof p50 | Client proof p95 | Server proofReady p50 | Server proofReady p95 | BFT wait | Finalization | Commitments / round | Host CPU busy | Aggregator CPU avg | Mongo CPU avg | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 4,000 | 119,704 / 119,704 | 119,704 / 119,704 | 119,200 / 119,704 (99.6%) | 1.013s | 1.027s | 782ms | 998ms | 415ms | 40ms | 2,090 | 30.1% / 77.1% | 196% | 160% | pass |
| 8,000 | 239,843 / 239,843 | 239,843 / 239,843 | 189,159 / 239,843 (78.9%) | 1.024s | 1.570s | 885ms | 1.161s | 442ms | 89ms | 4,818 | 41.3% / 82.4% | 404% | 208% | pass |

## Gateway Throughput

Gateway tests use the fixed 2-shard `bft-sharding-compose.yml` stack. Request path: gateway -> HAProxy -> active shard leader.

| Shards | Target RPS | Achieved RPS | Submitted | Proofs verified | Host CPU busy avg/max | Gateway CPU avg | Perf client CPU avg | Aggregator CPU total avg | Mongo CPU total avg | HAProxy CPU total avg | Shard proofReady p50 | Client proof p50 | BFT wait | Finalization | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 4,000 | 3,992.79 | 119,981 / 119,981 | 119,981 / 119,981 | 40.5% / 75.7% | 183% | 329% | 445% | 150% | 37% | 1.949s | 2.018s | 1.154s | 47ms | pass |
| 2 | 6,000 | 5,988.67 | 179,981 / 179,981 | 179,981 / 179,981 | 47.3% / 83.2% | 228% | 458% | 494% | 169% | 43% | 1.875s | 2.027s | 1.137s | 69ms | pass |
| 2 | 7,000 | 6,970.45 | 209,844 / 209,844 | 209,844 / 209,844 | 48.4% / 93.1% | 216% | 494% | 512% | 175% | 46% | 1.895s | 2.058s | 1.120s | 85ms | pass |
| 2 | 8,000 | 7,876.84 | 238,321 / 238,321 | 238,321 / 238,321 | 53.0% / 98.9% | 216% | 562% | 562% | 210% | 57% | 2.301s | 3.069s | 1.113s | 371ms | boundary |

The 8k gateway run hit the latency ceiling rather than a correctness or connection ceiling: all submissions and proofs completed, but proof request duration, finalization time, aggregator CPU, Mongo CPU, perf-client CPU, and host CPU all rose together. This points to whole-stack pressure on the single test machine, with proof polling and larger finalization batches amplifying backend load once the system is near saturation.

### Gateway Mock Backend Ceiling

This isolates the gateway path by replacing real aggregators with lightweight mock backends. It keeps gateway -> HAProxy -> backend routing and delayed proof polling, but removes BFT, MongoDB, Redis, and proof verification.

| Target RPS | Submitted | Submit rate | Host CPU busy avg/max | Gateway CPU avg | Load client CPU avg | Mock backend CPU total avg | HAProxy CPU total avg | Proof p50 | Proof p95 | Proof request p50 | Proof request p95 | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 15,000 | 449,600 | ~14,987/s | 73.7% / 88.5% | 639% | 451% | 563% | 137% | 2.015s | 2.031s | 13ms | 26ms | clean |
| 18,000 | 539,116 | ~17,971/s | 77.4% / 97.4% | 732% | 479% | 616% | 157% | 2.017s | 2.050s | 15ms | 42ms | clean |
| 19,000 | 563,907 | ~18,797/s | 78.5% / 98.6% | 709% | 481% | 619% | 159% | 2.019s | 2.505s | 16ms | 230ms | boundary |
| 20,000 | 561,367 | ~18,712/s | 83.5% / 99.1% | 737% | 514% | 711% | 172% | 2.608s | 4.088s | 377ms | 879ms | overloaded |

The mock test shows the gateway path can exceed the real aggregator-stack ceiling on this host. The clean single-machine mock ceiling is around 18k/s; above that, proof request latency rises as gateway, mock backends, HAProxy, and the load client compete for the same CPU.
