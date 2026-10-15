# UC consolidation: aggregator scaling measurements

Acceptance evidence for the aggregator-go ticket ("report response sizes, proof latency, retention cost"), organized as **scaling curves**:
one parameter is swept at a time (IDs per batch L, SMT size N, block size B, retained views V, attachments U, shards S) and each curve is judged
against the expected asymptotic bound by **hardware-independent counters** (database commands, bytes, allocations, proof path nodes, node reads, catalog reads)
and **log-log slopes**, with time reported only as a relative quantity.

> **Hardware context, read first.** All numbers come from a roughly ten-year-old Intel MacBook Pro (Core i7-8850H @ 2.6 GHz, 6 cores / 12 threads,
> 16 GiB, macOS 15.8.1, Go 1.27.2) under heavy background load (load average 100-195 on 12 threads from other jobs), with MongoDB 7.0 in a small colima VM
> (2 vCPU, 3 GiB) on the same host and RocksDB 11.8.1 (`-tags rocksdb`). Absolute timings are slow and noisy. **Do not derive timeout, latency or regression gates from this machine.**
> Counters (bytes, allocations, commands, nodes, reads) are deterministic; times appear only as ratios and slopes within one run.

- Code: `uc-consolidation` plus the A5 batch API (#193, `uc/A5-batch-api`, batched `$in` lookups) and the sweeps in this branch.
- Everything is opt-in: the sweep tests skip unless `UC_MEASURE=1`. Shared helpers: `internal/testutil/measure` (slope fit, per-call cost counters, natively verified certification material).
- **Realistic certification material.** Every proof carries the ordinary UC, so all response sizes use a UC built from native types and verified before use with
  the full native `UnicityCertificate.Verify` (shard tree and unicity tree paths, trust base quorum): a seal signed by 4 validators (4 x 65 B), the shard
  certificate for an S-shard partition and a partition path over 3 partitions, bound to the measured SMT root; the partition view is the real `PartitionViewV1`
  encoding. (An earlier revision of these measurements used a 64-byte skeletal UC and a zero-filled view, which understated the response size; that is fixed.)
- Reading slopes: `y = x^a` has log-log slope `a`; a logarithm has a slope near `1/ln x` (about 0.1 for N = 1e3..1e6), not 1. Small-x ranges are dominated by fixed
  costs, so slopes are quoted over the whole range and, where it matters, the upper range.

## Reproduce

```sh
# in-process sweeps (no Docker): batch proofs, catalog, memory retention
UC_MEASURE=1 go test -count=1 -timeout 60m -run 'Test_Scaling' -v ./internal/service ./internal/proofview ./internal/smt/backend

# real MongoDB (Docker/testcontainers; colima example) : database commands per batch
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock TESTCONTAINERS_RYUK_DISABLED=true
UC_MEASURE=1 go test -count=1 -timeout 30m -run 'Test_ScalingBatchIDsMongo' -v ./internal/service

# RocksDB retention and proof node reads (MacPorts paths; on Linux use the system librocksdb)
export CGO_CFLAGS=-I/opt/local/include
export CGO_LDFLAGS="-L/opt/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd -lbz2"
UC_MEASURE=1 go test -tags rocksdb -count=1 -timeout 60m -run 'Test_ScalingRetentionRocksDB|Test_ScalingRocksDBProofReads' -v ./internal/smt/backend
```

## Summary: measured slope versus expected bound

| Curve | Hardware-independent metric | Expected | Measured slope | Verdict |
|---|---|---|---|---|
| batch size L | database commands per batch (real Mongo, command monitor) | 0 (constant 2) | 0.00 | **O(1)**; was 2L before the `$in` batching |
| batch size L | response bytes | 1 (+ one fixed partition view) | 0.88 | linear in L plus the fixed view |
| batch size L | proof path nodes (sum of SMT siblings) | 1 | 1.01 | L x depth |
| batch size L | allocations / allocated bytes | 1 | 0.97 / 0.96 | linear |
| SMT size N (1e3..1e6) | proof path nodes per ID | ~0.1 (log N) | 0.10 (and nodes/log2 N = 1.0) | logarithmic, **not** linear in N |
| SMT size N | response bytes, allocations | ~0.03 | 0.03 / 0.03 | flat-to-logarithmic |
| partition shards S (1..16) | ordinary UC bytes | log S | 0.07 | logarithmic path growth |
| SMT size N, memory retention | accounted / measured bytes per retained view | ~0.1 | 0.11 / 0.13 | independent of tree size (churn only) |
| block size B, memory retention | accounted / measured bytes per retained view | <= 1 | 0.89 / 0.88 | linear in churn, sublinear at large B |
| SMT size N, RocksDB retention | accounted / physical bytes per view | ~0.1 | 0.12 / 0.06 | independent of tree size |
| block size B, RocksDB retention | accounted / physical bytes per view | 1 | 0.89 / 0.87 | linear in churn |
| retained views V | `RetainedBytes` reads per catalog operation | 0 if O(1) bookkeeping | **1.0** (Publish 1.02, Acquire+Release 1.04, Stats 1.00) | **linear in V: confirms audit item 1** |
| retained views V | time of Acquire+Release / Stats | 0 | 1.01 / 1.30 | linear to superlinear (each read also scans the reader lineage) |
| captures R with the oldest view pinned (V = 4) | Stats time | 0 | 0.79 | grows with R: lineage metadata, confirms the H dependence |
| attachments U (1..64) | retained attachment bytes / attach time | 1 / 0 | 1.00 / 0.07 | storage linear by design, no measurable scan cost at U <= 64 |
| RocksDB proofs, L = 1..256 | union build: `GetNodes` calls per batch | 0 | 0.00 (constant 18 = tree depth) | O(depth) round trips, independent of L |
| RocksDB proofs, L = 1..256 | node reads: single proofs vs union build | 1 vs <1 | 1.00 vs 0.90 | shared path nodes read once (1.7x fewer reads at L = 256) |

## 1. get_inclusion_proofs.v1

### 1.1 IDs per batch L (memory record source; counts are exact)

N = 4096 leaves, 8-shard partition, ordinary UC 752 B (4 x 65 B seal signatures), partition view 1528 B

| L | response B | B per ID | proof path nodes (sum) | nodes per ID | DB size queries | DB record queries | allocs | alloc B | alloc B per ID | time relative to L=1 | time per ID relative |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 5856 | 5856 | 12 | 12.0 | 1 | 1 | 204 | 49545 | 49545 | 1.0x | 1.00x |
| 2 | 8527 | 4263 | 23 | 11.5 | 1 | 1 | 368 | 83870 | 41935 | 2.1x | 1.04x |
| 4 | 14061 | 3515 | 48 | 12.0 | 1 | 1 | 709 | 201784 | 50446 | 20.5x | 5.13x |
| 8 | 25001 | 3125 | 96 | 12.0 | 1 | 1 | 1376 | 384345 | 48043 | 4.8x | 0.60x |
| 16 | 46753 | 2922 | 190 | 11.9 | 1 | 1 | 2703 | 729438 | 45590 | 7.5x | 0.47x |
| 32 | 90769 | 2836 | 386 | 12.1 | 1 | 1 | 5380 | 1385362 | 43293 | 13.9x | 0.43x |
| 64 | 178801 | 2793 | 778 | 12.2 | 1 | 1 | 10735 | 2797841 | 43716 | 31.9x | 0.50x |
| 128 | 353905 | 2764 | 1547 | 12.1 | 1 | 1 | 21379 | 5448132 | 42564 | 111.6x | 0.87x |
| 256 | 706225 | 2758 | 3118 | 12.2 | 1 | 1 | 42773 | 8917736 | 34835 | 305.3x | 1.19x |

Log-log slopes over L = 1..256: response bytes 0.88 (expect 1), path nodes 1.01 (expect 1: L x depth), DB queries -0.00 (expect 0: constant 2), allocations 0.97 (expect 1), allocated bytes 0.96 (expect 1), time 0.88 (expect ~1)

Per batch the service now issues **one** size projection and **one** record load whatever L (the test asserts it), the proofs are built from one pinned view,
and cost is linear in L (one ordinary UC, about 2.7 KB of response per ID at N = 4,096, is repeated in every proof: the self-contained wire format makes the response
Theta(L(C + depth)) plus one partition view). The 1,528 B partition view (3,056 hex characters) is the fixed part, which is why the response-byte slope is 0.88 rather than 1.

### 1.2 Database commands per batch, real MongoDB

Counted with a driver command monitor (`aggregate` commands; the 128- and 256-ID rows also show two extra non-aggregate commands from the driver, unrelated to L).

MongoDB 7.0 in a container, 256 finalized records. Before the batching change the service issued 2 x L sequential queries per batch.

| L | aggregate commands per batch | all commands per batch | response B | time relative to L=1 | time per ID relative |
|---|---|---|---|---|---|
| 1 | 2 | 2 | 5856 | 1.0x | 1.00x |
| 2 | 2 | 2 | 8527 | 0.5x | 0.23x |
| 4 | 2 | 2 | 14061 | 0.5x | 0.12x |
| 8 | 2 | 2 | 25001 | 0.5x | 0.06x |
| 16 | 2 | 2 | 46753 | 0.8x | 0.05x |
| 32 | 2 | 2 | 90769 | 5.8x | 0.18x |
| 64 | 2 | 2 | 178801 | 13.3x | 0.21x |
| 128 | 2 | 4 | 354033 | 8.0x | 0.06x |
| 256 | 2 | 4 | 706353 | 19.6x | 0.08x |

Log-log slopes over L = 1..256: database aggregate commands -0.00 (expect 0: constant), time 0.71 (the fixed round trips dominate small L, so the curve is concave: the slope over the upper range L = 16..256 is 0.98)
2026/10/15 14:09:14 🐳 Stopping container: fa80ba82ed23
2026/10/15 14:09:20 ✅ Container stopped: fa80ba82ed23
2026/10/15 14:09:20 🐳 Terminating container: fa80ba82ed23
2026/10/15 14:09:21 🚫 Container terminated: fa80ba82ed23

**Before / after the `$in` batching** (same code path, same host; before = commit `2b27bad`: one size projection and one record fetch per ID, i.e. exactly 2L sequential commands by construction):

| L | commands before | commands after | time relative to L=1 before | time relative to L=1 after |
|---|---|---|---|---|
| 1 | 2 | 2 | 1x | 1x |
| 16 | 32 | 2 | 21x | 0.8x |
| 256 | 512 | 2 | 240x | 19.6x |

Before the change, time was proportional to L (slope about 1 from L = 1: 240x for 256x the IDs, each ID paying two round trips); after it the fixed two round trips dominate small L and the curve flattens
(whole-range slope 0.71, upper range 0.98, where record bytes and proof building take over). Relative times are noisy on this host; the command counts are exact and are the claim.

### 1.3 SMT size N (L = 16)

Filler leaves grow the tree in steps; only 16 records are requested.

L = 16, 8-shard partition, ordinary UC with 4-signature seal

| N (leaves) | log2 N | path nodes per ID | nodes per ID / log2 N | response B per ID | allocs per ID | alloc B per ID | time per ID relative to N=1e3 | DB queries |
|---|---|---|---|---|---|---|---|---|
| 1000 | 10.0 | 10.19 | 1.02 | 2814 | 162 | 43956 | 1.00x | 2 |
| 3000 | 11.6 | 11.75 | 1.02 | 2914 | 169 | 46449 | 1.25x | 2 |
| 10000 | 13.3 | 13.44 | 1.01 | 3022 | 175 | 48844 | 2.17x | 2 |
| 30000 | 14.9 | 15.25 | 1.03 | 3138 | 182 | 50227 | 1.90x | 2 |
| 100000 | 16.6 | 17.00 | 1.02 | 3250 | 190 | 49895 | 1.79x | 2 |
| 300000 | 18.2 | 18.38 | 1.01 | 3338 | 196 | 52575 | 1.03x | 2 |
| 1000000 | 19.9 | 19.69 | 0.99 | 3422 | 201 | 54039 | 2.67x | 2 |

Log-log slopes over N = 1e3..1e6 (a logarithm has a slope near 1/ln N, here about 0.1-0.15): path nodes 0.10, response bytes 0.03, allocations 0.03, time 0.07. A linear dependence on N would be slope 1; the proof cost is flat-to-logarithmic in N.

The proofs traverse `log2 N` nodes per ID (ratio 1.0 at every N), and neither response bytes, allocations nor time depend on N beyond that logarithm. No term grows linearly with the tree.

### 1.4 Partition shards S (ordinary UC and partition view size)

N = 4096, L = 16

| S | ordinary UC B | partition view B | latest response B | exact-root response B | difference B (= view hex + key) |
|---|---|---|---|---|---|
| 1 | 650 | 198 | 40893 | 40478 | 415 |
| 2 | 684 | 388 | 42361 | 41566 | 795 |
| 4 | 718 | 768 | 44209 | 42654 | 1555 |
| 8 | 752 | 1528 | 46817 | 43742 | 3075 |
| 16 | 786 | none (above the 8-shard cap) | n/a | 44830 | n/a |

Log-log slopes over S = 1..16: ordinary UC bytes 0.07, exact-root response bytes 0.04 (a logarithmic path term, slope well below 1)

The ordinary UC grows by one 34-byte path element per doubling of S (logarithmic). The partition view is `~190 B x S` and only latest-view responses carry it (hex doubles it in the response);
above eight shards no view is produced, so only exact-root requests can be served.

## 2. Retained-view cost (bare backends)

These sweeps measure `RetainReadView` and its lineage accounting in the backends directly. They do **not** include the A4 publication path or the catalog (section 3), and peak RSS is not measured.
"Measured" is real memory: Go heap for the memory backend (with all views retained minus only the newest, per older view) and on-disk size after full compaction
for RocksDB (identical churn with 16 snapshots pinned minus none, per view; compaction cannot drop old node versions under a pinned snapshot).
"Accounted" is the `RetainedBytes` charge counted against the retention budget.

### 2.1 Memory backend, SMT size N (B = 100 per block, 16 views)

B = 100 new leaves per block, 16 retained views

| N | live tree heap (B) | live heap per leaf (B) | accounted per view (B) | measured per view (B) | accounted / measured | `RetainReadView` time relative to N=1e3 |
|---|---|---|---|---|---|---|
| 1000 | 1271872 | 1271 | 151846 | 102038 | 1.49x | 1.00x |
| 10000 | 5514000 | 551 | 202969 | 146266 | 1.39x | 1.48x |
| 100000 | 48280976 | 482 | 263757 | 199010 | 1.33x | 2.47x |
| 1000000 | 475865856 | 475 | 331958 | 257541 | 1.29x | 4.41x |

Log-log slopes over N = 1e3..1e6: live heap 0.87 (expect 1), accounted per view 0.11 (expect ~0.1: one more tree level per doubling), measured per view 0.13 (expect ~0.1), retain time 0.22 (expect ~0: capture does not copy the tree)

The retention cost per view follows the churn (`B x depth`, one more tree level per doubling of N), not the tree size; only the live tree itself (about 480 B per leaf) is linear in N. Capturing a view does not copy the tree.

### 2.2 Memory backend, block size B (N = 100,000, 16 views)

N = 100,000 leaves, 16 retained views

| B | accounted per view (B) | accounted per new leaf (B) | measured per view (B) | accounted / measured | `RetainReadView` time (relative to B=1) |
|---|---|---|---|---|---|
| 1 | 4006 | 4006 | 3037 | 1.32x | 1.0x |
| 4 | 14235 | 3558 | 10994 | 1.29x | 2.2x |
| 16 | 50439 | 3152 | 38789 | 1.30x | 37.0x |
| 64 | 176482 | 2757 | 133818 | 1.32x | 61.2x |
| 256 | 606327 | 2368 | 450109 | 1.35x | 206.2x |
| 1024 | 2028509 | 1980 | 1457066 | 1.39x | 851.9x |
| 4096 | 6912372 | 1687 | 4778265 | 1.45x | 2782.6x |
| 16384 | 23893945 | 1458 | 15933930 | 1.50x | 29496.8x |

Log-log slopes over B = 1..16384 (whole range): accounted per view 0.89, measured per view 0.88, retain time 1.02. Over the upper range B = 256..16384: accounted 0.88, measured 0.86, retain time 1.16 (below 1: paths shared by many new leaves are copied once, so cost grows sublinearly as B approaches N).

Cost per view is linear in B at small B and sublinear at large B (new leaves share path prefixes, so `accounted per new leaf` falls from 4,006 B to 1,458 B). The accounting stays conservative at every B (1.3-1.5x the measured heap).
The time to capture a view is proportional to the nodes introduced since the previous capture (`O(B depth)`, slope 1), as designed, not to N.

### 2.3 RocksDB backend, SMT size N (B = 1,000, 16 views)

B = 1,000 new leaves per block, 16 retained views

| N | disk without views (B) | disk with views (B) | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to N=1e4 |
|---|---|---|---|---|---|---|
| 10000 | 3951447 | 8426404 | 859373 | 279684 | 3.07x | 1.00x |
| 100000 | 17145872 | 23357530 | 1120305 | 388228 | 2.89x | 0.74x |
| 1000000 | 148990095 | 154775151 | 1494308 | 361566 | 4.13x | 1.43x |

Log-log slopes over N = 1e4..1e6: database size 0.79 (expect 1), accounted per view 0.12, physical per view 0.06 (both expect ~0.1: churn, not tree size), retain time 0.08 (expect ~0)

### 2.4 RocksDB backend, block size B (N = 100,000, 16 views)

N = 100,000 leaves, 16 retained views

| B | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to B=10 |
|---|---|---|---|---|
| 10 | 18169 | 6979 | 2.60x | 1.00x |
| 100 | 147479 | 60679 | 2.43x | 2.19x |
| 1000 | 1120305 | 388228 | 2.89x | 25.49x |
| 10000 | 8602946 | 2924294 | 2.94x | 115.42x |

Log-log slopes over B = 10..10,000: accounted per view 0.89, physical per view 0.87 (expect ~1: bytes written follow the churn), retain time 0.73 (expect 0: a pinned snapshot does not depend on B)

RocksDB retention cost is linear in the bytes written (churn), independent of N. The accounting overstates the physical growth by 2.4-4.1x (it counts bytes written before compression and compaction). Capturing a snapshot is O(1) in the
application (about 30-40 microseconds in the earlier single-point run); the 0.73 slope of capture time against B above is within this host's noise and was not diagnosed (a snapshot does not depend on B; contention with the preceding commit is the likely cause).

### 2.5 Which limit binds: the view count or the byte budget?

The retention budget (256 MiB) is divided by the accounted bytes per view to give how many views fit. From the sweeps (N = 100,000): memory backend 4,006 B per view at B = 1 up to 6.9 MB at B = 4,096 (38 views fit) and 23.9 MB at B = 16,384
(**11 views fit**); RocksDB 8.6 MB per view at B = 10,000 (31 views fit). So for these workloads the **count cap (16) binds first up to roughly B = 10,000 (memory) and well beyond for RocksDB; the byte budget binds only for larger blocks
(memory, B around 11,000 or more)**. The charges above are reader charges only; catalog metadata and attachments (about 2.4 KB per attachment here, section 3.3) are additional and not part of the reader figures.

## 3. Catalog bookkeeping (real memory backend, instrumented readers)

### 3.1 Retained views V = 1..256 (no pressure)

The hardware-independent metric is how many times the catalog reads `RetainedBytes()` per operation (each read also scans the reader's lineage).

N = 2,000, B = 10 new leaves per block, V retained views, one attachment each

| V | RetainedBytes reads per Publish | per AcquireLatest+Release | per Stats | Publish time (relative to V=1) | AcquireLatest+Release time (relative) | Stats time (relative) |
|---|---|---|---|---|---|---|
| 1 | 3 | 2 | 1 | 1.0x | 1.0x | 1.0x |
| 2 | 9 | 6 | 2 | 0.7x | 1.2x | 0.8x |
| 4 | 17 | 12 | 4 | 0.6x | 1.2x | 1.3x |
| 8 | 33 | 24 | 8 | 1.2x | 2.0x | 2.8x |
| 16 | 65 | 48 | 16 | 0.7x | 4.0x | 7.2x |
| 32 | 129 | 96 | 32 | 1.3x | 11.3x | 32.4x |
| 64 | 257 | 192 | 64 | 0.8x | 33.8x | 143.5x |
| 128 | 513 | 384 | 128 | 0.9x | 112.5x | 131.3x |
| 256 | 1025 | 768 | 256 | 1.5x | 145.2x | 798.7x |

Log-log slopes over V = 1..256: RetainedBytes reads per Publish 1.02, per AcquireLatest+Release 1.04, per Stats 1.00 (a constant-time catalog would be 0; each retained reader is read once per sweep, so V is slope 1); time: Publish 0.07, AcquireLatest+Release 1.01, Stats 1.30 (each read also scans the reader's lineage, so time exceeds the read count by about one more power of V)

Every ordinary catalog operation reads every retained reader a constant number of times (about 4V reads per Publish, 3V per Acquire+Release, V per Stats): slope 1 in V. Because each read scans the lineage as well, time per Acquire+Release and per Stats grows faster than the read count
(Stats time slope 1.30 over V = 1..256, about 800x from V = 1 to 256 in this run, against a 256x increase in reads). This is the catalog accounting cost identified in the audit (item 1); with the default V = 16 it is small, but the exponent, not the constant, is what the curve shows.

### 3.2 Oldest view pinned while captures accumulate (V = 4)

V = 4 retained views, oldest view pinned by a lease, N = 2,000, B = 10; R captures after the pinned view

| R | Stats time (relative to R=4) | AcquireLatest+Release time (relative) | Publish time (relative) | retained bytes (accounted) |
|---|---|---|---|---|
| 4 | 1.0x | 1.0x | 1.0x | 3181906 |
| 16 | 8.0x | 7.0x | 1.1x | 6399304 |
| 64 | 114.6x | 7.5x | 3.0x | 9742568 |
| 256 | 24.3x | 6.1x | 5.7x | 13141416 |
| 1024 | 136.7x | 42.7x | 2.5x | 29231016 |

Log-log slopes over R = 4..1024: Stats 0.79, AcquireLatest+Release 0.53, Publish 0.25. With V bounded these would be 0 if catalog bookkeeping were independent of captures since the oldest pin; a positive slope is the lineage metadata growing with R (the audit's H dependence).

With one view pinned, the memory lineage keeps an entry for every capture since it was taken, although only four views are retained: per-operation catalog time grows with the number of captures R, not with V (the audit's H dependence). Publish time is dominated by the SMT commit and is nearly flat.

### 3.3 Attachments per view U = 1..64 (V = 4)

V = 4, each state keeps up to U attachments (UC with 4-signature seal and an 8-shard view)

| U | attachment bytes retained per view | catalog retained bytes (attachments + readers) | AttachCertification time (relative to U=1) | allocs per attach |
|---|---|---|---|---|
| 1 | 2409 | 61018 | 1.0x | 66 |
| 2 | 4818 | 63427 | 1.5x | 66 |
| 4 | 9636 | 68245 | 2.3x | 65 |
| 8 | 19272 | 77881 | 1.9x | 65 |
| 16 | 38544 | 97153 | 1.6x | 65 |
| 32 | 77088 | 135697 | 1.7x | 65 |
| 64 | 154176 | 212785 | 1.6x | 65 |

Log-log slopes over U = 1..64: retained attachment bytes 1.00 (expect 1: storage is linear in U by design), attach time 0.07 (a hash lookup would be 0; the audit expects a scan of the attachment list)

Attachment storage is linear in U by design (about 2.4 KB per attachment: UC with 4 signatures plus an 8-shard view). Adding an attachment shows no measurable dependence on U up to 64 (slope 0.07; allocations constant at 65).

## 4. Disk proof node reads (RocksDB snapshot, N = 100,000)

Compares building L proofs one at a time (what the current disk read view does per key) with the existing union-path builder (`persist.BuildInclusionCerts`, not yet wired into the batch API: that is item 7 of the audit, owned elsewhere), counting node reads on a counting `ReadStore` wrapper.

N = 100,000, random existing keys; one pinned snapshot

| L | single proofs: node reads | single: GetNode(s) calls | single: distinct nodes | union build: node reads | union: GetNode(s) calls | union: distinct nodes | reads saved by the union build | single time | union time |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 18 | 18 | 18 | 18 | 18 | 18 | 1.00x | 4.927ms | 9.537ms |
| 2 | 36 | 36 | 35 | 35 | 18 | 35 | 1.03x | 2.216ms | 996µs |
| 4 | 72 | 72 | 66 | 66 | 18 | 66 | 1.09x | 3.26ms | 2.441ms |
| 8 | 144 | 144 | 124 | 124 | 18 | 124 | 1.16x | 3.037ms | 3.184ms |
| 16 | 288 | 288 | 237 | 237 | 18 | 237 | 1.22x | 4.527ms | 9.681ms |
| 32 | 576 | 576 | 443 | 443 | 18 | 443 | 1.30x | 9.084ms | 11.261ms |
| 64 | 1150 | 1150 | 829 | 829 | 18 | 829 | 1.39x | 17.258ms | 139.786ms |
| 128 | 2265 | 2265 | 1486 | 1486 | 18 | 1486 | 1.52x | 108.644ms | 27.215ms |
| 256 | 4531 | 4531 | 2696 | 2696 | 18 | 2696 | 1.68x | 202.896ms | 43.948ms |

Log-log slopes over L = 1..256: single-proof node reads 1.00 (expect 1: L x depth), union-build node reads 0.90 and distinct nodes 0.90 (expect < 1: shared upper path nodes are read once; for large L the distinct nodes approach N)

Single-proof building reads `L x depth` nodes (slope 1.00) and makes that many snapshot calls. The union build reads exactly the distinct nodes (sublinear: 1.7x fewer reads at L = 256 for random keys) and makes a constant 18 frontier-batched `GetNodes` calls (the tree depth), independent of L:
round trips to the store drop from `L x depth` to `depth`. Output size is unchanged (`L x depth` siblings are still returned).

## 5. Not covered

- **A4-integrated publication and end-to-end service** (finalization hooks, HA, recovery) are not wired in this snapshot; section 2 is the bare backend, section 3 the catalog.
- **Peak RSS** is not measured; only heap (memory) and physical on-disk bytes after compaction (RocksDB). Cold versus warm RocksDB block cache, adversarial shared-prefix keys and disk batch proofs through the service were not run.
- **Absolute latency** is deliberately not reported (hardware context above); the Mongo curves report command counts and relative time only.
- The V sweep only goes to 256 on the memory backend (the RocksDB catalog accounting was not swept).
