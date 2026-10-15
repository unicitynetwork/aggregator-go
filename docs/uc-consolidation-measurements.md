# UC consolidation: aggregator scaling measurements

Acceptance evidence for the aggregator-go ticket ("report response sizes, proof latency, retention cost"), organized as **scaling curves**:
one parameter is swept at a time (IDs per batch L, SMT size N, block size B, retained views V, attachments U, shards S) and each curve is judged
against the expected asymptotic bound by **hardware-independent counters** (database commands, bytes, allocations, proof path nodes, node reads, catalog reads)
and **log-log slopes**, with time reported only as a relative quantity.

> **Hardware context, read first.** All numbers come from a roughly ten-year-old Intel MacBook Pro (Core i7-8850H @ 2.6 GHz, 6 cores / 12 threads,
> 16 GiB, macOS 15.8.1, Go 1.27.2) under heavy background load (load average 100-195 on 12 threads from other jobs), with MongoDB 7.0 in a small colima VM
> (2 vCPU, 3 GiB) on the same host and RocksDB 11.8.1 (`-tags rocksdb`). Absolute timings are slow and noisy. **Do not derive timeout, latency or regression gates from this machine.**
> Counters (bytes, allocations, commands, nodes, reads) are deterministic; times appear only as ratios and slopes within one run.

- Code: `uc-consolidation` @ `7d052fc` (A2-A6, D1 disk batch proofs, P1-P4 asymptotic fixes merged) plus the sweeps in this branch. The retention, proof and catalog curves were regenerated on that code.
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
| batch size L | database commands per batch, all commands incl. `getMore` (real Mongo, command monitor) | 0 (constant 2) | 0.00 | **O(1)**; was 2L before the `$in` batching |
| batch size L | response bytes | 1 (+ one fixed partition view) | 0.88 | linear in L plus the fixed view |
| batch size L | proof path nodes (sum of SMT siblings) | 1 | 1.01 | L x depth |
| batch size L | allocations / allocated bytes | 1 | 0.97 / 0.99 | linear |
| SMT size N (1e3..1e6) | proof path nodes per ID | ~0.1 (log N) | 0.10 (and nodes/log2 N = 1.0) | logarithmic, **not** linear in N |
| SMT size N | response bytes, allocations | ~0.03 | 0.03 / 0.03 | flat-to-logarithmic |
| partition shards S (1..16) | ordinary UC bytes | log S | 0.07 | logarithmic path growth |
| SMT size N, memory retention | accounted / measured bytes per retained view | ~0.1 | 0.11 / 0.13 | independent of tree size (churn only) |
| block size B, memory retention | accounted / measured bytes per retained view | <= 1 | 0.89 / 0.88 | linear in churn, sublinear at large B |
| SMT size N, RocksDB retention | accounted / physical bytes per view | ~0.1 | 0.12 / 0.06 | independent of tree size |
| block size B, RocksDB retention | accounted / physical bytes per view | 1 | 0.89 / 0.87 | linear in churn |
| retained views V | `RetainedBytes` reads per catalog operation | 0 if O(1) bookkeeping | before #200: **1.0**; merged code with the backend accountant: **0.00** | audit item 1 confirmed, and fixed by #200 |
| retained views V | time of Acquire+Release / Stats (relative) | 0 | before: 1.25 / 1.49; after, with accountant: 0.06 / 0.09 | flat after the fix |
| captures R with the oldest view pinned (V = 4) | Stats time (relative) | 0 | before: 0.79 (lineage entries kept 0.96, visited per call 0.99); after: 0.01 | grew with R before the fix, flat after |
| attachments U (1..64) | measured attachment bytes / allocations per attach | 1 / 0 | 1.00 / 0.00 | storage linear by design; dedup is an index lookup (was a linear scan) |
| RocksDB proofs, L = 1..256 | union build: `GetNodes` calls per batch | 0 | 0.00 (constant 18 = tree depth) | O(depth) round trips, independent of L |
| RocksDB proofs, L = 1..256 | node reads: single proofs vs union build | 1 vs <1 | 1.00 vs 0.90 | shared path nodes read once (1.7x fewer reads at L = 256) |

## 1. get_inclusion_proofs.v1

### 1.1 IDs per batch L (memory record source; counts are exact)

N = 4096 leaves, 8-shard partition, ordinary UC 752 B (4 x 65 B seal signatures), partition view 1528 B

| L | response B | B per ID | proof path nodes (sum) | nodes per ID | DB size queries | DB record queries | allocs | alloc B | alloc B per ID | time relative to L=1 | time per ID relative |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 5856 | 5856 | 12 | 12.0 | 1 | 1 | 205 | 49680 | 49680 | 1.0x | 1.00x |
| 2 | 8527 | 4263 | 23 | 11.5 | 1 | 1 | 374 | 84686 | 42343 | 1.7x | 0.83x |
| 4 | 14061 | 3515 | 48 | 12.0 | 1 | 1 | 717 | 199537 | 49884 | 3.6x | 0.91x |
| 8 | 25001 | 3125 | 96 | 12.0 | 1 | 1 | 1388 | 382894 | 47862 | 8.0x | 0.99x |
| 16 | 46753 | 2922 | 190 | 11.9 | 1 | 1 | 2726 | 752556 | 47035 | 14.4x | 0.90x |
| 32 | 90769 | 2836 | 386 | 12.1 | 1 | 1 | 5420 | 1424154 | 44505 | 25.3x | 0.79x |
| 64 | 178801 | 2793 | 778 | 12.2 | 1 | 1 | 10809 | 2972408 | 46444 | 51.2x | 0.80x |
| 128 | 353905 | 2764 | 1547 | 12.1 | 1 | 1 | 21511 | 5702989 | 44555 | 107.5x | 0.84x |
| 256 | 706225 | 2758 | 3118 | 12.2 | 1 | 1 | 43058 | 12026814 | 46980 | 210.2x | 0.82x |

Log-log slopes over L = 1..256: response bytes 0.88 (expect 1), path nodes 1.01 (expect 1: L x depth), DB queries -0.00 (expect 0: constant 2), allocations 0.97 (expect 1), allocated bytes 0.99 (expect 1), time 0.97 (expect ~1)

Per batch the service now issues **one** size projection and **one** record load whatever L (the test asserts it), the proofs are built from one pinned view,
and cost is linear in L (one ordinary UC, about 2.7 KB of response per ID at N = 4,096, is repeated in every proof: the self-contained wire format makes the response
Theta(L(C + depth)) plus one partition view). The 1,528 B partition view (3,056 hex characters) is the fixed part, which is why the response-byte slope is 0.88 rather than 1.

### 1.2 Database commands per batch, real MongoDB

Counted with a driver command monitor. **Every command counts** (aggregate and getMore alike; only connection and session housekeeping is excluded). The cursors are exhausted by the first reply (the aggregate batch size is the chunk size + 1, #193 `769a0fd`); with the driver default first batch of 101 documents the count would have been a staircase, 2 x (1 + floor((L-1)/101)) per cursor pair, i.e. 4 commands at L >= 102 and 6 at L > 202.

MongoDB 7.0 in a container, 256 finalized records. Every command counts (aggregate and getMore alike). Before the batching change the service issued 2 x L sequential queries per batch.

| L | commands per batch (all) | of which aggregate | of which getMore | response B | time relative to L=1 | time per ID relative |
|---|---|---|---|---|---|---|
| 1 | 2 | 2 | 0 | 5856 | 1.0x | 1.00x |
| 2 | 2 | 2 | 0 | 8527 | 1.1x | 0.56x |
| 4 | 2 | 2 | 0 | 14061 | 1.3x | 0.32x |
| 8 | 2 | 2 | 0 | 25001 | 1.7x | 0.21x |
| 16 | 2 | 2 | 0 | 46753 | 2.3x | 0.14x |
| 32 | 2 | 2 | 0 | 90769 | 3.6x | 0.11x |
| 64 | 2 | 2 | 0 | 178801 | 5.8x | 0.09x |
| 128 | 2 | 2 | 0 | 354033 | 10.5x | 0.08x |
| 256 | 2 | 2 | 0 | 706353 | 20.5x | 0.08x |

Log-log slopes over L = 1..256: database commands per batch (all) -0.00 (expect 0: constant 2), relative time 0.54 (the fixed round trips dominate small L; upper range L = 16..256: 0.79)

**Before / after the `$in` batching** (same code path, same host; before = commit `2b27bad`: one size projection and one record fetch per ID, i.e. exactly 2L sequential commands by construction):

| L | commands before | commands after | time relative to L=1 before | time relative to L=1 after |
|---|---|---|---|---|
| 1 | 2 | 2 | 1x | 1x |
| 16 | 32 | 2 | 21x | 2.3x |
| 256 | 512 | 2 | 240x | 20.5x |

Before the change, time was proportional to L (slope about 1 from L = 1: 240x for 256x the IDs, each ID paying two round trips); after it the fixed two round trips dominate small L and the curve flattens
(whole-range slope 0.54, upper range 0.79, where record bytes and proof building take over). Relative times are noisy on this host; the command counts are exact and are the claim.

### 1.3 SMT size N (L = 16)

Filler leaves grow the tree in steps; only 16 records are requested.

L = 16, 8-shard partition, ordinary UC with 4-signature seal

| N (leaves) | log2 N | path nodes per ID | nodes per ID / log2 N | response B per ID | allocs per ID | alloc B per ID | time per ID relative to N=1e3 | DB queries |
|---|---|---|---|---|---|---|---|---|
| 1000 | 10.0 | 10.19 | 1.02 | 2814 | 164 | 45052 | 1.00x | 2 |
| 3000 | 11.6 | 11.75 | 1.02 | 2914 | 170 | 46845 | 0.91x | 2 |
| 10000 | 13.3 | 13.44 | 1.01 | 3022 | 177 | 48401 | 0.92x | 2 |
| 30000 | 14.9 | 15.25 | 1.03 | 3138 | 184 | 49623 | 0.95x | 2 |
| 100000 | 16.6 | 17.00 | 1.02 | 3250 | 191 | 51185 | 1.27x | 2 |
| 300000 | 18.2 | 18.38 | 1.01 | 3338 | 197 | 53257 | 1.73x | 2 |
| 1000000 | 19.9 | 19.69 | 0.99 | 3422 | 202 | 53978 | 1.36x | 2 |

Log-log slopes over N = 1e3..1e6 (a logarithm has a slope near 1/ln N, here about 0.1-0.15): path nodes 0.10, response bytes 0.03, allocations 0.03, time 0.08. A linear dependence on N would be slope 1; the proof cost is flat-to-logarithmic in N.

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
| 1000 | 1266416 | 1266 | 151846 | 102091 | 1.49x | 1.00x |
| 10000 | 5519408 | 551 | 202969 | 146244 | 1.39x | 1.21x |
| 100000 | 48279480 | 482 | 263757 | 198949 | 1.33x | 1.38x |
| 1000000 | 475867544 | 475 | 331958 | 257619 | 1.29x | 1.43x |

Log-log slopes over N = 1e3..1e6: live heap 0.87 (expect 1), accounted per view 0.11 (expect ~0.1: one more tree level per doubling), measured per view 0.13 (expect ~0.1), retain time 0.05 (expect ~0: capture does not copy the tree)

The retention cost per view follows the churn (`B x depth`, one more tree level per doubling of N), not the tree size; only the live tree itself (about 480 B per leaf) is linear in N. Capturing a view does not copy the tree.

### 2.2 Memory backend, block size B (N = 100,000, 16 views)

N = 100,000 leaves, 16 retained views

| B | accounted per view (B) | accounted per new leaf (B) | measured per view (B) | accounted / measured | `RetainReadView` time (relative to B=1) |
|---|---|---|---|---|---|
| 1 | 4006 | 4006 | 3025 | 1.32x | 1.0x |
| 4 | 14235 | 3558 | 11013 | 1.29x | 3.1x |
| 16 | 50439 | 3152 | 38780 | 1.30x | 8.6x |
| 64 | 176482 | 2757 | 133953 | 1.32x | 54.1x |
| 256 | 606327 | 2368 | 449998 | 1.35x | 261.8x |
| 1024 | 2028509 | 1980 | 1457096 | 1.39x | 1091.0x |
| 4096 | 6912372 | 1687 | 4777746 | 1.45x | 3665.8x |
| 16384 | 23893945 | 1458 | 15933297 | 1.50x | 27604.4x |

Log-log slopes over B = 1..16384 (whole range): accounted per view 0.89, measured per view 0.88, retain time 1.06. Over the upper range B = 256..16384: accounted 0.88, measured 0.86, retain time 1.10 (below 1: paths shared by many new leaves are copied once, so cost grows sublinearly as B approaches N).

Cost per view is linear in B at small B and sublinear at large B (new leaves share path prefixes, so `accounted per new leaf` falls from 4,006 B to 1,458 B). The accounting stays conservative at every B (1.3-1.5x the measured heap).
The time to capture a view is proportional to the nodes introduced since the previous capture (`O(B depth)`, slope 1), as designed, not to N.

### 2.3 RocksDB backend, SMT size N (B = 1,000, 16 views)

B = 1,000 new leaves per block, 16 retained views

| N | disk without views (B) | disk with views (B) | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to N=1e4 |
|---|---|---|---|---|---|---|
| 10000 | 3951444 | 8426403 | 859373 | 279684 | 3.07x | 1.00x |
| 100000 | 17145871 | 23357530 | 1120305 | 388228 | 2.89x | 0.79x |
| 1000000 | 148990146 | 154775145 | 1494308 | 361562 | 4.13x | 0.86x |

Log-log slopes over N = 1e4..1e6: database size 0.79 (expect 1), accounted per view 0.12, physical per view 0.06 (both expect ~0.1: churn, not tree size), retain time -0.03 (expect ~0)

### 2.4 RocksDB backend, block size B (N = 100,000, 16 views)

N = 100,000 leaves, 16 retained views

| B | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to B=10 |
|---|---|---|---|---|
| 10 | 18169 | 6979 | 2.60x | 1.00x |
| 100 | 147479 | 60679 | 2.43x | 1.23x |
| 1000 | 1120305 | 388228 | 2.89x | 1.22x |
| 10000 | 8602946 | 2924294 | 2.94x | 1.58x |

Log-log slopes over B = 10..10,000: accounted per view 0.89, physical per view 0.87 (expect ~1: bytes written follow the churn), retain time 0.06 (expect 0: a pinned snapshot does not depend on B)

RocksDB retention cost is linear in the bytes written (churn), independent of N. The accounting overstates the physical growth by 2.4-4.1x (it counts bytes written before compression and compaction). Capturing a snapshot is O(1) in the application and independent of B (retain-time slope 0.06 over B = 10..10,000 in this run; an earlier run on a more loaded host showed 0.73, which was noise).

### 2.5 Which limit binds: the view count or the byte budget?

The retention budget (256 MiB) is divided by the accounted bytes per view to give how many views fit. From the sweeps (N = 100,000): memory backend 4,006 B per view at B = 1 up to 6.9 MB at B = 4,096 (38 views fit) and 23.9 MB at B = 16,384
(**11 views fit**); RocksDB 8.6 MB per view at B = 10,000 (31 views fit). So for these workloads the **count cap (16) binds first up to roughly B = 10,000 (memory) and well beyond for RocksDB; the byte budget binds only for larger blocks
(memory, B around 11,000 or more)**. The charges above are reader charges only; catalog metadata and attachments (about 2.4 KB per attachment here, section 3.3) are additional and not part of the reader figures.

## 3. Catalog bookkeeping (real memory backend, instrumented readers)

The retained-view catalog was measured twice: before the catalog/lineage indexing fix (#200, the audit's item 1) and on the merged code, which can keep an O(1) retained-bytes total (`proofview.AccountantFor`) and has an attachment digest index.

> **Correction.** An earlier revision of this document said the production wiring already used the accountant. It did not: the production constructor (`RoundManager.initProofViews`) called `proofview.NewCatalog(limits)` without it, so production ran the linear fallback (O(V) totals, and pressure passes of O(V) each) and the "with the backend accountant (production wiring)" tables describe the configuration measured with `AccountantFor` passed explicitly. Production now builds the catalog through `newProofViewCatalog`, which passes `proofview.AccountantFor(backend)` (memory and RocksDB backends; checked by `TestProductionCatalogConstructorWiresTheAccountant`, which counts accounting reads through the real constructor and shows they are flat in V, while the fallback is not). The publication lookups by certification digest also scanned the retained views; they are an index probe now (`TestDigestLookupDoesNotScanTheRetainedViews`). Numbers below were not re-measured for this correction.
The "before" numbers are from the earlier revision of this document (same sweeps, same host); the "after" tables below are regenerated on the merged code, once without the backend accountant (the catalog sums its readers, the fallback)
and once with it (the wiring production uses since the correction above).

### 3.1 Retained views V = 1..256 (no pressure)

The hardware-independent metric is how many times the catalog reads `RetainedBytes()` per operation.

**Without the backend accountant**

N = 2,000, B = 10 new leaves per block, V retained views, one attachment each

| V | RetainedBytes reads per Publish | per AcquireLatest+Release | per Stats | Publish time (relative to V=1) | AcquireLatest+Release time (relative) | Stats time (relative) |
|---|---|---|---|---|---|---|
| 1 | 3 | 2 | 1 | 1.0x | 1.0x | 1.0x |
| 2 | 9 | 6 | 2 | 0.9x | 1.1x | 1.5x |
| 4 | 17 | 12 | 4 | 0.9x | 1.2x | 2.4x |
| 8 | 33 | 24 | 8 | 0.9x | 1.5x | 4.2x |
| 16 | 65 | 48 | 16 | 0.9x | 1.9x | 7.9x |
| 32 | 129 | 96 | 32 | 0.9x | 3.2x | 15.4x |
| 64 | 257 | 192 | 64 | 1.1x | 5.6x | 31.3x |
| 128 | 513 | 384 | 128 | 1.6x | 11.9x | 69.1x |
| 256 | 1025 | 768 | 256 | 1.8x | 22.9x | 145.4x |

Log-log slopes over V = 1..256: RetainedBytes reads per Publish 1.02, per AcquireLatest+Release 1.04, per Stats 1.00 (0 for a constant-time catalog; 1 when each retained reader is read once per operation); relative time: Publish 0.10, AcquireLatest+Release 0.57, Stats 0.91

**With the backend accountant (production wiring since the correction above)**

N = 2,000, B = 10 new leaves per block, V retained views, one attachment each

| V | RetainedBytes reads per Publish | per AcquireLatest+Release | per Stats | Publish time (relative to V=1) | AcquireLatest+Release time (relative) | Stats time (relative) |
|---|---|---|---|---|---|---|
| 1 | 1 | 0 | 0 | 1.0x | 1.0x | 1.0x |
| 2 | 1 | 0 | 0 | 0.7x | 1.2x | 1.0x |
| 4 | 1 | 0 | 0 | 1.1x | 1.2x | 1.0x |
| 8 | 1 | 0 | 0 | 1.2x | 1.3x | 1.0x |
| 16 | 1 | 0 | 0 | 2.3x | 1.4x | 3.0x |
| 32 | 1 | 0 | 0 | 1.3x | 1.6x | 1.3x |
| 64 | 1 | 0 | 0 | 1.3x | 1.5x | 2.0x |
| 128 | 1 | 0 | 0 | 1.4x | 1.7x | 1.2x |
| 256 | 1 | 0 | 0 | 2.1x | 1.2x | 1.6x |

Log-log slopes over V = 1..256: RetainedBytes reads per Publish 0.00, per AcquireLatest+Release 0.00, per Stats 0.00 (0 for a constant-time catalog; 1 when each retained reader is read once per operation); relative time: Publish 0.13, AcquireLatest+Release 0.06, Stats 0.09

| Metric | Before #200 | After, without accountant | After, with accountant |
|---|---|---|---|
| `RetainedBytes` reads per Publish / Acquire+Release / Stats, slope in V | 1.02 / 1.04 / 1.00 | 1.02 / 1.04 / 1.00 | **0.00 / 0.00 / 0.00** |
| relative time slope: Acquire+Release / Stats | 1.25 / 1.49 | 0.57 / 0.91 | **0.06 / 0.09** |

With the accountant every ordinary catalog operation reads no reader at all (reads per operation are 0 for every V), and the time per Acquire+Release and per Stats is flat in V. Without it the reads are still linear in V by construction (the catalog sums its own readers),
which is why production wires the accountant; the fallback is kept for backends that cannot total their readers.

### 3.2 Oldest view pinned while captures accumulate (V = 4)

Each point is measured at exactly the labelled R (no captures happen during a measurement).

**Without the backend accountant**

V = 4 retained views, oldest view pinned by a lease, N = 2,000, B = 10; R = captures after the pinned view (exact)

| R | RetainedBytes reads per Stats call | Stats time (relative to R=4) | AcquireLatest+Release time (relative) | retained bytes (accounted) |
|---|---|---|---|---|
| 4 | 4 | 1.0x | 1.0x | 95292 |
| 16 | 4 | 1.0x | 0.8x | 375292 |
| 64 | 4 | 1.0x | 0.8x | 1503298 |
| 256 | 4 | 1.1x | 0.8x | 6295542 |
| 1024 | 4 | 0.9x | 0.7x | 27350712 |

Log-log slopes over R = 4..1024: RetainedBytes reads per Stats call -0.00 (bounded by V = 4, so 0); relative time: Stats -0.01, AcquireLatest+Release -0.05 (0 when the per-operation work does not depend on the captures since the pinned view).

**With the backend accountant (production wiring since the correction above)**

V = 4 retained views, oldest view pinned by a lease, N = 2,000, B = 10; R = captures after the pinned view (exact)

| R | RetainedBytes reads per Stats call | Stats time (relative to R=4) | AcquireLatest+Release time (relative) | retained bytes (accounted) |
|---|---|---|---|---|
| 4 | 0 | 1.0x | 1.0x | 95292 |
| 16 | 0 | 1.0x | 0.9x | 375292 |
| 64 | 0 | 1.0x | 1.0x | 1503298 |
| 256 | 0 | 1.0x | 1.1x | 6295542 |
| 1024 | 0 | 1.0x | 0.8x | 27350712 |

Log-log slopes over R = 4..1024: RetainedBytes reads per Stats call 0.00 (bounded by V = 4, so 0); relative time: Stats 0.01, AcquireLatest+Release -0.01 (0 when the per-operation work does not depend on the captures since the pinned view).

Before the fix, per-operation catalog time grew with the number of captures since the pinned view (lineage entries kept grew as R + 1 and the entries visited by one `Stats` call as 4R + 1, slopes 0.96 / 0.99, Stats time slope 0.79). On the merged code the lineage is a linked list of alive neighbors with cumulative deltas:
operation time is flat in R (slopes about 0) in both variants. The backend's own counter tests (`lineage_accounting_test.go`, `catalog_scaling_test.go`) assert that the per-operation lineage work is constant, which is the hardware-independent evidence for this curve; the accounted retained bytes still grow with R (the pinned view really keeps those versions alive).

### 3.3 Attachments per view U = 1..64 (V = 4)

V = 4, each state keeps up to U attachments (UC with 4-signature seal and an 8-shard view)

| U | catalog retained bytes (measured) | attachment bytes retained (measured: growth over U=1) | allocs per attach | attach time (relative to U=1) |
|---|---|---|---|---|
| 1 | 61018 | 2409 | 67 | 1.0x |
| 2 | 63427 | 4818 | 67 | 1.8x |
| 4 | 68245 | 9636 | 67 | 1.0x |
| 8 | 77881 | 19272 | 67 | 1.4x |
| 16 | 97153 | 38544 | 67 | 1.0x |
| 32 | 135697 | 77088 | 67 | 0.9x |
| 64 | 212785 | 154176 | 67 | 0.9x |

Log-log slopes over U = 1..64: measured attachment bytes retained 1.00 (expect 1: storage is linear in U by design), allocations per attach -0.00 (expect 0: the digest index makes the dedup a map lookup, asserted by the catalog's own counter tests), attach time -0.09 (relative)

Attachment storage is linear in U by design (about 2.4 KB per attachment: UC with 4 signatures plus an 8-shard view; slope 1.00, measured from the catalog's own accounting). Before the digest index, the dedup was a scan (exactly U digest comparisons per attach, slope 1.00; invisible in time at U <= 64);
now it is a map lookup, and allocations and time per attach are flat in U (slopes 0.00 and -0.09).

## 4. Disk proof node reads (RocksDB snapshot, N = 100,000)

Compares building L proofs one at a time (what the current disk read view does per key) with the existing union-path builder (`persist.BuildInclusionCerts`, which the disk read view now calls for a batch since D1, #203), counting node reads on a counting `ReadStore` wrapper.

N = 100,000, random existing keys; one pinned snapshot

| L | single proofs: node reads | single: GetNode(s) calls | single: distinct nodes | union build: node reads | union: GetNode(s) calls | union: distinct nodes | reads saved by the union build | single time | union time |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 18 | 18 | 18 | 18 | 18 | 18 | 1.00x | 123µs | 92µs |
| 2 | 36 | 36 | 35 | 35 | 18 | 35 | 1.03x | 130µs | 2.401ms |
| 4 | 72 | 72 | 66 | 66 | 18 | 66 | 1.09x | 301µs | 289µs |
| 8 | 144 | 144 | 124 | 124 | 18 | 124 | 1.16x | 542µs | 472µs |
| 16 | 288 | 288 | 237 | 237 | 18 | 237 | 1.22x | 1.008ms | 885µs |
| 32 | 576 | 576 | 443 | 443 | 18 | 443 | 1.30x | 1.977ms | 1.554ms |
| 64 | 1150 | 1150 | 829 | 829 | 18 | 829 | 1.39x | 3.984ms | 2.907ms |
| 128 | 2265 | 2265 | 1486 | 1486 | 18 | 1486 | 1.52x | 7.838ms | 5.431ms |
| 256 | 4531 | 4531 | 2696 | 2696 | 18 | 2696 | 1.68x | 17.126ms | 10.39ms |

Log-log slopes over L = 1..256: single-proof node reads 1.00 (expect 1: L x depth), union-build node reads 0.90 and distinct nodes 0.90 (expect < 1: shared upper path nodes are read once; for large L the distinct nodes approach N)

Single-proof building reads `L x depth` nodes (slope 1.00) and makes that many snapshot calls. The union build reads exactly the distinct nodes (sublinear: 1.7x fewer reads at L = 256 for random keys) and makes a constant 18 frontier-batched `GetNodes` calls (the tree depth), independent of L:
round trips to the store drop from `L x depth` to `depth`. Output size is unchanged (`L x depth` siblings are still returned).

## 5. Not covered

- **A4-integrated publication and end-to-end service** (finalization hooks, HA, recovery) are not wired in this snapshot; section 2 is the bare backend, section 3 the catalog.
- **Peak RSS** is not measured; only heap (memory) and physical on-disk bytes after compaction (RocksDB). Cold versus warm RocksDB block cache, adversarial shared-prefix keys and disk batch proofs through the service were not run.
- **Absolute latency** is deliberately not reported (hardware context above); the Mongo curves report command counts and relative time only.
- The V sweep only goes to 256 on the memory backend (the RocksDB catalog accounting was not swept). The retention sweeps in section 2 and the proof reads in section 4 call the backends directly; they were not re-run end to end through the D1 batch API.
