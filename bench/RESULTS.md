# BEAM Storage Backend Benchmarks — redb vs fjall

**Date:** 2026-08-20
**Hardware:** test machine (Rust 2024, release profile)
**BEAM version:** 0.16.0
**Branch:** `feature/fjall-storage-adapter`

## Results Summary

| Benchmark | redb | fjall | Winner | Speedup |
|---|---|---|---|---|
| **write_storm (sequential)** | 977 elem/s | 2,999 elem/s | **fjall** | **3.1x** |
| **concurrent_write_storm (4 tasks)** | 1,195 elem/s | 4,836 elem/s | **fjall** | **4.0x** |
| **read_storm (random)** | 610 elem/s | 447 elem/s | **redb** | 1.4x |

## Detailed Results

### write_storm / sequential

Sequential puts to unique keys, single-threaded.

```
write_storm/sequential_redb
    time:   [1.0156 s 1.0232 s 1.0318 s]
    thrpt:  [969.20  elem/s 977.29  elem/s 984.61  elem/s]

write_storm/sequential_fjall
    time:   [299.83 ms 333.48 ms 359.07 ms]
    thrpt:  [2.7849 Kelem/s 2.9987 Kelem/s 3.3352 Kelem/s]
```

**fjall is 3.1x faster** on sequential writes. This is the expected
result — fjall's journal append (write() to OS page cache, no fsync)
vs redb's B+tree copy-on-write + fsync per commit.

### concurrent_write_storm / 4 tasks

4 concurrent tasks writing to unique keys.

```
concurrent_write_storm/redb_x4_tasks
    time:   [767.88 ms 836.51 ms 900.41 ms]
    thrpt:  [1.1106 Kelem/s 1.1954 Kelem/s 1.3023 Kelem/s]

concurrent_write_storm/fjall_x4_tasks
    time:   [167.99 ms 206.78 ms 254.25 ms]
    thrpt:  [3.9331 Kelem/s 4.8360 Kelem/s 5.9527 Kelem/s]
```

**fjall is 4.0x faster** on concurrent writes. The gap widens under
concurrency because redb's single-writer MVCC serializes commits
(each commit fsyncs), while fjall's journal appends are non-blocking
and can be issued concurrently from multiple tasks.

### read_storm / random

Random point lookups (1000 keys, 10k iterations per sample).

```
read_storm/random_redb
    time:   [1.4820 s 1.6392 s 1.7917 s]
    thrpt:  [558.13  elem/s 610.07  elem/s 674.78  elem/s]

read_storm/random_fjall
    time:   [1.0416 s 2.2372 s 3.7984 s]
    thrpt:  [263.27  elem/s 447.00  elem/s 960.06  elem/s]
```

**redb is ~1.4x faster** on random reads. This is expected — redb's
B+tree with mmap provides O(log N) page lookups with direct memory
access, while fjall's LSM-tree has to check memtable → L0 SSTables
→ L1..Ln SSTables (multiple levels). The high variance on fjall
(min=263, max=960 elem/s) suggests background compaction causes
latency spikes — a known LSM-tree trade-off.

## Analysis

### Why fjall wins on writes
- redb: every `commit()` = `fsync()` (milliseconds, blocking)
- fjall: `insert()` = `write()` to OS page cache (microseconds, non-blocking)
- The `spawn_blocking` overhead in redb adds thread pool scheduling on top of fsync
- fjall's direct-async pattern (no spawn_blocking for puts) eliminates this overhead

### Why redb wins on reads
- redb: B+tree + mmap = direct memory access for page lookups
- fjall: LSM-tree = multi-level lookup (memtable → L0..Ln SSTables)
- LSM-trees trade read performance for write performance — this is the fundamental design trade-off

### Recommendation
- **P2P/multi-node deployments**: fjall is the better choice. Write throughput
  is the bottleneck in P2P sync (peers flood puts during resync), and the 3-4x
  write advantage outweighs the 1.4x read disadvantage.
- **Single-node read-heavy**: redb remains the better choice for deployments
  where read latency matters more than write throughput.
- **Mixed workload**: consider a read-cache layer (in-memory BTreeMap) on top
  of fjall to get the best of both worlds.

The LSM-tree trade-off (write-optimized, read-penalized) is well-understood
in the database community. RocksDB makes the same trade-off vs WiredTiger.
