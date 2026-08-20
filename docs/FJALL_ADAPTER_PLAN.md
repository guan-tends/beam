# Fjall Storage Adapter — Implementation Plan

**Branch:** `feature/fjall-storage-adapter`
**Date:** 2026-08-20
**Author:** Guan (Pema Lhamo)
**Status:** Ready for implementation

---

## 1. Context & Motivation

### 1.1 Why Fjall?

BEAM currently ships two persistent storage backends:
- **redb** (default): B+tree, MVCC, ACID, copy-on-write. Excellent for single-node deployments. Every `commit()` fsyncs — safe but expensive under high write load.
- **persy** (optional): CoW, single-file, read-committed. The author has publicly stated development is slowing and there are unresolved high-concurrency crash issues.

**Fjall** is an LSM-tree (RocksDB-like) storage engine in 100% safe Rust. Its architecture is fundamentally better suited to BEAM's P2P sync workload:

| Property | redb (B+tree) | fjall (LSM-tree) |
|---|---|---|
| Write path | Copy-on-write B+tree pages + **fsync per commit** | Journal append to OS buffers (microseconds, **no fsync**) |
| Read path | O(log N) page lookups via mmap | O(log N) multi-level (memtable → L0..Ln SSTables) |
| Batch writes | Loop in one transaction → 1 fsync | `WriteBatch` → 1 journal entry |
| Compression | None | LZ4 default (free disk space reduction) |
| Background work | None | Compaction + memtable flush (automatic) |
| Range/prefix | `table.range()` | `keyspace.prefix()`, `keyspace.range()` |
| Thread safety | Arc\<Database\>, single-writer MVCC | Internally synchronized, multi-threaded |

### 1.2 Original Architectural Decisions

BEAM's storage adapter system was designed to be pluggable from the start:
- `Actor` trait defines the contract (`handle`, `try_clone_storage`, lifecycle hooks)
- `Router` splits storage into read/write actors for concurrent access
- Each adapter handles `Get`, `Put`, `BatchPut`, `Flush` messages
- `_ack`/`_err` sentinel convention for ack replies (uniform across adapters)
- LWW (last-write-wins) conflict resolution per child node
- Always-reply-when-ack invariant (checksum suppression only for non-ack broadcasts)
- `BackendKind` enum in benchmarks enables head-to-head comparison

### 1.3 The Key Insight: spawn_blocking

The redb adapter uses `tokio::task::spawn_blocking` for every Put and BatchPut because `redb::WriteTransaction::commit()` calls `fsync()` — a multi-millisecond blocking syscall.

Fjall's `insert()` writes to the WAL (write-ahead log) as a `write()` syscall to **OS page cache** — microseconds, not milliseconds. Fjall's default durability matches RocksDB: crash-safe via WAL recovery, but not fsync'd until explicit `persist()`.

**Therefore: spawn_blocking is unnecessary and counterproductive for fjall's Put/BatchPut/Get. It IS necessary for Flush, which calls `persist(PersistMode::SyncAll)` = real fsync.**

This is embracing the difference, not papering over it.

---

## 2. Fjall API Surface (Verified from docs.rs source)

### 2.1 Database

```rust
// Open/create
let db = fjall::Database::builder(&path).open()?;

// Keyspace (column family = separate LSM-tree)
let keyspace = db.keyspace("beam_nodes_v1", KeyspaceCreateOptions::default())?;

// Write (microseconds — journal append to OS buffers)
keyspace.insert(key_bytes, value_bytes)?;

// Read (memtable → SSTable lookup)
let value: Option<fjall::Slice> = keyspace.get(key_bytes)?;

// Delete
keyspace.remove(key_bytes)?;

// Range/prefix (first-class)
for kv in keyspace.prefix("user/alice/") { /* ... */ }
for kv in keyspace.range("a"..="z") { /* ... */ }

// Explicit durability (fsync)
db.persist(fjall::PersistMode::SyncAll)?;
```

### 2.2 WriteBatch (Atomic Batch)

```rust
// Create from Database
let mut batch = db.batch(); // or db.batch_with_capacity(n)

// Add entries
batch.insert(&keyspace, key_bytes, value_bytes);
batch.remove(&keyspace, key_bytes);

// Optional: set explicit durability per batch
batch = batch.durability(Some(PersistMode::SyncAll));

// Commit atomically
batch.commit()?;
```

### 2.3 PersistMode

```rust
pub enum PersistMode {
    Buffer,    // Flush to OS buffers (default, crash-safe via WAL)
    SyncData,  // fdatasync
    SyncAll,   // fsync (full durability)
}
```

### 2.4 Builder Options

```rust
let db = fjall::Database::builder(&path)
    .cache_capacity(bytes)           // block cache size
    .flush_workers(n)                // background worker threads (default: min(CPU, 4))
    .max_journal_size(bytes)          // max WAL size (default: 512 MiB)
    .auto_flush(false)                // manual persistence (default: false = auto)
    .temporary(true)                  // delete on drop
    .open()?;
```

---

## 3. Design Decisions

### 3.1 Schema

Two keyspaces within one `Database`:
- `beam_nodes_v1`: key = `node_id` (as bytes), value = `postcard::to_allocvec(Children)` bytes
- `beam_meta_v1`: key = metadata key (as bytes), value = `u64` timestamp (as bytes)

This mirrors redb's two-table schema exactly. Keyspace names match the existing wire-format identifier convention (DO NOT CHANGE).

### 3.2 Async Pattern

| Operation | Async handling | Why |
|---|---|---|
| `Get` | Direct `keyspace.get()` in `handle()` | Reads from memtable (RAM) or SSTable (mmap). No blocking syscall. |
| `Put` | Direct `keyspace.insert()` in `handle()` | Journal append to OS buffers. Microseconds. No fsync. |
| `BatchPut` | Direct `db.batch()` → loop inserts → `commit()` | Single journal entry. Microseconds. No fsync. |
| `Flush` | `spawn_blocking` → `db.persist(SyncAll)` | Real fsync. Milliseconds. Block here. |

### 3.3 LWW Conflict Resolution

Same algorithm as redb/persy: for each child, compare `updated_at`. Newer wins. If equal, incoming wins (same convention). Read existing → merge → write.

### 3.4 Ack Sentinels

Same `_ack`/`_err` convention as redb/persy. The ack is sent immediately after `insert()` returns (no spawn_blocking overhead). This means acks come back **faster** than redb — the caller doesn't wait for fsync.

### 3.5 Read/Write Actor Split

Keep `try_clone_storage()` returning `Some(Box::new(self.clone()))` — same pattern as redb/persy. The split provides backpressure isolation. With fjall it's less critical (writes don't block reads via fsync), but it matches the existing architecture.

### 3.6 Feature Gate

```toml
[features]
fjall = ["dep:fjall"]
```

Native-only (`cfg(not(target_arch = "wasm32"))`). Same pattern as persy.

### 3.7 Error Type

Fjall errors: `fjall::Error`. Map to string via `Debug` (same convention as persy adapter, which maps `String` errors in its ack). The `_err` sentinel carries the error string.

---

## 4. Implementation Tasks

### Task 1: Add fjall dependency to Cargo.toml

**Files:** `Cargo.toml`
**Effort:** S

Add `fjall` as an optional dependency under native-only target, behind a feature gate. Mirror the persy pattern exactly.

```toml
# Under [target.'cfg(not(target_arch = "wasm32"))'.dependencies]
fjall = { version = "2", optional = true }

# Under [features]
fjall = ["dep:fjall"]
```

**Verification:** `cargo check --features fjall`

---

### Task 2: Write `src/adapters/fjall_storage.rs`

**Files:** `src/adapters/fjall_storage.rs` (NEW)
**Effort:** M

The adapter implementation. Module structure:

```
//! Module docs (schema, semantics, fjall-specific design)
// Imports
// Constants: BEAM_NODES, BEAM_META (keyspace names — wire format identifiers)
// Struct: FjallStorage { db, nodes, meta, path }
// impl Clone
// impl Default
// impl FjallStorage:
//   new()
//   new_with_config(config, path, _max_size)
//   new_with_path(path)
//   handle_get(get, ctx)          — direct keyspace.get(), no spawn_blocking
//   apply_put(keyspace, put)      — read-merge-write, LWW
//   handle_put_internal(put)      — calls apply_put, returns Result
//   handle_batch_put(batch)       — WriteBatch, single journal entry
// impl Actor:
//   pre_start                    — log, maybe warm keyspaces
//   stopping                     — log
//   handle(Get)                   — direct
//   handle(Put)                  — direct, immediate ack (no spawn_blocking)
//   handle(BatchPut)             — direct, immediate ack (no spawn_blocking)
//   handle(Flush)                — spawn_blocking(persist SyncAll), then ack
//   try_clone_storage            — Box::new(self.clone())
// Unit tests:
//   test_fjall_creates_db
//   test_fjall_default
//   test_fjall_clone
//   test_fjall_put_then_get_roundtrips
//   test_fjall_lww_merge_prefers_newer
//   test_fjall_get_missing_returns_empty
//   test_fjall_always_replies_when_in_response_to_set
```

**Key differences from redb adapter (to document in comments):**
1. No `spawn_blocking` for Put/BatchPut — direct `insert()` call (microseconds, journal append)
2. `spawn_blocking` ONLY for Flush — `persist(SyncAll)` is real fsync
3. `keyspace.get()` is direct — no `begin_read()`/`open_table()` ceremony
4. BatchPut uses `WriteBatch` — single journal entry for N puts
5. Built-in LZ4 compression — no manual encoding

**Idiomatic Rust notes:**
- Use `?` operator for error propagation in internal methods
- `match` for error handling in `handle()` (same as redb/persy — log and return)
- `unwrap_or_return!` macro equivalent: inline match (fjall errors are different type)
- Keep the ack-building logic DRY: reuse the persy pattern of `build_ack_children` as a free function

**Verification:**
- `cargo check --features fjall --lib`
- `cargo test --features fjall --lib` (unit tests)
- `cargo clippy --features fjall -- -D warnings`

---

### Task 3: Wire into `src/adapters/mod.rs`

**Files:** `src/adapters/mod.rs`
**Effort:** S

Add module declaration and re-export, feature-gated and native-only:

```rust
#[cfg(feature = "fjall")]
mod fjall_storage;

#[cfg(feature = "fjall")]
pub use fjall_storage::FjallStorage;
```

Update module-level docs to list FjallStorage alongside RedbStorage and PersyStorage.

**Verification:** `cargo check --features fjall`

---

### Task 4: Add to benchmark system

**Files:** `benches/my_benchmark.rs`
**Effort:** S

1. Add `Fjall` variant to `BackendKind` enum (behind `#[cfg(feature = "fjall")]`)
2. Add to `BackendKind::all()` list
3. Add `setup_node` match arm — `fjall::Database::builder(path).open()` + keyspace creation
4. Add `BackendKind::name()` match arm

**Verification:** `cargo bench --features fjall -- --no-run`

---

### Task 5: Write `tests/fjall_e2e.rs`

**Files:** `tests/fjall_e2e.rs` (NEW)
**Effort:** M

End-to-end tests through the full Node → Router → FjallStorage → ack reply path. Mirror `tests/persy_e2e.rs`:

```
#![cfg(feature = "fjall")]

// Test: e2e_fjall_put_await_durability
//   - Create FjallStorage, Node, put a value, await ack
//   - Verify value is retrievable via get
//   - Clean up

// Test: e2e_fjall_persistence_across_restart
//   - Create FjallStorage at path P, put value, flush, stop
//   - Reopen FjallStorage at path P
//   - Verify value is still there (fjall recovers from journal)

// Test: e2e_fjall_batch_put_atomicity
//   - Create FjallStorage, batch put 3 values
//   - Verify all 3 are retrievable

// Test: e2e_fjall_lww_conflict_resolution
//   - Put child "x" @ updated_at=100, then @ updated_at=50
//   - Verify newer value (100) wins
```

**Verification:** `cargo test --features fjall --test fjall_e2e`

---

### Task 6: Run benchmarks and compare

**Files:** N/A (data collection)
**Effort:** S (execution time varies)

Run head-to-head benchmarks:
```bash
# Write throughput
cargo bench --features fjall -- write_storm

# Concurrent write throughput
cargo bench --features fjall -- concurrent_write_storm

# Read throughput
cargo bench --features fjall -- read_storm

# Mixed workload
cargo bench --features fjall -- mixed_workload
```

Compare redb vs fjall on:
- Sequential write ops/sec
- Concurrent write ops/sec
- Read ops/sec
- Mixed workload ops/sec
- Disk space usage

**Deliverable:** Results summary (verbal or in `bench/RESULTS.md`)

---

### Task 7: Commit and push

**Files:** N/A
**Effort:** S

```bash
git add -A
git commit -m "feat: add FjallStorage adapter — LSM-tree backend for high-write workloads

- FjallStorage implements Actor trait with LSM-native async pattern:
  no spawn_blocking for Put/BatchPut/Get (journal append = microseconds),
  spawn_blocking only for Flush (persist SyncAll = fsync)
- WriteBatch for atomic BatchPut (single journal entry)
- Same LWW conflict resolution, _ack/_err sentinels, always-reply invariant
- Feature-gated behind 'fjall' feature, native-only
- Unit tests + e2e tests + benchmark integration
- Benchmarks: redb vs fjall head-to-head on BEAM's workload

Research: wing_beam/storage-backend-research in MemPalace"
git push origin feature/fjall-storage-adapter
```

---

## 5. Risk Assessment

| Risk | Likelihood | Mitigation |
|---|---|---|
| Fjall insert() is slower than expected in async context | Low | Benchmark first. If it blocks, add spawn_blocking. |
| WriteBatch API has unexpected limitations | Low | Fall back to loop insert(). Fjall batches internally. |
| Fjall background compaction causes latency spikes | Medium | Tune compaction config. Acceptable for P2P (peers have copies). |
| Fjall v2 vs v3 API differences | Low | Verified against docs.rs/latest. Plan uses v2 API. |
| Key encoding mismatch with redb (wire format) | None | Keys are raw bytes (node_id strings). No encoding layer. |

---

## 6. Success Criteria

- [ ] `cargo check --features fjall` passes
- [ ] `cargo clippy --features fjall -- -D warnings` passes (zero warnings)
- [ ] `cargo test --features fjall --lib` passes (all unit tests green)
- [ ] `cargo test --features fjall --test fjall_e2e` passes (all e2e tests green)
- [ ] `cargo bench --features fjall -- --no-run` compiles (benchmark integration works)
- [ ] All code documented (module docs, function docs, inline comments for non-obvious logic)
- [ ] Benchmark results collected (redb vs fjall comparison)
- [ ] Committed and pushed to `feature/fjall-storage-adapter`
