# ADR-005: Redb Storage Adapter

## Status
Accepted, 2026-05-18

## Context

Rod supports three storage adapters:
- **MemoryStorage** — ephemeral, test-safe, no persistence
- **SledStorage** — disk-backed via sled.rs BwTree
- **RedbStorage** — disk-backed via redb MVCC (new)

**Sled deadlock under load.** During Mnemos transference sessions (agent memory system built on Rod), SledStorage triggered reproducible deadlocks under concurrent write pressure. Root cause: sled's BwTree lock ordering under heavy write contention. This blocked Mnemos Phase 2 (persistent storage) and Phase 5 (cross-process flush proof).

**Requirements for replacement:**
1. Pure Rust (no C dependencies, WASM-compatible)
2. ACID transactions with fsync durability
3. Handles `node_id = ""` (Rod root) without special-casing
4. Spawn-blocking friendly for tokio async runtimes
5. Single-file deployment (tent-scale infrastructure)

## Decision

Adopt [redb](https://github.com/cberner/redb) as the default disk storage adapter. Sled becomes legacy/deprecated.

## Consequences

### Positive
- **No deadlocks.** redb uses MVCC (multi-version concurrency control) with optimistic locking. No BwTree lock ordering issues.
- **ACID guarantees.** Write transactions commit atomically with fsync.
- **Pure Rust.** No unsafe C bindings. Cross-compiles to WASM targets.
- **Single file.** One `.redb` file per database. Easy backup, rsync, copy.
- **Schema migration friendly.** Table definitions are versioned; new tables added without migration scripts.

### Negative
- **Single writer.** redb allows only one active write transaction at a time. Mitigated by Actor message-loop serialization (only one `handle_put` runs at a time) + `spawn_blocking` to free the async runtime during disk I/O.
- **Write amplification.** Each put rewrites the entire `Children` BTreeMap for a node. Acceptable for tent-scale; may need priority eviction for production-scale deployments.
- **No built-in eviction.** Unlike sled's priority system, redb has no automatic size cap. Application-level eviction deferred to Phase 2.

## Schema

```rust
TableDefinition::<&str, &[u8]>::new("rod_data")
// Key   = node_id (String) — "empty string "" is valid (root node)"
// Value = bincode(BTreeMap<String, NodeData>)

TableDefinition::<&str, u64>::new("rod_meta")
// Key   = "size"
// Value = total bytes stored (approximate)
```

Rationale: Rod's mental model is `HashMap<String, Children>` where `Children = BTreeMap<String, NodeData>`. Storing the entire `Children` blob per node_id matches this exactly. No custom Key trait, no delimiter escaping.

## Write Path

```
Router::handle_put → RedbStorage::handle_put
  → spawn_blocking:
    → write_txn = db.begin_write()
    → table = write_txn.open_table(ROD_DATA)
    → table.insert(node_id, bincode(children))?
    → write_txn.commit()  // fsync here
  → JoinHandle awaited in actor loop
```

**Critical:** No `.await` between `begin_write` and `commit`. The entire transaction body is inside `spawn_blocking`.

## Read Path

```
Router::handle_get → RedbStorage::handle_get
  → read_txn = db.begin_read()
  → table = read_txn.open_table(ROD_DATA)
  → value = table.get(node_id)?
  → deserialize bincode → BTreeMap
```

Read stays in async context — fast hot path.

## Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| Fix sled deadlock | Familiar API, priority eviction | Root cause in BwTree, upstream unmaintained | ❌ Rejected |
| SQLite via rusqlite | Battle-tested, SQL | C dependency, schema mismatch with graph model | ❌ Rejected |
| rocksdb via rust-rocksdb | Production scale | C++ dependency, large binary, complex build | ❌ Rejected |
| redb (chosen) | Pure Rust, ACID, single file | Single writer, write amp | ✅ Accepted |

## Migration Path

1. New `mnemos init` defaults to `kind = "redb"`
2. Existing sled databases continue working (backward compat)
3. `tracing::warn!` emitted when sled is selected at runtime
4. Sled removal scheduled for v0.3.0 (after migration tooling)

## References

- redb: https://github.com/cberner/redb
- Sled deadlock analysis: See Mnemos Phase 5 session archive (2026-05-14)
- Implementation: `src/adapters/redb_storage.rs`
- Tests: `tests/integration.rs` — `redb_storage_persists`, `flush_d2_ack`
