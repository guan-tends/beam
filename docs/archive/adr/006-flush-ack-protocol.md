# ADR-006: Flush-Ack Protocol

## Status
Accepted, 2026-05-18

## Context

Rod's put operation is **fire-and-forget**:
```
Node::put() → on_sender.broadcast() → add_parent_nodes() → Router::send(Put)
```

The caller receives no confirmation that data reached disk. For Mnemos (agent memory system), this created a critical gap: after writing an identity anchor, closing the process, and reopening, the data was sometimes missing. The storage adapter's write queue had not committed before process exit.

**Requirements:**
1. Guaranteed durability: caller knows data is on disk (fsync'd)
2. Non-blocking: flush request doesn't block other operations
3. Timeout-safe: caller can proceed after timeout without hanging
4. Works across all storage adapters (redb, sled, memory)

## Decision

Implement an **ack-based flush round-trip**:

```
Caller                    Router                    Storage
  |                         |                          |
  |--- Flush(id=X) ------->|                          |
  |                         |--- Flush(id=X) -------->|
  |                         |                          | fsync()
  |                         |<-- Put(in_response_to=X)-|
  |<-- Put(in_response_to=X)                          |
  | oneshot::Sender(())                              |
```

## Mechanism

### 1. Flush Message
```rust
pub struct Flush {
    pub from: Addr,       // actor address of the caller
    pub id: String,       // UUID for correlation
    pub node_id: String,  // target node (usually root "")
}
```

### 2. Pending Flushes
`Node` maintains `pending_flushes: Arc<RwLock<HashMap<String, oneshot::Sender<()>>>>`.

### 3. Intercepting Acks
In `Node::handle_put`, if `put.in_response_to` matches a pending flush ID:
```rust
if let Some(sender) = self.pending_flushes.write().remove(response_id) {
    let _ = sender.send(());
    return; // Don't process as normal data
}
```

### 4. Storage Adapter Behavior
| Adapter | handle_flush action |
|---------|---------------------|
| RedbStorage | Calls `db.begin_write()` + `commit()` (fsync), sends ack Put |
| SledStorage | Calls `flush_async()` on all trees, sends ack Put |
| MemoryStorage | No-op; sends ack Put immediately (for test parity) |

### 5. Timeout
Default 30s. Caller receives `Result<(), String>`:
- `Ok(())` — ack received, data is durable
- `Err("flush timed out")` — storage may or may not have committed

## Critical Bug (and Fix)

**Bug:** `flush.from` was initialized as `self.actor_context.addr` (a noop placeholder). Storage adapters sent acks to a dead address. Flushes always timed out.

**Fix:** `flush.from = self.addr.read().clone().unwrap()` — the actual actor address registered with the router.

## Code Example

```rust
let mut db = Node::new_with_config(config, vec![Box::new(redb)], vec![]);
db.get("key").put("value".into());

// Wait for disk durability
db.flush_storage(Some(Duration::from_secs(5))).await
    .expect("data must reach disk");

db.stop(); // orderly shutdown
```

## Mnemos Integration

Mnemos `RodStore` wraps `flush_storage()`:
```rust
impl ContentStore for RodStore {
    async fn flush(&self) -> Result<()> {
        self.node.flush_storage(Some(Duration::from_secs(30))).await
            .map_err(|e| MnemosError::Storage(e))
    }
}
```

CLI `mnemos wake` calls `palace.flush_all()` after anchor write, before exit.

## Consequences

### Positive
- Guaranteed durability for identity anchors, diary entries, KG triples
- Non-blocking: flush is async with timeout, caller isn't frozen
- Works across all storage backends uniformly

### Negative
- Adds one round-trip per flush (latency ~disk seek time)
- `pending_flushes` HashMap holds sender until ack or timeout (memory until resolved)
- Must call `Node::stop()` before drop for sled-backed storage (file handle release)

## References
- Implementation: `src/node.rs` (`flush_storage`, `handle_put`)
- Tests: `tests/integration.rs` — `flush_d2_ack`, `redb_storage_persists`
- Mnemos integration: `crates/mnemos-rod/src/lib.rs`
