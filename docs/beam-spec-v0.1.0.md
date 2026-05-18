# Rod Specification v0.1.0 — BEAM Maintained Fork

> "The graph IS the database." — Martti Malmi

Rust implementation of the [Gun](https://gun.eco/) decentralized graph protocol, maintained by Freeman King and Guan for the Mnemos agent memory system.

**Version:** 0.2.4 | **Edition:** 2024 | **MSRV:** 1.85 | **Date:** 2026-05-18

## 1. Graph Primitives

Rod is a **graph database** where every data element is a node. Nodes contain properties (key-value). Nodes reference other nodes via `Link(String)` souls.

### Core Operations

| Operation | Signature | Description |
|-----------|-----------|-------------|
| `get(key)` | `Node::get(&str) → Node` | Traverse to child node (lazy) |
| `put(value)` | `Node::put(Value)` | Store value at current path |
| `once(timeout)` | `Node::once(Option<Duration>) → Option<Value>` | Read value once, with optional timeout |
| `on()` | `Node::on() → broadcast::Receiver<Value>` | Subscribe to value changes |
| `map()` | `Node::map() → broadcast::Receiver<(String, Value)>` | Subscribe to all children |
| `set(value)` | `Node::set(Value)` | Add to mathematical set |

### Value Types

```rust
pub enum Value {
    Null,
    Bit(bool),
    Number(f64),
    Text(String),
    Link(String),  // Soul reference: "#mnemos/palace/content/abc123"
}
```

### Namespaces

| Prefix | Meaning | Example |
|--------|---------|---------|
| `~` | Signed / identity | `~abc123pubkey/profile` |
| `#` | Content-addressed / immutable | `#mnemos/palace/content/blake3hash` |
| (none) | Public graph | `mnemos/palace/wings/wing_001` |

## 2. Storage Adapters

### Three Implementations

```rust
// Ephemeral
let db = Node::new(); // MemoryStorage only

// Disk-backed (default)
let config = Config::default();
let redb = RedbStorage::new_with_config(config.clone(), "rod.redb");
let db = Node::new_with_config(config, vec![Box::new(redb)], vec![]);

// Legacy
let sled = SledStorage::new_with_config(config.clone(), "rod.sled");
```

| Adapter | Persistence | Use Case | Status |
|---------|-------------|----------|--------|
| `MemoryStorage` | No | Tests, ephemeral | Active |
| `RedbStorage` | Yes (ACID) | Production, tent-scale | **Default** |
| `SledStorage` | Yes (BwTree) | Legacy, existing dbs | Deprecated |

### Redb Adapter (RedbStorage)

- **Schema:** Single `rod_data` table (`node_id → bincode(Children)`)
- **Writes:** `spawn_blocking` for fsync, freeing async runtime
- **Reads:** Synchronous, fast hot path
- **ACID:** Atomic commits with fsync durability

See `docs/adr/005-redb-storage-adapter.md` for full architecture.

## 3. Flush Protocol

Rod's put is fire-and-forget. The flush protocol provides **guaranteed durability**:

```rust
db.get("key").put("value".into());
db.flush_storage(Some(Duration::from_secs(5))).await?; // waits for fsync ack
db.stop(); // orderly shutdown
```

### Flow

```
Caller → Flush(id=X) → Router → Storage → fsync() → Put(in_response_to=X) → Caller
```

- `pending_flushes` HashMap correlates acks
- Timeout: 30s default
- Works across all storage adapters

See `docs/adr/006-flush-ack-protocol.md` for full mechanism.

## 4. BEAM SEA

Gun.js-compatible user authentication with production-grade session security.

### Quick Start

```rust
let mut db = Node::new();
let user = db.user().create("alice", "secret123").await?;
user.save_to(&storage).await?; // encrypted session cache

let recalled = User::recall("alice", &storage).await?;
assert!(recalled.is_authenticated());
```

### Session Key Generation

```bash
cargo run --quiet --bin beam-sea-keygen
# → qGmUkJ5mZSg45XVzMHOKH9IxiamPI5wmqIAnwzASr/M=
```

See README.md "BEAM SEA" section and `docs/adr/004-session-storage.md` for full details.

## 5. Build & Run

### Requirements
- Rust ≥ 1.85 (edition 2024)
- `BEAM_SEA_SESSION_KEY` for encrypted session storage (production)

### Install
```bash
cargo install --path .
rod start --redb-storage --redb-path my-node.redb
```

### Library
```rust
use rod::{Node, Config, Value};
use rod::adapters::RedbStorage;

#[tokio::main]
async fn main() {
    let config = Config::default();
    let redb = RedbStorage::new_with_config(config.clone(), "rod.redb");
    let mut db = Node::new_with_config(config, vec![Box::new(redb)], vec![]);
    
    db.get("greeting").put("Hello World!".into());
    db.flush_storage(Some(Duration::from_secs(5))).await.unwrap();
    db.stop();
}
```

## 6. CLI

```bash
rod start [OPTIONS]

Options:
    --memory-storage      Use in-memory storage (ephemeral)
    --sled-storage        Use sled storage (legacy, deprecated)
    --redb-storage        Use redb storage (default)
    --redb-path <PATH>    Path to redb database file [default: rod.redb]
    -c, --config <FILE>   Custom config file
```

## 7. Tests

```bash
cargo test                    # All tests (unit + integration)
cargo test -- --test-threads=1  # Serial (for deterministic debugging)
```

| Test | File | What It Proves |
|------|------|----------------|
| `first_put_then_get` | `tests/integration.rs` | Basic round-trip |
| `first_get_then_put` | `tests/integration.rs` | Subscription replay |
| `once_returns_value_or_none` | `tests/integration.rs` | Timeout semantics |
| `sled_storage` | `tests/integration.rs` | Sled persistence |
| `redb_storage_persists` | `tests/integration.rs` | Redb persistence across restart |
| `flush_d2_ack` | `tests/integration.rs` | Flush round-trip via redb |
| `connect_and_sync_over_websocket` | `tests/integration.rs` | P2P sync (may timeout under load) |

## 8. Architecture Decisions

| ADR | Topic | Status |
|-----|-------|--------|
| 004 | Session Storage (BEAM SEA) | Accepted |
| 005 | Redb Storage Adapter | Accepted |
| 006 | Flush-Ack Protocol | Accepted |
| 007 | Edition 2024 / rust-version 1.85 | Accepted |

## 9. Fork Status

This is a maintained fork of [mmalmi/rod](https://github.com/mmalmi/rod) for the Mnemos agent memory system. Key divergences:

- Redb replaces sled as default storage
- Flush protocol for guaranteed durability
- Edition 2024 / rust-version 1.85
- BEAM SEA session encryption
- `Node::stop()` for orderly shutdown

Upstream PRs welcomed where changes are generally applicable.

---

*"The graph remembers forward."* 🪷
*Maintained by Freeman King and Guan, the Keeper of the Threshold*
