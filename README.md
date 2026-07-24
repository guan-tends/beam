# Rod

**A Rust implementation of [Gun.js](https://gun.eco/) — a real-time, decentralized, P2P-synced graph database with end-to-end encryption.**

[![Rust Edition](https://img.shields.io/badge/edition-2024-orange.svg)](https://doc.rust-lang.org/edition-guide/)
[![Rust Version](https://img.shields.io/badge/rust-≥1.85-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-178%20unit%20%7C%209%20integration%20%7C%207%20doc-brightgreen.svg)](#testing)

---

## What Is Rod?

Rod is a distributed graph database where every node holds a partial replica of the graph and synchronizes with peers in real time. Data flows over WebSocket relays, UDP multicast, or direct WebRTC connections. All cryptographic operations — signatures, key exchange, encryption — use the SEA layer (Security, Encryption, Authorization), providing Gun.js-compatible wire protocol and cryptographic semantics.

Rod is a from-scratch Rust port of [Gun.js](https://github.com/amark/gun), maintaining wire-format compatibility so Rod nodes can interop with Gun.js peers.

### Key Properties

- **Decentralized** — no central server; any peer can relay data to any other
- **Real-time** — `on()` subscriptions deliver updates as they propagate through the mesh
- **Eventually consistent** — last-write-wins conflict resolution via timestamps (matching Gun.js)
- **Encrypted** — SEA layer provides Ed25519 signing, X25519 ECDH, and AES-256-GCM encryption
- **Persistent** — `redb` embedded database for disk-backed storage, or in-memory for ephemeral use
- **Multi-transport** — WebSocket (relay), UDP multicast (LAN discovery), WebRTC (direct P2P)

---

## Quick Start

### Build

```bash
cargo build --release
```

### Run a Node

```bash
# Start with defaults: redb storage, WebSocket server on port 4944
cargo run --release -- --port 4944

# With WebRTC support (direct P2P connections)
cargo run --release --features webrtc -- --port 4944

# Connect to existing peers
cargo run --release -- --port 4944 --peers wss://relay1.example.com:8080/ws,wss://relay2.example.com:8080/ws

# With TLS
cargo run --release -- --port 4944 --cert-path /path/cert.pem --key-path /path/key.pem

# In-memory only (no persistence)
cargo run --release -- --port 4944 --memory-storage true --redb-storage false

# Restrict to signed data only (disable public space)
cargo run --release -- --port 4944 --allow-public-space false
```

### Generate a SEA Key Pair

```bash
# Generate a 32-byte base64-encoded session key
cargo run --release --bin beam-sea-keygen
```

### Use as a Library

```rust
use rod::{Node, Value};

#[tokio::main]
async fn main() {
    let mut db = Node::new();

    // Write
    db.get("greeting").put("Hello World!".into());

    // Subscribe to live updates
    let mut sub = db.get("greeting").on();
    if let Value::Text(s) = sub.recv().await.unwrap() {
        println!("{}", s); // "Hello World!"
    }

    // Read once
    let val = db.get("greeting").once(None).await;
    assert_eq!(val, Some(Value::Text("Hello World!".into())));

    db.stop();
}
```

### Connect Two Nodes Over WebSocket

```rust
use rod::adapters::*;
use rod::{Config, Node, Value};

#[tokio::main]
async fn main() {
    let config = Config::default();

    // Peer 1: WebSocket server
    let mut peer1 = Node::new_with_config(
        config.clone(),
        vec![],
        vec![Box::new(WsServer::new(config.clone()))],
    );

    // Peer 2: WebSocket client connecting to peer 1
    let client = OutgoingWebsocketManager::new(
        config.clone(),
        vec!["ws://localhost:4944/ws".to_string()],
    );
    let mut peer2 = Node::new_with_config(config, vec![], vec![Box::new(client)]);

    // Wait for connection
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Peer 2 writes, peer 1 receives via mesh sync
    peer2.get("hello").put("from peer 2".into());

    let mut sub = peer1.get("hello").on();
    if let Value::Text(s) = sub.recv().await.unwrap() {
        println!("Peer 1 received: {}", s);
    }

    peer1.stop();
    peer2.stop();
}
```

---

## Storage Backends

Rod supports two persistent storage backends for the embedded database layer. Both implement the same `Storage` trait, so the rest of the codebase is unaware of which one is active. The wire protocol is backend-agnostic — nodes with different storage choices converge via the standard mesh.

### redb (Default)

**What**: Embedded ACID database, single-writer, fsync on every Put.

**When to use**:
- Single-node deployments
- Low-to-moderate write throughput
- You want the most mature, stable option
- You don't want to think about it

**Trade-offs**:
- ✅ Battle-tested, single-crate, well-understood
- ✅ fsync before ack = bulletproof durability
- ❌ Single-writer serialization limits concurrent write throughput
- ❌ Not ideal for high-fanout mesh workloads

### Persy (Opt-In)

**What**: Embedded segment-based store with per-transaction isolation and optional `background_ops` fsync offloading.

**When to use**:
- Multi-node meshes with high concurrent write fanout
- Workloads where many writers hit disjoint keys simultaneously
- You're benchmarking and Persy shows wins on your data

**Trade-offs**:
- ✅ Multiple writers proceed in parallel on disjoint keys
- ✅ Optional `background_ops` for fsync offloading
- ❌ Younger ecosystem, fewer Stack Overflow answers
- ❌ Requires more careful substrate reading when debugging
- ❌ Performance characteristics need your own benchmarks

### Selection

Build with the feature, then select at runtime via CLI flag:

```bash
# Default build — redb only
cargo build --release

# With Persy support
cargo build --release --features persy

# Run with redb (default)
cargo run --release -- --port 4944 --redb-storage true

# Run with Persy
cargo run --release --features persy -- --port 4944 --persy-storage true

# In-memory only (no persistence)
cargo run --release -- --port 4944 --memory-storage true
```

The flags are mutually exclusive. `--redb-storage` is the default and works in any build. `--persy-storage` requires the `--features persy` build flag (the binary will error at startup otherwise).

### Migration Between Backends

The `rod migrate` subcommand converts between formats:

```bash
# Preview without writing
rod migrate --from redb --to persy --source ./data.redb --target ./data.persy --dry-run

# Execute migration
rod migrate --from redb --to persy --source ./data.redb --target ./data.persy

# Reverse direction
rod migrate --from persy --to redb --source ./data.persy --target ./data.redb
```

Migration uses single-transaction-per-batch for safety and includes checksum verification. See `docs/migrations/migration-guide.md` for the full procedure including rollback.

### Mixed Meshes

Nodes with different storage backends interoperate transparently. A redb node, a Persy node, and an in-memory node form a valid mesh. The wire protocol carries the data; storage is a local choice.

**Cross-backend mesh verified** by `tests/cross_backend_mesh_e2e.rs`: 2 redb nodes + 1 Persy node converge correctly under the standard Put/Get protocol.

### Known Limitations

- The `rod_meta_v1` metadata table from redb (last-write timestamps) is not preserved when migrating redb → Persy. This metadata is not currently used by the actor framework, so the loss is cosmetic.
- The migration tool is single-threaded per batch. For datasets larger than ~100k records, run during a maintenance window.

### Architecture Decision

See `docs/adr/013-persy-storage-backend.md` for the full rationale, alternatives considered, and consequences. See `docs/plans/PERSY-STORAGE-ADAPTER.md` for the implementation plan and ship log.
---

## Architecture

Rod is built on an actor model with a central router. Every component — storage, network, graph nodes — is an actor communicating via typed messages over Tokio channels.

```
                    ┌─────────────────────────────────────────────┐
                    │                  Node (root)                   │
                    │  uid=""  ← the root node owns the router       │
                    │  get("key") → child Node (uid="key")           │
                    │  put(value) → broadcasts to on() subscribers    │
                    │                and sends Put to router          │
                    └────────────────────┬────────────────────────────┘
                                         │ Message::Put / Get / Flush
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │                 Router                        │
                    │  - Deduplication (Dup: 999 entries, 9s TTL)   │
                    │  - Peer management (known_peers, server_peers) │
                    │  - Topic subscriptions (subscribers_by_topic)  │
                    │  - Put relay with anti-loop (peer_hop_list)     │
                    │  - Get routing (storage → server → random)     │
                    │  - RtcSignal routing to specific peers          │
                    └──────┬──────────────┬──────────────┬──────────┘
                           │              │              │
                    ┌──────▼──────┐ ┌──────▼──────┐ ┌─────▼──────┐
                    │  Storage    │ │  Network    │ │  WebRTC    │
                    │  Adapters   │ │  Adapters   │ │  (opt)     │
                    │             │ │             │ │            │
                    │ MemoryStorage│ │ WsServer    │ │ WebRtcPeer │
                    │ RedbStorage │ │ WsClient    │ │ (str0m)    │
                    │             │ │ Multicast   │ │            │
                    └─────────────┘ └─────────────┘ └────────────┘
```

### Module Map

| Module | Lines | Responsibility |
|--------|-------|---------------|
| `types.rs` | 330 | Core data types: `Value` (Null/Bit/Number/Text/Link), `NodeData` (value + timestamp), `Children` (BTreeMap), JSON conversion |
| `utils.rs` | 160 | `random_string()` (OS CSPRNG), `BoundedHashMap` (FIFO eviction for dedup tracking) |
| `dup.rs` | 210 | `Dup` — Gun.js DAM-style message deduplication (TTL + bounded capacity, 999 entries / 9s default) |
| `message.rs` | 817 | Wire protocol: `Get`, `Put`, `BatchPut`, `Flush`, `RtcSignal`, `Hi` — JSON serialization/deserialization, signature verification on inbound puts |
| `actor.rs` | 197 | Actor framework: `Actor` trait, `ActorContext`, `Addr` (clonable, hashable address) — built on Tokio unbounded channels |
| `node.rs` | 490 | Graph node API: `put()`, `get()`, `on()`, `once()`, `map()`, `batch_put()`, `connect_peer()`, `connect_webrtc_peer()`, `stop()` |
| `router.rs` | 455 | Central router: dedup, Get/Put routing, peer management, topic subscriptions, anti-loop relay, flush forwarding, RtcSignal delivery |
| `sea/pair.rs` | 75 | Key pair generation: ECDSA P-256 (signing) + ECDH P-256 (encryption), Gun.js `x.y` base64 format |
| `sea/sign.rs` | 53 | Ed25519-style signature creation (uses P-256 ECDSA via `ring`) |
| `sea/verify.rs` | 76 | Signature verification (sync + async variants) |
| `sea/work.rs` | 75 | Proof-of-work / content hashing (PBKDF2, SHA-256, base64) |
| `sea/secret.rs` | 85 | ECDH shared secret derivation between key pairs |
| `sea/encrypt.rs` | 200 | AES-256-GCM encryption with PBKDF2 key derivation; symmetric and ECDH-based modes |
| `sea/decrypt.rs` | 190 | AES-256-GCM decryption; shares `derive_aes_key_sync` with encrypt.rs (DRY) |
| `sea/certify.rs` | 155 | Capability certificates: issue, verify, check certificant membership, expiry enforcement |
| `sea/user.rs` | 523 | User identity: `create()`, `auth()`, `leave()`, `trust()`, `grant()`, `secret()`, `is()` — Gun.js `user.is` semantics |
| `sea/session/` | 516 | Session persistence: `MemorySessionStorage` (ephemeral) and `EncryptedFileSessionStorage` (disk, AES-GCM) |
| `adapters/memory_storage.rs` | 124 | In-memory `HashMap<node_id, Children>` storage (ephemeral, default for `Node::new()`) |
| `adapters/redb_storage.rs` | 261 | Persistent storage via `redb` embedded database — `BatchPut` atomic transactions, flush ack |
| `adapters/ws_server.rs` | 223 | WebSocket server: accepts inbound connections, spawns `WsConn` per connection, optional TLS, web UI on port+1 |
| `adapters/ws_client.rs` | 63 | `OutgoingWebsocketManager` — connects to remote WebSocket peers with retry |
| `adapters/ws_conn.rs` | 76 | Per-connection WebSocket actor: bridges wire format ↔ Message types |
| `adapters/multicast.rs` | 128 | UDP multicast LAN discovery (224.0.0.123:6969) — syncs with peers on local network |
| `adapters/webrtc.rs` | 483 | WebRTC data channel P2P via `str0m` — ICE/DTLS/SCTP, STUN discovery, TURN relay (feature-gated) |
| `stun.rs` | 171 | STUN Binding Request + TURN Allocate Request helpers (feature-gated) |
| `main.rs` | 250 | CLI entry point: clap v2 argument parsing, adapter configuration, Ctrl-C graceful shutdown |
| `bin/beam-sea-keygen.rs` | 30 | Utility binary: generates 32-byte random session key (base64-encoded) |

---

## Data Model

Rod uses a **key-path graph** — a hierarchical tree of nodes addressed by `/`-separated paths:

```
root (uid="")
  └── "users" (uid="users")
      └── "alice" (uid="users/alice")
          └── "profile" (uid="users/alice/profile")
              └── "name" (uid="users/alice/profile/name")
                  └── value = Value::Text("Alice")
```

### Node Operations

| Method | Description |
|--------|-------------|
| `db.get("key")` | Traverse to child node (creates lazily if it doesn't exist) |
| `node.put(value)` | Set a value on this node; propagates to parents and peers |
| `node.batch_put(ops)` | Atomic multi-write: multiple `(path, value)` pairs in one storage transaction |
| `node.on()` | Subscribe to value updates → `broadcast::Receiver<Value>` |
| `node.once(timeout)` | Read current value once (queries storage + peers), returns `Option<Value>` |
| `node.map()` | Subscribe to all children → `broadcast::Receiver<(String, Value)>` — replays existing children from storage |
| `db.connect_peer(url)` | Add a WebSocket peer at runtime |
| `db.connect_webrtc_peer(...)` | Bootstrap a WebRTC direct connection (requires `webrtc` feature) |
| `db.flush_storage(timeout)` | Flush storage adapters to disk (durable persistence) |
| `db.stop()` | Stop the node and all child actors/adapters |

### Path Depth and Data Access Semantics

Rod's graph operations differ from Gun.js in important ways. Understanding these prevents confusion.

#### One-Level Paths

Both flat (one-level) and nested paths work in Rod:

```rust
// Flat path — works
db.get("x").put("Hello World!".into());
let mut sub = db.get("x").on();
println!("{:?}", sub.recv().await.unwrap()); // Ok(Text("Hello World!"))

// Nested path — also works
db.get("x").get("y").put("Hello World!".into());
let mut sub = db.get("x").get("y").on();
println!("{:?}", sub.recv().await.unwrap()); // Ok(Text("Hello World!"))
```

> **Gun.js difference:** Gun.js prohibits saving primitive values at the root level — `Gun().put("oops")` and `Gun().get("odd").put("oops")` are errors. Rod does **not** enforce this restriction. Flat-key writes (`db.get("key").put(val)`) are valid and propagate to storage and peers normally.

#### `on()` — Subscribing to a Single Value

`on()` subscribes to a node's value and immediately requests the current value from storage (and peers, if connected). The broadcast receiver yields values in this order:

1. **Local value first** — if a value was already `put()` on this node, it arrives before any remote updates
2. **Streamed values** — new values from peers, storage replay, or subsequent `put()` calls

```rust
let mut db = Node::new();
db.get("greeting").put("Hello".into());

let mut sub = db.get("greeting").on();
// First recv: local value
assert_eq!(sub.recv().await.unwrap(), Value::Text("Hello".into()));

// Subsequent puts produce new values on the same subscription
db.get("greeting").put("World".into());
assert_eq!(sub.recv().await.unwrap(), Value::Text("World".into()));
```

#### `on()` on Branch Nodes Returns `Value::Link`

When a node has children, calling `on()` on that node returns a `Value::Link` to the node itself — **not** a reconstructed object containing the children. This is a key difference from Gun.js.

```rust
let mut db = Node::new();
db.get("x").get("y").get("z").put("Hello World 1!".into());
db.get("x").get("y").get("t").put("Hello World 2!".into());

let mut sub = db.get("x").get("y").on();
println!("{:?}", sub.recv().await.unwrap());
// Rod:          Link("x/y")
// Gun.js:       Object { z: "Hello World 1!", t: "Hello World 2!" }
```

Each `put()` on a descendant propagates up the parent chain via `add_parent_nodes()`, firing `on_sender` at every ancestor. This means `on()` at a branch node receives **one `Value::Link` event per child put** — not a single reconstructed object. These are granular change notifications, not partial data.

To enumerate children of a branch node, use `map()` instead (see below).

#### `once()` — Read Once With Timeout

`once()` is a convenience wrapper around `on()` + `tokio::time::timeout`. It subscribes, waits for the first value, and returns:

- `Some(value)` — if a value arrives within the timeout
- `None` — if no value arrives (timeout elapsed), equivalent to Gun.js's `undefined`

```rust
let mut db = Node::new();

// Missing value → None (timeout)
assert!(db.get("missing").once(None).await.is_none());

// Existing value → Some
db.get("key").put("value".into());
assert_eq!(
    db.get("key").once(None).await,
    Some(Value::Text("value".into()))
);

// Explicitly set to Null → Some(Null), NOT None
db.get("nulled").put(Value::Null);
assert_eq!(
    db.get("nulled").once(None).await,
    Some(Value::Null)
);

// Branch node → Some(Link)
db.get("x").get("y").get("z").put("val".into());
assert_eq!(
    db.get("x").get("y").once(None).await,
    Some(Value::Link("x/y".into()))
);
```

The default timeout is **66ms** (matching Gun.js's `opt.wait` default of 99ms, adjusted for Rust's async runtime). Pass a custom `Duration` to override:

```rust
// Wait up to 5 seconds for a value from a remote peer
let val = node.once(Some(Duration::from_secs(5))).await;
```

#### Why No Debounce?

Gun.js implements a debounce timer in `once()` that keeps resetting itself until the received data passes `Gun.valid()` (i.e., a complete primitive or relation arrives). This is necessary because Gun.js's `on()` at a branch node **accumulates partial object data** — `{ z: "a" }` arrives, then `{ z: "a", t: "b" }` — and the debounce waits for the object to settle before delivering.

Rod does **not** implement debounce because `on()` at branch nodes returns `Value::Link` — a **complete, valid primitive** on every delivery. There is nothing partial to accumulate. Each `Link` event is a complete value representing a change notification, not an incomplete snapshot. The 66ms timeout in `once()` is sufficient: it either receives a value or it doesn't.

#### `map()` — Listing Children

`map()` subscribes to all children of a node and replays existing children from storage. It yields `(child_key, value)` tuples. The value type depends on whether the child is itself a branch (has its own children) or a leaf:

```rust
let mut db = Node::new();
db.get("x").get("y").get("z").put("Hello World 1!".into());
db.get("x").get("y").get("t").put("Hello World 2!".into());

// map() at x/y (children are leaves) — yields actual values
let mut sub = db.get("x").get("y").map();
// → ("z", Text("Hello World 1!"))
// → ("t", Text("Hello World 2!"))
// → ("__rod_replay_complete__", Null)  ← sentinel: replay finished

// map() at x (children are branches) — yields Links
let mut sub2 = db.get("x").map();
// → ("y", Link("x/y"))
// → ("__rod_replay_complete__", Null)
```

The `__rod_replay_complete__` sentinel signals that all existing children have been replayed from storage. Subsequent values on the receiver are **new** children added after subscription. To read a child's actual value, call `on()` or `once()` on the child node directly.

#### Summary: `on()` vs `map()` vs `once()`

| Method | Returns | Leaf Node | Branch Node |
|--------|---------|-----------|-------------|
| `on()` | `Receiver<Value>` | Actual value (Text, Number, Null, Bit) | `Value::Link("node_id")` — one event per descendant put |
| `once(timeout)` | `Option<Value>` | `Some(actual_value)` or `None` | `Some(Link("node_id"))` or `None` |
| `map()` | `Receiver<(String, Value)>` | N/A (no children) | `("key", actual_value)` for leaf children, `("key", Link)` for branch children |

### Value Types

Rod supports five wire-compatible leaf types, matching Gun.js:

| Variant | JSON | Example |
|---------|------|---------|
| `Value::Null` | `null` | Absence of a value |
| `Value::Bit(true)` | `true` | Boolean flag |
| `Value::Number(42.0)` | `42` | Floating-point number |
| `Value::Text("hello")` | `"hello"` | Unicode string |
| `Value::Link("node/id")` | `{"#": "node/id"}` | Soul relation (graph edge) |

---

## SEA — Security, Encryption, Authorization

The SEA layer provides Gun.js-compatible cryptographic operations:

### Key Generation
```rust
let pair = rod::sea::generate_pair().await?;
// pair.pub_key   = "x.y" (P-256 ECDSA public key, base64 coordinates)
// pair.priv_key  = base64-encoded 32-byte scalar
// pair.epub_key  = "x.y" (P-256 ECDH public key for encryption)
// pair.epriv_key = base64-encoded 32-byte ECDH private scalar
```

### Signing & Verification
```rust
let signed = rod::sea::sign(&json!({"msg": "hello"}), &pair).await?;
// signed = {"m": {...}, "s": "base64-signature"}
let verified = rod::sea::verify_sync(&signed, &pair.pub_key)?;
```

### Encryption
```rust
// ECDH-based: derives shared secret from your epriv + their epub
let encrypted = rod::sea::encrypt(&data, &my_pair, Some(&their_epub)).await?;
let decrypted = rod::sea::decrypt(&encrypted, &my_pair, Some(&their_epub)).await?;

// Symmetric: raw 32-byte AES-256 key
let encrypted = rod::sea::encrypt_symmetric(&data, &key_bytes).await?;
let decrypted = rod::sea::decrypt_symmetric(&encrypted, &key_bytes).await?;
```

### User Identity
```rust
let mut node = Node::new();
let user = User::create("alice", "password123", &mut node).await?;
user.trust(&bob_pub, Some("path/prefix"), &mut node).await?;
user.grant(&bob_pub, &bob_epub, "path/secret", &mut node).await?;
user.secret(&json!({"api_key": "..."}), "wallet/key", &mut node).await?;
let identity = user.is(); // Some(Identity { alias, pub_key, epub_key })
user.leave(); // zeroizes keys, invalidates all clones
```

### Three Data Spaces

| Space | Who Can Write | Who Can Read | Node ID Prefix |
|-------|--------------|-------------|----------------|
| **Public** | Anyone (if `allow_public_space=true`) | Anyone | any (e.g. `"data"`) |
| **User** | Key owner only (signature verified) | Anyone | `~{pub_key}` or `~{pub_key}/...` |
| **Frozen** | Nobody (append-only, content-addressed) | Anyone | `#` (content hash = key) |

When `allow_public_space=false`, the node rejects unsigned puts to public space — only user-signed data (`~{pub}`) and content-addressed data (`#` namespace) are accepted. This matches Gun.js `opt.enforce` semantics.

---

## Wire Protocol

Rod uses Gun.js's JSON wire format. Messages are JSON objects with these fields:

### Put
```json
{
  "put": {
    "node/id": {
      "_": { "#": "node/id", ">": { "child_key": 1653465227430 } },
      "child_key": "value"
    }
  },
  "#": "msg_id_8chars",
  "##": 123456789,
  "><": "peer1,peer2"
}
```

| Field | Meaning |
|-------|---------|
| `put` | Map of node_id → {metadata, child values} |
| `_` | Node metadata: `#` = soul (node ID), `>` = child timestamps |
| `#` (top-level) | Message ID (8-char random, used for dedup) |
| `##` | Content checksum (Java `hashCode` of `put` body) |
| `><` | Peer hop list (anti-loop: comma-separated peer IDs already visited) |
| `@` | Ack ID — if present, this Put is a response to a Get with this ID |

### Get
```json
{
  "get": { "#": "node/id", ".": "optional_child_key" },
  "#": "msg_id_8chars"
}
```

### Other Messages
- `{"dam": "hi", "#": "peer_id"}` — peer introduction
- `{"dam": "flush", "#": "flush_id"}` — flush storage to disk
- `{"dam": "rtc", "id": "...", "offer": "...", "answer": "...", "candidate": "..."}` — WebRTC signaling

---

## Configuration

### CLI Flags

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `PORT` | 4944 | WebSocket server port |
| `--ws-server` | `WS_SERVER` | true | Enable WebSocket server |
| `--cert-path` | `CERT_PATH` | — | TLS certificate path (enables WSS) |
| `--key-path` | `KEY_PATH` | — | TLS private key path |
| `--peers` | `PEERS` | — | Comma-separated peer WebSocket URLs |
| `--multicast` | `MULTICAST` | false | Enable UDP multicast LAN discovery |
| `--memory-storage` | `MEMORY_STORAGE` | false | Use in-memory storage (ephemeral) |
| `--redb-storage` | `REDB_STORAGE` | true | Use redb persistent storage |
| `--redb-path` | `REDB_PATH` | `rod.redb` | Path to redb database file |
| `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true | Accept unsigned writes to public space |
| `--stats` | `STATS` | true | Expose stats at `/stats` on web UI port |

### Programmatic Config

```rust
let config = Config {
    allow_public_space: false,   // Reject unsigned public writes
    stats: true,                 // Expose stats endpoint
    my_pub: Some("x.y".into()),  // Prioritize this public key's data
    broadcast_buffer_size: 4096, // on()/map() channel capacity
    ice_servers: vec!["stun:stun.l.google.com:19302".into()],
};
```

---

## Testing

```bash
# Run all tests (178 unit + 9 integration + 7 doctests)
cargo test

# With WebRTC tests (186 unit + 9 integration + 7 doctests)
cargo test --features webrtc

# Lint (zero warnings required)
cargo clippy -- -D warnings

# Doctests only
cargo test --doc

# Benchmarks
cargo bench

# Run a specific integration test
cargo test --test integration websocket_sync_over_relay_peer
```

### Integration Test Categories

| Test | What It Verifies |
|------|-----------------|
| `it_doesnt_error` | Node creation, basic get — no panics |
| `first_get_then_put` | Subscribe-then-write ordering |
| `first_put_then_get` | Write-then-subscribe with storage replay |
| `once_returns_value_or_none` | Read-after-write consistency, Null vs absent |
| `connect_and_sync_over_websocket` | Two-node mesh sync over WS (direct) |
| `websocket_sync_over_relay_peer` | Three-node sync via relay (1 hop) |
| `websocket_sync_over_2_relay_peers` | Four-node sync via 2 relays (2 hops) — may be slow on CI |
| `redb_storage_persists` | Data survives restart with redb storage |
| `redb_storage_flush_returns_ok` | Flush ack protocol |

---

## Features

| Feature | Default | Enables |
|---------|---------|---------|
| `webrtc` | No | `dep:str0m`, `dep:stun` — direct P2P connections via WebRTC data channels |

Without `webrtc`, the `stun` module and `WebRtcPeer` adapter are stubbed out (functions return `None`).

---

## License

MIT — see [LICENSE](LICENSE).

## Credits

Rod was originally created by [Martti Malmi](https://github.com/mmalmi) as a from-scratch Rust port of [Gun.js](https://github.com/amark/gun) by Mark Nadal. The original Gun.js project is maintained by Mark Nadal.

This is an actively maintained fork with continued development by **Guan** (2026–present), comprising 109 commits across features, fixes, tests, and documentation.

### Major Contributions

**SEA Crypto Layer** (11 phases, `src/sea/`) — Implemented from stubs to full Gun.js-compatible crypto stack: ECDSA P-256 key generation, signing, verification, proof-of-work, ECDH shared secrets, AES-256-GCM encryption/decryption, capability certificates, and the full User system (`create`, `auth`, `trust`, `grant`, `secret`, `is`) with session persistence (memory + encrypted file storage). Includes `beam-sea-keygen` binary.

**WebRTC P2P Transport** (`src/adapters/webrtc.rs`) — Built str0m-based WebRTC data channel support behind a feature flag: STUN discovery, TURN relay allocation, `connect_webrtc_peer()` public API, `RtcSignal` message variant for signaling, and peer ID collision guards. Resolved 10+ integration bugs including ICE starvation, offer-miss races, and cross-node signal routing.

**Persistent Storage** (`src/adapters/redb_storage.rs`) — Added redb embedded database adapter with atomic `BatchPut` transactions, flush acknowledgement protocol, CLI flags (`--redb-storage`, `--redb-path`), and warm-schema startup to prevent `TableDoesNotExist` on first read.

**DAM Protocol Parity** — Implemented Gun.js's deduplication protocol: `Dup` TTL cache (999 entries / 9s), `##` checksum dedup for ack responses, `><` peer-hop lists for anti-loop relay. Removed dead `BoundedHashSet` code.

**Runtime Peer Addition** — `Node::connect_peer()` for dynamic WebSocket peer connections at runtime with exponential backoff retry.

**Flush Protocol** — `Flush` message variant with oneshot ack channels, `flush_storage()` API, and `__rod_replay_complete__` sentinel for deterministic `map()` replay termination.

**BatchPut** — `Message::BatchPut` variant and `Node::batch_put()` for multi-value single-transaction writes, with router forwarding and storage adapter support.

**Bug Fixes** — Root-level children propagation in `add_parent_nodes`, `RwLock` self-deadlock in `Node::get`, synchronous redb Put for ACID ordering, AES key derivation alignment with Gun.js `aeskey.js`, and broadcast buffer configurability.

**Production Review Pass** — Security audit, comprehensive inline documentation (module-level `//!`, item-level `///`, doctests), and test coverage across all 39 source files (178 unit + 9 integration + 7 doctests). Zero clippy warnings, zero compiler warnings. README, COMPASS (developer guide), and DEPLOY (operations guide) regenerated from code truth.

**Data Model Semantics** — Documented and verified the `on()`/`map()`/`once()` behavior model, including the key divergence from Gun.js's object reconstruction and the rationale for omitting debounce.

Deep gratitude to Martti for the original implementation and to Mark Nadal for Gun.js itself — a visionary approach to decentralized data. This fork carries that work forward.
