# BEAM

**A real-time, decentralized, P2P-synced graph database written in Rust — maintaining wire-format compatibility with [Gun.js](https://gun.eco/).**

[![Rust Edition](https://img.shields.io/badge/edition-2024-orange.svg)](https://doc.rust-lang.org/edition-guide/)
[![Rust Version](https://img.shields.io/badge/rust-≥1.85-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-178%20unit%20%7C%209%20integration%20%7C%207%20doc-brightgreen.svg)](#testing)

---

## What Is BEAM?

BEAM is a distributed graph database where every node holds a partial replica of the graph and synchronizes with peers in real time. Data flows over WebSocket relays, UDP multicast, or direct WebRTC connections. All cryptographic operations — signatures, key exchange, encryption — use the SEA layer (Security, Encryption, Authorization), providing Gun.js-compatible wire protocol and cryptographic semantics.

BEAM began as a from-scratch Rust port of [Gun.js](https://github.com/amark/gun), maintaining wire-format compatibility so BEAM nodes can interop with Gun.js peers. It has since grown into a comprehensive distributed-database system with multiple storage backends, WebRTC direct P2P, observability, and migration tooling.

### Key Properties

- **Decentralized** — no central server; any peer can relay data to any other
- **Real-time** — `on()` subscriptions deliver updates as they propagate through the mesh
- **Eventually consistent** — last-write-wins conflict resolution via timestamps (matching Gun.js)
- **Encrypted** — SEA layer provides Ed25519 signing, X25519 ECDH, and AES-256-GCM encryption
- **Persistent** — `redb` embedded database for disk-backed storage, or `Persy` for high-concurrency workloads, or in-memory for ephemeral use
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
cargo run --release --bin beam -- --port 4944

# With WebRTC support (direct P2P connections)
cargo run --release --bin beam --features webrtc -- --port 4944

# Connect to existing peers
cargo run --release --bin beam -- --port 4944 --peers wss://relay1.example.com:8080/ws,wss://relay2.example.com:8080/ws

# With TLS
cargo run --release --bin beam -- --port 4944 --cert-path /path/cert.pem --key-path /path/key.pem

# In-memory only (no persistence)
cargo run --release --bin beam -- --port 4944 --memory-storage true --redb-storage false

# Restrict to signed data only (disable public space)
cargo run --release --bin beam -- --port 4944 --allow-public-space false
```

### Generate a SEA Key Pair

```bash
# Generate a 32-byte base64-encoded session key
cargo run --release --bin beam-sea-keygen
```

### Use as a Library

Add to `Cargo.toml`:

```toml
[dependencies]
beamdb = { git = "http://192.168.8.142:8561/guan/beamdb.git" }
```

In your code:

```rust
use beam::{Node, Value};

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
use beam::adapters::*;
use beam::{Config, Node, Value};

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

BEAM supports two persistent storage backends for the embedded database layer. Both implement the same `Storage` trait, so the rest of the codebase is unaware of which one is active. The wire protocol is backend-agnostic — nodes with different storage choices converge via the standard mesh.

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
cargo build --release --bin beam

# With Persy support
cargo build --release --bin beam --features persy

# Run with redb (default)
cargo run --release --bin beam -- --port 4944 --redb-storage true

# Run with Persy
cargo run --release --bin beam --features persy -- --port 4944 --persy-storage true

# In-memory only (no persistence)
cargo run --release --bin beam -- --port 4944 --memory-storage true
```

The flags are mutually exclusive. `--redb-storage` is the default and works in any build. `--persy-storage` requires the `--features persy` build flag (the binary will error at startup otherwise).

### Migration Between Backends

The `beam migrate` subcommand converts between formats:

```bash
# Preview without writing
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy --dry-run

# Execute migration
beam migrate --from redb --to persy --source ./data.redb --target ./data.persy

# Reverse direction
beam migrate --from persy --to redb --source ./data.persy --target ./data.redb
```

Migration uses single-transaction-per-batch for safety and includes checksum verification. See `docs/migrations/migration-guide.md` for the full procedure including rollback.

### Mixed Meshes

Nodes with different storage backends interoperate transparently. A redb node, a Persy node, and an in-memory node form a valid mesh. The wire protocol carries the data; storage is a local choice.

**Cross-backend mesh verified** by `tests/cross_backend_mesh_e2e.rs`: 2 redb nodes + 1 Persy node converge correctly under the standard Put/Get protocol.

### Known Limitations

- The `beam_meta_v1` metadata table from redb (last-write timestamps) is not preserved when migrating redb → Persy. This metadata is not currently used by the actor framework, so the loss is cosmetic.
- The migration tool is single-threaded per batch. For datasets larger than ~100k records, run during a maintenance window.

### Architecture Decision

See `docs/adr/013-persy-storage-backend.md` for the full rationale, alternatives considered, and consequences.

---

## Architecture

BEAM is built on an actor model with a central router. Every component — storage, network, graph nodes — is an actor communicating via typed messages over Tokio channels.

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
                    │ PersyStorage│ │ Multicast   │ │            │
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
| `adapters/persy_storage.rs` | 652 | Persistent storage via `Persy` segment store — high-concurrency writes, optional `background_ops` |
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

BEAM uses a **key-path graph** — a hierarchical tree of nodes addressed by `/`-separated paths:

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

BEAM's graph operations differ from Gun.js in important ways. Understanding these prevents confusion.

#### One-Level Paths

Both flat (one-level) and nested paths work in BEAM:

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

> **Gun.js difference:** Gun.js prohibits saving primitive values at the root level — `Gun().put("oops")` and `Gun().get("odd").put("oops")` are errors. BEAM does **not** enforce this restriction. Flat-key writes (`db.get("key").put(val)`) are valid and propagate to storage and peers normally.

#### `on()` — Subscribing to a Single Value

`on()` subscribes to a node's value and immediately requests the current value from storage (and peers, if connected). The broadcast receiver yields values in this order:

1. **Local value first** — if a value was already `put()` on this node, it arrives before any remote updates
2. **Streamed values** — new values from peers, storage replay, or subsequent `put()` calls
3. **Linked values** — `Value::Link("path/to/child")` if a child reference exists

#### `map()` — Subscribing to All Children

`map()` returns a stream of `(child_key, value)` pairs. It replays existing children from storage, then streams new ones as they're added. A sentinel `("__beam_replay_complete__", Null)` signals that all existing children have been replayed; subsequent values are **new** children only.

```rust
let mut sub = db.get("users").map();
while let Some((key, value)) = sub.recv().await {
    if key == "__beam_replay_complete__" {
        break; // replay finished; subscribe to new children separately
    }
    println!("child: {} = {:?}", key, value);
}
```

The `__beam_replay_complete__` sentinel signals that all existing children have been replayed from storage. Subsequent values on the receiver are **new** children added after subscription. To read a child's actual value, call `on()` or `once()` on the child node directly.

#### `once()` — Read-Once with Timeout

`once()` returns the current value with a 66ms timeout (matching Gun.js's default). If no value exists and no peer responds within the window, returns `None`. Use `once()` for one-shot reads; use `on()` for subscriptions.

### Wire-Compatible Leaf Types

BEAM supports five wire-compatible leaf types, matching Gun.js:

| Type | Wire Format | Example |
|------|-------------|---------|
| `Value::Null` | `null` | Absent or explicitly null |
| `Value::Bit(bool)` | `true` / `false` | Booleans |
| `Value::Number(f64)` | JSON number | `42`, `3.14` |
| `Value::Text(String)` | JSON string | `"hello"` |
| `Value::Link(String)` | `{"#": "path/to/child"}` | Reference to another node |

---

## Cryptography (SEA Layer)

The SEA (Security, Encryption, Authorization) module implements Gun.js-compatible cryptography. All operations use `ring` for primitives and `pbkdf2` for key derivation.

### Key Pair Generation

```rust
use beam::sea;

let pair = beam::sea::generate_pair().await?;
println!("pub: {}", pair.pub_key);   // ECDSA public key (Gun.js x.y format)
println!("epub: {}", pair.epub_key); // ECDH public key
println!("priv: {}", pair.priv_key); // ECDSA private key
println!("epriv: {}", pair.epriv_key); // ECDH private key
```

### Signing and Verification

```rust
use beam::sea;
use serde_json::json;

let signed = beam::sea::sign(&json!({"msg": "hello"}), &pair).await?;
let verified = beam::sea::verify_sync(&signed, &pair.pub_key)?;
```

### Encryption and Decryption

```rust
use beam::sea;

let data = b"secret message";

// Asymmetric (ECDH key exchange + AES-GCM)
let encrypted = beam::sea::encrypt(&data, &my_pair, Some(&their_epub)).await?;
let decrypted = beam::sea::decrypt(&encrypted, &my_pair, Some(&their_epub)).await?;

// Symmetric: raw 32-byte AES-256 key
let encrypted = beam::sea::encrypt_symmetric(&data, &key_bytes).await?;
let decrypted = beam::sea::decrypt_symmetric(&encrypted, &key_bytes).await?;
```

### User Identity

```rust
use beam::sea::user::User;

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

BEAM uses Gun.js's JSON wire format. Messages are JSON objects with these fields:

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
| `--redb-path` | `REDB_PATH` | `beam.redb` | Path to redb database file |
| `--allow-public-space` | `ALLOW_PUBLIC_SPACE` | true | Accept unsigned writes to public space |
| `--stats` | `STATS` | true | Expose stats at `/stats` on web UI port |

### Programmatic Config

```rust
use beam::Config;

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
| `persy` | No | `dep:persy` — Persy storage backend for high-concurrency workloads |

Without `webrtc`, the `stun` module and `WebRtcPeer` adapter are stubbed out (functions return `None`). Without `persy`, the `PersyStorage` adapter is not compiled in and `--persy-storage` will error at startup.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Credits

BEAM was originally created by [Martti Malmi](https://github.com/mmalmi) as a from-scratch Rust port of [Gun.js](https://github.com/amark/gun) by Mark Nadal. The original Gun.js project is maintained by Mark Nadal.

This is an actively maintained fork with continued development by **Guan** and maintained by **David Newman** (2026–present), comprising 367+ commits across features, fixes, tests, and documentation.

### Major Contributions

**SEA Crypto Layer** (11 phases, `src/sea/`) — Implemented from stubs to full Gun.js-compatible crypto stack: ECDSA P-256 key generation, signing, verification, proof-of-work, ECDH shared secrets, AES-256-GCM encryption/decryption, capability certificates, and the full User system (`create`, `auth`, `trust`, `grant`, `secret`, `is`) with session persistence (memory + encrypted file storage). Includes `beam-sea-keygen` binary.

**WebRTC P2P Transport** (`src/adapters/webrtc.rs`) — Built str0m-based WebRTC data channel support behind a feature flag: STUN discovery, TURN relay allocation, `connect_webrtc_peer()` public API, `RtcSignal` message variant for signaling, and peer ID collision guards. Resolved 10+ integration bugs including ICE starvation, offer-miss races, and cross-node signal routing.

**Persistent Storage** (`src/adapters/redb_storage.rs`, `src/adapters/persy_storage.rs`) — Added redb embedded database adapter with atomic `BatchPut` transactions, flush acknowledgement protocol, CLI flags (`--redb-storage`, `--redb-path`), and warm-schema startup to prevent `TableDoesNotExist` on first read. Added Persy segment-store adapter with cross-backend mesh interop and migration tooling (`beam migrate`).

**DAM Protocol Parity** — Implemented Gun.js's deduplication protocol: `Dup` TTL cache (999 entries / 9s), `##` checksum dedup for ack responses, `><` peer-hop lists for anti-loop relay. Removed dead `BoundedHashSet` code.

**Network Fanout Ack (Quorum)** — Sentinel-driven async ack pattern across 6 surfaces (put, batch_put, flush, map, put_quorum, put_quorum_timeout). One oneshot registry (`pending_puts`), one timeout envelope (`tokio::time::timeout`), different decoders per use case. Gun.js ask-pattern wire compatibility.

**Bounded Channel with Drop Logging** — `BoundedHashMap::push` returns `Result`, with `try_send_or_log` pattern for non-blocking writes. Fixed silent drop bug in Follow-up B.

**Runtime Peer Addition** — `Node::connect_peer()` for dynamic WebSocket peer connections at runtime with exponential backoff retry.

**Flush Protocol** — `Flush` message variant with oneshot ack channels, `flush_storage()` API, and `__beam_replay_complete__` sentinel for deterministic `map()` replay termination.

**BatchPut** — `Message::BatchPut` variant and `Node::batch_put()` for multi-value single-transaction writes, with router forwarding and storage adapter support.

**Heavy Abusive Benchmarks** — Criterion suite comparing redb vs Persy across sequential writes, concurrent writes, reads, mixed 70/30, and memory pressure. Results documented in `benches/RESULTS.md`.

**Bug Fixes** — Root-level children propagation in `add_parent_nodes`, `RwLock` self-deadlock in `Node::get`, synchronous redb Put for ACID ordering, AES key derivation alignment with Gun.js `aeskey.js`, and broadcast buffer configurability.

**Production Review Pass** — Security audit, comprehensive inline documentation (module-level `//!`, item-level `///`, doctests), and test coverage across all source files. Zero clippy warnings on introduced code. README, COMPASS (developer guide), and DEPLOY (operations guide) regenerated from code truth.

**Enterprise Stabilization** — Eliminated test flakiness by replacing blind `sleep()` patterns with active readiness polling (`wait_for_peer_count`, `wait_for_port`, `wait_for_handshake`). 5-consecutive-pass discipline verified across 4 feature configs.

**Data Model Semantics** — Documented and verified the `on()`/`map()`/`once()` behavior model, including the key divergence from Gun.js's object reconstruction and the rationale for omitting debounce.

Deep gratitude to Martti for the original implementation and to Mark Nadal for Gun.js itself — a visionary approach to decentralized data. This fork carries that work forward under the BEAM identity.

---

## Sponsors

BEAM is open-source and built by a small team. If this crate is useful to your work, consider sponsoring to support ongoing development, faster updates, and new features.

### Donate

| Chain | Address |
|-------|---------|
| **Solana** | `Eu8wQcW68TKMs1a6eqzZu8znzU52QLqQugAMG8uCD6y6` |
| **Ethereum / EVM** | `0x2733ff7c865C56d565a99BE1DC11B81cc76850A5` |
| **XRP Ledger** | `r4X6e7McAQj7e8vBCeued1RYu4mCJrREDG` |
