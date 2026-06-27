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

## Origin

Rod is a Rust port of [Gun.js](https://github.com/amark/gun) by Martti Malmi. The original Gun.js project is maintained by Mark Nadal.
