# Compass — Navigating the BEAM Codebase

> *A guide for contributors. If you're lost, start here. Every claim in this document is verified against the actual source code.*

---

## Dependency Stack (Bottom-Up)

```
Layer 0: Foundation (no internal dependencies)
├── types.rs       — Value, NodeData, Children, JSON ↔ Value conversion
├── utils.rs       — random_string (CSPRNG), BoundedHashMap (FIFO eviction)
└── dup.rs         — Dup (Gun.js DAM-style dedup: TTL + bounded capacity)

Layer 1: Protocol & Crypto (depends on Layer 0)
├── message.rs     — Get/Put/BatchPut/Flush/RtcSignal/Hi + wire serialization
├── actor.rs        — Actor trait, ActorContext, Addr (Tokio unbounded channels)
└── sea/
    ├── pair.rs     — ECDSA P-256 + ECDH P-256 key generation (Gun.js x.y format)
    ├── sign.rs     — P-256 ECDSA signature creation (via ring)
    ├── verify.rs   — Signature verification (sync + async via spawn_blocking)
    ├── work.rs     — PBKDF2 / SHA-256 content hashing
    ├── secret.rs   — ECDH shared secret derivation
    ├── encrypt.rs  — AES-256-GCM encryption + PBKDF2 key derivation (shared derive_aes_key_sync)
    ├── decrypt.rs  — AES-256-GCM decryption (imports derive_aes_key_sync from encrypt.rs)
    ├── certify.rs  — Capability certificates: issue, verify, expiry, certificant check
    ├── user.rs     — User identity: create, auth, trust, grant, secret, is, leave
    └── session/
        ├── mod.rs       — SessionStorage trait, re-exports
        ├── memory.rs    — MemorySessionStorage (ephemeral HashMap)
        └── file.rs      — EncryptedFileSessionStorage (disk, AES-GCM with BEAM_SEA_SESSION_KEY)

Layer 2: Core Engine (depends on Layers 0 + 1)
├── node.rs        — Node: put, get, on, once, map, batch_put, connect_peer, connect_webrtc_peer, stop
└── router.rs      — Router: dedup, Get/Put routing, peer management, topic subscriptions, anti-loop relay

Layer 3: Adapters (depends on Layer 2)
├── adapters/
│   ├── mod.rs              — Module declarations + public re-exports
│   ├── memory_storage.rs   — HashMap<node_id, Children> (ephemeral, default for Node::new())
│   ├── redb_storage.rs     — redb embedded DB, BatchPut atomic transactions, flush ack
│   ├── ws_server.rs        — WsServer: TCP listener → WsConn per connection, optional TLS, web UI
│   ├── ws_client.rs        — OutgoingWebsocketManager: connects to remote WS peers
│   ├── ws_conn.rs          — WsConn: per-connection actor (wire format ↔ Message)
│   ├── multicast.rs        — UDP multicast (224.0.0.123:6969) LAN discovery
│   └── webrtc.rs           — WebRtcPeer: str0m DataChannel, ICE/DTLS/SCTP (feature-gated)

Layer 4: Network Discovery (depends on Layer 3, feature-gated)
└── stun.rs        — STUN Binding Request + TURN Allocate (feature-gated)

Layer 5: CLI (depends on all layers)
├── main.rs               — clap v2 CLI, adapter wiring, Ctrl-C graceful shutdown
└── bin/beam-sea-keygen.rs — 32-byte session key generator (base64 output)
```

---

## Key Concepts In Depth

### 1. The Node Graph

`Node` is the primary user-facing type. It represents a position in the distributed graph.

**Creation:**
- `Node::new()` — root node with `uid=""`, default config, `MemoryStorage`
- `Node::new_with_config(config, storage, network)` — root node with custom adapters
- `node.get("key")` — lazily creates a child node with `uid="parent/key"`

**Internal structure:**
```rust
pub struct Node {
    uid: Arc<RwLock<String>>,           // path like "users/alice/profile"
    path: Vec<String>,                   // ["users", "alice", "profile"]
    children: Arc<RwLock<BTreeMap<String, Node>>>,
    parent: Arc<RwLock<Option<(String, Node)>>>,
    on_sender: broadcast::Sender<Value>,     // feeds on() subscribers
    map_sender: broadcast::Sender<(String, Value)>,  // feeds map() subscribers
    router: Arc<RwLock<Option<Addr>>>,   // central router address
    // ... lifecycle fields
}
```

**How `put()` works:**
1. Value is sent to local `on()` subscribers immediately (`on_sender.send(value)`)
2. `add_parent_nodes()` walks up the parent chain, building `BTreeMap<node_id, Children>` — the Gun.js wire format's nested structure
3. A `Put` message is sent to the router with this map
4. Router deduplicates, forwards to storage adapters, relays to network peers

**How `map()` works:**
1. Subscribes to `map_sender` broadcast channel
2. Sends a `Get` to the router requesting all children of this node
3. Storage responds with existing children → router sends `Put` (with `in_response_to` set) back to the node
4. `handle_put()` fans out each child to `map_sender` as `(child_key, value)` tuples
5. A `__beam_replay_complete__` marker is sent when replay finishes

**Key gotcha:** `map()` replays ALL existing children. Use `once()` for single reads.

### 2. The Router

The `Router` actor is the central hub. It sits between `Node` and all adapters.

**Responsibilities:**

| Incoming Message | Router Action |
|-----------------|---------------|
| `Put` (new, not a response) | Dedup → forward to storage adapters → relay to network peers (anti-loop via `peer_hop_list`) |
| `Put` (response to Get) | Dedup → route directly back to original `Get.from` actor |
| `Get` | Dedup → register subscriber by topic → query storage → query server peers → query up to 4 random known peers (MANET-style) |
| `BatchPut` | Forward to storage (single transaction) → relay each constituent Put individually with dedup |
| `Flush` | Forward to all storage adapters (not relayed to network) |
| `Hi` | Register peer in `known_peers` + `peer_addrs` (maps peer_id → Addr for RtcSignal routing) |
| `RtcSignal` | If `to` peer_id is known, deliver directly. Otherwise broadcast to all known peers. |

**Deduplication (two layers, matching Gun.js):**
1. **Message ID** (`#` field) — via `Dup` (TTL cache, 999 entries, 9s expiry)
2. **Response checksum** (`@` + `##` fields) — if same ack+hash seen, suppress duplicate response

**Anti-loop mechanism:**
- Each `Put` carries a `peer_hop_list` — a set of peer IDs that have already seen this message
- Peers in the hop list are skipped during relay
- The sender's address is always added to the hop list before relay

### 3. The Actor System

A minimal actor framework built on Tokio unbounded channels, inspired by [Alice Ryhl's "Actors with Tokio"](https://ryhl.io/blog/actors-with-tokio/).

**Core types:**
- `Actor` trait — `handle(msg, ctx)`, `pre_start(ctx)`, `stopping(ctx)`, `subscribe_to_everything()`
- `ActorContext` — per-actor context: `peer_id`, `router` address, `start_actor()`, `child_task()`, `stop()`
- `Addr` — clonable, hashable address (hashes by `id` field, not channel sender)

**Lifecycle:**
1. `pre_start` — called once before message processing begins
2. `handle` — called for each `Message` received
3. `stopping` — called once after the message loop exits (stop signal or channel closed)

**Address semantics:**
- `Addr::send(msg)` → `Ok(())` or `Err(())` (channel closed = actor stopped)
- `Addr::noop()` — discards all messages; used as placeholder before real address is set
- Two `Addr`s are equal iff they refer to the same actor (hash by `id`, not sender)

### 4. The Dup Table

`Dup` is Gun.js's message deduplication tracker. It prevents the same message from being processed or forwarded more than once across the P2P mesh.

**Configuration:**
- `max: 999` — maximum entries before forced eviction
- `age: 9 seconds` — TTL; entries older than this are expired

**Eviction strategies:**
- **Lazy** — on `check(id)`, if the entry is expired, it's removed and returns `false`
- **Periodic** — on `track(id)`, if `last_drop` was more than `age/2` ago, run `drop()` to clean all expired entries
- **Forced** — if entries exceed `max`, the oldest third (`max/3`) is evicted

### 5. SEA Cryptographic Layer

#### Key Format (Gun.js Compatible)
```
pub_key  = "x.y"     — P-256 ECDSA public key (base64-encoded coordinates, no padding)
priv_key = "base64"  — 32-byte ECDSA private scalar (base64, no padding)
epub_key = "x.y"     — P-256 ECDH public key (same format, for encryption)
epriv_key = "base64" — 32-byte ECDH private scalar
```

#### Cryptographic Primitives
| Operation | Algorithm | Implementation |
|-----------|-----------|----------------|
| Key generation | P-256 ECDSA + ECDH | `p256` crate with `OsRng` CSPRNG |
| Signing | ECDSA P-256 | `ring::signature::EcdsaKeyPair` |
| Verification | ECDSA P-256 | `ring::signature::UnparsedPublicKey` |
| Key exchange | ECDH P-256 | `p256::ecdh::diffie_hellman` |
| Encryption | AES-256-GCM | `aes_gcm::Aes256Gcm` |
| Key derivation | PBKDF2-SHA256, 100k iterations | `pbkdf2` + `sha2` crates |
| Content hashing | SHA-256 | `ring::digest` or `sha2` |

#### Session Storage
- **MemorySessionStorage** — `HashMap<alias, KeyPair>` in memory, lost on restart
- **EncryptedFileSessionStorage** — persists to `~/.config/beam/sessions/` (or platform equivalent), encrypted with `BEAM_SEA_SESSION_KEY` environment variable (AES-256-GCM)
- Both implement the `SessionStorage` trait (async `save`, `load`, `clear`)

#### User Identity Flow
```
User::create(alias, password, node)
  → generate key pair
  → PBKDF2 derive encryption key from password
  → encrypt epriv_key with derived key
  → store ~@alias → pub_key, ~{pub} → {epub, epriv_encrypted} in graph
  → authenticate session

User::auth(alias, password, node)
  → read ~@alias → pub_key from graph
  → read ~{pub} → encrypted epriv from graph
  → PBKDF2 derive key from password
  → decrypt epriv_key
  → authenticate session

User::leave()
  → zeroize all key material in memory
  → invalidate all clones (shared Arc<RwLock<SessionState>>)
```

### 6. Storage Adapters

#### MemoryStorage
- `HashMap<String, Children>` — node_id → child map
- Handles: `Put` (insert/update), `Get` (lookup), `BatchPut` (delegate each Put), `Flush` (no-op)
- Conflict resolution: `updated_at` timestamp comparison — newer wins

#### RedbStorage
- `redb::Database` — embedded key-value store
- Single table: `beam_nodes_v1` (`&str` → `&[u8]` via bincode serialization)
- `BatchPut` — single `read_write_transaction` for all puts (atomic)
- `Flush` — commits and acknowledges via `in_response_to` matching
- Schema warmed at startup to prevent `TableDoesNotExist` on first read

### 7. Network Adapters

#### WsServer
- Binds TCP on `config.port` (default 4944)
- Accepts WebSocket connections (plain or TLS via `tokio-native-tls`)
- Spawns a `WsConn` actor per connection
- Web UI on `port + 1` (default 4945): `/peer_id` endpoint, `/stats/*` static files
- `subscribe_to_everything() = true` — receives all messages for relay

#### OutgoingWebsocketManager
- Connects to one or more WebSocket URLs
- Each connection gets its own `WsConn` actor
- `subscribe_to_everything() = true` — relay peers receive all messages
- Used with `--peers ws://host:port/ws,wss://host2:port/ws`

#### WsConn
- Per-connection actor bridging wire format ↔ `Message` types
- On receive: parses JSON, calls `Message::try_from(json, addr, allow_public_space)`
- On send: serializes `Message::to_string()` and writes to WebSocket
- Signature verification happens in `Message::try_from` for user-space puts (`~{pub}` prefix)

#### Multicast
- UDP multicast group: `224.0.0.123:6969`
- Broadcasts all messages to the local network
- `subscribe_to_everything() = true`
- For LAN discovery only — not suitable for public internet

#### WebRtcPeer (feature-gated: `webrtc`)
- Uses `str0m` for ICE/DTLS/SCTP stack
- `WebRtcRole::Offerer` — initiates connection
- `WebRtcRole::Answerer` — waits for offer
- Signaling flows over the WebSocket mesh via `Message::RtcSignal`
- STUN discovery via `stun::webrtc_stun::stun_binding_request()`
- TURN relay via `stun::webrtc_stun::turn_allocate_request()`
- Data channel carries the same JSON wire protocol as WebSocket

### 8. Message Verification

Inbound `Put` messages undergo signature verification in `Message::try_from()`:

1. **Public space** (`allow_public_space=true`): accepted without verification
2. **Public space** (`allow_public_space=false`): rejected — only signed data accepted
3. **User space** (`~{pub_key}/...`): `verify_sig()` extracts pub key from node_id prefix, verifies ECDSA signature on each child value. Values are JSON `{"m": message, "s": signature}` format.
4. **Content-addressed** (`#` namespace): key is the SHA-256 hash of the value. If hash matches key, data is authentic — no signature needed.
5. **Alias registry** (`~@alias`): unsigned by design — public lookup data mapping aliases to public keys.

---

## Where to Look

| You want to... | Start at |
|----------------|----------|
| Understand the data model | `types.rs` (Value, NodeData) → `node.rs` (get, put, map) |
| Understand wire protocol | `message.rs` (Get, Put, BatchPut, serialization) |
| Understand message routing | `router.rs` (handle_put, handle_get, relay, dedup) |
| Understand the actor system | `actor.rs` (Actor trait, ActorContext, Addr) |
| Understand deduplication | `dup.rs` (Dup, check, track, drop) |
| Add a new storage backend | `adapters/memory_storage.rs` (simplest example) → implement `Actor` trait handling `Get`/`Put`/`BatchPut`/`Flush` |
| Add a new transport | `adapters/ws_server.rs` (server pattern) or `adapters/ws_client.rs` (client pattern) → implement `Actor` with `subscribe_to_everything()` |
| Work with crypto | `sea/pair.rs` (key gen) → `sea/sign.rs` / `sea/verify.rs` → `sea/encrypt.rs` / `sea/decrypt.rs` |
| Work with user identity | `sea/user.rs` (create, auth, trust, grant, secret) |
| Work with session persistence | `sea/session/memory.rs` (ephemeral) → `sea/session/file.rs` (encrypted disk) |
| Understand WebRTC | `adapters/webrtc.rs` (WebRtcPeer) → `stun.rs` (ICE discovery) |
| Understand the CLI | `main.rs` (clap args, adapter wiring) |
| Run tests | `tests/integration.rs` (non-webrtc), `tests/webrtc_datachannel.rs` + `tests/webrtc_node_sync.rs` (feature-gated) |
| Run benchmarks | `benches/my_benchmark.rs` (criterion: memory storage, websocket, JSON parse+verify) |
| Inspect redb storage | `examples/dump_redb.rs` (standalone CLI tool) |

---

## Common Patterns

### Writing a Storage Adapter

Implement the `Actor` trait and handle these messages:

```rust
#[async_trait]
impl Actor for MyStorage {
    async fn handle(&mut self, msg: Message, ctx: &ActorContext) {
        match msg {
            Message::Put(put) => { /* store updated_nodes, conflict-resolve by updated_at */ }
            Message::Get(get) => { /* lookup node_id in storage, send Put response back to get.from */ }
            Message::BatchPut(batch) => { /* atomic transaction for all puts */ }
            Message::Flush(flush) => { /* fsync / durable persist, ack via Put with in_response_to */ }
            _ => {}
        }
    }
}
```

See `adapters/memory_storage.rs` for the simplest example, `adapters/redb_storage.rs` for persistent storage with transactions.

### Adding a New Message Type

1. Add variant to `Message` enum in `message.rs`
2. Add serialization in `Message::to_string()` and deserialization in `Message::from_json_obj()`
3. Handle in `Router::handle()` in `router.rs`
4. Handle in relevant adapters' `handle()` methods

### Writing Tests

- **Unit tests**: `#[cfg(test)] mod tests` at the bottom of each source file
- **Integration tests**: `tests/integration.rs` — uses `wait_for_port()` for deterministic WS readiness, 30s timeouts on recv
- **WebRTC tests**: `tests/webrtc_*.rs` — feature-gated with `#![cfg(feature = "webrtc")]`
- **Doctests**: in `///` comments — run with `cargo test --doc`

---

## Gotchas

1. **`uid = ""` is the ROOT node** — not an error, not empty, it's the root. `add_parent_nodes` reaches root and stops.
2. **`map()` replays ALL existing children** — use `once()` for single reads. The `__beam_replay_complete__` marker signals end of replay.
3. **`stun.rs` is feature-gated** — `mod stun;` only exists with `#[cfg(feature = "webrtc")]`. The module has stub implementations for non-webrtc builds.
4. **`Addr` hashes by `id` field** — not the channel sender. Two `Addr`s are equal iff they refer to the same actor. This is why `HashSet<Addr>` works correctly for peer tracking.
5. **Signature verification happens on receive** — in `Message::try_from()`, not in the router. The `allow_public_space` flag is passed through to control whether unsigned puts are accepted.
6. **`~@alias` is unsigned by design** — the alias registry maps human-readable names to public keys. It's public lookup data, not authenticated.
7. **`websocket_sync_over_2_relay_peers`** — may time out on slow CI. Uses 30s per-subscription timeouts. If it fails, re-run before investigating.
8. **`derive_aes_key_sync` is shared** — lives in `encrypt.rs` as `pub(crate)`, imported by `decrypt.rs` and `user.rs`. DRY refactor from the production review.
9. **`BoundedHashMap` eviction is FIFO** — not LRU. The oldest *inserted* entry is evicted, not the oldest *accessed*.
10. **`extern crate clap;` in main.rs** — unnecessary in Rust 2024 edition but harmless. Kept for compatibility with the original code.
11. **`redb` schema must be warmed** — `RedbStorage::pre_start` opens the table to prevent `TableDoesNotExist` errors on first read. This was a bug fix (commit `979139b`).
12. **Stats reporting is a placeholder** — `Router::update_stats()` is a no-op. The `msg_counter` atomic tracks total messages but doesn't expose them yet.

## Wire Compatibility Test Suite

### Architecture

Three-layer testing strategy to prove BEAM's wire protocol compatibility with Gun.js:

1. **Layer 1: Golden JSON Fixtures** (`tests/wire/`)
   - 36 fixture files across 7 categories: handshake, put, get, dam, batch, edge
   - Each fixture is a JSON file containing a raw Gun.js wire message and expected BEAM parser behaviour
   - Single `#[test]` function (`tests/wire_tests.rs`) discovers and asserts all fixtures
   - No external test framework — `serde_json` + `std::fs` only (suckless)
   - Fixtures double as human-readable protocol specification

2. **Layer 2: Node.js Mirror** (planned — `tests/wire-mirror/`)
   - Same JSON fixtures run against real Gun.js via `node:test`
   - If both BEAM and Gun.js pass the same fixtures, wire compat is proven by construction

3. **Layer 3: Live Integration** (planned — `tests/wire-live/`)
   - Real bidirectional WebSocket sync between BEAM and Gun.js relay
   - `#[ignore]` gated, separate CI job

### Adding Wire Fixtures

Drop a `.json` file into the appropriate category subdirectory under `tests/wire/fixtures/`. No code changes needed — the harness discovers fixtures by walking the directory tree at test time.

### Fixture Schema

```json
{
  "name": "unique_identifier",
  "description": "what this fixture tests",
  "category": "put|get|handshake|dam|batch|edge",
  "input": "<raw Gun.js wire message as JSON string>",
  "allow_public_space": false,
  "expected": {
    "parses": true,
    "kind": "Put|Get|Hi|RtcSignal",
    "souls": ["soul1"],
    "fields": { "soul1": ["key1"] },
    "values": { "soul1": { "key1": "value" } },
    "timestamps": { "soul1": { "key1": 12345.0 } },
    "error": null
  }
}
```

- `parses: false` → expect `Err` (check `error` substring)
- `parses: true` + `kind` → check `Message` variant
- `souls`, `fields`, `values`, `timestamps` → checked when present (Put messages)
- `allow_public_space: true` → pass `allow_public_space=true` to parser (unsigned souls)
- **Note**: BEAM's `Value::Number(f64)` always produces floats — use `30.0` not `30` in expected values

### Bug Found by Fixtures

The wire fixture suite found a real bug in `Message::try_from`: the parser used `obj["#"]` (BTreeMap indexing) to access the message ID, which panics on missing key instead of returning an error. Fixed to use `obj.get("#").and_then(|v| v.as_str())` — now returns `Err("msg id not a string")` gracefully.
