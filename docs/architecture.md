# BEAM Architecture

This document provides the deep architectural view of BEAM — a maintained fork of [rod](https://github.com/mmalmi/rod), a from-scratch Rust port of [Gun.js](https://github.com/amark/gun). For quick-start usage, see `README.md`.

## High-Level Actor Model

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
                    │  (opt-in)   │ │             │ │            │
                    └─────────────┘ └─────────────┘ └────────────┘
```

## Storage Backend Selection

The storage adapter slot is filled at startup based on CLI flags. Both backends implement the same `Storage` trait, so the rest of the codebase is unaware of which is active.

```
                            ┌──────────────────────┐
                            │  beam CLI startup     │
                            │  (src/main.rs)       │
                            └──────────┬───────────┘
                                       │
                            parse --memory-storage
                                  --redb-storage
                                  --persy-storage
                                       │
                                       ▼
                            ┌──────────────────────┐
                            │  Mutually exclusive  │
                            │  flag selection      │
                            └──────────┬───────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            │                          │                          │
            ▼                          ▼                          ▼
   ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
   │ MemoryStorage   │       │  RedbStorage    │       │  PersyStorage   │
   │  (ephemeral)    │       │   (default)     │       │   (opt-in)      │
   │                 │       │                 │       │                 │
   │ HashMap<id,     │       │ TableDefinition │       │ Segment-based   │
   │ Children>       │       │ <&str, &[u8]>   │       │ with bg_ops     │
   │                 │       │                 │       │                 │
   │ No fsync        │       │ Fsync on commit │       │ Optional fsync  │
   │ No persistence  │       │ ACID            │       │ isolation       │
   └─────────────────┘       └─────────────────┘       └─────────────────┘
            │                          │                          │
            └──────────────────────────┼──────────────────────────┘
                                       │
                                       ▼
                            ┌──────────────────────┐
                            │  implements Actor    │
                            │  trait, handling:    │
                            │  - Put (store)       │
                            │  - Get (lookup)      │
                            │  - BatchPut (atomic) │
                            │  - Flush (fsync+ack) │
                            └──────────────────────┘
```

### Build-Time Gating

Persy is feature-gated. Building without `--features persy` compiles the Persy adapter out entirely:

```toml
# Cargo.toml
[features]
persy = ["dep:persy"]
default = []  # redb is always available
```

At runtime, if `--persy-storage true` is set but the binary was not built with `--features persy`, startup fails with a clear error.

### Data Format Translation

redb stores `Children` directly as `TableDefinition<&str, &[u8]>`. Persy wraps it as `NodeRecord { node_id: String, children: Children }` because Persy's segment-based storage requires the key to be embedded in the record.

The `beam migrate` subcommand handles the translation:

```
   ┌──────────────────┐                    ┌──────────────────┐
   │  redb source     │   beam migrate --   │  Persy target    │
   │  ─────────       │   from redb        │  ─────────       │
   │  "key1" → Bytes  │   --to persy       │  id=key1 → Bytes │
   │  "key2" → Bytes  │                    │  id=key2 → Bytes │
   │  ...             │   ────────────►    │  ...             │
   └──────────────────┘                    └──────────────────┘
```

The `Children` inner format is identical between backends. Only the wrapper changes. See `docs/migrations/migration-guide.md` for the full procedure.

## Wire Protocol (Backend-Agnostic)

The Put/Get message format is opaque to storage. A redb node, a Persy node, and an in-memory node form a valid mesh:

```
   ┌──────────┐     Wire: { @: "id",     ┌──────────┐
   │ redb     │     updated_nodes: {...} │ Persy    │
   │ node     │ ◄──────────────────────► │ node     │
   └──────────┘                          └──────────┘
        │                                     │
   Put in_response_to                       │
   sends _ack/_err                         │
   sentinel via Put reply                  ◄──┘
```

The always-reply invariant (commit `b6a3d7b`) is honored by all adapters: when `Put.in_response_to` is set, the receiving adapter MUST send an ack reply, regardless of backend.

## Module Map

| Module | Responsibility |
|--------|----------------|
| `types.rs` | Core data types: `Value` (Null/Bit/Number/Text/Link), `NodeData` (value + timestamp), `Children` (BTreeMap), JSON conversion |
| `utils.rs` | `random_string()` (OS CSPRNG), `BoundedHashMap` (FIFO eviction for dedup tracking) |
| `dup.rs` | `Dup` — Gun.js DAM-style message deduplication (TTL + bounded capacity, 999 entries / 9s default) |
| `message.rs` | Wire protocol: `Get`, `Put`, `BatchPut`, `Flush`, `RtcSignal`, `Hi` — JSON serialization/deserialization, signature verification on inbound puts |
| `actor.rs` | Actor framework: `Actor` trait, `ActorContext`, `Addr` (clonable, hashable address) — built on Tokio unbounded channels |
| `ack.rs` | Ack protocol: `AckPolicy` (any/quorum/all), `ReplicationStatus`, sentinel-driven async ack for put, batch_put, flush, map replay |
| `metrics.rs` | Observability: `Metrics` struct with atomic counters for puts, gets, peer connections, message routing. Shared via `Arc<Metrics>` between Node and Router |
| `node.rs` | Graph node API: `put()`, `get()`, `on()`, `once()`, `map()`, `batch_put()`, `connect_peer()`, `connect_webrtc_peer()`, `stop()` |
| `router.rs` | Central router: dedup, Get/Put routing, peer management, topic subscriptions, anti-loop relay, flush forwarding, RtcSignal delivery |
| `migration.rs` | `beam migrate` logic: format translation, single-tx-per-batch, dry-run, empty source handling |
| `sea/pair.rs` | Key pair generation: ECDSA P-256 (signing) + ECDH P-256 (encryption), Gun.js `x.y` base64 format |
| `sea/sign.rs` | P-256 ECDSA signature creation via `ring` |
| `sea/verify.rs` | Signature verification (sync + async variants) |
| `sea/work.rs` | Proof-of-work / content hashing (PBKDF2, SHA-256, base64) |
| `sea/secret.rs` | ECDH shared secret derivation between key pairs |
| `sea/session/` | Session persistence: `MemorySessionStorage` (ephemeral) and `EncryptedFileSessionStorage` (disk, AES-GCM) |
| `adapters/memory_storage.rs` | In-memory `HashMap<node_id, Children>` storage (ephemeral, default for `Node::new()`) |
| `adapters/redb_storage.rs` | Persistent storage via `redb` embedded database — `BatchPut` atomic transactions, flush ack |
| `adapters/persy_storage.rs` | Persistent storage via `Persy` (opt-in via `--features persy`) — per-tx isolation, optional `background_ops` fsync |
| `adapters/ws_server.rs` | WebSocket server: accepts inbound connections, spawns `WsConn` per connection, optional TLS, web UI on port+1 |
| `adapters/ws_client.rs` | `OutgoingWebsocketManager` — connects to remote WebSocket peers with retry |
| `adapters/ws_conn.rs` | Per-connection WebSocket actor: bridges wire format ↔ Message types |
| `adapters/multicast.rs` | UDP multicast peer discovery |
| `adapters/webrtc.rs` | WebRTC peer connections (str0m-based), NAT traversal |

## Architectural Decisions

Key design choices in the codebase:

- **Sentinel-driven async acks** — Put quorum, batch_put, Flush, and map replay all use sentinel markers flowing through the actor pipeline to track completion (see `ack.rs`)
- **Shared observability** — `Arc<Metrics>` shared between Node and Router for atomic counter collection (see `metrics.rs`)
- **Persy as opt-in storage backend** — feature-gated alongside the default redb backend

## Cross-References

- `README.md` — user-facing quick start and feature overview
- `docs/migrations/migration-guide.md` — `beam migrate` procedure
- `tests/cross_backend_mesh_e2e.rs` — 2 redb + 1 Persy mesh verification
- `tests/persy_e2e.rs` — single-node Persy CRUD tests
- `tests/migration_e2e.rs` — 6 migration path tests
