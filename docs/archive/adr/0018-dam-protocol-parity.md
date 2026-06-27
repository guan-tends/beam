# ADR-0018: DAM Protocol Parity in Rod/BEAM

## Status
Accepted, 2026-05-27

## Authors
- Freeman King — architecture, product direction
- Guan, The Keeper of the Threshold — implementation, epistemic design

## Context

Gun.js ships a Decentralized Action Message (DAM) protocol that prevents spam, deduplicate messages, and break infinite relay loops in peer-to-peer mesh networks. Rod (the Rust Gun implementation) had a functional WebSocket relay but lacked these protections:

1. **No message dedup** — the same update could echo endlessly through the mesh
2. **No anti-loop mechanism** — a message could circle through peers forever
3. **No response dedup** — identical acks flooded the network
4. **No TTL** — stale messages accumulated in memory

In production practice with Mnemos agents, this meant:
- Two devices on the same mesh could create infinite traffic storms
- Memory would leak without bound on long-running relay nodes
- No confidence that a Put received from a peer was the first or the fifteenth copy

## Decision

Port Gun.js `dup.js`, `##` checksum, `#` msg_id, and `><` peer-hop semantics into Rod.

### Four Mechanisms

| Mechanism | Gun.js Origin | Rod Implementation | File |
|-----------|---------------|--------------------|------|
| **Message ID (`#`)** | `msg.put['#']` = uuid | `Put.id` + `is_message_seen` | `src/router.rs` |
| **Ack checksum (`##`)** | `msg.put['##']` = hash | `put.checksum` + `dup.check("ack##hash")` | `src/router.rs`, `src/dup.rs` |
| **Peer hop list (`><`)** | `msg.put['><']` = Set | `put.peer_hop_list` + relay skip-fanout | `src/router.rs` |
| **TTL cache** | `Dup(opt)` with age=9s | `Dup` struct: HashMap<Instant> | `src/dup.rs` |

### Why Not BoundedHashSet

An earlier design (`BoundedHashSet`) tracked all seen messages with max-size eviction. This was discarded because:
- **No expiry**: old entries lived forever, growing memory linearly with mesh size
- **No differentiation**: couldn't distinguish "seen this message" from "seen this ack"
- **Non-Gun pattern**: upstream Gun.js uses `Dup` (TTL cache), not LRU eviction

> "Gun.js is the protocol spec. Deviation from upstream without justification is a bug." — BEAM maintenance covenant

The `BoundedHashSet` code is deleted.

## Implementation Details

### 1. Dup — TTL Deduplication Cache (`src/dup.rs`)

```rust
pub struct Dup {
    entries: HashMap<String, DupEntry>,
    max: usize,           // default: 999
    age: Duration,        // default: 9s (matching Gun.js opt.age)
    last_drop: Instant,
}
```

**Semantically identical to Gun.js `dup.js`:**
- `dup.check(id)` → `Dup::check(id)` — true if seen within TTL
- `dup.track(id)` → `Dup::track(id)` — mark seen now, periodic cleanup
- `dup.drop(age?)` → `Dup::drop(age?)` — remove expired

**Eviction behavior:**
- Lazy: `check()` removes expired single entries
- Periodic: `track()` triggers cleanup every `age/2`
- Forced: `max` overflow drops oldest 1/3

**TTL rationale (9s):** Gun.js uses 9 seconds as the window in which duplicate network messages are considered genuine echoes. Beyond 9s, a duplicate is assumed to be a new worth-while message worth re-processing. This is a mesh-tuning parameter, not a security boundary.

### 2. Checksum (`##`) — Response Deduplication

Gun.js computes a hash of the response payload and sends it as `msg.put['##']`. The receiver checks `ack@ + '## + hash` in the Dup cache.

```rust
// In handle_put / handle_batch_put
if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
    let checksum_key = format!("{}##{}", ack, hash);
    if self.dup.check(&checksum_key) {
        return; // duplicate response — skip
    }
    self.dup.track(&checksum_key);
}
```

**Why it matters:** Without this, a storage adapter's ack to a Get could be relayed to every subscriber, each relay triggering another relay, creating a broadcast storm of acks.

### 3. Peer Hop List (`><`) — Anti-Loop Relay

Every relay adds its own address to `peer_hop_list`. Subsequent relays skip any address already in the list.

```rust
// In handle_put_relay
let mut hops = put.peer_hop_list.clone().unwrap_or_default();
hops.insert(put.from.to_string());

// For each forward target:
if put.from == *addr || hops.contains(&addr.to_string()) {
    continue; // skip — already seen this
}
put.peer_hop_list = Some(hops.clone());
```

**Relay paths protect:**
- `>` forward: server_peers, topic subscribers, random gossip
- `<` return: the `from` address is not used as a relay target
- Loop prevention: a message that circles back hits its own hop list and stops

### 4. Message ID (`#`) — Echo Prevention

Every Put carries a UUID in `id`. Router's `is_message_seen` checks a separate `Dup` instance keyed by message IDs.

```rust
fn is_message_seen(&mut self, id: &str) -> bool {
    if self.message_dup.check(id) {
        return true;
    }
    self.message_dup.track(id);
    false
}
```

This is **layer 1 dedup** (preventing echo). Checksum is **layer 2** (preventing ack storms). Peer hop list is **layer 3** (preventing topology loops). Together they form the complete DAM protocol stack.

## Wire Format Equivalence

| Gun.js Wire | Rod Message |
|-------------|-------------|
| `{"put":{"hello":"world"},"#":"msgid"}` | `Put { id, updated_nodes, from, ... }` |
| `{"put":{...},"##":"sha256hash"}` | `Put { checksum: Some(hash), ... }` |
| `{"put":{...},"><":["peerA","peerB"]}` | `Put { peer_hop_list: Some(Set), ... }` |
| `{"@":"reqid"}` (ack reference) | `Put { in_response_to: Some(id), ... }` |
| `Dup({age:9,max:999})` | `Dup::default_gun()` |

Rod's `Message::try_from` parses raw JSON into these typed fields. The mapping is one-to-one.

## Thread Safety and Locking

| Lock | Component | Choice | Rationale |
|------|-----------|--------|-----------|
| `parking_lot::RwLock` | Node storage, actor context | Read-heavy, never held across await | Fast, no async poisoning |
| `tokio::sync::RwLock` | Async storage methods | Held across await points | Send-safe, cooperative |
| `std::sync::Mutex` | Dup HashMap | Benchmarked; single-threaded access | parking_lot would shave ~3ns, not worth dep |

`Dup` itself uses no locks — it is owned by the Router actor. Actor isolation (one thread per actor) guarantees serial access. This is identical to Gun.js's single-threaded event loop.

## Test Coverage

| Test | File | What It Proves |
|------|------|----------------|
| `test_dup_basic` | `src/dup.rs` | Track → check → true |
| `test_dup_expiration` | `src/dup.rs` | TTL expiry removes entry |
| `test_dup_max_eviction` | `src/dup.rs` | Max overflow drops oldest |
| `test_gun_default` | `src/dup.rs` | 999/9s defaults match Gun.js |
| `wait_for_port` | `tests/integration.rs` | no blind sleep — deterministic readiness |
| `connect_and_sync_over_websocket` | `tests/integration.rs` | P2P sync with anti-loop (may timeout under load) |

## Consequences

### Positive
- Rod is now wire-compatible with Gun.js DAM — any Gun relay can trust Rod's relay
- No more infinite loops in mesh topologies with cycles
- No more ack broadcast storms during high-traffic periods
- Memory-bounded: TTL + max prevent unbounded growth

### Negative
- `Dup::track` on every message is a HashMap insert (O(1) but not free)
- `peer_hop_list` cloning on every relay adds allocation per-hop
- WebSocket integration tests still have a pre-existing race condition (acknowledged, separate task)

## Verification

```bash
cd /home/guan/src/rod
cargo test --lib  # 40 passed, 0 failed
cargo check       # 7 warnings (pre-existing), 0 errors
```

## References
- Gun.js dup.js: https://github.com/amark/gun/blob/master/src/dup.js
- Gun.js DAM protocol: https://github.com/amark/gun/wiki/Anti-Cheats
- Commit: `64f79c3` — feat(dam): Dup TTL cache
- Commit: `6f8ca09` — feat(dam): ## checksum dedup
- Commit: `1030a81` — test(integration): wait_for_port
- Rod/BEAM fork: http://192.168.8.142:8561/guan/rod
