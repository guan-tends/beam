---
name: rod-gun-api
description: Comprehensive Rod (Rust Gun Protocol) API crash course. Maps Gun.js concepts to Rod/Rust idioms with source-verified facts from node.rs, types.rs, message.rs, and integration tests.
---

# Rod (Rust Gun Protocol) — API Crash Course

## What Rod Is

Rod is the Rust implementation of the Gun.js decentralized graph database protocol. It implements the same wire-format and conceptual model (UUID-addressed nodes, P2P sync, eventual consistency) but in Rust with an actor-model architecture, tokio async runtime, and hierarchical key-path traversal.

**Key architectural shift from JS:** Rod is a *key-path graph*, not a KV store. Every `.get()` call traverses a tree, building a `Node` whose `uid` is the joined path (`parent/child/grandchild`). Values live at leaves; intermediate nodes are structural. This is the same semantics as Gun.js, but explicit in the type system.

## Core Data Model

```
Node {                              // Graph node (structural + value)
    uid: Arc<RwLock<String>>,       // e.g. "", "greeting", "alpha/name"
    path: Vec<String>,              // path segments: ["alpha", "name"]
    children: BTreeMap<String, Node>, // child Nodes for traversal
    parent: Option<(String, Node)>, // back-reference for put() chain-walking
    on_sender: broadcast::Sender<Value>,     // live value subscription
    map_sender: broadcast::Sender<(String, Value)>, // child enumeration subscription
    actor_context: ActorContext,    // tokio actor runtime
    router: Option<Addr>,           // message router to storage/network adapters
    addr: Option<Addr>,             // own actor address
}
```

A Node is **both** a position in the graph (structural) and a value emitter (when at a leaf). `Node::new()` gives you a root node with `uid = ""`.

**Value enum** (from `types.rs`):
```rust
pub enum Value {
    Null,
    Bit(bool),
    Number(f64),
    Text(String),
    Link(String),      // cross-graph reference (Gun soul reference)
}
```

Auto-conversions exist: `&str`, `String`, `usize`, `u64`, `f32` all `Into<Value>`.

### How Storage Sees It (message format)

The wire protocol and storage layer see nodes as `BTreeMap<String, Children>` where:
- Outer key = node_id (uid)
- Inner `Children` = `BTreeMap<String, NodeData>` (child_key -> value+timestamp)

Example JSON wire-format (from `message.rs`):
```json
{
  "put": {
    "alpha": {
      "_": {"#": "alpha", ">": {"name": 1653465227430}},
      "name": "Amandil"
    }
  },
  "#": "msgid"
}
```

This is Gun.js wire-format — Rod is wire-compatible.

## Constructor API

### `Node::new()` — Minimal in-memory graph
```rust
let mut db = Node::new();
```
- Creates root node with `MemoryStorage` automatically
- No network adapters
- Suitable for single-process, in-memory use

### `Node::new_with_config(config, storage_adapters, network_adapters)` — Full control
```rust
use rod::{Node, Config, Value};
use rod::adapters::{MemoryStorage, OutgoingWebsocketManager};

let config = Config::default();
let memory_storage = Box::new(MemoryStorage::new());
let ws_client = Box::new(OutgoingWebsocketManager::new(
    config.clone(),
    vec!["wss://relay.example.com/ws".to_string()],
));
let mut db = Node::new_with_config(
    config.clone(),
    vec![memory_storage],      // storage adapters
    vec![ws_client],           // network adapters
);
```

**Config fields** (`node.rs`):
```rust
pub struct Config {
    pub allow_public_space: bool,  // allow unsigned writes at root
    pub my_pub: Option<String>,    // ECDSA public key for authenticated space
    pub stats: bool,               // expose /stats endpoint (WsServer)
}
```

## Traversal: `.get(key: &str) -> Node`

```rust
let mut node = db.get("greeting");        // child of root
let mut nested = db.get("alpha").get("name"); // chain: uid = "alpha/name"
```

**Rules (from `node.rs` source):**
- `.get("")` returns `self.clone()` (identity)
- Every `.get()` call checks `self.children` BTreeMap. If existing → returns clone. If not → `new_child()` creates a new Node with `uid = path.join("/")`.
- Keys must be non-empty (assertion in `new_child`)
- `.get()` returns by value (`Node`), not reference. Node is cheap to clone (Arc/RwLock internals).

**Gun.js equivalent:** `gun.get('key')` — same semantics, but Rod explicitly builds the path tree.

## Write: `.put(value: Value)`

```rust
node.put("Hello World!".into());
node.put(42usize.into());
node.put(Value::Null);
```

**What happens internally (from `node.rs`):**
1. Broadcasts value to local `on_sender` subscribers
2. Walks parent chain via `add_parent_nodes()`, building `BTreeMap<String, Children>`
3. Sends `Message::Put(updated_nodes)` to router
4. Router fans out to storage adapters + network adapters
5. Storage saves: `store[node_id] = { child_key: NodeData{value, updated_at} }`

**Critical:** `.put()` only sets values on **leaf positions** (where you called `.put()`), but it walks UP the tree updating every parent node with a `Link` to the child. This maintains the hierarchical graph structure.

**Values NOT supported** (rejected at conversion layer, like Gun.js):
- Arrays (use `.map()` enumeration instead)
- `undefined`, `NaN`, `Infinity`

## Read: `.on() -> broadcast::Receiver<Value>`

Live subscription. Returns a new value every time this node receives an update (local `.put()` OR synced from peer).

```rust
let mut sub = db.get("greeting").on();
db.get("greeting").put("Hello!".into());
if let Value::Text(s) = sub.recv().await.unwrap() {
    println!("{}", s); // "Hello!"
}
```

**Rules:**
- Subscribe (`.on()`) THEN put, or you miss the first value.
- The receiver is a `tokio::sync::broadcast::Receiver<Value>` — `.recv().await` blocks until next value.
- `.on()` is idempotent: each call returns a NEW independent receiver.

**Gun.js equivalent:** `gun.on((data, key) => { ... })` — same live callback semantics.

## Read: `.once(wait: Option<Duration>) -> Option<Value>`

Single read with optional timeout. Uses `.on()` internally with `tokio::time::timeout`.

```rust
// Default wait: 66ms
let val = db.get("name").once(None).await;

// Custom wait
use std::time::Duration;
let val = db.get("name").once(Some(Duration::from_secs(2))).await;
```

**Rules:**
- Returns `Some(Value)` if received within timeout
- Returns `None` if not found / timeout
- Will receive a value that was `.put()` BEFORE `.once()` was called if storage has it (storage replays on Get)

**Gun.js equivalent:** `gun.once((data, key) => { ... })` — same one-shot read.

## List: `.map() -> broadcast::Receiver<(String, Value)>`

Subscribe to **all children** of the current node. Returns `(child_key, value)` pairs.

```rust
let mut sub = db.get("users").map();
db.get("users").get("alice").put("Alice Data".into());
if let (key, Value::Text(val)) = sub.recv().await.unwrap() {
    assert_eq!(key, "alice");
    assert_eq!(val, "Alice Data");
}
```

**CRITICAL: `.map()` REPLAYS EXISTING CHILDREN** (verified from `node.rs` `handle_put`):
When storage receives a `Get{node_id, key=None}`, it returns ALL children of that node. The router fans these out through `map_sender`. This means `.map()` immediately yields any children already stored BEFORE you subscribed.

**Gun.js equivalent:** `gun.map().once((data, key) => { ... })` — same child enumeration.

**NOT IMPLEMENTED:** There is no `.set(data)` in Rod. You add children by `.get(child).put(value)` and subscribe via `.map()`.

## The Two Data Patterns (Verified from Source)

Rod supports **two** usage patterns. Mixing them causes confusion.

### Pattern A: Hierarchical (Idiomatic Rod / Gun Graph)
```rust
// Build tree: root -> "mnemos" -> "palace" -> "wings" -> "abc123"
store.get("mnemos").get("palace").get("wings").get("abc123").put(value);

// List children of "wings"
let mut sub = store.get("mnemos").get("palace").get("wings").map();
// WORKS — replays child "abc123"
```

### Pattern B: Flat Keys (Index-style)
```rust
// Single flat key at root
store.get("mnemos/palace/wings/abc123").put(value);

// Try to list
store.get("mnemos").get("palace").get("wings").map();
// EMPTY — nothing under "wings" node; the data is at root under a long key
```

**Rule: Pick one pattern, stick to it.** Mnemos and your application code should use Pattern A (hierarchical).

## Value Types & Conversions

From `types.rs`:

| Source | Converts To | Example |
|--------|------------|---------|
| `&str` | `Value::Text` | `"hello".into()` |
| `String` | `Value::Text` | `s.into()` |
| `usize` | `Value::Number` | `42usize.into()` |
| `u64` | `Value::Number` | `99u64.into()` |
| `f32` | `Value::Number` | `3.14f32.into()` |
| `bool` | `Value::Bit` | `true.into()` |
| `()` | Not supported | Use `Value::Null` |

**JSON interop:** `Value` ↔ `serde_json::Value` via `TryFrom` / `From`. Links serialize as `json!({"#": soul_id})`.

## Config & Adapters

Storage adapters implement `Actor` trait and persist data:
- `MemoryStorage::new()` — in-memory, non-persistent
- `SledStorage::new_with_config(config, sled_config, Option<encryption>)` — on-disk via sled

Network adapters implement `Actor` and handle P2P sync:
- `WsServer::new(config)` — accept incoming websocket connections
- `WsServer::new_with_config(config, WsServerConfig{port, ..})` — custom port
- `OutgoingWebsocketManager::new(config, vec!["ws://host/ws"])` — connect to peers
- `Multicast::new(config)` — LAN multicast discovery (experimental)

**P2P Sync Pattern** (from integration tests):
```rust
// Relay topology
let mut relay = Node::new_with_config(config, vec![], vec![Box::new(WsServer::new(config))]);
let mut peer1 = Node::new_with_config(config, vec![], vec![Box::new(ws_client_to_relay)]);
let mut peer2 = Node::new_with_config(config, vec![], vec![Box::new(ws_client_to_relay)]);

// Any put on any peer propagates to all peers subscribed via .on()
```

## What Is NOT Implemented (Gun.js → Rod Gap)

| Gun.js API | Rod Status | Notes |
|-----------|-----------|-------|
| `gun.set(data)` | ❌ NOT IMPLEMENTED | Use `.get(key).put(value)` + `.map()` |
| `gun.not(cb)` | ❌ NOT IMPLEMENTED | Handle None from `.once()` manually |
| `gun.open(cb)` | ❌ NOT IMPLEMENTED | Use `.on()` with recursive get |
| `gun.load(cb)` | ❌ NOT IMPLEMENTED | Use `.once()` |
| `gun.then(cb)` | ❌ NOT NEEDED | Rust async/await replaces promises |
| `gun.bye()` | ❌ NOT IMPLEMENTED | No server-side disconnect hooks |
| `gun.later(cb, s)` | ❌ NOT IMPLEMENTED | Use tokio::time::sleep + .put |
| `gun.unset(node)` | ❌ NOT IMPLEMENTED | No first-class set/list removal |
| `gun.user.*` | ❌ NOT IMPLEMENTED | SEA crypto layer not in Rod |
| `gun.back(n)` | ❌ NOT NEEDED | Rust ownership makes back references awkward; clone parent explicitly |

## Critical Rust Idioms

### Node is Clone-Safe
```rust
let mut db = Node::new();
let node_a = db.get("alpha");     // owns a clone of child
let node_b = db.get("alpha");     // same child, another clone
```
Both `node_a` and `node_b` refer to the same underlying actor. `node_a.put()` will be visible to `node_b.on()`.

### Subscribe Before Send
```rust
let mut sub = db.get("key").on();  // subscribe FIRST
db.get("key").put("value".into()); // THEN put
```
Reversing this order causes `.recv().await` to hang forever on the first call.

### `.stop()` for Cleanup
```rust
db.stop(); // Shuts down ActorContext, closes all adapters
```
Always call `.stop()` on root Node before test/function exit. Otherwise tokio tasks leak.

### tokio Required
Rod is fully async. All examples need `#[tokio::main]` or `tokio::test`.

### Path Length Matters for `.on()`
From `node.rs`:
```rust
if self.path.len() > 1 {
    key = self.path.iter().nth(self.path.len() - 1).cloned();
} else {
    key = None;
}
```
When subscribing to a node with path length ≤ 1 (root or direct child of root), the Get message has `child_key = None`, meaning storage returns ALL children. For deeper paths, `child_key` is the last segment, so Get targets the specific value.

## Full Working Example

```rust
use rod::{Node, Config, Value};
use rod::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    // In-memory local-only node
    let mut db = Node::new();
    
    // Subscribe
    let mut sub = db.get("greeting").on();
    
    // Write
    db.get("greeting").put("Hello World!".into());
    
    // Read
    if let Value::Text(s) = sub.recv().await.unwrap() {
        println!("{}", s); // "Hello World!"
    }
    
    // Single read
    db.get("name").put("Rod".into());
    if let Some(Value::Text(name)) = db.get("name").once(None).await {
        println!("Name: {}", name); // "Rod"
    }
    
    // Child enumeration
    db.get("users").get("alice").put("Alice".into());
    db.get("users").get("bob").put("Bob".into());
    let mut sub = db.get("users").map();
    // Receives existing children: ("alice", Text("Alice")), ("bob", Text("Bob"))
    
    db.stop();
}
```

## Source-Verified Facts

This document is derived from reading the following source files directly:
- `src/node.rs` — Node struct, get/put/on/once/map/stop implementations
- `src/types.rs` — Value enum, Children, NodeData
- `src/message.rs` — Get/Put wire-format, JSON serialization
- `src/lib.rs` — public exports
- `src/adapters/mod.rs` — adapter module exports
- `tests/integration.rs` — 8+ integration tests covering full API surface
- `examples/hello.rs`, `flat_test.rs`, `minimal.rs` — usage patterns
- `gun.eco.wiki/API.md` — canonical Gun.js API reference (for comparison)

**Golden rule when using Rod:** Trust the source, not this document. If behavior diverges, `src/node.rs` wins.
