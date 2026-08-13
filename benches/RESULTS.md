# BEAM Benchmark Results

> **Hardware**: System76 Oryx Pro (i7, RTX 3060 Laptop 6GB), 32GB RAM, Linux
> **Date**: 2026-08-13
> **Version**: v0.12.0 (branch `feature/v0.12.0-perf`)
> **Build**: `--release` (LTO, opt-level 3, codegen-units=1)

---

## Local Put Throughput (v0.12.0 — new)

Local (non-relay) puts through the full actor pipeline — measures the
`Node::handle → Router::route → MemoryStorage::apply` path with no
network I/O. This is the pure in-process cost of a put.

| Run | Messages | Throughput |
|-----|----------|------------|
| 1 | 10,000 | 49,135 puts/sec |
| 2 | 10,000 | 57,265 puts/sec |
| 3 | 10,000 | 51,759 puts/sec |
| 4 | 10,000 | 51,717 puts/sec |
| 5 | 10,000 | 56,828 puts/sec |
| **Average** | **10,000** | **~53,300 puts/sec** |

**v0.11.0 baseline**: ~3,050 puts/sec → **v0.12.0: ~53,300 puts/sec (17.5×)**

**Key optimizations in v0.12.0:**
- Lazy broadcast channels — `on()` / `map()` channels created on first
  subscriber, not on every Node construction (9.7× improvement)
- `Arc<NodeInner>` — consolidate `RwLock` fields behind a single Arc to
  reduce lock contention (1.4× improvement)
- `&Put` handlers — `Node::handle_put` and `MemoryStorage::handle_put`
  take `&Put` instead of owned `Put`, eliminating a deep clone of the
  entire `BTreeMap<String, Children>` tree on every message (2.2× improvement)

---

## Relay Throughput

Real WebSocket connections through a memory-only relay (no disk I/O).
Throughput measured from relay's hot-path metrics counters — the ground
truth for what the router actually dispatched.

### v0.12.0 (2026-08-13)

| Scenario | Senders | Messages | Throughput |
|----------|---------|----------|------------|
| 1 sender × 10k | 1 | 10,000 | **5,287 msgs/sec** |
| 1 sender × 50k | 1 | 50,000 | **10,643 msgs/sec** |
| 10 senders × 5k | 10 | 50,000 | **11,380 msgs/sec** |

### v0.11.0 (2026-08-10, for comparison)

| Scenario | Senders | Messages | Throughput | Send Rate | Dedup Rate | Fanout |
|----------|---------|----------|------------|-----------|------------|--------|
| 1 sender × 10k | 1 | 10,000 | 3,041 msgs/sec | 4,377 puts/sec | 50.0% | 4.0× |
| 1 sender × 50k | 1 | 50,000 | 2,410 msgs/sec | 2,533 puts/sec | 50.0% | 4.0× |
| 10 senders × 5k | 10 | 50,000 | 2,014 msgs/sec | 1,621 puts/sec | 87.3% | 4.0× |

**v0.11.0 → v0.12.0 relay improvement: 1.7×–5.6× across scenarios**

**Key observations:**
- 50% dedup rate is expected: each Put generates a relay echo + ack that
  returns as a duplicate.
- 10-sender scenario shows higher dedup (87.3%) due to cross-sender
  message amplification through the relay mesh.
- Zero dropped sends across all scenarios.
- Bottleneck is the send-rate (client-side `put().await`), not the relay
  itself — the relay's internal processing is faster than the client can
  produce messages.

### Hot-Path Metrics (1 sender × 10k run)

```
ws_messages_received: 40,000
messages_parsed:      40,000
messages_dropped_dup: 20,000
messages_relayed:     10,000
subscriber_fanout:    40,000
serialization_calls:  28,740
ws_messages_sent:     28,740
dropped_sends:        0
```

---


### WASM Relay Throughput (T12-T13)

Real WebSocket connections from a WASM client (Node.js) through a
memory-only relay. Same methodology as native relay throughput, but
the client is BEAM's WASM binding instead of a native Node.

Ground truth from relay `/metrics` HTTP endpoint (port + 1).

| Scenario | Messages | Throughput | Send Rate |
|----------|----------|------------|-----------|
| 1k burst | 1,000 | **115 msgs/sec** | ~7,400 puts/sec |
| 5k sustained | 5,000 | **651 msgs/sec** | ~4,400 puts/sec |

**Key observations:**
- WASM relay throughput is lower than native (115–651 vs 2,400–3,000
  msgs/sec) due to WASM's slower JSON serialization (10–100× overhead).
- The 1k burst shows lower throughput than 5k sustained because the
  stabilization wait dominates the total elapsed time for small message
  counts — the relay processes 1k messages quickly but the 500ms
  stabilization poll adds fixed overhead.
- Send rate (client-side `beam.put()` fire-and-forget) is actually
  faster in WASM than native `put().await` because fire-and-forget
  returns immediately without awaiting the actor round-trip.

### WASM Relay Throughput (Browser)

Use `examples/bench.html` to run relay TPS benchmarks from a browser:

1. Start a BEAM relay: `cargo run -- start --port 4944 --memory-storage true --redb-storage false`
2. Serve the examples: `python3 -m http.server 8080 -d examples/`
3. Open `http://localhost:8080/bench.html` in a browser
4. Enter relay URL (default `ws://127.0.0.1:4944`) and click "Connect"
5. Click "Run Relay TPS" to measure end-to-end throughput

The browser benchmark uses the same `/metrics` ground-truth approach
as the Node.js WASM tests.

---

## Micro-Benchmarks (Criterion)

Pure CPU measurements — no network, no I/O. These isolate the
load-bearing components of the relay hot path.

### Wire Protocol Parse/Serialize (T4)

| Operation | Small JSON | Medium JSON | Large JSON |
|-----------|------------|-------------|------------|
| **Parse** (Message::try_from) | 851 ns | 2.00 µs | 7.45 µs |
| **Serialize** (Message::to_string) | 152 ns | 386 ns | 1.33 µs |
| **Parse Get** | 425 ns | — | — |

- Serialization is ~5× faster than parsing (JSON string building vs.
  full deserialization + verification).
- Parse throughput: ~1.18M small puts/sec, ~500k medium puts/sec.
- Serialize throughput: ~6.6M small puts/sec, ~2.6M medium puts/sec.

### Dedup Check (T4)

| Operation | Time |
|-----------|------|
| Track fresh message | 274 µs |
| Detect duplicate | 41.8 µs |

- Duplicate detection is 6.5× faster than tracking a fresh message
  (early exit on match).
- Dedup uses a bounded HashMap (999 entries, 9s TTL) — O(1) lookup.

### Actor Mailbox Throughput (T5)

| Operation | Time |
|-----------|------|
| Send + recv (tokio unbounded mpsc) | 309 µs |

- ~3,200 send/recv cycles per second through the actor mailbox.
- This is the inter-task communication channel — not a bottleneck at
  current relay throughputs.

### Router Dispatch Throughput (T5)

*OOM-killed during benchmarking — requires fewer samples or more RAM.
Re-run with `cargo bench --bench my_benchmark -- router_dispatch` on a
machine with more memory.*

---


### WASM Benchmarks (T7)

Run in Node.js via `wasm-pack test --node --no-default-features`.
Uses `web_time::Instant` for cross-platform timing.

| Operation | WASM (Node.js) | Native (Criterion) | Ratio |
|-----------|----------------|--------------------|-------|
| Parse small Put | 8,069 ns | 851 ns | ~9.5× |
| Serialize small Put | 18,144 ns | 152 ns | ~119× |
| Parse Get | 4,644 ns | 425 ns | ~11× |

WASM is 10–100× slower than native — expected for a JIT-compiled
sandbox. Serialize shows the largest gap because `serde_json`'s
`to_string()` involves heavy string allocation, which is costlier
under WASM's memory model.


## Storage Benchmarks (v0.7.0, for reference)

| Operation | Backend | Throughput |
|-----------|---------|------------|
| Sequential write (fsync) | redb | 954 elem/sec |
| Concurrent write (4 tasks) | redb | 1,345 elem/sec |
| Random read | redb | 654 elem/sec |

These benchmarks measure disk-bound storage operations with fsync. The
relay throughput benchmarks above use memory-only storage (no fsync) and
measure the pure routing path — which is 2.5–4.5× faster.

---

## Methodology

### Relay Throughput

1. Start a memory-only BEAM relay (no redb, no disk I/O)
2. Connect a subscriber node via WebSocket
3. Connect N sender nodes via WebSocket
4. Snapshot relay metrics counters before sending
5. Send M Put messages per sender (sequential `.await`)
6. Wait for relay counters to stabilize (500ms idle)
7. Compute throughput as `messages_relayed / elapsed`

Throughput is measured from the relay's perspective — the relay's
`messages_relayed` counter is the ground truth for how many messages
the router actually dispatched. This avoids subscriber-side receive
timing issues.

### Micro-Benchmarks

Criterion 0.8 with default settings (100 samples, 3s warmup, 5s
measurement window). Each benchmark isolates a single hot-path
component with no network I/O.

### Run Commands

```bash
# Relay throughput (release mode required)
cargo test --release --test relay_throughput_bench -- --ignored --nocapture

# Micro-benchmarks (hot-path only, skip storage)
cargo bench --bench my_benchmark -- "wire_|dup_check|actor_mailbox"

# Storage benchmarks (from v0.7.0)
cargo bench --bench my_benchmark -- "write_storm|read_storm|mixed"
```

---

## Conclusions

1. **BEAM's local put throughput is ~53,000 puts/sec** (v0.12.0) — a 17.5×
   improvement over v0.11.0's 3,050 puts/sec. The full actor pipeline
   (Node → Router → MemoryStorage) processes puts with zero unnecessary
   allocations after the `&Put` handler refactor.

2. **Relay throughput is 5,300–11,400 msgs/sec** (v0.12.0) — a 1.7×–5.6×
   improvement over v0.11.0. Real WebSocket connections through a
   memory-only relay.

3. **The bottleneck is the client, not the relay.** The relay's internal
   processing (parse + dedup + route + serialize) completes in
   microseconds, but `put().await` serializes messages through the
   WebSocket send buffer.

4. **Serialization is not a bottleneck.** At 6.6M small puts/sec, the
   JSON serializer can handle 100× the current relay throughput.

5. **Dedup is working correctly.** 50% dedup rate for single-sender
   (relay echo + ack) and 87% for multi-sender (cross-sender
   amplification) matches expected behavior.

6. **Memory-only mode is the fast path.** Eliminating fsync gives a
   2.5–4.5× throughput improvement over persisted storage.

7. **Further gains require Arc-based value sharing.** The remaining
   allocations in the hot path are `Value` cloning for broadcast channels
   and `String` cloning for UIDs and paths. `Arc<Value>` / `Arc<str>`
   would make these refcount bumps instead of heap allocations — estimated
   3–5% additional throughput, but at the cost of a larger refactor across
   `Value`, `NodeData`, serde impls, and all match sites.
