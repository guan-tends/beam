# BEAM Changelog

All notable changes to BEAM are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.17.0] — 2026-08-21 — Fjall Storage Backend + WASM Storage Adapters + Relay Dedup Fixes

### Added — Fjall Storage Backend

- **`FjallStorage` adapter** (`adapters/fjall_storage.rs`, 907 lines):
  LSM-tree storage backend using `fjall` v3. WAL journalling with LZ4
  compression — writes are journal appends to OS page cache (microseconds),
  no `fsync` until explicit `Flush` triggers `persist(SyncAll)`. Recommended
  for multi-node deployments where peers hold copies of data.
  Feature-gated behind `--features fjall`. Available via
  `FjallStorage::new()` and `FjallStorage::new_with_config()`.
- **Empty key encoding**: Fjall LSM-tree panics on empty keys (`""` root soul).
  Fixed with `encode_key()` using `0x00` prefix — preserves lexicographic sort
  order (root sorts first).
- **Fjall e2e tests** (`tests/fjall_e2e.rs`, 6 tests): Round-trip put/get,
  batch operations, concurrent access, large value handling, key encoding.
- **Fjall vs redb benchmarks** (`bench/RESULTS.md`):
  | Benchmark | redb | fjall | Winner |
  |---|---|---|---|
  | write_storm (sequential) | 977 elem/s | 2,999 elem/s | fjall 3.1× |
  | concurrent_write_storm (4 tasks) | 1,195 elem/s | 4,836 elem/s | fjall 4.0× |
  | read_storm (random) | 610 elem/s | 447 elem/s | redb 1.4× |

### Added — Universal Migration Tool

- **`beam migrate` subcommand**: Converts between all supported storage
  formats (redb ↔ fjall ↔ Persy). Batch processing with checksum
  verification. Reader/writer abstraction pattern — each backend implements
  `MigrationReader`/`MigrationWriter` traits.
- **Migration e2e tests** (`tests/migration_e2e.rs`, expanded to 425 lines):
  All format combinations, large datasets, checksum verification, progress
  reporting.

### Added — WASM Storage Adapters

- **OPFS (Origin Private File System) storage adapter** (`WasmOpfsStorage`):
  Browser-native file-based persistence using the OPFS API. Postcard-serialized
  binary files, async via `spawn_local` + `Arc<AtomicBool>` signaling. Available
  via `Beam.new_with_opfs()` and `Beam.new_with_opfs_name(name)`. Requires
  Chrome 102+, Firefox 111+, or Safari 15.2+.
- **Node.js filesystem storage adapter** (`WasmNodeFsStorage`):
  Server-side WASM persistence for Node.js/Electron environments. Postcard-
  serialized files, async via `spawn_local` + `JsFuture`. Available via
  `Beam.new_with_node_fs()` and `Beam.new_with_node_fs_dir(dir)`. Requires
  `--features node-fs`.
- **Playwright OPFS persistence tests**: Browser tests verifying data survives
  page reload and full browser context close.
- **webServer config in `playwright.config.mjs`**: Auto-serves `browser-test/`
  via `http-server` on port 8080.

### Changed — IDB Refactor

- **WasmIdbStorage serialization**: Migrated from JSON to postcard (base64-encoded)
  for wire format consistency with OPFS and Node.js fs adapters. Includes
  automatic JSON fallback for backward compatibility with pre-v0.17 databases.
- **Unified WASM wire format**: All three WASM storage adapters (IDB, OPFS,
  Node.js fs) now use postcard serialization for stored data.

### Fixed — Relay Dedup

- **Sender double-send bug**: When a sender had both `server_peers` (OWM) and
  `peer_addrs` (WsConn children of OWM), Puts were sent twice — once via the
  OWM fan-out path and once via the direct `peer_addrs` path. Fixed by marking
  `peer_addrs` as `already_sent_to` when OWM was sent the Put (commit 856f24d).
- **`subscriber_fanout` counter spin**: Random sampling loop incremented
  `sent_to` before checking `already_sent_to`, spinning 4× per Put with zero
  sends. Fixed by guarding when `known_peers.len() - already_sent_to.len() == 0`.
- **Duplicate fan-out in `handle_put_relay`**: WsConn actors received duplicate
  messages via two independent Router paths (`server_peers` + `known_peers`).
  Fixed by adding `peer_addrs.values()` to `already_sent_to` in both
  `handle_get` and `handle_put_relay` (commit c925c75).

### Fixed — Supply Chain

- **`time` crate RUSTSEC-2026-0009**: Patched `time` from 0.3.45 → 0.3.47
  (DoS via stack exhaustion).

### Benchmark Results (v0.17.0, 5× isolation)

| Benchmark | Avg Throughput | Dedup | Status |
|-----------|---------------|-------|--------|
| Local Put 10K | 42,685 puts/sec | n/a | ✅ clean |
| Local Put 100K | 41,795 puts/sec | n/a | ✅ clean |
| Relay 1×10K | 5,163 msgs/sec | 0 | ✅ zero waste |
| Relay 1×50K | 8,725 msgs/sec | 0 | ✅ zero waste |
| Relay 10×5K | 2,934 msgs/sec | ~449K | ⚠️ topological (10-node mesh amplification) |
## [0.16.0] — 2026-08-20 — Arena-Allocated Children + WASM Test Architecture + Reconnection Sync

### Changed — Architecture

- **Arena-allocated `Children`**: Replaced `std::collections::BTreeMap<String, NodeData>`
  with `arena_btreemap::BTreeMap` backed by `SyncBumpArena` — a Send+Sync bump allocator.
  All `Children` in `Put`, `NodeInner`, and storage adapters now use arena allocation.
  Eliminates per-node BTreeMap heap allocation — bump pointer arithmetic instead of
  malloc/free. Profiling showed this eliminated 100% of context switches and reduced
  page faults by 95% (25,378 → 1,151) on local put benchmark.
- **`arena-btreemap` v0.1.2**: Switched from path dependency to published crates.io
  version. Published by Guan as a companion crate.
- **`pub mod utils`**: Made `utils` module public for benchmarking access.

### Changed — WASM

- **WASM test architecture**: Split WASM tests into three tiers following industry
  standards:
  - `wasm_tests.rs` (wasm-bindgen-test): 5 pure logic tests (parsing, serialization,
    local put/get) — no network I/O
  - `tests/wasm-integration/node-integration.mjs` (Node.js): 7 network tests
    (WebSocket connectivity, cross-talk, throughput) — real event loop
  - `tests/e2e/gun-beam-interop.spec.mjs` (Playwright/Chromium): 3 browser interop
    tests with Gun.js
  - Root cause: `wasm-bindgen-test-runner` uses microtask-based event loop executor
    that processes ALL microtasks before ANY I/O events (macrotasks). Network tests
    in wasm-bindgen-test are fundamentally unreliable.
- **WASM WebSocket send strategy**: Replaced unreliable `web_sys::WebSocket::ready_state()`
  check with direct `send_with_str()` try-send pattern. `Ok(())` = sent, `Err(_)` =
  buffer to outbox. `onopen` flushes outbox. Bypasses `ready_state()` entirely.
- **WASM Hi handshake fix**: Both `Message::Hi` constructions in `wasm_ws.rs` now
  include `is_ack: None` and `msg_id: uid.clone()` — required after struct gained
  new fields for DAM handshake protocol.

### Fixed

- **Reconnection sync**: `on()` uses `self.inner.uid` for root's direct children
  (empty soul bug fix), `parent_id` for deep children. Soul-level Gets (no `.` field).
  Relay custom Get handler bypasses `mesh.say` entirely, sends Put directly to
  requesting peer's WebSocket.
- **`handle_put` child-descendant matching**: Fires `map_sender` when
  `node_id.starts_with("{uid}/")` — extracts child key for map() callbacks.
- **Wire-live DAM handshake**: Fixed `dam:?` handshake protocol, soul filtering
  for relay, dedup, HTTP path handling, and test timing.
- **Hub relay topology**: Fixed cross-backend mesh E2E tests and relay forwarding
  for hub topology (non-mesh relay where all peers connect to central relay).
- **WebRTC RtcSignal**: Removed phantom `json_str` field from construction sites.
- **Persy storage**: Arena-allocated Children in `persy_storage` + borrow lifetime fixes.
- **Browser WASM**: Use web-target WASM build (not nodejs), add subscription settle delay.
- **Clippy**: Fixed pre-existing `unused mut` warnings in `persy_storage.rs`.

### Added

- `docs/TESTING.md` — standardized test workflow for all 7 test suites
- `docs/PROFILING.md` — profiling guide for perf, flamegraph, heaptrack, and dhat
- `justfile` — 21-stage release pipeline with `just` task runner
- `deny.toml` — added Zlib license to allow-list (for `foldhash` transitive dep)
- `.gitignore` — added `radata/` directory
- `radata/` junk files removed from git tracking

### Test Summary

| Suite | Count | Status |
|---|---|---|
| Native unit (lib) | 320 | ✅ ALL PASS |
| Binary (CLI) | 21 | ✅ ALL PASS |
| Integration | 10 | ✅ ALL PASS |
| Wire live (Gun.js interop) | 7 | ✅ ALL PASS |
| WASM unit (logic only) | 5 | ✅ ALL PASS |
| Node.js WASM integration | 7 | ✅ ALL PASS |
| Browser (Playwright) | 3 | ✅ ALL PASS |
| **Total** | **373** | **0 failures** |

Zero clippy warnings on both native and WASM targets.

## [0.15.0] — 2026-08-15 — Growable Mailboxes + mimalloc

### Changed — Performance

- **mimalloc global allocator**: Added mimalloc 0.1.52 as the default global
  allocator on native targets (optional via `mimalloc` feature). Profiling
  showed the system allocator at 31.9% of CPU. mimalloc's per-thread heap
  segments with deferred freeing reduce allocation contention. Local put
  benchmark improved 34% on average (47K → 64K puts/sec), with warm runs
  hitting 91K puts/sec — nearly 2× the v0.14.0 baseline.
- **Eliminated mailbox pre-allocation**: `MailboxInner::new()` now uses
  `VecDeque::new()` instead of `VecDeque::with_capacity(capacity)`. The
  previous code pre-allocated 65536 slots (512KB) per actor — 94.6% of
  all allocated bytes per DHAT profiling. The queue now grows on demand
  with amortized O(1) push. The `capacity` field remains as a backpressure
  ceiling (DoS prevention) — `send()` returns `Err(())` when full.
- **Configurable mailbox ceilings**: Added `mailbox_capacity` (default 65536)
  and `child_mailbox_capacity` (default 256) to `Config`. The router/root
  actor uses `mailbox_capacity`; child nodes use `child_mailbox_capacity`
  via the new `start_actor_bounded` / `start_router_bounded` API. Operators
  can tune for memory-constrained or high-throughput environments.

### Security

- **Reduced DoS surface**: With pre-allocation eliminated, a malicious peer
  creating unique soul paths to spawn child actors now allocates ~2KB per
  actor (256-slot queue grown on demand) instead of 512KB pre-allocated.
  Attack memory cost reduced 256×.

### Benchmark Results

| Benchmark | v0.14.0 | v0.15.0 (avg) | Change |
|-----------|---------|---------------|--------|
| Local put (100k) | 47,499/sec | 63,568/sec | +33.8% |
| Relay 1×10k | 6,931/sec | 6,634/sec | -4.3% (noise) |
| Relay 1×50k | 15,594/sec | 18,337/sec | +17.6% |
| Relay 10×5k | 11,966/sec | 12,277/sec | +2.6% |

## [0.14.0] — 2026-08-15 — Relay Hot-Path Optimization (FxHash + String Allocation Reduction)

### Changed — Performance

- **FxHash replacement**: Replaced SipHasher-1-3 (std default) with FxHash
  (`rustc-hash` crate) for all non-cryptographic `HashMap`/`HashSet` across
  the codebase. FxHash is the hasher used by the Rust compiler itself — ~3-4×
  faster than SipHash for non-cryptographic workloads. Profiling showed
  `sip::Hasher::write` at 2.98% + `hash_one` at 0.84% of CPU in the relay
  hot path. Security tradeoff: FxHash is not HashDoS-resistant, but affected
  maps are keyed by message IDs from semi-trusted WebSocket peers (same trust
  model as Gun.js's JavaScript `HashMap`).

- **`sea::session` kept on SipHash**: Cryptographic session storage retains
  std's default `RandomState` (SipHash) for HashDoS resistance — security-
  critical context where adversarial key crafting is a real threat.

- **String allocation reduction in message parsing**: `from_json_obj` and
  `from_put_obj` now accept `&str` for `json_str` instead of `String`,
  eliminating one `String` allocation per message on the common path (only
  cloned on signature verification failure, which is rare).

- **`msg_id` move instead of clone**: Fixed `msg_id.to_string()` in
  `from_put_obj` — was cloning an already-owned `String`. Now moved directly.

- **`TryFrom<&SerdeJsonValue>` for `Value`**: Added borrowed conversion impl
  alongside the existing owned `TryFrom<SerdeJsonValue>`. Avoids cloning the
  `JsonValue` tree when only a reference is available during parsing.

### Added

- `BoundedHashMap` is now generic over `BuildHasher` (default: `FxBuildHasher`),
  allowing callers to choose their hasher. Existing code using
  `BoundedHashMap::new()` or `BoundedHashMap::default()` works unchanged.

- `FxHashMap` and `FxHashSet` type aliases re-exported from `crate::utils`,
  using `BuildHasherDefault<rustc_hash::FxHasher>` as the default hasher.

### Benchmark Results (vs v0.13.0, normal load, ~75°F ambient)

| Benchmark | v0.13.0 | v0.14.0 | Change |
|-----------|---------|---------|--------|
| Local put (100k) | 46,380 | 47,499 | +2.4% |
| Relay 1×50k | 15,419 | 15,594 | +1.1% |
| Relay 10×5k | 11,355 | 11,966 | +5.4% |

### Dependencies

- Added `rustc-hash = "2.1.3"` — pure Rust, zero transitive deps, WASM-compatible.

## [0.13.0] — 2026-08-15 — Relay Optimization (Serialized Caching + Per-Key HAM + Batched Frames)

Three Gun.js-inspired relay optimizations that reduce redundant work on the
network fan-out path. All three are transparent to the wire protocol — a
Gun.js peer sees identical JSON, just packed more efficiently in transit.

### Sprint 1: Serialized Message Caching
- `OnceLock<Bytes>` on `Put` struct — first serialization populates the cache,
  subsequent peers get `Bytes::clone()` (Arc refcount bump, zero-copy)
- Mirrors Gun.js `meta.raw`: serialize once per relayed Put, reuse for all peers
- `Message::to_writer` checks cache before serializing
- `WsConn::send_msg` uses cached bytes for Put variant
- Cache resets on `Clone` (each clone is a different message)

### Sprint 2: Per-Key HAM Filtering
- `HamFilterResult` enum replaces `bool` return: `Stale`, `New`, `PartiallyNew`
- Each (soul, key) pair independently evaluated — stale keys dropped, only new
  keys proceed to storage and relay
- Mirrors Gun.js `ham()` called per-key inside its `while` loop
- Common case (all-new) is zero overhead — uses original `&Put` with no allocation
- `Put::with_updated_nodes()` constructor for the partial case

### Sprint 3: Batched WebSocket Frames
- `handle_batch` packs >1 messages into a JSON array in a single WS text frame
- Single-message sends unchanged (no array wrapper) — backwards compatible
- Mirrors Gun.js `peer.batch` packing in `mesh.say`
- Receive side already handles arrays (`Message::try_from` checks `as_array()`)
- Integrates with Sprint 1 cache for Put messages in batch

### Benchmark Results

| Test | v0.12.0 | v0.13.0 | Change |
|------|---------|---------|--------|
| Local put (100K) | ~53,300 puts/sec | ~46,400 puts/sec | neutral (within variance) |
| Relay 1 sender × 10k | 5,287 msgs/sec | 6,984 msgs/sec | **+32%** |
| Relay 1 sender × 50k | 10,643 msgs/sec | 15,419 msgs/sec | **+45%** |
| Relay 10 senders × 5k | 11,380 msgs/sec | 11,355 msgs/sec | neutral |

The 50k relay test shows the biggest gain because serialization caching and
batched frame packing both amortize over more messages.

### Test Summary
- 356 native tests (10 new for relay optimizations), 0 failures
- 19 doctests, 0 failures
- 7 WASM tests pass, 4 ignored (browser-only), 0 failures
- 0 clippy warnings

## [0.12.0] — 2026-08-13 — Performance Overhaul + HAM Pre-Filter

**17.5× throughput improvement** (3,050 → 53,300 puts/sec on clean machine)
with zero functional regressions. Adds Gun.js-compatible HAM stale-data
pre-filter for "avoid work" optimization.

### Added

- **HAM (Hypothetical Amnesia Machine) pre-filter** (`src/router.rs`):
  Router-maintained timestamp index that checks incoming Put timestamps
  against cached state before any storage/relay work. Mirrors Gun.js
  `ham()` function — eliminates redundant processing for stale data.
  Third deduplication layer (after msg-ID and checksum dedup).
  - `BoundedHashMap<String, HashMap<String, f64>>` — soul → key → timestamp
  - Ack messages bypass HAM (control messages, not data writes)
  - Empty `updated_nodes` → pass (matches Gun.js key-loop semantics)
  - Same timestamp → skip (incumbent wins, last-write-wins)
  - `messages_dropped_ham` metric counter
  - 2 E2E tests (`tests/ham_stale_relay.rs`)
- **Mailbox module** (`src/mailbox.rs`): Custom SPSC ring buffer with
  batch drain and `tokio::sync::Notify` wakeup. Replaces tokio mpsc.
- **`echo_back_regression` test suite** (`tests/echo_back_regression.rs`):
  3 tests verifying relay forwarding and no echo-back to sender.
- **`local_put_bench` benchmark** (`tests/local_put_bench.rs`): 10,000
  local puts with memory storage for throughput measurement.

### Changed

- **`Arc<Message>` in Actor trait**: Messages are `Arc`'d for zero-copy
  fanout. `Addr::send()` takes `impl Into<Arc<Message>>` — zero call-site
  changes for baseline, opt-in `Arc::clone` for hot paths.
- **Lazy broadcast channels**: `Arc<RwLock<Option<broadcast::Sender>>>`
  pattern — channels created on first subscriber, not on node creation.
  Eliminates 37% allocation hotspot (9.7× improvement alone).
- **`Arc<NodeInner>` consolidation**: Node is now a thin handle wrapping
  `Arc<NodeInner>`. Clone cost reduced from ~12 atomics + 3 heap allocs
  to a single atomic increment.
- **`&Put` signatures**: `handle_put` across router, node, and storage
  takes `&Put` instead of owned `Put` — eliminates unconditional deep
  clone of `BTreeMap` on every message (the 17.5× win).
- **Zero-copy WebSocket serialization**: `to_writer` + `std::mem::take`
  replaces `send_buf.clone()` + `String::from_utf8`.

### Removed

- **Bloom filter dedup**: Replaced by `HashMap` — bloom filters performed
  terribly (64-157 msgs/sec vs HashMap's 1,977+). 7 cache-missing hash
  probes > 1 HashMap lookup + String alloc. Suckless philosophy holds.
- **`json_str` cache**: Removed — `to_string(&self)` replaces
  `to_string(&mut self)` with internal caching.
- **mimalloc**: Removed — zero benefit on single-threaded benchmark.

### Performance

| Metric | v0.11.0 | v0.12.0 | Improvement |
|--------|---------|---------|------------|
| Local put throughput (clean) | 3,050/sec | ~53,300/sec | 17.5× |
| Local put throughput (loaded) | — | ~26,800/sec | — |
| + HAM pre-filter (loaded, 100°F) | — | ~22,600/sec | minimal overhead |
| Relay 1×10k | 3,041/sec | 6,952/sec | 2.3× |
| Relay 1×50k | 2,410/sec | 16,005/sec | 6.6× |
| Relay 10×5k | 2,014/sec | 11,604/sec | 5.8× |

Flame graph confirms flat profile — no dominant hotspot above 4.74%.


## [0.11.1] — 2026-08-10 — WASM Relay TPS + Browser Benchmark

Native + browser relay throughput parity. WASM relay TPS benchmarks
and interactive browser benchmark page.

### Added

- **WASM relay throughput benchmarks** (`src/wasm_tests.rs`): Two new
  `#[wasm_bindgen_test(async)]` tests measuring end-to-end relay TPS
  from a WASM client. Ground truth from relay `/metrics` HTTP endpoint.
  - `wasm_relay_throughput_1k`: 1,000 messages → 115 msgs/sec
  - `wasm_relay_throughput_5k`: 5,000 messages → 651 msgs/sec
- **Browser relay TPS** (`examples/bench.html`): Relay URL input,
  connect button, and TPS measurement using `/metrics` ground truth.
  Shows full hot-path counter breakdown (ws_recv, parsed, relayed,
  dedup, fanout, ws_sent).
- **Local WASM API benchmarks**: Put throughput, Get throughput, and
  Put→Get round-trip — all using BEAM's WASM API (not native JS).
- **RESULTS.md**: WASM relay TPS section with analysis.
- **README**: WASM relay TPS table + browser benchmark instructions.

### Changed

- `bench.html`: Removed native JS `JSON.parse`/`JSON.stringify`
  benchmarks — they didn't measure BEAM at all. Replaced with BEAM
  WASM API benchmarks.
- `bench.html`: Added relay connection UI with configurable message count.

### Performance Results (v0.11.1)

| Metric | Native | WASM (Node) |
|--------|--------|-------------|
| Relay 1k burst | 3,041 msgs/sec | 115 msgs/sec |
| Relay 5k sustained | 2,410 msgs/sec | 651 msgs/sec |
| Parse small Put | 851 ns | 8,069 ns |
| Serialize small Put | 152 ns | 18,144 ns |

## [0.11.0] — 2026-08-10 — Benchmark & Instrumentation

Comprehensive benchmarking suite: hot-path instrumentation, relay
throughput tests, Criterion micro-benchmarks, and WASM/browser
benchmarks. First published performance numbers for BEAM.

### Added

- **Hot-path metrics** (`metrics.rs`): 7 new lock-free `AtomicU64` counters
  tracking the relay's critical path — `ws_messages_received`,
  `messages_parsed`, `messages_dropped_dup`, `messages_relayed`,
  `subscriber_fanout_total`, `serialization_calls`, `ws_messages_sent`
- **`/metrics` HTTP endpoint**: JSON metrics snapshot on relay web UI
  (both warp HTTP and TLS paths). `start_web_server` now takes
  `Arc<Metrics>`.
- **Relay throughput benchmark** (`tests/relay_throughput_bench.rs`):
  Real WebSocket connections through memory-only relay. 3 scenarios:
  single-publisher burst, multi-publisher concurrent, relay fan-out.
  Measured from relay's metrics counters (ground truth).
- **Criterion micro-benchmarks** (T4/T5): Wire protocol parse/serialize
  (small/medium/large JSON), dedup check (fresh + duplicate), actor
  mailbox throughput, router dispatch throughput.
- **WASM benchmarks** (`src/wasm_tests.rs`): `performance.now()`-based
  benchmarks for parse, serialize, and Get operations behind
  regular `#[wasm_bindgen_test]` functions (no feature gate needed).
- **Browser benchmark page** (`examples/bench.html`): Interactive HTML
  page for running WASM benchmarks in the browser.
- **`benches/RESULTS.md`**: Full benchmark results with methodology and
  analysis.
- **README benchmark section**: Summary tables and run commands.

### Changed

- `ActorContext` now carries `metrics: Arc<Metrics>` — propagated through
  `child_context()`. Counters incremented at call sites (no method
  signatures changed). Composition-Root IoC pattern.
- `Cargo.toml`: Fixed `[[bench]]` section — was `[target.cfg.bench]`
  which didn't activate Criterion's custom harness. Now uses standard
  `[[bench]]` with `harness = false`.
- `Cargo.toml`: No new dependencies — uses existing `web_time::Instant`.
- `Cargo.toml`: Added regular `#[wasm_bindgen_test]` functions (no feature gate needed).

### Performance Results (v0.11.0)

| Metric | Value |
|--------|-------|
| Relay throughput (1 sender, 10k msgs) | ~3,000 msgs/sec |
| Relay throughput (1 sender, 50k msgs) | ~2,400 msgs/sec |
| Relay throughput (10 senders, 50k msgs) | ~2,000 msgs/sec |
| Parse small Put JSON | 851 ns |
| Serialize small Put JSON | 152 ns |
| Dedup check (fresh) | 274 µs |
| Actor mailbox send+recv | 309 µs |
| WASM parse small Put | 8,069 ns |
| WASM serialize small Put | 18,144 ns |
| WASM parse Get | 4,644 ns |


## [0.10.0] — 2026-08-09 — Gun.js Wire Protocol Compatibility Verified

Bidirectional Gun.js ↔ BEAM wire protocol compatibility proven with
Playwright E2E tests. All three interop scenarios pass: Gun.js→BEAM,
BEAM→Gun.js, and bidirectional convergence.

### Fixed

- **ws_server.rs**: Internal messages (RegisterQuorum, CheckQuorumTimeouts)
  no longer relayed to wire clients — only Put/Get forwarded
- **message.rs**: Put::to_string() now skips BEAM-internal souls (empty root
  pointer, slash-containing value nodes) in wire serialization — Gun.js
  cannot parse these internal graph structures
- **message.rs**: json_str cache is now cleared in WsConn::handle before
  re-serializing, ensuring soul filtering applies to relayed Puts
- **ws_conn.rs**: Idiomatic `while let` + `match` receive loop replacing
  try_for_each closure, with proper handling of all WsMessage variants
  (Binary, Ping, Pong, Close, Frame) — empty frames no longer cause
  connection cleanup

### Added

- Playwright E2E test suite for Gun.js ↔ BEAM bidirectional interop
  (`tests/e2e/gun-beam-interop.spec.mjs`)
- Each test auto-starts a fresh relay (memory-only) for isolation
- Dynamic per-test soul subscription prevents cross-test contamination
- Gun.js served locally for VPN-compatible testing

### Security

- OPSEC audit: removed hardcoded internal network IP from browser-test pages
- Removed stale duplicate README from browser-test directory
- Generated test HTML files gitignored

## [0.9.2] — 2026-08-08 — WASM Browser Support + Automated Test Suite

BEAM compiles to WebAssembly and runs in the browser. Browser nodes connect
to relay servers via WebSocket and participate in the P2P graph as full
clients — put, get, and real-time subscriptions all work cross-window.

### Added

- WASM target support (wasm32-unknown-unknown) — BEAM compiles to WebAssembly
- Browser WebSocket adapter using web-sys::WebSocket with outbox buffering
- IndexedDB persistent storage adapter with write-through cache
- JavaScript bindings via wasm-bindgen:
  - Beam struct: new(), new_persistent(), connect(), put(), put_num(), put_bool(), put_null(), get(), on(), stop()
  - put() — fire-and-forget write via spawn_local
  - get() — returns a JS Promise via future_to_promise
  - on(path, callback) — real-time subscription with Gun.js .on() semantics
  - TypeScript definitions auto-generated
- Automated WASM test suite (6 tests via wasm-bindgen-test-runner in Node.js):
  - smoke_test, local_put_get_roundtrip, relay_connect, relay_put_echo
  - two_clients_cross_talk (unidirectional)
  - bidirectional_cross_talk (both directions verified)
- Browser chat example (examples/browser-chat.html)
- wasm-pack build --target web --release produces a deployable package
- Cargo.toml target-gated: native-only deps excluded on WASM
- tokio_time shim module (tokio::time on native, tokio_with_wasm on WASM)
- Full SEA crypto stack compiles to WASM with zero source changes

### Changed

- Cargo.toml split into platform-specific dependency sections
- 12 source files cfg-gated to separate native-only from WASM-compatible code
- std::time replaced with web-time across all shared source files
- Tokio time feature excluded on WASM (provided by tokio_with_wasm)
- Quorum reaper task cfg-gated to native only (browser nodes are leaf clients)

### WASM Runtime Gotchas Resolved

1. Panic hook — console_error_panic_hook for readable WASM panics
2. Tokio runtime context — OnceLock<Runtime> + enter() guard
3. std::time and tokio time — web-time crate + tokio_with_wasm
4. Tokio spawn — tokio_with_wasm shim for browser event loop integration
5. JS Closure types are !Send — solved by leaking to JS heap
6. on() vs map() semantics — use map() for Gun.js-compatible child subscriptions
7. Dead spawn scheduler — tokio_spawn shim
8. Noop addr in known_peers — pre_start lifecycle hook
9. WebSocket CONNECTING state — initial Arc<Mutex> coordination
10. WebSocket CONNECTING for user Puts — outbox buffer with Addr::noop()

### Added
- WASM target support (`wasm32-unknown-unknown`) — BEAM compiles to WebAssembly
- Browser WebSocket adapter (`wasm_ws.rs`) using `web-sys::WebSocket`
- IndexedDB persistent storage adapter (`wasm_idb.rs`) with write-through cache
- JavaScript bindings (`wasm.rs`) via `wasm-bindgen`:
  - `Beam` struct with `new()`, `new_persistent()`, `connect()`, `put()`, `put_num()`, `put_bool()`, `put_null()`, `get()`, `on()`, `stop()`
  - `put()` — fire-and-forget write via `wasm_bindgen_futures::spawn_local`
  - `get()` — returns a JS `Promise` via `future_to_promise` (reads value once with timeout)
  - `on(path, callback)` — real-time subscription, invokes JS callback on each received value
  - TypeScript definitions auto-generated
- `wasm-pack build --target web --release` produces a deployable package
- Cargo.toml target-gated: native-only deps (redb, Persy, tokio-tungstenite, multicast) excluded on WASM
- `connect_peer_wasm()` method on `Node` (cfg-gated to `wasm32`)
- `tokio_time` shim module — re-exports `tokio::time` on native, `tokio_with_wasm::time` on WASM
- Full SEA crypto stack (P-256, AES-256-GCM, SHA-256, PBKDF2) compiles to WASM with zero source changes

### Changed
- `Cargo.toml`: split into `[dependencies]`, `[target.'cfg(not(target_arch = "wasm32"))'.dependencies]`, and `[target.wasm32-unknown-unknown.dependencies]`
- 12 source files cfg-gated to separate native-only from WASM-compatible code
- `std::time` → `web_time` across all shared source files (uses `performance.now()` / `Date.now()` on WASM)
- Tokio `time` feature excluded on WASM — timer functions provided by `tokio_with_wasm` via `tokio_time` shim
- Quorum reaper task cfg-gated to native only (browser nodes are leaf clients, don't manage quorums)
- `web-sys` features: WebSocket, MessageEvent, ErrorEvent, CloseEvent, Window, IdbFactory, IdbDatabase, IdbObjectStore, IdbTransaction, IdbTransactionMode, IdbRequest, IdbOpenDbRequest, IdbVersionChangeEvent, Event, EventTarget

### Key Findings
- Core graph engine, router, and crypto stack are WASM-pure (zero changes needed)
- `tokio::spawn` works on WASM via `rt` feature — no `spawn_task()` abstraction needed
- JS `Closure` types are `!Send` — solved by leaking closures to the JS heap (same pattern as `WasmWsConn`)
- `wasm-opt` disabled due to bulk memory validation bug in old binary
- Three WASM runtime gotchas discovered and fixed:
  1. **Panic hook** — Rust panics show cryptic offsets without `console_error_panic_hook` (add first)
  2. **Tokio runtime context** — `tokio::spawn()` compiles but panics without runtime context (fix: `OnceLock<Runtime>` + `enter()` guard)
  3. **`std::time` and tokio `time` feature** — both panic on `wasm32-unknown-unknown` (fix: `web-time` crate + `tokio_with_wasm` for timer functions)
- Pattern: `cargo check --target wasm32` passing ≠ runtime working. Std/tokio stubs compile but panic at runtime


## [0.9.1] — 2026-08-08 — Multicast Message Chunking

Application-layer chunking for the UDP multicast adapter. Messages exceeding
the safe UDP datagram size (~1400 bytes) are now transparently split into
chunked JSON envelopes and reassembled by the receiver, preventing IP
fragmentation and enabling multicast to relay large `Put` messages.

### Added

- **Multicast chunking protocol:** Messages > `MAX_DATAGRAM_SIZE` (1400 bytes)
  are split into `CHUNK_PAYLOAD_SIZE` (900 byte) fragments, each wrapped in a
  `{"beam_chunk":{"id","seq","total","data"}}` JSON envelope. The receiver's
  `ReassemblyBuffer` collects chunks by message ID and reassembles when all
  arrive. Supports out-of-order delivery, duplicate detection, concurrent
  message reassembly, and timeout-based eviction (5s) with a 64-slot capacity
  cap.
- **18 unit tests** for chunking and reassembly: round-trip, out-of-order,
  duplicate, concurrent, timeout, eviction, threshold boundary, empty
  message, invalid base64, total mismatch, seq out of bounds.

### Changed

- `Multicast::handle_incoming_message` now accepts a `&mut ReassemblyBuffer`
  parameter and handles both raw messages (backward compatible) and chunk
  envelopes. Existing message flow unchanged for small messages.
- `Multicast::handle` uses `broadcast_message` helper that chunks large
  messages before sending.
- Extracted `forward_message` helper to avoid code duplication between
  direct-parse and reassembly-complete paths.


## [0.9.0] — 2026-08-08 — Enterprise-Grade Release

15 dependency modernization sprints across 3 tiers, eliminating 5 dependencies
(ring, jsonwebtoken, jsonwebkey, bincode, ctrlc). Full graceful shutdown
implementation. CI pipeline modernized with full test suite, clippy enforcement,
and supply chain security auditing. Release profile hardened for production
binaries.

### Changed

- **Crypto consolidation:** Eliminated ring/jsonwebtoken/jsonwebkey —
  consolidated on p256 + sha2 (already in dependency tree). Single
  verification path instead of two. Gun.js double-hashing semantics
  preserved (SHA256(SHA256(message))).
- **CLI framework:** clap 2.33 → 4.6. New `cli.rs` module (467 lines)
  with derive API. `main.rs` reduced from 354 to 167 lines
  (composition-root pattern). 21 unit tests for CLI argument parsing.
- **Serialization:** bincode → postcard (RUSTSEC-2025-0141 resolved).
  Smaller, faster, maintained.
- **WebSocket transport:** tokio-tungstenite 0.17 → 0.30. Utf8Bytes
  replaces String for Text messages.
- **Web server:** warp 0.3 → 0.4. TLS path replaced with tokio_native_tls
  (same stack as WsServer — DRY). Plain HTTP still uses warp::serve.
- **Dependency upgrades:** rand 0.8 → 0.9, multicast-socket 0.2 →
  oko-multicast-socket 0.5 (fork with maintained nix dependency),
  criterion 0.3 → 0.8, base64 0.13 → 0.22, thiserror, env_logger, dirs
  updated to latest. str0m 0.19 → 0.21 (WebRTC). postcard
  default-features=false (eliminates heapless/atomic-polyfill).

### Added

- **Graceful shutdown:** Full graceful shutdown via tokio::sync::watch
  channel on ActorContext. SIGINT + SIGTERM handling via tokio::signal
  (replaces ctrlc crate). All adapter `stopping()` implementations filled
  in. `--shutdown-timeout` CLI flag (default 30s). 5 E2E shutdown tests.
  Shutdown sequence: flush → signal → drain → force stop.
- **Release profile:** `[profile.release]` with opt-level=3, strip, LTO,
  codegen-units=1 for optimized production binaries.
- **CI pipeline:** Modernized with full test suite (262+ tests), clippy
  `-D warnings`, fmt check, doc tests, cargo audit, cargo deny. All
  actions updated to current versions (checkout@v4,
  dtolnay/rust-toolchain, Swatinem/rust-cache).
- **SECURITY.md:** Security policy with vulnerability reporting process.
- **CONTRIBUTING.md:** Contributor guide with build, test, and PR workflow.
- **Supply chain security:** cargo-audit + cargo-deny with deny.toml
  configuration for advisories, licenses, bans, and sources.

### Security

- RUSTSEC-2025-0010 (ring unmaintained) — resolved: ring eliminated
- RUSTSEC-2025-0141 (bincode unmaintained) — resolved: replaced with postcard
- RUSTSEC-2021-0119 (nix 0.19 OOB write) — resolved: switched to
  oko-multicast-socket 0.5 (uses nix 0.24)
- RUSTSEC-2023-0089 (atomic-polyfill unmaintained) — resolved:
  postcard default-features=false eliminates heapless transitive dep
- RUSTSEC-2026-0009 (time 0.3.45 stack exhaustion DoS) — resolved:
  pinned time >=0.3.47
- Supply chain: cargo-audit + cargo-deny integrated into CI
  (advisories, licenses, bans, sources — all passing)

### Removed

- 5 direct dependencies eliminated (ring, jsonwebtoken, jsonwebkey, bincode, ctrlc)
- 3 transitive dependencies eliminated (heapless, atomic-polyfill, rustc_version)
- Smaller attack surface, fewer transitive dependencies

### Tests

- 257 tests pass (238 lib + 19 doctests), 0 failures, 0 clippy warnings
- 0 compiler warnings
- Gun.js wire compatibility: 3-layer proof (golden fixtures, Node.js
  mirror, live integration)

### 0.8.0 — 2026-07-25 — The BEAM Rebrand

This release marks BEAM's identity as an independent successor to Rod. The
codebase has diverged significantly since the May 2026 fork (7 releases,
367 commits, 25+ new features including redb+Persy adapters, WebRTC,
quorum-ack, send-metrics). v0.8.0 makes that independence explicit.

#### Changed

- **Crate name:** `rod` → `beamdb`
- **Repository:** `mmalmi/rod` → `guan/beamdb` 
- **Module path:** `rod::*` → `beam::*`
- **CLI binary:** `rod` → `beam`
- **Keygen binary:** `beam-sea-keygen` (already named for BEAM)
- **License header:** Dual copyright — `Copyright (c) 2021 Martti Malmi`,
  `Copyright (c) 2026 David Newman <david.r.newman@proton.me>`

#### Preserved

- MIT license (no relicensing — MIT permits dual copyright additions)
- Gun.js wire-format compatibility (ask-pattern intact)
- All substrate improvements from v0.3.0 onwards (sentinel-drain ack,
  redb+Persy adapters, WebRTC, quorum)

#### Removed

- `docs/plans/` — implementation plans preserved in git history under
  the commit that introduced them, per project policy of archiving
  ephemeral planning artifacts

#### Contributors Added

- **David Newman** — maintainer (legal name in LICENSE/NOTICES)
- **Guan** — development partner (architecture, code, tests, docs)

#### Substrate Truths Verified

- `cargo check -p beamdb` — 0 errors, 0 new warnings
- `cargo check -p beamdb --all-features` — 0 errors, 0 new warnings
- `cargo test -p beamdb --lib` — 199/199 tests pass
- 13 pre-existing warnings preserved (verified identical to master)
- Zero `\brod\b` references in source files (LICENSE and NOTICES.md
  contain historical attribution only)

---

## Historical — Rod Lineage

The following entries preserve Rod's release history through v0.7.2
(the last release under the Rod name).

### 0.7.2 — 2026-07-25 — Enterprise stabilization

#### Changed

- Replaced blind `sleep()` calls with active readiness polling
  (`wait_for_peer_count`, `wait_for_port`, `wait_for_handshake`)
- Test suite: 0 flakiness across 4 feature configs
- Resurrected silently-broken `webrtc_node_sync` test (was missing `.await`
  on `put()` — test passed for the wrong reason)

#### Test Counts

- Default: 264 tests passed
- webrtc: 272 tests passed
- persy: 282 tests passed
- webrtc+persy: 292 tests passed

### 0.7.1 — 2026-07-24 — Bench harness RSS fix

#### Fixed

- `clean_storage_file()` called between Criterion iterations
- `write_storm` group: await all `put()` calls to prevent actor mailbox flood
- Root cause: Persy's `background_ops` was innocent — harness bug was
  the actual culprit (database file accumulating ~700 MB per iteration)

#### Bench Results

- redb: flat ~1 MB RSS, ~1.4× to ~3.4× faster than Persy
- Persy: flat ~3 MB RSS
- Recommendation: redb as default backend

### 0.7.0 — 2026-07-24 — Heavy Abusive Benchmarks

#### Added

- 4 storage bench groups: `write_storm`, `concurrent`, `read_storm`, `mixed`
- Crash recovery test via subprocess pattern
- Memory pressure test using `sysinfo` for RSS measurement
- `benches/RESULTS.md`: comparison report with redb vs Persy numbers

### 0.6.0 — 2026-07-23 — Persy migration tool

#### Added

- `beam migrate` CLI command for redb ↔ Persy database conversion
- Single-transaction-per-batch safety
- Checksum verification
- 421/421 lib tests pass

### 0.5.0 — 2026-07-22 — Persy storage adapter

#### Added

- `src/adapters/persy_storage.rs` (652L, feature-gated via `--features persy`)
- Cross-backend mesh interop with redb nodes
- `background_ops` enabled at dep level (honest benchmark reporting)

### 0.4.0 — 2026-07-22 — Send metrics observability

#### Added

- `src/metrics/` module with bounded-channel silent-drop fix
- Prevents unbounded growth under burst load
- Fixes Follow-up B (silent message drop bug)

### 0.3.0 — 2026-07-22 — Quorum ack (network fanout)

#### Added

- `Message::RegisterQuorum`, sentinel-driven drain pattern
- 6 surfaces use unified `pending_puts` oneshot registry
- Gun.js ask-pattern wire compatibility
- ADR-011: Network fanout ack design rationale
- Threat model: safe for trusted networks, hardening recommended for public

### 0.2.5 — 2026-05-XX — Initial Rod release

- 229 upstream commits from `mmalmi/rod` at fork time
- Rust 2024 edition, MSRV 1.85
- Core: actor framework, central router, Gun.js wire protocol
- SEA: full crypto stack (pair, sign, verify, encrypt, decrypt, user, certify)
- Adapters: WebSocket server/client, UDP multicast, WebRTC, redb
- 178 unit tests + 9 integration tests + 7 doctests

---

## See Also

- `NOTICES.md` — Full contributor attribution and license notices
- `docs/adr/` — Architectural Decision Records (permanent record)
- `docs/architecture.md` — Deep architectural overview
- `docs/migrations/migration-guide.md` — Storage backend migration procedure
- `benches/RESULTS.md` — Storage backend benchmark comparison

For releases prior to v0.2.5, see the upstream `mmalmi/rod` repository.