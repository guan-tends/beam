# BEAM Changelog

All notable changes to BEAM are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [Unreleased — WASM Support]

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