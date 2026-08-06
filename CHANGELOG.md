# BEAM Changelog

All notable changes to BEAM are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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