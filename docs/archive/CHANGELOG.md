# Changelog

## [0.2.5] — 2026-05-23 — SEA.certify Capability Certificates

### Added

- `rod::sea::certify(certificants, policies?, authority)` — sign capability certificates in Gun.js-compatible `{m, s}` format
- `rod::sea::verify_certificate(signed, authority_pubkey)` — verify certificate signature and optional expiry (`e` field, ms since epoch)
- `rod::sea::is_pubkey_certified(payload, pubkey)` — check if a pubkey appears in the certificate's certificants list (`c` field)
- New `certify.rs` module with full documentation and 3 unit tests

### Details

Certificate payload shape (Gun.js compatible):
```json
{
  "c": ["pubkey1", "pubkey2"],
  "e": 1716460800000,
  "r": ".*",
  "w": "skills/",
  "rb": "",
  "wb": ""
}
```
Fields `e`, `r`, `w`, `rb`, `wb` are optional. Re-exports in `sea/mod.rs` alongside existing encrypt/decrypt/sign/verify APIs.

All notable changes to the Rod Rust Gun Protocol implementation (BEAM maintained fork).

## [0.2.4] — 2026-05-18 — Redb Storage Adapter & Flush Protocol

### Added

- `RedbStorage` adapter — ACID persistence via redb MVCC (`src/adapters/redb_storage.rs`)
- CLI flags `--redb-storage` and `--redb-path` for redb-backed node startup  
- `Node::flush_storage()` — fire-and-forget → ack-based flush round-trip
- `Flush` message type with `from`, `id`, `node_id` fields
- `pending_flushes` HashMap in `Node` for correlating acks
- Integration test `redb_storage_persists` — proves data survives node restart
- Integration test `flush_d2_ack` — proves flush round-trip via redb

### Changed

- **Default disk storage now redb.** Sled is deprecated; `tracing::warn!` emitted at runtime.
- Edition bumped to `2024`, `rust-version` set to `"1.85"` for downstream Mnemos compatibility
- `handle_put` in `Node` intercepts flush acks (`put.in_response_to`) before normal data processing
- `flush.from` uses real actor `addr` (not `actor_context.addr`) to ensure ack routing works

### Fixed

- `flush.from` was `noop` actor address — flush acks never reached caller. Fixed to use `self.addr.read().clone().unwrap()`.
- Sled deadlock under load (documented; root cause: lock ordering in sled's BwTree). Mitigation: migrate to redb.

### Architecture Notes

- `RedbStorage` uses `spawn_blocking` for writes to avoid blocking the async runtime. Read path stays synchronous.
- redb single writer is mitigated by Actor message-loop serialization + blocking offload.
- Schema: single `rod_data` table (`node_id → bincode(Children)`) + `rod_meta` table for size tracking.

---

## [0.2.3] — 2026-05-16 — BEAM SEA Session Storage

### Added

- `EncryptedFileSessionStorage` — AES-256-GCM encrypted session files
- `beam-sea-keygen` binary for generating 32-byte session keys
- `User::recall()` / `user.save_to()` for passwordless re-authentication
- `User::leave()` — zeroizes keys and invalidates all clones via `Arc<RwLock<SessionState>>`
- DevOps templates: systemd unit, Docker Compose, Kubernetes secret setup

### Security

- Session directory created with `0700` (owner-only)
- `zeroize` crate scrubs private keys on `Drop` and `leave()`
- Environment variable `BEAM_SEA_SESSION_KEY` for production deployments

---

## [0.2.2] — 2026-05-14 — Flush Protocol Foundation

### Added

- `ContentStore::flush()` trait method
- `Flush` message routing through `Router`
- `SledStorage::handle_flush()` — flushes all trees, sends ack Put
- `MemoryStorage::handle_flush()` — no-op ack for test parity

### Fixed

- `git reset --soft` + `git checkout --` bug: learned to use `git checkout HEAD -- file` for clean reverts

---

## [0.2.1] — 2026-05-13 — SEA Core

### Added

- `rod::sea::generate_pair()` — P-256 signing + encryption keypair
- `rod::sea::sign()` / `verify()` / `verify_async()` — ECDSA-SHA256
- `rod::sea::encrypt()` / `decrypt()` — AES-GCM with ECDH shared secret
- `rod::sea::work()` — PBKDF2 (100K iters) or SHA-256 hash
- `User` builder via `db.user().create()` / `db.user().auth()`

---

## [0.2.0] — 2026-05-10 — Rod × Mnemos Integration

### Changed

- Fork maintained by Freeman King and Guan for Mnemos agent memory system
- Edition bumped to `2024`, resolver to `3`

### Added

- `Node::stop()` for orderly actor shutdown
- `MemoryStorage` checksum deduplication (`should_not_reply_with_put_when_checksum_same_as_in_get`)

---

## [0.1.0] — 2022-05-15 — Original Release (Martti Malmi)

### Features

- Basic get/put/on/map graph primitives
- In-memory and sled storage
- Incoming/outgoing websockets
- Multicast (65KB size limit)
- TLS support
- Hash verification (`#` namespace)
- Signature verification (`~` namespace)

*See upstream repository for full original changelog:* https://github.com/mmalmi/rod
