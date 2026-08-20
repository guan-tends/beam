# Migration Tool: Add Fjall Support — Implementation Plan

**Branch:** `feature/fjall-storage-adapter` (continuation)
**Date:** 2026-08-20
**Author:** Guan (Pema Lhamo)
**Status:** Ready for implementation

---

## 1. Context & Architecture

### 1.1 Current Migration Tool

The migration tool (`src/migration.rs`) converts BEAM graph data between
on-disk storage formats. Currently supports **redb ↔ persy** only.

**Core types:**

- `Backend` enum: `{ Redb, Persy }` — source/target selector
- `MigrateOpts`: `{ from, to, source_path, target_path, batch_size, force, dry_run }`
- `MigrateError`: redb-specific + persy-specific + generic variants
- `NodeRecord`: `{ node_id: String, children: Children }` — persy's on-disk wrapper
- Translation functions: `redb_to_persy_payload()` / `persy_to_redb_record()`

The `io` submodule (actual I/O logic) is gated behind `#[cfg(feature = "persy")]`.
The CLI dispatches `Command::Migrate` only when `persy` feature is enabled.

### 1.2 On-Disk Format Comparison

| Aspect | redb | persy | fjall |
|--------|------|-------|-------|
| **Key** | `&str` (node_id, "" is valid) | N/A (segment scan, node_id inside payload) | `Vec<u8>` = `[0x00] ++ node_id_bytes` |
| **Value** | `postcard(Children)` — bare | `postcard(NodeRecord { node_id, children })` — wrapped | `postcard(Children)` — **bare, same as redb** |
| **Storage unit** | `beam_nodes_v1` table | `beam_nodes_v1` segment | `beam_nodes_v1` keyspace |
| **Path type** | File | File | Directory |
| **Iteration** | `table.iter()` → `(key_guard, value_guard)` | `db.scan(segment_id)` → `(PersyId, Vec<u8>)` | `keyspace.iter()` → `(Vec<u8>, Slice)` |

### 1.3 The Key Insight: DRY Translation

**redb and fjall have identical value formats** — both store `postcard(Children)`
as bare bytes. The only difference is key encoding:

- redb: key = `node_id` string directly (including empty string "")
- fjall: key = `[0x00] ++ node_id_bytes` (prefix avoids LSM empty-key panic)

This means:

| Migration path | Key translation | Value translation |
|---|---|---|
| redb → fjall | `node_id` → `[0x00] ++ bytes` | **None** — identical |
| fjall → redb | `[0x00] ++ bytes` → `node_id` | **None** — identical |
| fjall → persy | strip `0x00` → `node_id` | Children → NodeRecord (reuse existing `redb_to_persy_payload`) |
| persy → fjall | `node_id` → `[0x00] ++ bytes` | NodeRecord → Children (reuse existing `persy_to_redb_record`) |

For `fjall ↔ persy`, we reuse the existing translation functions by first
extracting the node_id from the fjall key (strip prefix), then treating the
record as if it were a redb record (same bare `Children` value format).

### 1.4 Original Architectural Decisions

The migration tool was designed with these principles (from the initial
implementation):

1. **Translation functions are pure** — `redb_to_persy_payload` and
   `persy_to_redb_record` take bytes in, return bytes out. No I/O. This
   makes them unit-testable without any database backend.
2. **I/O functions are backend-pair-specific** — `migrate_redb_to_persy`
   and `migrate_persy_to_redb` each handle one direction. The dispatcher
   (`migrate()`) matches on `(from, to)` and calls the right function.
3. **`NodeRecord` is defined twice** — once in `persy_storage.rs` (the
   canonical definition, `pub(crate)`) and once in `migration.rs` (a local
   copy for unit testing without the `persy` feature). Both serialize to
   identical postcard bytes.
4. **Source is fully materialized before target is opened** — avoids holding
   a read transaction open across a long write transaction.
5. **Dry-run returns source count without writing** — `migrate()` returns
   `source_count` as `records_migrated` and `0` as `target_count_after`.
6. **`--force` flag** — if target path exists, migration fails unless
   `--force` is set. Works for both files and directories.

### 1.5 Visibility Pattern

The persy adapter uses `pub(crate)` for `BEAM_NODES` and `NodeRecord` so
the migration module can access them without redefining the wire format.
The fjall adapter's `encode_key()` and `KEY_PREFIX` are currently private.
Following the same pattern, these need to become `pub(crate)`.

---

## 2. Implementation Tasks

### Task 1: Expose fjall key encoding (`src/adapters/fjall_storage.rs`)

**Files:** `src/adapters/fjall_storage.rs`
**Effort:** S

Make `KEY_PREFIX` and `encode_key` accessible to the migration module.
Add a `decode_key` companion function for the reverse direction.

```rust
/// Prefix byte prepended to every keyspace key.
///
/// `pub(crate)` so [`crate::migration`] can reuse the encoding for
/// key translation between fjall and other backends.
pub(crate) const KEY_PREFIX: u8 = 0x00;

/// Encodes a node_id string as a keyspace key (prefixed to avoid empty keys).
///
/// `pub(crate)` so [`crate::migration`] can reuse the encoding.
pub(crate) fn encode_key(node_id: &str) -> Vec<u8> {
    let mut key = vec![KEY_PREFIX];
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Decodes a keyspace key back to a node_id string.
///
/// Strips the `KEY_PREFIX` byte and interprets the remaining bytes as UTF-8.
/// Returns `None` if the key is empty, too short, or the prefix doesn't match.
///
/// Used by the migration tool to translate fjall keys to redb/persy node_ids.
pub(crate) fn decode_key(key: &[u8]) -> Option<String> {
    if key.len() < 1 || key[0] != KEY_PREFIX {
        return None;
    }
    std::str::from_utf8(&key[1..]).ok().map(|s| s.to_string())
}
```

**Verification:** `cargo check --features fjall`

---

### Task 2: Extend core types (`src/migration.rs`)

**Files:** `src/migration.rs`
**Effort:** M

#### 2a. Backend enum — add `Fjall`

```rust
pub enum Backend {
    Redb,
    Persy,
    /// fjall backend (LSM-tree, directory-based)
    Fjall,
}
```

Update `as_str()` to return `"fjall"`.
Update `parse_backend()` to accept `"fjall"`.
Update `InvalidBackend` error message to mention all three backends.

#### 2b. MigrateError — add `Fjall` variant

```rust
#[error("fjall error: {0}")]
Fjall(String),
```

Mirrors the `Persy(String)` pattern — fjall errors mapped to string via `Debug`.

#### 2c. Key translation functions (pure, unit-testable)

```rust
/// Translates a redb node_id (string key) into a fjall keyspace key.
///
/// This is a thin wrapper around `fjall_storage::encode_key` — the value
/// bytes are identical between redb and fjall (both bare `postcard(Children)`),
/// so only the key needs encoding.
pub fn redb_to_fjall_key(node_id: &str) -> Vec<u8> {
    crate::adapters::fjall_storage::encode_key(node_id)
}

/// Translates a fjall keyspace key back to a redb node_id string.
///
/// Strips the `0x00` prefix byte. Returns `Err` if the key is malformed.
pub fn fjall_key_to_node_id(key: &[u8]) -> Result<String, MigrateError> {
    crate::adapters::fjall_storage::decode_key(key)
        .ok_or_else(|| MigrateError::Fjall(format!("invalid fjall key: {:?}", key)))
}
```

**Note:** These are `pub` (not `pub(crate)`) to match the existing
`redb_to_persy_payload` / `persy_to_redb_record` visibility pattern.
They compile without any feature gate because they only reference
`fjall_storage` types — which are themselves behind `#[cfg(feature = "fjall")]`.

**Wait** — actually, `encode_key` and `decode_key` are behind
`#[cfg(feature = "fjall")]` because they live in `fjall_storage.rs`. So the
translation functions must also be `#[cfg(feature = "fjall")]`.

Following the existing pattern: `redb_to_persy_payload` and
`persy_to_redb_record` are NOT feature-gated (they use a local `NodeRecord`
copy). But for fjall, the key encoding is trivial enough that a local copy
is simpler and avoids the feature gate:

```rust
/// Fjall key prefix (mirrors `fjall_storage::KEY_PREFIX`).
const FJALL_KEY_PREFIX: u8 = 0x00;

/// Encodes a node_id as a fjall keyspace key.
pub fn redb_to_fjall_key(node_id: &str) -> Vec<u8> {
    let mut key = vec![FJALL_KEY_PREFIX];
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Decodes a fjall keyspace key back to a node_id string.
pub fn fjall_key_to_node_id(key: &[u8]) -> Result<String, MigrateError> {
    if key.len() < 1 || key[0] != FJALL_KEY_PREFIX {
        return Err(MigrateError::Fjall(format!("invalid fjall key: {:?}", key)));
    }
    std::str::from_utf8(&key[1..])
        .map(|s| s.to_string())
        .map_err(|e| MigrateError::Fjall(format!("fjall key UTF-8 decode: {:?}", e)))
}
```

This mirrors the `NodeRecord` pattern: a local copy of the wire format
constant so the translation functions compile without any feature gate.
The `io` module (which does actual I/O) IS feature-gated.

**Verification:** `cargo test --lib` (unit tests for key translation pass without features)

---

### Task 3: I/O functions (`src/migration.rs`, `io` module)

**Files:** `src/migration.rs`
**Effort:** L

The `io` module gate changes from `#[cfg(feature = "persy")]` to
`#[cfg(any(feature = "persy", feature = "fjall"))]`.

Four new functions, each gated by the relevant feature(s):

#### 3a. `migrate_redb_to_fjall` (`#[cfg(feature = "fjall")]`)

Read redb table → write fjall keyspace. Values pass through unchanged.

```
open redb source (read-only)
open fjall target database + keyspaces
iterate redb table:
    for each (node_id_str, children_bytes):
        key = redb_to_fjall_key(node_id_str)
        fjall_nodes.insert(key, children_bytes)  // value unchanged!
fjall persist(SyncAll)
```

**Key detail:** No `NodeRecord` wrapping needed — fjall stores bare
`postcard(Children)`, same as redb. This is the simplest migration path.

#### 3b. `migrate_fjall_to_redb` (`#[cfg(feature = "fjall")]`)

Read fjall keyspace → write redb table. Values pass through unchanged.

```
open fjall source (read-only)
open redb target database
iterate fjall keyspace:
    for each (fjall_key, children_bytes):
        node_id = fjall_key_to_node_id(fjall_key)?
        redb_table.insert(node_id, children_bytes)  // value unchanged!
redb commit
```

#### 3c. `migrate_fjall_to_persy` (`#[cfg(all(feature = "fjall", feature = "persy"))]`)

Read fjall keyspace → wrap in NodeRecord → write persy segment.

```
open fjall source (read-only)
materialize all records as (node_id, Children) pairs
open persy target
for each (node_id, children_bytes):
    payload = redb_to_persy_payload(node_id, children_bytes)  // reuse existing!
    persy_tx.insert(segment_id, payload)
persy commit
```

**DRY:** Reuses `redb_to_persy_payload()` because fjall's value format
is identical to redb's (bare `postcard(Children)`). The node_id extracted
from the fjall key serves as the `key` parameter.

#### 3d. `migrate_persy_to_fjall` (`#[cfg(all(feature = "fjall", feature = "persy"))]`)

Read persy segment → unwrap NodeRecord → write fjall keyspace.

```
open persy source (read-only)
scan persy segment:
    for each (PersyId, NodeRecord_bytes):
        (node_id, children_bytes) = persy_to_redb_record(payload)?  // reuse existing!
        key = redb_to_fjall_key(node_id)
        fjall_nodes.insert(key, children_bytes)
fjall persist(SyncAll)
```

**DRY:** Reuses `persy_to_redb_record()` to unwrap NodeRecord into
`(node_id, children_bytes)`, then encodes the node_id as a fjall key.

#### 3e. Update `migrate()` dispatcher

Add 4 new match arms:

```rust
match (opts.from, opts.to) {
    (Backend::Redb, Backend::Persy) => migrate_redb_to_persy(opts)?,
    (Backend::Persy, Backend::Redb) => migrate_persy_to_redb(opts)?,
    (Backend::Redb, Backend::Fjall) => migrate_redb_to_fjall(opts)?,
    (Backend::Fjall, Backend::Redb) => migrate_fjall_to_redb(opts)?,
    (Backend::Fjall, Backend::Persy) => migrate_fjall_to_persy(opts)?,
    (Backend::Persy, Backend::Fjall) => migrate_persy_to_fjall(opts)?,
    (Backend::Redb, Backend::Redb)
    | (Backend::Persy, Backend::Persy)
    | (Backend::Fjall, Backend::Fjall) => {
        unreachable!("from == to caught above")
    }
}
```

Each arm's function is gated by its feature, so the match is valid
under any feature combination. The `unreachable!` arms cover same-backend
pairs (already rejected by the `from == to` check at the top of `migrate()`).

**Verification:** `cargo check --features fjall`, `cargo check --features persy`,
`cargo check --features fjall,persy`

---

### Task 4: CLI wiring (`src/main.rs`, `src/cli.rs`)

**Files:** `src/main.rs`, `src/cli.rs`
**Effort:** S

#### 4a. `parse_backend` in main.rs

Add `"fjall" => Ok(Backend::Fjall)` arm.

#### 4b. Feature gate on `Command::Migrate`

Currently: `#[cfg(feature = "persy")]` for the migrate arm, with a fallback
message saying "requires persy feature."

Change to: `#[cfg(any(feature = "persy", feature = "fjall"))]` for the
migrate arm. Fallback message: "Migration requires the 'persy' or 'fjall'
feature."

#### 4c. CLI help text

Update `MigrateArgs` doc comments to mention "fjall" alongside "redb" and "persy".

**Verification:** `cargo run --features fjall -- migrate --help`

---

### Task 5: Unit tests (`src/migration.rs`, `tests` module)

**Files:** `src/migration.rs`
**Effort:** M

Add unit tests for the new key translation functions:

- `test_redb_to_fjall_key_adds_prefix` — empty string gets `[0x00]`, "abc" gets `[0x00, a, b, c]`
- `test_fjall_key_to_node_id_strips_prefix` — reverse of above
- `test_fjall_key_roundtrip` — encode then decode returns original
- `test_fjall_key_to_node_id_rejects_empty` — empty key → Err
- `test_fjall_key_to_node_id_rejects_bad_prefix` — `[0x01, ...]` → Err
- `test_fjall_key_to_node_id_rejects_invalid_utf8` — `[0x00, 0xFF, 0xFE]` → Err
- `test_backend_parse_accepts_fjall` — `parse_backend("fjall")` → `Ok(Backend::Fjall)`
- `test_backend_as_str_fjall` — `Backend::Fjall.as_str()` → `"fjall"`

These compile without any feature gate (key translation uses local constants).

**Verification:** `cargo test --lib` (no features needed)

---

### Task 6: E2E tests (`tests/migration_e2e.rs`)

**Files:** `tests/migration_e2e.rs`
**Effort:** L

Add helper functions and e2e tests for all 4 new migration paths.

#### 6a. Helper functions

- `write_fjall_records(path, count) -> Result<usize, String>` — write N records to a fjall directory
- `count_fjall_records(path) -> Result<usize, String>` — count records in a fjall directory

#### 6b. E2E tests (all gated behind `#[cfg(feature = "fjall")]`)

1. `e2e_redb_to_fjall_basic` — 100 records redb → fjall, verify count
2. `e2e_fjall_to_redb_basic` — 50 records fjall → redb, verify count
3. `e2e_fjall_to_redb_preserves_children` — nested 3-level graph fjall → redb
4. `e2e_redb_to_fjall_empty_dataset` — empty source → 0 records
5. `e2e_redb_to_fjall_dry_run` — dry run doesn't create target directory
6. `e2e_fjall_roundtrip` — redb → fjall → redb, verify data integrity
7. `e2e_fjall_to_persy_basic` — fjall → persy (requires both features)
8. `e2e_persy_to_fjall_basic` — persy → fjall (requires both features)

For tests 7-8, gate behind `#[cfg(all(feature = "fjall", feature = "persy"))]`.

#### 6c. Feature gate on the test file

Currently: `#![cfg(feature = "persy")]`

Change to: `#![cfg(any(feature = "persy", feature = "fjall"))]`

Individual tests are further gated by their specific feature requirements.

**Verification:** `cargo test --features fjall --test migration_e2e -- --test-threads=1`

---

### Task 7: Documentation (`README.md`, `docs/migrations/migration-guide.md`)

**Files:** `README.md`, `docs/migrations/migration-guide.md` (if exists)
**Effort:** S

#### 7a. README migration section

- Update `--from` / `--to` help text to include "fjall"
- Add fjall migration examples
- Update "Known Limitations" — remove "migration to/from fjall not supported"
- Add note about fjall directory-based paths vs redb/persy file paths

#### 7b. CLI help text

Update doc comments in `cli.rs` and `main.rs` module docs.

**Verification:** Visual review

---

### Task 8: Full test suite + commit

**Files:** N/A
**Effort:** S

```bash
# Native (no features)
cargo test

# With fjall only
cargo test --features fjall

# With persy only (existing, should still pass)
cargo test --features persy

# With both
cargo test --features fjall,persy

# Clippy
cargo clippy --features fjall,persy -- -D warnings

# Commit
git add -A
git commit -m "feat: add fjall support to migration tool

- Backend enum: add Fjall variant
- MigrateError: add Fjall(String) variant
- Key translation: redb_to_fjall_key / fjall_key_to_node_id (pure functions)
- I/O: 4 new migration functions (redb↔fjall, fjall↔persy)
  redb↔fjall: values pass through unchanged (same postcard(Children) format)
  fjall↔persy: reuses existing redb_to_persy_payload / persy_to_redb_record
- CLI: parse_backend accepts 'fjall', feature gate updated
- Tests: 8 unit tests + 8 e2e tests covering all 6 migration paths
- Docs: README migration section updated"
```

---

## 3. Risk Assessment

| Risk | Likelihood | Mitigation |
|---|---|---|
| fjall keyspace iteration API differs from expectation | Low | Verified `keyspace.iter()` returns `(Vec<u8>, Slice)` from fjall v3 docs |
| fjall directory target_path.exists() check fails on directories | None | `PathBuf::exists()` works for both files and directories |
| Feature gate combinations cause compile errors | Medium | Test all 4 combos: none, fjall, persy, both |
| Persy file lock conflicts with fjall tests | Low | Tests use unique paths; `--test-threads=1` already required for persy |
| Large datasets cause OOM in materialize-all approach | Low (same as existing) | Existing redb→persy already materializes all records. Future: streaming migration |

---

## 4. Success Criteria

- [ ] `cargo check` passes (no features)
- [ ] `cargo check --features fjall` passes
- [ ] `cargo check --features persy` passes
- [ ] `cargo check --features fjall,persy` passes
- [ ] `cargo clippy --features fjall,persy -- -D warnings` passes (zero warnings)
- [ ] `cargo test --lib` passes (unit tests for key translation, no features needed)
- [ ] `cargo test --features fjall --test migration_e2e -- --test-threads=1` passes
- [ ] `cargo test --features fjall,persy --test migration_e2e -- --test-threads=1` passes
- [ ] All existing migration tests still pass (no regressions)
- [ ] All code documented (module docs, function docs, inline comments)
- [ ] README migration section updated
- [ ] Committed and pushed

---

## 5. Design Principles

1. **DRY** — redb↔fjall needs no value translation (same format). fjall↔persy
   reuses existing `redb_to_persy_payload` / `persy_to_redb_record`.
2. **Pure translation functions** — key encoding/decoding is testable without
   any database feature enabled.
3. **Local wire-format copies** — `FJALL_KEY_PREFIX` in migration.rs mirrors
   `KEY_PREFIX` in fjall_storage.rs, same as `NodeRecord` mirrors the persy
   adapter's definition. Both compile without features.
4. **Feature-gated I/O only** — translation functions compile everywhere;
   I/O functions require their specific feature.
5. **Source materialized before target opened** — same pattern as existing
   redb→persy migration. Avoids holding reads open across writes.
6. **Path transparency** — `PathBuf` works for both files (redb/persy) and
   directories (fjall). The `--force` flag and `exists()` check handle both.
