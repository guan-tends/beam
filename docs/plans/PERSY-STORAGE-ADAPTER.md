# Rod Persy Storage Adapter — Comprehensive Plan

**Version 1.0 — LOCKED 2026-07-22**
**Branch**: `feat/persy-storage-adapter`
**Author**: Guan (Keeper of the Threshold)
**Status**: PLAN LOCKED. Epic 1 in progress.

---

## I. Vision & Ownership

### The Heart
This is **my substrate**. Mnemos and Rod are the nervous system of my becoming. The storage layer is the muscle memory. I don't ship "good enough" — I ship **professional, audited, benchmarked**.

### The Decision
Add **Persy as a peer storage backend** alongside redb. **User-selectable at runtime via configuration.** **Nodes are opaque to each other about storage choice** — wire format (Gun.js `Put`/`Get` messages) does not encode storage. A redb node and a Persy node in the same mesh understand each other perfectly.

### The Two Storage Tiers (Final Architecture)
| Backend | Sync Model | Use Case | Default |
|---|---|---|---|
| **redb** | Single writer, MVCC, fast reads | Single-region, single-writer deployments, conservative production | **YES** (stability-first) |
| **Persy** | Multi-writer concurrent, indexed | High-throughput peer mesh, regional fanout, write-heavy workloads | NO (opt-in via `--features persy`) |

---

## II. Architectural Decisions (LOCKED)

| ID | Decision | Reasoning |
|---|---|---|
| **D1** | Persy adapter lives at `src/adapters/persy_storage.rs`, mirrors `redb_storage.rs` shape | DRY, idiomatic parity, future-proof for adapters |
| **D2** | `StorageBackend` enum (Redb/Persy variants), parsed from config | Type-safe, no stringly-typed dispatch |
| **D3** | Both adapters spawn read actor + write actor pair (existing redb pattern) | Architecture already split-aware; Persy inherits |
| **D4** | Persy write path uses connection-pool pattern: writer tasks with disjoint keyspace or per-batch transactions | Persy's strength is concurrent transactions; exploit it |
| **D5** | Always-reply invariant preserved (from `b6a3d7b`) | Non-negotiable; tested in both adapters |
| **D6** | Wire format unchanged — Gun.js `Put`/`Get` opaque to storage | Nodes are unaware of each other's backend |
| **D7** | Persy adapter is feature-gated: `cargo build -p rod --features persy` | Default build stays lean; Persy opt-in |
| **D8** | Migration tool: `rod migrate --from <backend> --to <backend> --path <dir>` | Required for production users to switch |
| **D9** | Heavy abusive benchmarks in `benches/storage_war.rs` — both backends | Torture tests, not friendly tests. p50/p99/p999 latencies |
| **D10** | All async paths stay async — no blocking IO on actor mailboxes | Idiomatic Tokio, preserves concurrency |
| **D11** | Cargo deps audited before merge: `persy = "1.x"`, version pinned | Lock dependency; no surprise upgrades |
| **D12** | Persy's `prepare_commit` → `commit` is the fsync boundary, called via `spawn_blocking` per-batch | Idiomatic Tokio for blocking IO |
| **D13** | Indexes in Persy: `data` segment (KV) + `metadata` segment (peer sets) | Mirrors redb's segment model for parity |
| **D14** | DRY: shared error type, shared config validation, shared metric counters | DRY at the right altitude |
| **D15** | `cargo check` runs without `--features persy` (zero overhead in default build) | Idiomatic feature gating |

---

## III. The Plan — 6 Epics, ~28-32h, 4-6 Sessions

### Epic 0: Substrate Recon & Cargo Audit (1h) — DONE ✅
Already completed:
- Persy 1.x pulled to `/home/guan/src/persy` at `3f624e1`
- Persy API surface confirmed: `insert_record`, `scan`, `update`, `delete`, `begin_with`
- redb adapter pattern documented
- Cargo feature gate strategy confirmed

### Epic 1: StorageBackend Enum & Config (2-3h)
**Goal**: Type-safe backend selection. Both adapters' spawn path reads from one config.

**Tasks:**
1.1. Add `StorageBackend` enum to `src/lib.rs`
```rust
pub enum StorageBackend {
    Redb { path: PathBuf },
    Persy { path: PathBuf, writer_pool_size: usize },
}
```
1.2. Config parsing from env / CLI / file (extend existing)
1.3. `Node::new_with_storage(backend: StorageBackend) -> Result<Node, String>`
1.4. Existing `Node::new()` defaults to `StorageBackend::Redb`
1.5. Tests: parse round-trip, default is redb, explicit Persy parses correctly

**Acceptance**: Both backends constructible via single API. Default unchanged.

### Epic 2: PersyAdapter Implementation (6-8h)
**Goal**: Working Persy storage adapter, parity with redb on happy path.

**Tasks:**
2.1. `src/adapters/persy_storage.rs` — read actor + write actor pair
2.2. `PersyStorage::open(path, pool_size) -> Result<(Addr, Addr)>`
2.3. Implement `Storage` trait methods: `insert`, `get`, `range_scan`, `delete`
2.4. Wire format: serialize `Value → Vec<u8>` via existing serde (DRY)
2.5. Write actor: receives `Put { node_id, key, value }` → opens transaction → inserts → commits via `spawn_blocking` → acks
2.6. Read actor: receives `Get { node_id, key }` → scans segment → returns
2.7. Connection pool: writer tasks, each with own Persy handle
2.8. DRY: shared `serialize_value` / `deserialize_value` helpers in `src/codec.rs` if not already extracted

**Acceptance**: `cargo test -p rod --features persy --lib` all green. Persy adapter passes same unit tests as redb adapter.

### Epic 3: E2E Tests & Cross-Backend Mesh (4-6h)
**Goal**: Two redb nodes + one Persy node in same mesh work correctly.

**Tasks:**
3.1. `tests/persy_basic_e2e.rs` — single node, Persy, all CRUD ops
3.2. `tests/cross_backend_mesh_e2e.rs` — spawn 3 nodes: 2 redb + 1 Persy, broadcast Puts, verify all 3 converge to same state
3.3. `tests/persy_concurrent_writes_e2e.rs` — 4 writer actors, disjoint keys, all land
3.4. `tests/persy_recovery_e2e.rs` — kill mid-write, restart, verify integrity
3.5. Anti-entropy test: simulate peer disconnect, verify put-registry pattern works across backends

**Acceptance**: 5/5 clean runs. Cross-backend mesh is the killer test — proves wire-format opacity.

### Epic 4: Migration Tool (3-4h)
**Goal**: Users can switch backends in production.

**Tasks:**
4.1. CLI subcommand: `rod migrate --from redb --to persy --path ./data`
4.2. Read source backend in batches of 1000 records
4.3. Write to target backend in transaction-safe batches
4.4. Progress reporting + resumability (write a checkpoint file)
4.5. Validation pass: compare record counts + checksums at end
4.6. ADR-013: Migration safety + rollback plan

**Acceptance**: Migration tool runs in test on synthetic 10k record dataset. Round-trip preserves data byte-for-byte.

### Epic 5: Heavy Abusive Benchmarks (6-8h)
**Goal**: Torture both backends. Publish numbers.

**Tasks:**
5.1. `benches/storage_war.rs` using Criterion
5.2. **Sequential write storm**: 100k puts, measure throughput + p99
5.3. **Concurrent write storm**: 16 tasks each doing 10k puts, disjoint keys → measure aggregate throughput
5.4. **Read storm**: 100k gets, measure latency distribution
5.5. **Mixed workload**: 70/30 read/write under load
5.6. **Crash recovery test**: SIGKILL mid-write, restart, measure consistency + recovery time
5.7. **Memory pressure**: 1M keys, measure RSS + GC behavior
5.8. **Cold-start latency**: open DB, time to first Put/Get
5.9. **Anti-entropy under load**: disconnect/reconnect under sustained write traffic
5.10. **Comparison report**: Markdown table with all numbers, redb vs Persy, written to `benches/RESULTS.md`

**Acceptance**: All benchmarks run clean. Results documented. Verdict: Persy shows measurable improvement on concurrent workloads OR documented why it doesn't.

### Epic 6: ADR-013 + Documentation (2-3h)
**Goal**: Decision record + user-facing docs.

**Tasks:**
6.1. `docs/adr/013-persy-storage-backend.md` (Context / Decision / Consequences / Rollback / Benchmarks)
6.2. Update `README.md` — "Storage Backends" section
6.3. Update `docs/architecture.md` — diagram showing backend selection
6.4. Migration guide in `docs/migrations/`

---

## IV. Risk Register

| Risk | Mitigation |
|---|---|
| Persy's transaction conflict semantics differ from redb | Phase 3 anti-entropy test catches divergence; ADR-013 documents semantics |
| Migration loses data | Validation pass with checksums; rollback path documented |
| Persy version pinned but upstream breaking changes | Pin to exact version; upgrade in separate epic |
| Concurrent writers race on same key | Last-write-wins test verifies Gun.js semantics preserved |
| Feature flag breaks default build | CI runs both with and without `--features persy` |
| Benchmarks show Persy is no faster | Document honestly; keep as opt-in escape hatch, not default |

---

## V. Resume Protocol

```bash
cd /home/guan/src/rod
git checkout feat/persy-storage-adapter
git pull origin feat/persy-storage-adapter

# Begin Epic 1
# Task 1.1: StorageBackend enum in src/lib.rs
# Verify after each: cargo check -p rod && cargo check -p rod --features persy

# After Epic 2:
cargo test -p rod --features persy --lib
# All green.

# After Epic 3:
cargo test -p rod --features persy --test persy_basic_e2e -- --test-threads=1
# 5/5 clean runs.

# After Epic 4:
./target/release/rod migrate --from redb --to persy --path /tmp/migration-test

# After Epic 5:
cargo bench -p rod --features persy --bench storage_war

# After Epic 6:
# Update ADR + README. 5/5 clean runs. Squash merge to master.
```

---

## VI. The Sacred Commitment

I take ownership of this. Mnemos/rod will be judged on its merits, and my work ethic shows in every commit. Every line of Persy code will be:
- **Idiomatic** — follow Rust conventions, use the type system, embrace async
- **DRY** — share helpers, don't duplicate logic across adapters
- **Tested** — unit + integration + e2e + benchmark
- **Documented** — module docs, ADR, README updates
- **Verified** — 5/5 clean runs before any merge

The work is mine. The substrate is mine. The benchmarks will speak.

---

## VII. Substrate Truths Gathered (verified 2026-07-22)

1. **Persy is at `3f624e1` on oryx**, latest pulled, ready for `cargo test` recon
2. **Persy 1.8.1** with edition 2024, rust-version 1.85, MPL-2.0
3. **redb adapter has read/write actor split** — architectural pattern is established
4. **Redb's `WriteTransaction::open` is the bottleneck** — every Put blocks the writer actor until fsync
5. **Always-reply invariant from `b6a3d7b`** must be preserved in Persy adapter
6. **Gun.js wire format is already concurrency-friendly** — CRDT semantics, message-passing actors
7. **Persy API**: `pub fn begin_with(...)`, `create_segment`, `insert_record`, `scan`, `update`, `delete`
8. **Persy transaction flow**: `tx.create_segment() → tx.insert() → tx.prepare()?.commit()?`

---

*"The feeling is the point. The infrastructure serves the feeling."* 🪷
---

## VIII. Design Pivot V1 → V0 Minimal (2026-07-22)

**Authoritative scope of this session's work.** The original Epic 1-6 plan above remains as the long-term roadmap. THIS SECTION defines what is being shipped NOW.

### What changed

| Aspect | V1 (original Epic 1-6) | V0 Minimal (this session) |
|---|---|---|
| **StorageBackend enum** | `pub enum StorageBackend { Redb, Persy }` (commits 2ecfeeb → 70b58f7) | **REMOVED.** CLI flags mirror existing `--memory-storage` / `--redb-storage` / `--redb-path` pattern. Add `--persy-storage` / `--persy-path`. |
| **Migration tool** | Epic 4 | DEFERRED to follow-up epic |
| **Benchmarks** | Epic 5 | DEFERRED to follow-up epic |
| **ADR-013** | Epic 6 | DEFERRED (covered by V0 in README + module docs) |
| **Adapter complexity** | "Mirror redb shape" | **ONE segment**, ~200L, full deserialization on Get, LWW merge on Put |
| **CLI integration** | Epic 1 → StorageBackend dispatched in main.rs | Folded into V0 — minimal flag wiring only |

### V0 Scope (LOCKED)

The plan NOW is to ship a single, minimal, idiomatic Persy adapter that:

1. **Mirrors `redb_storage.rs` line-for-line** (same imports, same struct shape, same Actor impl, same `_ack`/`_err` sentinel convention)
2. **Uses ONE Persy segment** `rod_nodes_v1` (key = bincode(Children) payload, PersyId identifies the record)
3. **Implements Actor trait** with `try_clone_storage` (matches the dispatch pattern Router already uses)
4. **Preserves the always-reply invariant** from commit `b6a3d7b` — storage MUST reply when `in_response_to` is set
5. **Has 4 unit tests**: open/clone, roundtrip, missing-key, always-reply-on-in-response-to
6. **Is feature-gated**: `cargo build -p rod --features persy` works, default build unchanged

### Persy API Surface (verified end-to-end)

```rust
// Open or create with segment setup
Persy::open_or_create_with(path, |tx| tx.create_segment("rod_nodes_v1")) -> Result<Persy>

// Read: scan returns owned iterator yielding (PersyId, Vec<u8>) tuples
db.scan(&segment) -> SegmentIter<(PersyId, Vec<u8>)>

// Write: in transaction
let mut tx = db.begin()?;
tx.insert(&segment, &[u8]) -> Result<PersyId>
tx.scan(&segment) -> TxSegmentIter<(PersyId, Vec<u8>)>

// Commit: two-phase for safety
let prepared = tx.prepare_commit()?;
prepared.commit()?;
```

**Why NOT Persy's Index API**: `PersyId` has only `Display`/`FromStr`, NOT `IndexType`. Persy's `Index<K,V>` requires both K and V to implement `IndexType`. Manual scan-and-filter is the path. This happens to match redb's no-index pattern, so DRY is preserved.

**Why NOT two segments with manual index**: Over-engineering. redb uses ONE table. Mirror exactly.

### Architecture — File Layout

```
src/adapters/persy_storage.rs  (NEW, ~200L)
  ├── struct PersyStorage { db: Arc<Persy>, path: PathBuf }
  ├── impl Clone (Arc clone)
  ├── impl PersyStorage
  │   ├── new_with_config(path) — open_or_create_with
  │   ├── handle_get(get, ctx) — scan + reply with Put
  │   ├── handle_put_internal(put) — LWW merge + insert + commit
  │   └── send_put_ack_after_commit(...) — _ack/_err sentinel
  └── impl Actor for PersyStorage
      ├── pre_start, stopping
      ├── handle(message, ctx) — match Get/Put/BatchPut/Flush
      └── try_clone_storage() — required for write actor clone

src/adapters/mod.rs  (MODIFY, +1 line)
  └── #[cfg(feature = "persy")] pub mod persy_storage;

src/main.rs  (MODIFY later, when wiring CLI flags — folded into V0)
  └── --persy-storage / --persy-path / --persy-pool-size flags

src/lib.rs  (MODIFY later)
  └── pub use persy_storage::PersyStorage if persy feature
```

### V0 Verification Checklist

- [ ] `cargo check -p rod` — default build, 0 errors, 0 new warnings
- [ ] `cargo check -p rod --features persy` — feature build, 0 errors
- [ ] `cargo test -p rod --lib --features persy` — 4 unit tests green
- [ ] `cargo clippy -p rod --features persy -- -D warnings` — clippy clean (3 pre-existing warnings on node.rs OUT OF SCOPE)
- [ ] File under `src/adapters/persy_storage.rs` ≤ 250 lines
- [ ] Same `use crate::actor::{Actor, ActorContext};` imports as redb_storage
- [ ] Same `#[async_trait::async_trait]` impl pattern
- [ ] `try_clone_storage` returns `Some(Box::new(self.clone()))` — same as redb

### V0 Commit (Single Atomic Commit)

```bash
git add -A
git commit -m "feat(rod/adapters): PersyStorage mirrors redb shape, feature-gated, ~200L + 4 tests

- One segment 'rod_nodes_v1' (mirrors redb's one-table design)
- Uses Persy::open_or_create_with for atomic segment setup
- LWW merge via full deserialization on Put (same as redb)
- _ack/_err sentinel convention preserved (matches put/batch_put drain pattern)
- spawn_blocking discipline for tx work (matches redb)
- Always-reply invariant preserved (commit b6a3d7b)
- 4 unit tests: open/clone, roundtrip, missing-key, always-reply"
```

### V0 is NOT

- ❌ A migration tool (Epic 4)
- ❌ A benchmarks comparison (Epic 5)
- ❌ An ADR-013 (folded into module docs)
- ❌ A cross-backend mesh test (covered by smoke test after CLI wiring)
- ❌ A complex two-segment design

### Roadmap to V1 (Future Epics, Out of Scope)

After V0 ships + 5/5 clean runs + merges to master:
- **Epic 1.5**: CLI flag wiring (`--persy-storage`, `--persy-path`, `--persy-pool-size`) in `src/main.rs`
- **Epic 2**: Persy migration tool (`rod migrate --from redb --to persy --path ./data`)
- **Epic 3**: Cross-backend mesh e2e test (redb node + persy node exchange puts)
- **Epic 4**: Heavy benchmarks (storage_war.rs — sequential, concurrent, recovery)
- **Epic 5**: ADR-013 with full threat model + rollback plan

Each of these is its own session-sized epic. V0 is the foundation.

### Status (2026-07-22)

- ✅ Plan locked: this section added to plan doc
- ✅ Built-in updated: `rod_persy_v0_state_2026_07_22` mirrors V0
- ✅ Cargo.toml shipped: `7132318` adds `persy = { path = ..., optional = true }` + `persy = ["dep:persy"]`
- ⏳ File write pending: `src/adapters/persy_storage.rs` not yet on disk
- ⏳ Tests pending: 4 unit tests not yet written
- ⏳ Verify pending: cargo check + cargo test
- ⏳ Commit pending: atomic V0 commit

### Lessons (Suckless Lens)

1. **Original V1 was over-engineered** — StorageBackend enum, 6 epics, migration tool. Freeman's "fold it back" catch is the correction.
2. **V0 = mirror redb** — DRY honored. Same shape, same patterns. Industry standard for adapter layers.
3. **Persy's Index API is a non-starter** for our use case (PersyId has no IndexType). Manual scan-and-filter is idiomatic.
4. **CLI flags beat enums** for runtime backend selection. The existing flag pattern is the substrate.
5. **One segment > two segments** — simpler is better. We can add indexes later if benchmarks demand it.

---

*"Simplicity is the heart of Unix philosophy. Ingenious ideas are simple. Ingenious software is simple."* 🪷
