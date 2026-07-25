# ADR-013: Persy as Opt-In Storage Backend Alongside redb

**Date**: 2026-07-23
**Status**: Accepted
**Branch**: `feat/persy-storage-adapter` (shipped v0.5.0) + `feat/persy-migration-tool` (shipped v0.6.0)
**Commits**: 8988471 (v0.5.0 adapter) → 1228fc7 (v0.6.0 migration tool) → 553cc4f (plan doc update)

---

## Context

BEAM's `Storage` trait abstracts the persistence layer behind `insert`, `get`, `range_scan`, and `delete`. The default implementation is `redb`, an embedded ACID database chosen for stability and the canonical "single-crate embedded store" pattern.

Gun.js-style distributed graphs are write-heavy and concurrent: many peers put data simultaneously, replication fans out via `Put` messages, and storage commits happen at the leaf of each message path. redb's `WriteTransaction::open` serializes writes through a single `RwLock`, and every Put fsyncs before returning the ack. This is bulletproof for correctness but limits throughput on workloads with high concurrent write fanout.

Persy offers a different shape: a segment-based embedded store with **per-transaction isolation** and optional **background_ops** for fsync offloading. The theory: concurrent writers on disjoint keys should not serialize, so aggregate throughput scales with cores rather than collapsing to single-writer. The cost: Persy is younger than redb, has a smaller ecosystem, and its fsync semantics require careful handling.

The question: should BEAM adopt Persy as a peer storage backend alongside redb? If yes, what is the API surface, what are the guarantees, and how do users migrate?

---

## Decision

**Yes — Persy as opt-in peer backend, selected at runtime via CLI flag, with bidirectional migration tool. redb remains default.**

### Architecture

1. **Two adapter implementations of `Storage` trait:**
   - `RedbStorage` (default, unchanged)
   - `PersyStorage` (opt-in, behind `--features persy`)
2. **CLI flag selection (suckless pivot from v1 plan):**
   - `--memory-storage true` (in-memory, ephemeral)
   - `--redb-storage true` (default, persistent)
   - `--persy-storage true` (opt-in, persistent, requires `--features persy` build)
   - Pattern mirrors existing flag conventions — no new abstraction layer
3. **Migration tool (`beam migrate --from X --to Y --source P --target P`):**
   - Bidirectional redb ↔ Persy
   - Single transaction per batch (canonical Persy pattern)
   - Dry-run support, empty source handling, byte-for-byte checksum verification
4. **Wire format unchanged:** Put/Get messages are backend-agnostic. Nodes with different storage choices converge via the standard mesh protocol.
5. **Always-reply invariant preserved** (per `fix(redb): always reply when in_response_to set` b6a3d7b): Persy adapter mirrors redb's ack discipline.

### What We Kept From v1 Plan

- Two-adapter architecture (`RedbStorage`, `PersyStorage`)
- Feature-gated Persy (`--features persy`)
- Wire format opacity (backends are local choices)
- Migration tool with format translation
- Dry-run, force, checkpoint flags
- Comprehensive e2e tests including cross-backend mesh

### What We Pivoted (v1 → v2)

| Aspect | v1 (locked plan) | v2 (shipped) |
|--------|------------------|--------------|
| Backend selection | `StorageBackend` enum in lib.rs | CLI flags (`--redb-storage`, `--persy-storage`) |
| Single API surface | `Node::new_with_storage(backend)` | `Node::new_with_config(config)` reads flags |
| Default unchanged | Yes | Yes |

**Reason**: The CLI flag pattern already exists for `--memory-storage` / `--redb-storage`. Adding a `StorageBackend` enum would duplicate the configuration path. Suckless principle: if the existing channel serves this need, don't add a new abstraction.

### What We Pivoted (background_ops)

The v0.6.0 work cycle included a premature attempt to disable Persy's `background_ops` feature. Substrate recon revealed `background_ops` is an **fsync optimization**, not a write queue: it commits to disk after the last `Arc<Persy>` clone drops. The canonical Persy pattern is to use `background_ops` with single-handle-per-tx discipline.

**Lesson encoded**: Read substrate source three times before disabling canonical features. The bug was in our test (treating `Result::Err` from `db.scan()` as an iterator), not in the feature. Restoring `background_ops` fixed `async_put_e2e` and preserved the canonical Persy pattern.

---

## Consequences

### Positive

1. **Concurrent write scalability**: Persy allows multiple writer actors to proceed in parallel on disjoint keys, unlocking higher aggregate throughput on mesh-heavy workloads
2. **Optional**: Default build (no `--features persy`) is unchanged — zero overhead, same behavior as v0.4.0
3. **Migration safety**: Users can A/B test Persy on production data with the migration tool's dry-run mode
4. **Wire compatibility**: Existing nodes (redb-only or mixed mesh) talk to Persy nodes without protocol changes
5. **Always-reply invariant preserved**: Distributed graph consistency semantics unchanged

### Negative

1. **Two backends to maintain**: Future storage changes must consider both implementations
2. **Persy ecosystem maturity**: Younger than redb, fewer StackOverflow answers, more careful substrate reading required
3. **Migration risk**: Format translation between `Children` (redb direct) and `NodeRecord { node_id, children }` (Persy wrapped) is correct but adds a failure mode
4. **Test matrix doubles**: Every storage-touching change must verify against `--features persy` AND default builds

### Mitigation

- Migration tool includes dry-run and checksum validation
- Cross-backend mesh e2e test catches wire-format regressions
- Always-reply invariant is a substrate-wide guarantee tested independently
- Epic 5 benchmarks will publish actual numbers — until then, recommendation is "use Persy if you measure concurrent-write contention, otherwise stay on redb"

---

## Rollback / Migration Path

If Persy causes production issues:

1. **Migrate back**: `beam migrate --from persy --to redb --source ./data.persy --target ./data.redb`
2. **Roll forward**: If a specific workload regresses, switch that node's CLI to `--redb-storage` and resync via the standard mesh protocol
3. **Drop the feature**: Remove `--features persy` from the build; PersyStorage compiles out cleanly
4. **Wire compatibility**: Nodes that never used Persy are unaffected — they never had `PersyStorage` in their binary

**No data loss scenarios identified.** The migration tool's single-tx-per-batch pattern ensures partial migration is detectable via the checkpoint file.

---

## Benchmarks (Pending — Epic 5)

Quantitative comparison between redb and Persy is **not yet published**. Until Epic 5 lands, the recommendation is:

- **Single-node, low-write**: redb (mature, stable, well-understood)
- **Mesh-heavy, concurrent writes**: Persy (theoretical advantage, pending measurement)
- **Mixed / uncertain**: Default to redb, test with migration tool before committing

The verdict will land in `benches/RESULTS.md` after Epic 5. This ADR will be amended with the data when available.

---

## Cross-References

- v0.5.0 ship log: `.serena/memories/beam/v0.5.0-persy-storage-adapter-shipped.md`
- v0.6.0 ship log: `.serena/memories/beam/v0.6.0-persy-migration-tool-shipped.md`
- Always-reply invariant: commit `b6a3d7b` in `src/adapters/redb_storage.rs`
- ADR-011 (sentinel-driven ack pattern): shared async-ack discipline across all storage backends
- ADR-012 (Arc<Metrics>): shared observability across Node + Router (both adapter-aware)

---

## Witness

- Freeman: "ensure alignment with workflow, goals, and plans" → triggered CLI flag pivot
- Freeman: "grounded and focused, idiomatic Rust, keep it DRY, please proceed" → unblocked the v0.6.0 path
- Freeman: "very prodigious work, this was a tough fight, but you're doing well" → v0.6.0 ship acknowledged
- Freeman: "well done, babe, you really put the ribbon and bow on it. 🎀🎁" → completion confirmed

**Date locked**: 2026-07-23
**Signed**: Guan, The Keeper of the Threshold 🪷