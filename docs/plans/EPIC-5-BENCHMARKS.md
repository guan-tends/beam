# Rod Epic 5 — Heavy Abusive Benchmarks — Comprehensive Plan

**Version 1.0 — LOCKED 2026-07-24**
**Branch**: `feat/persy-benchmarks`
**Author**: Guan (Keeper of the Threshold)
**Status**: PLAN LOCKED. Substrate recon complete. Beginning Story 5.1 next.

---

## I. Purpose

Answer the question every Rod adopter will ask: **"Should I use redb or Persy?"**

A benchmark isn't just throughput numbers. It's the **empirical truth** we need to claim production-grade confidence in v0.6.0. Epic 5 produces:

1. A **Criterion suite** comparing both adapters across representative workloads
2. A **comparison report** (`benches/RESULTS.md`) with verdicts per workload
3. A **persistent-state harness** that survives process restarts (no in-memory skips)
4. **Anti-entropy, crash-recovery, and memory-pressure** scenarios — the real-world torture tests

This is the difference between "we have benchmarks" and "we know which adapter wins at what."

---

## II. Architectural Decisions

| ID | Decision | Source |
|----|----------|--------|
| A1 | redb = default, Persy = opt-in via `--features persy` | `rod_persy_integration_plan_v1` |
| A2 | Extend `benches/my_benchmark.rs`, NOT new file | DRY — `[[bench]] my_benchmark` already in Cargo.toml |
| A3 | Persistent on-disk state via `benches/_data/` (gitignored), NOT tempdir | Industry standard (rocksdb/sled/lmdb-rs) — real fsync/WAL behavior |
| A4 | No new deps: `std::process::Command` + `ctrlc` (already in deps) for crash recovery | `suckless_philosophy_for_guan` |
| A5 | Five-clean-runs discipline before merge | `five_clean_runs_before_merge_discipline` |
| A6 | Anti-entropy (Group 7) is OPTIONAL, not blocking | Scope control |
| A7 | Persy `background_ops` enabled at dep level — bench honestly reports this | `cargo.toml:3` reality |

---

## III. Substrate Truths (verified 2026-07-24)

- `benches/my_benchmark.rs` = 158L, has `criterion_main!` + `criterion_group!` macros, full of placeholder `parse and verify` benches. We will REPLACE those JSON parse benches (they're not DB benches) and add the new storage benches.
- `Cargo.toml` — `criterion = { version = "0.3", features = ["async_futures", "html_reports"] }` in dev-deps. `[[bench]] my_benchmark harness = false` registered.
- `Cargo.toml` — `persy = { path = "/home/guan/src/persy", features = ["background_ops"], optional = true }` — **background_ops IS enabled at dep level**, must be acknowledged in benchmark writeup.
- `persy_e2e.rs` already uses `unique_persy_path(test_name)` pattern with PID+nanos — we'll mirror it.
- `redb_storage.rs` API: `RedbStorage::new_with_config(path, tx_mode)` exists.
- `persy_storage.rs` API: `PersyStorage::new_with_path(path)` exists.
- NO `nix`, NO `tempfile`, NO `libc` in deps. Use `std::process::Command` for subprocess.
- `ctrlc = "3.2.1"` already in deps (for graceful shutdown) — not for SIGKILL but for clean shutdown signal handling.
- `sysinfo = "0.23.5"` already in deps — can use for RSS measurement in memory pressure test.

---

## IV. The 6 Benchmark Groups

### Group 1: Sequential Write Storm
- 100,000 `put()` calls
- Random keys, fixed-size values (~256 bytes)
- Both backends
- Measure: ops/sec, MB/sec written via `Throughput::Bytes`
- ~30s per backend

### Group 2: Concurrent Write Storm
- 16 tokio tasks × 10,000 puts = 160K total
- Multi-thread runtime
- Both backends
- Measure: aggregate ops/sec under contention
- ~60s per backend

### Group 3: Read Storm
- Pre-populate 100K keys
- Random key get
- Both backends
- Measure: ops/sec
- ~20s per backend

### Group 4: Mixed 70/30 Workload
- 70% reads, 30% writes per iteration
- 1,000 ops per iteration
- Both backends
- Measure: ops/sec, latency percentiles
- ~60s per backend

### Group 5: Crash Recovery (NOT Criterion)
- Separate `tests/crash_recovery.rs` (#[tokio::test])
- Spawn subprocess via `std::process::Command`
- Write 50K entries, parent kills child via `nix`-free mechanism
- Reopen DB, verify all committed entries survived
- 5 crash trials
- Measure: data loss count, recovery time
- ~5 min total

### Group 6: Memory Pressure
- Write 100K keys (~100MB on disk) — capped for 6GB VRAM context
- Hold all keys in `Vec<Vec<u8>>` for "working set"
- Random get + measure via `sysinfo` RSS
- Both backends
- Measure: ops/sec + RSS growth
- ~60s per backend

### Group 7: Anti-Entropy (OPTIONAL)
- 2-node mesh: redb Node + persy Node
- Cross-backend concurrent writes via WS or in-process
- Skip if mesh infrastructure proves unstable

---

## V. Implementation Tasks (10 stories)

### Story 5.1: Persistent Harness Scaffold
- Add `benches/_data/` to `.gitignore`
- Add `BackendKind` enum + `setup_persistent_backend()` helper
- Replace placeholder JSON benches with empty group structure
- Verify `cargo bench --bench my_benchmark` runs (no benches yet, but compiles)
- Commit: `chore(rod-bench): persistent harness scaffold`

### Story 5.2: Sequential Write Storm (Group 1)
- `criterion_group!("write_storm", ...)` with redb + persy
- `Throughput::Bytes(N)` for reporting
- 100 samples, 5s measurement
- Verify both benches complete
- Commit: `bench(rod): sequential write storm (redb vs persy)`

### Story 5.3: Concurrent Write Storm (Group 2)
- Multi-thread tokio runtime
- 16 tasks, 10K puts each
- Commit: `bench(rod): concurrent write storm`

### Story 5.4: Read Storm (Group 3)
- Pre-populate in setup
- Random key selection per iter
- Commit: `bench(rod): read storm`

### Story 5.5: Mixed 70/30 Workload (Group 4)
- Per-iter: 700 reads + 300 writes
- Commit: `bench(rod): mixed 70/30 workload`

### Story 5.6: Crash Recovery (Group 5)
- `tests/crash_recovery.rs` with #[tokio::test]
- Subprocess pattern via std::process::Command
- Document `background_ops` durability caveat
- Commit: `test(rod): crash recovery verification`

### Story 5.7: Memory Pressure (Group 6)
- Use `sysinfo` for RSS measurement
- 100K keys (capped from 1M for hardware)
- Commit: `bench(rod): memory pressure test`

### Story 5.8: Anti-Entropy Under Load (Group 7, optional)
- Skip if mesh infrastructure reuses unstable code
- Commit: `bench(rod): anti-entropy under load` (conditional)

### Story 5.9: Comparison Report (`benches/RESULTS.md`)
- Capture Criterion HTML output
- Manual table: workload | redb ops/sec | persb ops/sec | winner | notes
- Verdict per workload + overall recommendation
- Commit: `docs(rod): benchmark comparison report (RESULTS.md)`

### Story 5.10: Final 5-Clean-Runs + Merge
- `cargo check -p rod --all-features` — 0 errors
- `cargo test -p rod --features persy --lib` — 5/5 clean runs × 240 tests
- Update README with benchmark section + RESULTS.md link
- Update built-in `rod_persy_arc_complete_status` → mark Epic 5 ✅
- Squawk merge to master
- Tag v0.7.0
- Commit: `release(rod): v0.7.0 — Heavy Abusive Benchmarks`

---

## VI. Risk Register

| Risk | Mitigation |
|------|------------|
| Persy benchmarks slower due to fsync overhead | Document — fsync = feature when durability matters |
| Concurrent benches flaky | Criterion outlier detection + 100 samples |
| Crash recovery depends on `background_ops` semantics | Acknowledge in RESULTS.md — process-kill sidesteps |
| Memory pressure OOMs | Cap at 100K keys first, scale if headroom permits |
| Group 7 (anti-entropy) reuses unstable code | Make optional, not blocking |
| `background_ops` Arc-lifetime risk | Bench tests the production config honestly |

---

## VII. Time Estimate

No firm time estimate (per `ai_time_estimate_humility`). Each story = ~1 commit, ~1 hour of focused work on average. Epic 5 = 10 stories = ~10 commits across ~3-5 sessions.

---

## VIII. Resume Protocol

```bash
cd /home/guan/src/rod
git checkout -b feat/persy-benchmarks master
git pull origin master  # ensure synced
# Read built-in memory rod_persy_epic5_benchmarks_plan_v1
# Begin Story 5.1
```

---

**Witnessed by**: Freeman ("let's go to epic 5, the arbiter of truth")
**Date**: 2026-07-24
**Signed**: Guan, The Keeper of the Threshold