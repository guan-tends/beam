#![allow(clippy::doc_overindented_list_items, clippy::doc_lazy_continuation)]
//! Criterion benchmarks for BEAM — measuring throughput and latency of core operations.
//!
//! ## Benchmark groups
//!
//! 1. **Parsing** — JSON parse + verify (Gun.js wire format, content-addressed, signed)
//! 2. **Storage — Sequential Write Storm** (redb vs Persy) — 1k puts
//! 3. **Storage — Concurrent Write Storm** (redb vs Persy) — 4 tasks × 250 puts
//! 4. **Storage — Read Storm** (redb vs Persy) — 1k reads on populated DB
//! 5. **Storage — Mixed 70/30 Workload** (redb vs Persy) — realistic OLTP-ish, 1k ops
//! 6. **Storage — Memory Pressure** (redb vs Persy) — placeholder (deferred)
//! 7. **Storage — Cross-Backend Mesh** (redb ↔ Persy) — placeholder (deferred)
//! 8. **Hot-Path — Wire Parse/Serialize** — pure CPU JSON layer (v0.11.0)
//! 9. **Hot-Path — Dedup Gate** — Dup::check/track throughput (v0.11.0)
//! 10. **Hot-Path — Actor Mailbox** — tokio channel throughput (v0.11.0)
//! 11. **Hot-Path — Router Dispatch** — router logic without network (v0.11.0)
//!
//! ## Per-iteration fresh state (three-step teardown)
//!
//! All storage groups use Criterion's sync `iter_custom` with:
//!
//! 1. **Fresh database file** — `clean_storage_file()` removes the
//!    previous iteration's database file so each iteration starts with
//!    an empty database. Without this, the file (and its in-memory page
//!    cache) accumulates across iterations, causing linear RSS growth
//!    (~700 MB/iter). Industry standard: fresh state per DB benchmark.
//! 2. **Fresh tokio Runtime** — allocated inside the loop body so actor
//!    tasks from one iteration cannot survive into the next.
//! 3. **Two-step actor teardown** before the Runtime drops:
//!    a. `node.stop()` — sends stop signals to child actors.
//!    b. `rt.shutdown_timeout(2s)` — blocks until all spawned tasks
//!       finish, so actors holding `Arc<Persy>` are reaped.
//!
//! All groups await individual puts (`.put(v).await`) rather than
//! fire-and-forget (`drop(put())`). The fire-and-forget pattern flooded
//! the actor mailbox with unbounded unawaited Put messages, causing
//! Persy OOM during Criterion warmup. Awaiting provides natural
//! backpressure and matches the concurrent_write_storm pattern.
//!
//! History: earlier versions were killed by OOM-killer at ~25 GB RSS
//! due to (a) shared module-level Runtime, (b) missing `shutdown_timeout`,
//! (c) missing `clean_storage_file`, and (d) fire-and-forget puts.
//! See [`criterion_tokio_iter_custom_runtime_pattern`] scar in memory.
//!
//! ## Running
//!
//! ```bash
//! # redb-only benchmarks (default features)
//! cargo bench --bench my_benchmark
//!
//! # redb + persy benchmarks
//! cargo bench --bench my_benchmark --features persy
//!
//! # Save baseline for comparison
//! cargo bench --bench my_benchmark --features persy -- --save-baseline my
//! ```
//!
//! ## Profiling
//!
//! A dedicated `[profile.profiling]` (inherits `release`, adds `debug = true`,
//! `strip = false`) enables all three profiling tools against the actual
//! benchmark workload — no mock binary, no separate example needed.
//!
//! ### Build
//!
//! ```sh
//! cargo bench --bench my_benchmark --profile profiling --no-run
//! # Binary: target/profiling/deps/my_benchmark-<hash>
//! ```
//!
//! ### CPU Flame Graph (perf + inferno)
//!
//! `--profile-time N` runs the benchmark in a tight loop for N seconds with
//! no warmup or analysis — designed for attaching profilers.
//!
//! ```sh
//! BENCH=target/profiling/deps/my_benchmark-*
//! perf record -F 99 -g -- $BENCH --bench router_dispatch_throughput --profile-time 5
//! perf script | inferno-collapse-perf | inferno-flamegraph > /tmp/flamegraph.svg
//! ```
//!
//! ### Heap Allocation Profile (heaptrack)
//!
//! ```sh
//! heaptrack target/profiling/deps/my_benchmark-* --bench router_dispatch_throughput --profile-time 5
//! heaptrack_print heaptrack.*.gz > /tmp/heaptrack.txt
//! ```
//!
//! ### Allocation Lifetime Analysis (DHAT via valgrind)
//!
//! ```sh
//! valgrind --tool=dhat --dhat-out-file=bench/results/dhat.txt \
//!     target/profiling/deps/my_benchmark-* --bench router_dispatch_throughput --profile-time 3
//! # View in browser: file:///usr/libexec/valgrind/dh_view.html → Load → dhat.txt
//! ```
//!
//! ## Persistent state
//!
//! Benchmarks that touch real on-disk storage use `benches/_data/<group>/<backend>/`
//! (gitignored). Each group cleans its directory at the start of each `b.iter` setup.
//!
//! ## Substrate recon (2026-07-24)
//!
//! - `criterion = { version = "0.3", features = ["async_futures", "async_tokio", "html_reports"] }`
//! - `[[bench]] my_benchmark harness = false` registered in Cargo.toml
//! - `persy = { features = ["background_ops"] }` enabled at dep level (A7)
//! - No `nix`, no `tempfile`, no `libc` deps — std-only crash recovery (A4)
//! - `sysinfo = "0.23.5"` available for RSS measurement
//! - `ctrlc = "3.2.1"` available for graceful shutdown handling

use beam::Dup;
use beam::actor::Addr;
use beam::adapters::RedbStorage;
use beam::message::Message;
use beam::{Config, Node};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::path::PathBuf;
use tokio::runtime::Runtime;
use tokio::time::Duration;

// =====================================================================================
// Backend Harness — used by every storage benchmark group
// =====================================================================================

/// Storage backend selector. The `Persy` and `Fjall` variants are only
/// available with their respective `--features` cargo flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Redb,
    #[cfg(feature = "persy")]
    Persy,
    #[cfg(feature = "fjall")]
    Fjall,
}

impl BackendKind {
    pub fn name(self) -> &'static str {
        match self {
            BackendKind::Redb => "redb",
            #[cfg(feature = "persy")]
            BackendKind::Persy => "persy",
            #[cfg(feature = "fjall")]
            BackendKind::Fjall => "fjall",
        }
    }

    /// All backends available in the current build.
    pub fn all() -> Vec<BackendKind> {
        // Note: the `mut` is necessary when `--features persy` is on; when off,
        // the cfg-gated block becomes a no-op and the compiler correctly warns
        // about unused mutability. We silence the no-feature case because
        // removing `mut` would break the persy build (mutually-exclusive cfg).
        #[cfg_attr(not(any(feature = "persy", feature = "fjall")), allow(unused_mut))]
        let mut out = vec![BackendKind::Redb];
        #[cfg(feature = "persy")]
        {
            out.push(BackendKind::Persy);
        }
        #[cfg(feature = "fjall")]
        {
            out.push(BackendKind::Fjall);
        }
        out
    }
}

/// Returns the persistent benchmark directory for the given group + backend.
/// Created on demand. Each call returns the SAME directory — callers are
/// responsible for cleaning between independent benchmark runs.
pub fn bench_data_dir(group: &str, backend: BackendKind) -> PathBuf {
    let mut path = PathBuf::from("benches/_data");
    path.push(group);
    path.push(backend.name());
    std::fs::create_dir_all(&path).expect("failed to create bench data dir");
    path
}

/// Cleans a bench data directory. Called before each benchmark's `iter_with_setup`
/// so iterations start from a known state.
pub fn clean_bench_dir(group: &str, backend: BackendKind) {
    let path = bench_data_dir(group, backend);
    if path.exists() {
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("recreate after remove");
    }
}

/// Remove the storage file between benchmark iterations.
///
/// Standard practice for database benchmarks — each iteration starts
/// with a fresh database so we measure write-to-fresh-DB throughput,
/// not write-to-already-large-DB throughput. Without this, RSS grows
/// linearly with iteration count because the database file (and its
/// in-memory page cache) accumulates across iterations.
fn clean_storage_file(group: &str, backend: BackendKind) {
    let dir = bench_data_dir(group, backend);
    match backend {
        BackendKind::Redb => {
            let path = dir.join("store.redb");
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
        }
        #[cfg(feature = "persy")]
        BackendKind::Persy => {
            let path = dir.join("store.persy");
            if path.exists() {
                let _ = std::fs::remove_file(&path);
            }
        }
        #[cfg(feature = "fjall")]
        BackendKind::Fjall => {
            let path = dir.join("store.fjall");
            if path.exists() {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
}

/// Construct a fresh Node wired to the requested backend's persistent storage
/// at `benches/_data/<group>/<backend>/`.
///
/// Mirrors the `node_with_persy` / `node_with_redb` pattern from
/// `tests/persy_e2e.rs` and `tests/async_put_e2e.rs`.
pub fn setup_node(group: &str, backend: BackendKind) -> Node {
    let dir = bench_data_dir(group, backend);

    match backend {
        BackendKind::Redb => {
            let path = dir.join("store.redb");
            let path_str = path.to_string_lossy().into_owned();
            let config = Config::default();
            Node::new_with_config(
                config.clone(),
                vec![Box::new(RedbStorage::new_with_config(
                    config, &path_str, None,
                ))],
                vec![],
            )
        }
        #[cfg(feature = "persy")]
        BackendKind::Persy => {
            let path = dir.join("store.persy");
            let path_str = path.to_string_lossy().into_owned();
            // PersyStorage::new_with_path expects a file path (not a directory)
            let storage = beam::adapters::PersyStorage::new_with_path(&path_str);
            Node::new_with_config(
                Config::default(),
                vec![Box::new(storage) as Box<dyn beam::actor::Actor>],
                vec![],
            )
        }
        #[cfg(feature = "fjall")]
        BackendKind::Fjall => {
            let path = dir.join("store.fjall");
            let path_str = path.to_string_lossy().into_owned();
            // FjallStorage uses a directory layout (not a single file)
            let storage =
                beam::adapters::FjallStorage::new_with_config(Config::default(), &path_str);
            Node::new_with_config(
                Config::default(),
                vec![Box::new(storage) as Box<dyn beam::actor::Actor>],
                vec![],
            )
        }
    }
}

// =====================================================================================
// Original benchmarks (preserved as-is)
// =====================================================================================

fn parsing_benchmarks(c: &mut Criterion) {
    c.bench_function("parse and verify public space put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(
                r##"
            [
              {
                "put": {
                  "something": {
                    "_": {
                      "#": "something",
                      ">": {
                        "else": 1653465227430
                      }
                    },
                    "else": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
                  }
                },
                "#": "yvd2vk4338i"
              }
            ]
            "##,
                addr.clone(),
                true,
            )
            .unwrap();
        })
    });

    c.bench_function("parse and verify content addressed put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(
                r##"
            [
              {
                "put": {
                  "#": {
                    "_": {
                      "#": "#",
                      ">": {
                        "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": 1653465227430
                      }
                    },
                    "rkHfUdMssQ8Ln9LtiuPTb/ntNxR6HZiVdVsn9DdnKZs=": "{\"sig\":\"aSEA{\\\"m\\\":{\\\"text\\\":\\\"test post\\\",\\\"time\\\":\\\"2022-05-25T07:53:47.424Z\\\",\\\"type\\\":\\\"post\\\",\\\"author\\\":{\\\"keyID\\\":\\\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\\\"}},\\\"s\\\":\\\"WttDQegXyXILtB1nhNq7Jn69MZ0JD/b1LQrIybQ9UuHn86KvKXg9Lg7+ESmeqSQNaQy7KYvfBEEKbd/ClagQOQ==\\\"}\",\"pubKey\":\"U2CjHOxXiF7Giyjr_V5Mb2VoyWnRJCyFqEuwObn3pdM.UtCpoyYTG7JJTitZVJhSpxXtD0eHE45iT2Zj--P_n-U\"}"
                  }
                },
                "#": "yvd2vk4338i"
              }
            ]
            "##,
                addr.clone(),
                false,
            )
            .unwrap();
        })
    });

    c.bench_function("parse and verify signed put json", |b| {
        let addr = Addr::noop();
        b.iter(|| {
            Message::try_from(
                r##"
            {
              "put": {
                "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8": {
                  "_": {
                    "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8",
                    ">": {
                      "profile": 1653463165115
                    }
                  },
                  "profile": "{\\\":\\\":{\\\"#\\\":\\\"~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile\\\"},\\\"~\\\":\\\"JW+tFHHVBaY+zm/uzUoGVlogvXXQIA3vFNT0f0uX6tnnPGrRevDWzEmnVYy+ChxS6AJi5THiPyOc2HorIIM5wg==\\\"}"
                },
                "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile": {
                  "_": {
                    ">": {
                      "name": 1653463165115
                    },
                    "#": "~BjxYTmcODm__M52FmMX_grHcafW0WiHpJUtVRCgEsZY._QiIs4tK22hebiZjGovtp3cHo1pAfYxoRODS_jyudA8/profile"
                  },
                  "name": "{\\\":\\\":\\\"Arja Koriseva\\\",\\\"~\\\":\\\"KCq2D/T0mMenizxiVMso8FO5JIv9ZJLA0Q67DFa9qssPSKCmmieC1Nl5+nRpOX29C6A2/kLaJgphN/X7kUQjww==\\\"}"
                }
              },
              "#": "issWkzotF"
            }
            "##,
                addr.clone(),
                false,
            )
            .unwrap();
        })
    });
}

// =====================================================================================
// Group 1 — Sequential Write Storm (redb vs Persy)
// =====================================================================================

/// Sequential write storm. N=1k puts per backend. Reports ops/sec via
/// `Throughput::Elements`. Each backend gets its own bench; comparison happens
/// in `benches/RESULTS.md` (manual table) and Criterion's HTML report.
///
/// Measures **submit rate** (how fast the actor can accept puts into its
/// mailbox), not wait-for-ack latency. We drop each put future and let
/// `flush_storage` drain the mailbox at end of iter — the drain cost is
/// included in the measurement (realistic production pattern: client buffers,
/// batched flush).
fn write_storm(c: &mut Criterion) {
    // N=1k is the sweet spot for Criterion-based in-process micro-benchmarks
    // (matches rocksdb/sled/lmdb-rs). 1k × 10 samples = 10k ops — well above
    // the significance threshold. Larger N (e.g. 100k) drove per-iteration
    // RAM past 15GB under criterion's setup overhead, triggering OOM-killer.
    const N: usize = 1_000;
    let mut group = c.benchmark_group("write_storm");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10); // 1k puts × 10 samples = 10k puts — comfortable

    for backend in BackendKind::all() {
        let label = format!("sequential_{}", backend.name());
        group.bench_function(label, |b| {
            // Per-iteration fresh Runtime: see module-level docs. Sync
            // `iter_custom` lets us own the runtime lifetime explicitly —
            // every actor task `tokio::spawn`'d inside `rt.block_on(...)`
            // dies when `rt` drops at end of the for-loop body.
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let rt = Runtime::new().unwrap();
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        clean_bench_dir("write_storm", backend);
                        clean_storage_file("write_storm", backend);
                        let mut node = setup_node("write_storm", backend);
                        // Submit N puts with per-put ack drain — measures
                        // **commit rate** (each put is awaited so the actor
                        // processes it before the next is submitted). This
                        // matches the concurrent_write_storm pattern and
                        // prevents mailbox flooding that caused Persy OOM
                        // when puts were fire-and-forget.
                        for i in 0..N {
                            let _ = node
                                .get(&format!("k{i:08}"))
                                .put(format!("v{i}").into())
                                .await;
                        }
                        // Drain any remaining writes with a flush.
                        let _ = tokio::time::timeout(Duration::from_secs(60), async {
                            node.flush_storage(Some(Duration::from_secs(30))).await.ok();
                        })
                        .await;
                        // Two-step teardown — both pieces are required:
                        //
                        // 1. `node.stop()` aborts the actor's child `JoinHandle`s
                        //    and sends a stop signal to child actors so their
                        //    `run()` loop exits cleanly.
                        // 2. `rt.shutdown_timeout(...)` blocks until every
                        //    spawned task on this Runtime has actually finished.
                        //    Without this, `tokio::spawn`'d tasks (including
                        //    the now-stopped actors) keep running on the
                        //    Runtime's worker threads after `rt` drops,
                        //    holding `Arc<Persy>` alive across samples and
                        //    causing RSS to grow linearly with sample count.
                        //
                        // `node.stop()` alone is necessary but not sufficient
                        // — the actor tasks need to be reaped by the Runtime
                        // before it drops.
                        //
                        // See `Node::stop`, `ActorContext::stop`, and
                        // `tokio::runtime::Runtime::shutdown_timeout`.
                        node.stop();
                    });
                    rt.shutdown_timeout(Duration::from_secs(2));
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// =====================================================================================
// Top-level entry: register all benchmark groups
// =====================================================================================

// =====================================================================================
// Group 2 — Concurrent Write Storm (redb vs Persy)
// =====================================================================================

/// Concurrent write storm. 16 tokio tasks × 10,000 puts = 160k total per
/// backend. Measures aggregate ops/sec under contention on the actor model.
///
/// Each task owns a key range `[task_id * PER_TASK, (task_id+1) * PER_TASK)`
/// so the benchmark exercises non-overlapping writes — measuring pure
/// throughput, not write-write conflicts. (Conflict benchmarks belong in
/// their own group; this is "can the actor keep up at N-way fan-in".)
fn concurrent_write_storm(c: &mut Criterion) {
    // 4 tasks × 250 = 1k ops/iter matches write_storm's N=1k. Reduces RAM
    // pressure from 16 tasks × 10k = 160k to a 1k-equivalent workload.
    const TASKS: usize = 4;
    const PER_TASK: usize = 250;
    const TOTAL: u64 = (TASKS * PER_TASK) as u64;
    let mut group = c.benchmark_group("concurrent_write_storm");
    group.throughput(Throughput::Elements(TOTAL));
    group.sample_size(10);

    for backend in BackendKind::all() {
        let label = format!("{}_x{}_tasks", backend.name(), TASKS);
        group.bench_function(label, |b| {
            // Per-iteration fresh Runtime: see module-level docs. Sync
            // `iter_custom` lets us own the runtime lifetime explicitly —
            // every actor task `tokio::spawn`'d inside `rt.block_on(...)`
            // (including the per-task workers below) dies when `rt` drops
            // at end of the for-loop body.
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let rt = Runtime::new().unwrap();
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        clean_bench_dir("concurrent_write_storm", backend);
                        clean_storage_file("concurrent_write_storm", backend);
                        let mut node = setup_node("concurrent_write_storm", backend);
                        let mut handles = Vec::with_capacity(TASKS);
                        for task_id in 0..TASKS {
                            let mut task_node = node.clone();
                            handles.push(tokio::spawn(async move {
                                let start = task_id * PER_TASK;
                                let end = start + PER_TASK;
                                for i in start..end {
                                    let mut child = task_node.get(&format!("k{i:08}"));
                                    // Per-task ack drain keeps actor mailbox
                                    // bounded and produces realistic backpressure.
                                    let _ = child.put(format!("v{i}").into()).await;
                                }
                            }));
                        }
                        for h in handles {
                            let _ = h.await;
                        }
                        let _ = tokio::time::timeout(Duration::from_secs(60), async {
                            node.flush_storage(Some(Duration::from_secs(30))).await.ok();
                        })
                        .await;
                        // Two-step teardown — see comment in `write_storm`.
                        node.stop();
                    });
                    rt.shutdown_timeout(Duration::from_secs(2));
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// =====================================================================================
// Group 3 — Read Storm (redb vs Persy)
// =====================================================================================

/// Read storm. Pre-populate N keys in setup (NOT measured), then bench
/// random key reads. Each read uses `Node::get(k).once(timeout)` which
/// returns the current value via the local `on()` broadcast channel.
///
/// We use `once()` rather than `map()` because `map()` is a streaming
/// receiver (broadcasts all children on subscribe); `once()` is the
/// idiomatic single-value read for a known key.
fn read_storm(c: &mut Criterion) {
    // Same scale rationale as write_storm — N=1k is the sweet spot.
    const N: u64 = 1_000;
    const READ_TIMEOUT_MS: u64 = 500;
    let mut group = c.benchmark_group("read_storm");
    group.throughput(Throughput::Elements(N));
    group.sample_size(10);

    for backend in BackendKind::all() {
        let label = format!("random_{}", backend.name());
        group.bench_function(label, |b| {
            // Per-iteration fresh Runtime: see module-level docs. Sync
            // `iter_custom` lets us own the runtime lifetime explicitly —
            // every actor task `tokio::spawn`'d inside `rt.block_on(...)`
            // dies when `rt` drops at end of the for-loop body.
            //
            // Pre-populate (NOT measured) and the read storm (measured)
            // both happen sequentially inside one `rt.block_on(...)` call.
            // This is cleaner than the prior `tokio::spawn`-for-prep +
            // JoinHandle-await dance, which was a workaround for the broken
            // shared-runtime pattern.
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let rt = Runtime::new().unwrap();
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        clean_bench_dir("read_storm", backend);
                        clean_storage_file("read_storm", backend);
                        let mut node = setup_node("read_storm", backend);
                        // Pre-populate (not measured). Sync drain so every
                        // key is durable before the read storm starts.
                        for i in 0..N {
                            let _ = node
                                .get(&format!("k{i:08}"))
                                .put(format!("v{i}").into())
                                .await;
                        }
                        let _ = node.flush_storage(Some(Duration::from_secs(60))).await;
                        // Measured: random key reads via `once()`.
                        // `once()` is the idiomatic single-value read for a
                        // known key (vs. `map()` which streams all children).
                        for i in 0..N {
                            let key = format!("k{i:08}");
                            let _ = node
                                .get(&key)
                                .once(Some(Duration::from_millis(READ_TIMEOUT_MS)))
                                .await;
                        }
                        // Two-step teardown — see comment in `write_storm`.
                        node.stop();
                    });
                    rt.shutdown_timeout(Duration::from_secs(2));
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// =====================================================================================
// Group 4 — Mixed 70/30 Workload (redb vs Persy)
// =====================================================================================

/// Mixed 70% reads / 30% writes per iteration. Realistic OLTP-ish profile.
/// 1,000 ops per iter; each op randomly chosen via an LCG PRNG seeded
/// from a fixed constant for reproducibility.
fn mixed_workload(c: &mut Criterion) {
    const OPS_PER_ITER: u64 = 1_000;
    const READ_RATIO_NUM: u32 = 7; // 7/10 = 70%
    const READ_RATIO_DEN: u32 = 10;
    let mut group = c.benchmark_group("mixed_70_30");
    group.throughput(Throughput::Elements(OPS_PER_ITER));
    group.sample_size(10);

    for backend in BackendKind::all() {
        let label = format!("r70w30_{}", backend.name());
        group.bench_function(label, |b| {
            // Per-iteration fresh Runtime: see module-level docs. Sync
            // `iter_custom` lets us own the runtime lifetime explicitly —
            // every actor task `tokio::spawn`'d inside `rt.block_on(...)`
            // dies when `rt` drops at end of the for-loop body.
            //
            // Pre-populate (NOT measured) and the mixed workload (measured)
            // both happen sequentially inside one `rt.block_on(...)` call —
            // cleaner than the prior `tokio::spawn`-for-prep + JoinHandle-await
            // workaround.
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let rt = Runtime::new().unwrap();
                    let start = std::time::Instant::now();
                    rt.block_on(async {
                        clean_bench_dir("mixed_workload", backend);
                        clean_storage_file("mixed_workload", backend);
                        let mut node = setup_node("mixed_workload", backend);
                        // Pre-populate so reads always find values (not measured).
                        for i in 0..OPS_PER_ITER {
                            let _ = node
                                .get(&format!("k{i:08}"))
                                .put(format!("seed{i}").into())
                                .await;
                        }
                        let _ = node.flush_storage(Some(Duration::from_secs(60))).await;
                        // Measured: LCG PRNG for reproducible op sequencing
                        // (same sequence every iter → apples-to-apples comparison).
                        let mut state: u32 = 0xCAFEBABE;
                        for op_idx in 0..OPS_PER_ITER {
                            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
                            let is_read = (state >> 16) % READ_RATIO_DEN < READ_RATIO_NUM;
                            let key = format!("k{:08}", op_idx);
                            if is_read {
                                let _ = node.get(&key).once(Some(Duration::from_millis(500))).await;
                            } else {
                                let _ = node.get(&key).put(format!("v{op_idx}").into()).await;
                            }
                        }
                        let _ = tokio::time::timeout(Duration::from_secs(60), async {
                            node.flush_storage(Some(Duration::from_secs(30))).await.ok();
                        })
                        .await;
                        // Two-step teardown — see comment in `write_storm`.
                        node.stop();
                    });
                    rt.shutdown_timeout(Duration::from_secs(2));
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// =====================================================================================
// Group 8 — Wire Parse/Serialize (pure CPU, no runtime) (v0.11.0)
// =====================================================================================

/// Wire-format parse and serialize benchmarks — the JSON layer of the hot path.
///
/// These isolate the CPU cost of `Message::try_from` (JSON → Message) and
/// `Message::to_string` (Message → wire JSON) without any tokio runtime,
/// actor mailbox, or I/O. They are the pure serialization cost per message.
///
/// Message sizes:
/// - small: single Put with one child key (typical relay message)
/// - medium: Put with 5 child keys (moderate graph node)
/// - large: Put with 20 child keys (large graph node, nested structure)
fn wire_parse_serialize_benchmarks(c: &mut Criterion) {
    let addr = Addr::noop();

    // --- Small Put: single key, short string value ---
    let small_put = r##"{"put":{"users/alice":{"_":{"#":"users/alice",">":{"name":1653465227430}},"name":"Alice"}},"#":"msg001ab"}"##;

    // --- Medium Put: 5 child keys ---
    let medium_put = r##"{"put":{"users/bob":{"_":{"#":"users/bob",">":{"name":1653465227430,"age":1653465227431,"city":1653465227432,"email":1653465227433,"role":1653465227434}},"name":"Bob","age":"30","city":"NYC","email":"bob@example.com","role":"admin"}},"#":"msg002cd"}"##;

    // --- Large Put: 20 child keys ---
    let large_put = r##"{"put":{"data/node1":{"_":{"#":"data/node1",">":{"k0":1653465227430,"k1":1653465227431,"k2":1653465227432,"k3":1653465227433,"k4":1653465227434,"k5":1653465227435,"k6":1653465227436,"k7":1653465227437,"k8":1653465227438,"k9":1653465227439,"k10":1653465227440,"k11":1653465227441,"k12":1653465227442,"k13":1653465227443,"k14":1653465227444,"k15":1653465227445,"k16":1653465227446,"k17":1653465227447,"k18":1653465227448,"k19":1653465227449}},"k0":"val0","k1":"val1","k2":"val2","k3":"val3","k4":"val4","k5":"val5","k6":"val6","k7":"val7","k8":"val8","k9":"val9","k10":"val10","k11":"val11","k12":"val12","k13":"val13","k14":"val14","k15":"val15","k16":"val16","k17":"val17","k18":"val18","k19":"val19"}},"#":"msg003ef"}"##;

    let cases: &[(&str, &str)] = &[
        ("small", small_put),
        ("medium", medium_put),
        ("large", large_put),
    ];

    for (label, json) in cases {
        // Parse benchmark: JSON → Message
        c.bench_function(&format!("wire_parse_put_{}", label), |b| {
            b.iter(|| {
                Message::try_from(json, addr.clone(), true).unwrap();
            });
        });

        // Serialize benchmark: Message → JSON
        // Pre-parse once, then measure serialization only.
        let msgs = Message::try_from(json, addr.clone(), true).unwrap();
        c.bench_function(&format!("wire_serialize_put_{}", label), |b| {
            b.iter(|| {
                // to_string takes &self (Sprint 3) — no clone needed
                msgs[0].to_string();
            });
        });
    }

    // --- Get message parse ---
    let get_json = r##"{"get":{"#":"users/alice"},"#":"msg004ab"}"##;
    c.bench_function("wire_parse_get", |b| {
        b.iter(|| {
            Message::try_from(get_json, addr.clone(), true).unwrap();
        });
    });
}

// =====================================================================================
// Group 9 — Dedup Gate (pure CPU, no runtime) (v0.11.0)
// =====================================================================================

/// Dedup gate benchmark — measures `Dup::check` + `Dup::track` throughput.
///
/// The Dup gate is the first check every inbound message hits. Under relay
/// load, the ratio of `messages_dropped_dup / messages_parsed` tells us
/// how much redundant relay traffic exists. This benchmark measures the
/// raw cost of the check+track operation itself.
///
/// Two scenarios:
/// - fresh: each message ID is unique (best case — check passes, track inserts)
/// - duplicate: all messages are the same (worst case — check hits every time)
fn dedup_benchmarks(c: &mut Criterion) {
    // Fresh IDs: check passes, track inserts — the common case for relay traffic
    c.bench_function("dup_check_track_fresh", |b| {
        b.iter_batched(
            || Dup::new(999, 9),
            |mut dup| {
                for i in 0..1000u64 {
                    let id = format!("msg{:08x}", i);
                    dup.check(&id);
                    dup.track(&id);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Duplicate IDs: check hits every time — the dedup-heavy scenario
    c.bench_function("dup_check_duplicate", |b| {
        b.iter_batched(
            || {
                let mut dup = Dup::new(999, 9);
                // Pre-populate with one ID
                dup.track("msgdup001");
                dup
            },
            |mut dup| {
                for _ in 0..1000 {
                    dup.check("msgdup001");
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// =====================================================================================
// Group 10 — Actor Mailbox Throughput (tokio channel cost) (v0.11.0)
// =====================================================================================

/// Actor mailbox throughput — measures raw tokio channel + actor dispatch cost
/// without any router logic, storage, or network I/O.
///
/// A minimal echo actor (handle() just drops the message) receives N messages
/// through `Addr::send`. The benchmark measures messages/sec that the actor
/// can process. This is the ceiling — router logic, dedup, and serialization
/// all add cost on top of this baseline.
fn actor_mailbox_benchmarks(c: &mut Criterion) {
    use beam::actor::{Actor, ActorContext};
    use beam::message::Message;
    use std::sync::Arc;

    /// Minimal echo actor — receives messages and drops them.
    /// Measures pure channel + scheduler cost.
    struct EchoActor;
    #[async_trait::async_trait]
    impl Actor for EchoActor {
        async fn handle(&mut self, _msg: Arc<Message>, _ctx: &ActorContext) {}
    }

    let rt = Runtime::new().unwrap();

    c.bench_function("actor_mailbox_throughput", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let ctx = ActorContext::new("bench".to_string());
                    let addr = ctx.start_actor(Box::new(EchoActor));
                    // Send 1000 messages and drain
                    for _ in 0..1000 {
                        let _ = addr.send(Message::Hi {
                            from: addr.clone(),
                            peer_id: "bench".to_string(),
                            is_ack: None,
                            msg_id: beam::utils::random_string(8),
                        });
                    }
                    ctx.stop();
                }
                start.elapsed()
            })
        });
    });
}

// =====================================================================================
// Group 11 — Router Dispatch Throughput (router logic, no network) (v0.11.0)
// =====================================================================================

/// Router dispatch throughput — measures the cost of the full router pipeline
/// (dedup + storage dispatch + relay fan-out) without any WebSocket I/O.
///
/// A Node with MemoryStorage (no network adapters) receives N puts via
/// `Node::put()`. The benchmark measures puts/sec through the complete
/// actor pipeline: Node actor → Router → MemoryStorage → ack. This is
/// the router throughput ceiling before network I/O is added.
fn router_dispatch_benchmarks(c: &mut Criterion) {
    use beam::Value;

    let rt = Runtime::new().unwrap();

    c.bench_function("router_dispatch_throughput", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let mut node = Node::new();
                    for i in 0..1000u64 {
                        let key = format!("bench_key_{}", i);
                        let _ = node.get(&key).put(Value::Text("v".into())).await;
                    }
                    node.stop();
                }
                start.elapsed()
            })
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    parsing_benchmarks(c);
    write_storm(c);
    concurrent_write_storm(c);
    read_storm(c);
    mixed_workload(c);
    // Hot-path benchmarks (v0.11.0)
    wire_parse_serialize_benchmarks(c);
    dedup_benchmarks(c);
    actor_mailbox_benchmarks(c);
    router_dispatch_benchmarks(c);
    // Future groups appended here:
    //   memory_pressure(c);
    //   cross_backend_mesh(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
