# BEAM Profiling Guide

This document describes how to run the four profiling tools used in BEAM's
release workflow: `perf`, flame graphs (via `inferno`), `heaptrack`, and `dhat`.

## Prerequisites

All tools are installed on the build machine. The profiling binary is built with a dedicated
`[profile.profiling]` in Cargo.toml:

```toml
[profile.profiling]
inherits = "release"
debug = true
strip = false
```

This gives us debug symbols (for symbol resolution) without LTO (which flattens
the symbol table).

## Key Rules

1. **Sequential, not parallel.** Running profilers in parallel contaminates each
   other's results. Always run one at a time: perf → heaptrack → dhat.

2. **Disable mimalloc for allocation profiling.** mimalloc's per-thread heaps
   mask allocation patterns from heaptrack/dhat. Use `--no-default-features`
   when profiling allocation behavior.

3. **Results persist in `bench/results/`.** This directory is gitignored but
   data stays on disk for cross-version comparison.

4. **LTO + strip break profiling.** Never profile with the release profile
   (`lto=true, strip=true`). Always use `--profile profiling`.

## Building the Profiling Binary

```bash
source ~/.cargo/env
cargo test --profile profiling --test relay_throughput_bench --no-run
```

The binary lands at `target/profiling/deps/relay_throughput_bench-*`.

## 1. perf stat (CPU Counters)

Lowest overhead. Captures IPC, cache misses, context switches, page faults.

```bash
BINARY=$(find target/profiling -name "relay_throughput_bench-*" -type f | head -1)
perf stat -e cycles,instructions,cache-misses,context-switches,page-faults,cpu-migrations \
    "$BINARY" --bench 2>&1 | tee bench/results/perf-stat-$(date +%FT%H-%M).txt
```

Key metrics:
- **IPC** (instructions/cycle): >0.5 = CPU-bound, <0.5 = memory-bound
- **Cache miss rate**: high = poor locality
- **Context switches**: high = excessive scheduling (actor model overhead)
- **Page faults**: high = allocator mmap churn

## 2. Flame Graph (perf record + inferno)

Captures call-stack samples for flame graph visualization.

```bash
# Record
perf record -F 999 -g --call-graph dwarf -o /tmp/beam-perf.data "$BINARY" --bench

# Generate flame graph SVG
perf script -i /tmp/beam-perf.data | inferno-collapse-perf > bench/results/flamegraph-$(date +%FT%H-%M).svg
```

Open the SVG in a browser. Width = time spent, height = call depth.
Look for wide bars = hot spots.

**Note:** Use SIGINT to stop perf, never SIGKILL (corrupts data).

## 3. Heaptrack

Heap allocation profiler. Tracks every allocation with full call stacks.

```bash
heaptrack -o bench/results/heaptrack-$(date +%FT%H-%M) \
    "$BINARY" --bench --no-default-features 2>&1 | tail -20
```

Analyze:
```bash
heaptrack_print bench/results/heaptrack-*.zst | tee bench/results/heaptrack-analysis.txt
```

Key metrics:
- **Total allocations** and **allocation rate** (allocs/sec)
- **Peak heap** and **peak RSS**
- **Top allocation sites** by call count and by peak bytes
- **Temporary allocations** (allocated then freed quickly)

**Note:** `--no-default-features` disables mimalloc, exposing the system
allocator's allocation patterns.

## 4. DHAT (Valgrind)

Detailed heap profiling with allocation lifetimes and access patterns.

```bash
valgrind --tool=dhat "$BINARY" --bench 2>&1 | tee bench/results/dhat-$(date +%FT%H-%M).txt
```

For interactive viewing, DHAT writes a file that can be opened in a browser:
```bash
# DHAT output file is dhat.out.<PID>
# Open in browser: dh_view.html (from valgrind distribution) + load the file
```

Key metrics:
- **Total bytes** and **blocks** per allocation site
- **% of total** — identifies dominant allocation sources
- **Allocation lifetime** — short-lived = temporary, long-lived = persistent

**Note:** DHAT is the highest overhead (10-50x slowdown). Run last.

## Using the Justfile

The `justfile` automates all four profilers in sequence:

```bash
just profiling
```

This runs all 4 tools sequentially, saving results to `bench/results/`.

## Historical Results

Previous profiling results are stored in MemPalace:
- v0.12.0: Initial flame graph analysis (Session 14-15)
- v0.14.0: Full profiling report with CPU, allocation, and DHAT breakdown (Session 38)
- v0.15.0: Post-mimalloc profiling (Session 41)

## Troubleshooting

### "perf: No symbols found"
LTO is stripping symbols. Use `--profile profiling` (not `--release`).

### Stale binary gives wrong results
After changing Cargo.toml profile settings, always `cargo clean` and rebuild.
Stale binaries produce catastrophically wrong numbers (e.g., 64 msgs/sec vs
1,893 actual — a 30x phantom regression).

### perf data corrupted
Never use `kill -9` (SIGKILL) on perf. Use Ctrl+C (SIGINT) or let it finish.

### RUST_LOG=debug causes OOM on benchmarks
Debug logging in tight benchmark loops allocates massive amounts of string.
Keep RUST_LOG at info or warning during profiling.

### mimalloc breaks allocation profiling
mimalloc's per-thread heaps are invisible to heaptrack/dhat. Always use
`--no-default-features` when profiling allocation patterns.
