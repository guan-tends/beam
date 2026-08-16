# BEAM Profiling Results

## Methodology
All profilers run sequentially (never in parallel) to avoid mutual contamination.
Profiling binary built with `cargo bench --profile profiling --no-run`.

### Execution Order (by overhead, lowest first)
1. **perf + inferno** (flame graph) — lowest overhead, ~1-3%
2. **heaptrack** (heap allocation) — moderate overhead
3. **valgrind --tool=dhat** (allocation lifetime) — highest overhead (10-50x)

### Commands
```bash
# Build profiling binary
cargo bench --profile profiling --no-run

# 1. Flame graph
perf record -F 99 -g -- $BENCH --bench router_dispatch_throughput --profile-time 10
perf script > bench/results/perf_tier1.data
~/.cargo/bin/inferno-collapse-perf < bench/results/perf_tier1.data | ~/.cargo/bin/inferno-flamegraph > bench/results/flamegraph_tier1.svg

# 2. Heaptrack
heaptrack -o bench/results/heaptrack_tier1 -- $BENCH --bench router_dispatch_throughput --profile-time 10
heaptrack_print bench/results/heaptrack_tier1*.zst > bench/results/heaptrack_tier1_analysis.txt

# 3. DHAT
valgrind --tool=dhat --dhat-out-file=bench/results/dhat_tier1.out -- $BENCH --bench router_dispatch_throughput --profile-time 3
```

## Files

### Tier 1+1.5 (2026-08-14)
- `flamegraph_tier1.svg` — CPU flame graph (7,720 samples, 10s)
- `perf_tier1.data` — raw perf data
- `heaptrack_tier1.zst` — raw heaptrack data
- `heaptrack_tier1_analysis.txt` — readable heaptrack analysis
- `dhat_tier1.out` — DHAT JSON output
- `dhat_tier1_stdout.txt` — DHAT stdout summary

### Pre-Tier 1 (2026-08-14, earlier same day)
- `flamegraph-20260814T061301.svg` — CPU flame graph (1,591 samples)
- `heaptrack-20260814T061337.zst` — raw heaptrack data
- `heaptrack-analysis-20260814T061405.txt` — readable analysis
- `dhat-20260814T061604.txt` — DHAT summary

## Key Comparison (Pre-Tier 1 vs Tier 1+1.5)

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| clone_subtree CPU | 1.79% | 0.59% | -67% |
| clone_subtree peak memory | 2GB | 13.89KB | -99.999% |
| Put::clone CPU | (subsumed) | 0.19% | now Arc refcount |
| Addr::to_string() temp allocs | 115,828 | 0 | eliminated |
| malloc+free aggregate | ~24% | ~21% | -3pp |

## Note
Profiling builds use LTO disabled + codegen-units=256 (criterion default for profiling).
This distributes clone/allocation cost, hiding true impact. Production LTO builds
inline surrounding overhead, making the clone a LARGER proportion of execution time.
Always verify with production-mode benchmarks after fixing what the profiler shows.
Take profiling results as LOWER BOUNDS, not ground truth.

---

## v0.16.0 Profiling Battery (2026-08-16)

### Methodology (refined)
Profiling binaries built with `cargo build --profile profiling --test local_put_bench --test relay_throughput_bench`.
For heaptrack/dhat without mimalloc: `cargo build --profile profiling --no-default-features --features native --test local_put_bench --test relay_throughput_bench`.

Two sets of binaries:
- **With mimalloc** (default features): for perf/flamegraph (production-like allocator)
- **Without mimalloc** (`--no-default-features --features native`): for heaptrack/dhat (system allocator exposes individual allocations)

### Execution Order (by overhead, lowest first) — for BOTH local put and relay:
1. **perf + inferno** (flame graph) — `perf record --call-graph dwarf -F 99 -o bench/results/v0XX/perf_XXX.data -- $BENCH $TEST --ignored --nocapture`
2. **heaptrack** (heap allocation) — `heaptrack -o bench/results/v0XX/heaptrack_XXX -- $BENCH_NO_MI $TEST --ignored --nocapture`
3. **valgrind --tool=dhat** (allocation lifetime) — `valgrind --tool=dhat --dhat-out-file=bench/results/v0XX/dhat_XXX.out -- $BENCH_NO_MI $TEST --ignored --nocapture`

### Binaries
- Local put (mimalloc): `target/profiling/deps/local_put_bench-<hash>`
- Local put (no mimalloc): `target/profiling/deps/local_put_bench-<hash>` (different hash)
- Relay (mimalloc): `target/profiling/deps/relay_throughput_bench-<hash>`
- Relay (no mimalloc): `target/profiling/deps/relay_throughput_bench-<hash>` (different hash)
- Find latest: `ls -lt target/profiling/deps/ | grep -E "local_put|relay_throughput" | grep -v '\.d$' | head -4`

### Test names
- Local put: `local_put_100k --ignored --nocapture`
- Relay: `relay_throughput_1_sender_50k --ignored --nocapture`

### Long-running tools (dhat takes 2-5 min under valgrind)
- Use `nohup` for dhat: `nohup bash -c 'valgrind ... > output.txt 2>&1; echo "DONE=$?" >> output.txt' &`
- Poll with: `grep "DONE=" output.txt`
- tmux sessions can die silently — nohup is more reliable

### Files
See `bench/results/v016/SUMMARY.md` for full results.
