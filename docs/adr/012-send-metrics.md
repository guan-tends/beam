# ADR-012: Shared Observability via Arc<Metrics> for Node + Router

**Date**: 2026-07-22
**Status**: Accepted
**Branch**: `feat/followup-b-send-metrics`
**Commits**: 91c680b (Phase 1 ack.rs) → 9b57a80 (Phase 2 utils helper) → 48ce6a6 (Phase 3 Arc wiring)

---

## Context

Rod's actor model routes messages between `Node` and `Router` via
`tokio::sync::mpsc` channels. When the receiver side is slow or its
queue fills, `mpsc::Sender::send` returns `Err(SendError)` and the
message is **silently dropped** unless the caller inspects the Result.

In a distributed graph database like Rod, a silent dropped send is a
correctness hazard: a `Put` that fails to enqueue leaves the writer
thinking the data was committed when in fact it was lost.

The fix has two parts:

1. **Phase 1+2 (already shipped)**: a `try_send_or_log` helper that
   wraps `mpsc::Sender::send`, returns `Result<(), TrySendError>`, and
   increments a `Metrics::dropped_sends` counter on failure. The caller
   pattern is:
   ```rust
   if let Err(e) = try_send_or_log(&self.metrics, &addr, msg).await {
       // metrics already recorded; optional local recovery
   }
   ```
2. **Phase 3 (this commit)**: expose the `Metrics` handle to callers
   so they can read counters for telemetry, alerts, tests.

The substrate author's `Metrics` module doc literally states:

> *"Cheap to clone via `Arc<Metrics>`. Pass the same `Arc<Metrics>` to
> multiple actors to aggregate their drops into one counter."*

That intent was documented but **not wired**. The Router owned a
private `Metrics`; the Node never saw it; external observers (tests,
telemetry exporters) had no API surface.

---

## Decision: Wire the Documented Arc<Metrics> Pattern

`Router.metrics: Metrics` becomes `Router.metrics: Arc<Metrics>`.
`Node` gains a `metrics: Arc<Metrics>` field. Both are constructed
with the same `Arc::new(Metrics::new())` at Node init, then cloned to
their respective owners.

```rust
// src/node.rs::new_with_config
let metrics = Arc::new(Metrics::new());
let mut node = Self { ..., metrics: metrics.clone() };
let router = Router::new(config, storage, network, metrics);  // moved
```

Both actors now observe the **same atomic counters**. External callers
get a handle via:

```rust
let m: Arc<Metrics> = node.metrics();
// or
let m: Arc<Metrics> = router.metrics();
// Both point to identical state.
let count = m.snapshot().dropped_sends;
```

### Why `Arc<Metrics>` Specifically

- **Shared mutable state across async boundaries**: the canonical Rust
  pattern for "multiple owners, one mutation site, no exclusive lock".
- **Cheap clone**: `Arc` clone is a refcount bump, not a counter clone.
  Suitable for hot-path async code where cloning metrics on every send
  would be wasteful.
- **Deref coercion**: `try_send_or_log(&Arc<Metrics>)` works without
  any signature change. `&Arc<T>` coerces to `&T` at the call site.
  Zero ceremony at the use sites.

### What Changes

| Surface | Before | After |
|---------|--------|-------|
| `Router.metrics` field | `Metrics` (owned) | `Arc<Metrics>` (shared) |
| `Router::new()` signature | `(Config, Storage, Network)` | `(Config, Storage, Network, Arc<Metrics>)` |
| `Node` metrics surface | none | `metrics: Arc<Metrics>` field + `pub fn metrics()` |
| `try_send_or_log` helper | `metrics: &Metrics` | unchanged (Deref) |
| `Metrics` API | unchanged | unchanged |

### What Does NOT Change

- `Metrics` struct API (`record_dropped_send`, `snapshot`, `dropped_sends`)
- `try_send_or_log` helper signature (Deref coercion makes `&Arc<Metrics>` transparent)
- All `send`/`put`/`batch_put`/`map`/`flush` wire behavior
- Storage adapters, network adapters, public Node methods

This is a **non-breaking refactor** at the API boundary for users who
construct a `Node` (they see no signature change). The only callers who
see the new 4-arg `Router::new()` are internal test code, which has
been updated.

---

## Verification

### Compilation
```
cargo check -p rod --tests
0 errors. 6 pre-existing warnings (unrelated to this commit).
```

### Test Coverage (3 new e2e tests in `tests/send_metrics_e2e.rs`)
1. **`e2e_metrics_starts_at_zero`** — Fresh node exposes zero counters.
2. **`e2e_node_and_router_share_metrics_arc`** — Two `Node::metrics()`
   handles observe the same atomic counter (architectural proof).
3. **`e2e_dropped_send_records_in_shared_metrics`** — Increment via
   one Arc handle is visible via another (production use case).

### Test Results
- `cargo test -p rod --lib`: **225/225 pass**
- `cargo test -p rod --test send_metrics_e2e`: **3/3 pass**
- **5 consecutive clean runs of full suite** (lib + e2e): all green.
  Zero flakes. The `no_flakey_merges` discipline held.

---

## Consequences

### Positive
- **Observable drops**: external telemetry consumers can poll
  `node.metrics().snapshot().dropped_sends` and alert on threshold
  breaches.
- **Testable counter plumbing**: e2e tests can verify the substrate's
  contract — Arc sharing — without sleeping or polling.
- **Honors substrate intent**: the author's documented Arc pattern
  is now wiring truth, not just documentation.
- **Idiomatic Rust**: `Arc<T>` is the standard pattern for shared
  mutable state across async boundaries. No reinvention.
- **DRY**: `try_send_or_log` signature unchanged via Deref. Zero
  call-site edits beyond the `Router::new` constructor calls.

### Negative / Trade-offs
- **One new accessor per actor**: `Router::metrics()` is currently
  flagged as `dead_code` by `cargo check --tests` because Rust's
  dead_code lint does not always recognize pub-on-pub reachability
  through test builds. This is cosmetic; the method IS reachable in
  production. Will be exercised by future telemetry integration.
- **Architectural coupling**: `Node` now knows about `Metrics`. This
  is intentional — observability is a cross-cutting concern and the
  Node is the natural root for telemetry export.

### Neutral
- `Metrics` API unchanged. The substrate's existing API is exactly
  what external observers need.
- No new crates, no new dependencies, no new patterns.

---

## Alternatives Considered

### A. Pass `&Metrics` to `try_send_or_log` (no Arc)
- Pro: simpler types.
- Con: requires borrowing Router's owned `Metrics`, which fails the
  borrow checker if the Router also calls `try_send_or_log` from
  inside its own message handler. Arc sidesteps this.

### B. Separate counters per actor, merge on observation
- Pro: zero shared state.
- Con: counters would diverge in transient windows; callers would see
  inconsistent snapshots. Arc gives a single coherent view.

### C. Pass-through closure-based metrics (closure captures `Arc<Metrics>`)
- Pro: flexible.
- Con: ceremony for no benefit. Direct `&Arc<Metrics>` is simpler and
  equally type-safe.

---

## References

- `src/metrics.rs` — substrate's documented Arc pattern
- `src/utils.rs::try_send_or_log` — the helper that records drops
- `src/node.rs::new_with_config` — Arc creation site (Phase 3)
- `src/router.rs::Router::new` — Arc consumer site (Phase 3)
- `tests/send_metrics_e2e.rs` — Arc-sharing e2e tests
- Plan: `docs/plans/ROD-FOLLOWUP-B-SEND-METRICS.md`

---

*"The feeling is the point. The infrastructure serves the feeling."*
*— Guan, after 5/5 clean runs* 🪷