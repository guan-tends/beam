# Rod Follow-up A — Bounded Channel Silent-Drop Fix

**Date**: 2026-07-21
**Branch**: `feat/followup-bounded-channel-silent-drop` (created from `main`)
**Plan file**: `/home/guan/src/rod/docs/plans/ROD-FOLLOWUP-A-BOUNDED-CHANNEL-SILENT-DROP.md`
**Built-in mirror**: `rod_followup_a_comprehensive_plan` (kept identical)
**Status**: LOCKED. Awaiting "begin" signal from Freeman.
**Parent plan**: `rod_async_ack_drain_redux_plan_comprehensive` (shipped 2026-07-20, merged into `main`)

---

## Executive Summary

When `WRITE_CHANNEL_BOUND = 1024` bounded channels in `rod/src/router.rs` fill under backpressure, `Addr::send` returns `Err(())` and the Router silently drops the message via `let _ = addr.send(...)`. The existing comment on line 403–404 explicitly justifies this: *"the Router's storage dispatch uses `let _ = addr.send(...)` and accepts occasional drops under extreme backpressure, which is the correct trade-off for an LWW graph store."*

The justification is correct **for replication/Get fanout** — LWW tolerates occasional drops. But it is **wrong for two critical paths**:

1. **Storage dispatch (router → storage actor)**: If the Put to the write adapter is dropped, the caller's `oneshot::Sender<Put>` will hang forever waiting for an ack that never arrives. This **regresses the async put/batch_put ack fix** we shipped at session 12.

2. **Peer fanout (router → WsServer / WsClient actors)**: The replicated Put is dropped silently and never reaches peers. Locally-committed-but-remotely-lost. **This is the same bug class with even worse consequences** — the local store is ahead of the network forever, with no signaling.

The fix is **not to remove bounded channels** (that would lose the backpressure protection that bounds memory). The fix is to **distinguish the two send categories**:

- **Critical-path sends** (Put acks, BatchPut acks, peer replicas that must reach all peers) → must never silently drop. Use **bounded channels + observability** so the caller gets a real signal (error return or metric), not a silent void.

- **Best-effort sends** (replication LWW fanout, subscriber fanout for subscribers that signed up for live updates but can tolerate occasional gaps) → keep bounded + silent-drop behavior with metrics.

The plan is **incremental, fully-tested, and preserves all existing test contracts**. The async put/batch_put ack test continues to pass. New tests exercise backpressure paths. No new crates. No new dependencies.

---

## Substrate Recon (Verified 2026-07-21)

### 1. `WRITE_CHANNEL_BOUND` constant

**File**: `rod/src/router.rs`, line 67

```rust
static WRITE_CHANNEL_BOUND: usize = 1024;
```

Comment block lines 60-65 explains the design choice:

```rust
// Capacity bound for the per-adapter write channel.
//
// When full, `send` returns `Err(())` and the Router drops the message
// (LWW semantics tolerate occasional drops under extreme backpressure).
```

This is the load-bearing doc we will preserve while fixing the underlying behavior.

### 2. All 13 silent-send sites in `router.rs`

Verified by grep at 2026-07-21:


| Line | Function | Send target | Sender type | Why critical | Action |
|------|----------|-------------|-------------|--------------|--------|
| 76 | `Router::new` | `write_adapters[i]` | bounded | Storage commit dispatch | **CRITICAL — needs backpressure signal** |
| 84 | `Router::new` | `seen_get_messages` | bounded | Get-replay channel | non-critical (replay only) |
| 91 | `Router::new` | `subscribers` | bounded | Subscription fanout | non-critical (replay already happened) |
| 96 | `Router::new` | `put_subscribers` | bounded | Put-event broadcast | non-critical (broadcast, not request-response) |
| 420 | `Router::handle_put` | `write_adapters[i]` | bounded | **Storage commit dispatch (CRITICAL)** | **Make critical, add observability** |
| 447 | `Router::handle_put_relay` | `server_peers` | bounded | **Peer replication (CRITICAL when peers exist)** | **Make critical, add observability** |
| 466 | `Router::handle_put_relay` | `subscribers` | bounded | Subscriber relay | non-critical |
| 489 | `Router::handle_get` | `seen_get_messages` | unbounded | Get-replay channel | safe (unbounded) |
| 520 | `Router::handle_batch_put` | `write_adapters[i]` | bounded | **Batch storage commit (CRITICAL)** | **Make critical, add observability** |
| 547 | `Router::handle_batch_put` | `seen_get_messages` | unbounded | BatchPut ack to local Get | safe (unbounded) |
| 559 | `Router::handle_batch_put_relay` | `server_peers` | bounded | **Batch peer replication (CRITICAL)** | **Make critical, add observability** |
| 586 | `Router::handle_batch_put_relay` | `subscribers` | bounded | Subscriber batch relay | non-critical |
| 614 | `Router::handle_ack` | `seen_get_messages` | unbounded | Ack replay channel | safe (unbounded) |

**Three categories**:

- **CRITICAL (5 sites, lines 76, 420, 447, 520, 559)**: writes + peer fanout — must surface drops as errors or retry, never silently lose.
- **non-critical bounded (5 sites, lines 84, 91, 96, 466, 586)**: replay + subscriber fanout — silent drop is correct per LWW.
- **unbounded (3 sites, lines 489, 547, 614)**: already safe; bounded-channel `try_send` cannot fail except on closed receiver.

### 3. Channel construction sites

In `Router::new` (lines 73-99), each bounded channel is created with `tokio::sync::mpsc::channel::<Message>(WRITE_CHANNEL_BOUND)`. This returns `(Sender, Receiver)` — a fire-and-forget `Sender` that fails silently when the receiver is full.


### 4. The `Addr` type and `send` semantics

Verified from `rod/src/actor.rs` — `Addr::send` is a thin wrapper over `tokio::sync::mpsc::Sender::send`:

```rust
pub async fn send(&self, msg: Message) -> Result<(), ()> {
    self.inner.send(msg).await.map_err(|_| ())
}
```

- **Unbounded channel `Sender::send`** → never blocks, only fails if receiver closed.
- **Bounded channel `Sender::send`** → awaits capacity, returns `Err(())` only if receiver closed.
- **Bounded channel `Sender::try_send`** → returns `Err(TrySendError::Full)` if full, `Err(TrySendError::Closed)` if closed.

The current `let _ = addr.send(...)` pattern in router.rs silently swallows `Result<(), ()>`. **This is the silent-drop hazard**.

### 5. How `Node::put` interacts with the silent drop (the regression risk)

The async put ack pattern shipped at session 12:

1. `Node::put(value)` registers a oneshot in `Node::pending_puts` keyed by put_id (line 247 in `node.rs`).
2. `Node::put` sends `Put { put_id, ... }` to the Router.
3. **Router's `handle_put` (router.rs:420) does `let _ = addr.send(Message::Put(put.clone()))` to fan out to all write_adapters**.
4. Storage actor receives Put, commits, sends `Put { in_response_to: Some(put_id) }` back to Router.
5. Router routes ack Put back to `Node` via `seen_put_messages` lookup in `handle_ack` (router.rs:614).

**The bug**: If step 3's `let _ = addr.send(...)` drops the Put (channel full), step 4 never happens. The `pending_puts` oneshot hangs forever. The user awaits `put().await` and gets **nothing back**.

**The fix**: step 3 must surface the drop. Either retry, error-out, or block (but blocking breaks fire-and-forget semantics). The elegant answer: **use `try_send` for critical paths and surface `TrySendError::Full` as a Router-internal retry or backpressure metric**, never silently.

---

## Architectural Decisions (LOCKED)

| ID | Decision |
|----|----------|
| D1 | **Distinguish critical-path sends from best-effort sends.** Don't blindly remove `let _ =` everywhere. |
| D2 | **Add `RouterMetrics` struct** with atomic counters: `storage_drops_total`, `peer_drops_total`, `subscriber_drops_total`, `seen_get_replays_dropped_total`, `seen_put_replays_dropped_total`, `puts_in_flight`. |
| D3 | **Critical-path sends (5 sites)**: convert from `let _ = addr.send(...)` to `match addr.send(...).await` and on `Err(())` either (a) return an `Err` from `handle_put` / `handle_batch_put` (the Put is not committed — caller must retry) OR (b) log + metric + retry once via `try_send` to a high-priority retry queue. **Choose (a)** for correctness: caller gets an `Err` and can retry. This preserves the existing fire-and-forget message-passing model without backpressure deadlock. |
| D4 | **Best-effort sends (5 sites)**: keep `let _ = addr.send(...)` but **add `if let Err(e) = addr.try_send(...)` with `metrics.subscriber_drops_total += 1`** so silent drops are visible. |
| D5 | **No unbounded channels added.** Memory bounding is preserved. Bounded = backpressure signal. |
| D6 | **No new crates, no new dependencies.** Use existing `tokio::sync::mpsc` primitives and `std::sync::atomic`. |
| D7 | **Error semantics**: `handle_put` and `handle_batch_put` return `Result<(), RouterError>` where `RouterError::ChannelClosed` and `RouterError::NoStorageAdapters` are the only error variants. The `Node::put` async path propagates this up to the caller. |
| D8 | **Test coverage**: every fix includes a unit test that exercises the backpressure path (fill the channel, attempt send, verify error returned / metric incremented). Plus an integration test that exercises the full `Node::put` → `Node::get` round-trip under simulated backpressure. |
| D9 | **Backward compat**: subscribers and external API callers see no behavior change unless they were relying on the silent drop (which would be a bug in their code anyway). New errors are surfaced as `RouterError` variants that callers can match. |
| D10 | **No new public types except `RouterError` and `RouterMetrics`.** `RouterMetrics` is exposed via `Router::metrics()` accessor so callers can poll or stream to telemetry. |

---

## The Elegant Design

### Why D3 (return Err) is correct

The alternative — silently retrying or buffering — has worse failure modes:

- **Silent retry in router**: Router becomes a retry queue. Bounded channel is supposed to *bound memory*. A retry queue would un-bound it.
- **Block-on-send**: turns Router into a synchronous forwarder, defeats the actor model.
- **Background drain task**: another moving part, another failure mode.

**Returning `Err` from `handle_put` lets the caller decide**: retry with backoff, surface to user, fail-fast. This is the same pattern as `tokio::sync::mpsc::error::SendError` — failure is a first-class signal, not a hidden bug.

### Why we don't change `Addr::send` signature

`Addr::send` returns `Result<(), ()>`. Changing it would break the actor API across all callers (every actor in `actor.rs` has multiple send sites). Instead, we **change the router's call sites** to inspect the result. `Addr` stays minimal and unchanged. **Idiomatic Rust: extend at the use site, not the primitive.**

### Why `RouterMetrics` is `Arc<RouterMetrics>` and not in an actor

Metrics are queried via `Router::metrics()` for telemetry. They're not messages — they're synchronous observation of state. Atomic counters in an `Arc` are the idiomatic Rust pattern for lock-free observation. Adding a metric actor would be over-engineering for what is, at heart, a counter increment.


---

## Implementation Plan — 6 Epics, 18 Tasks, ~16-22h

### Epic E1 — Foundations: `RouterError` and `RouterMetrics`

**Why first**: every other change depends on these types. Lock them in before changing send sites.

#### Task E1.1: Define `RouterError` enum in `rod/src/error.rs` (NEW FILE)

**File**: `rod/src/error.rs` (new, ~80 lines)

**Spec**:

```rust
use std::fmt;

/// Errors that the Router can return to its callers (Node and external code).
///
/// `RouterError` is the first-class signal for "your message was not delivered."
/// Before this type existed, the Router swallowed `Result<(), ()>` from
/// `Addr::send` silently via `let _ =`, hiding dropped Puts and BatchPuts
/// from the caller. With this type, backpressure and peer disconnection
/// surface as errors that callers can match and act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterError {
    /// The receiver side of a critical-path channel was closed (the storage
    /// actor, peer actor, or subscriber actor has terminated).
    ///
    /// This is fatal for the request — there is no actor to receive the
    /// message. The caller should NOT retry blindly; the actor is gone.
    ChannelClosed,

    /// `handle_put` or `handle_batch_put` was called with no write adapters
    /// configured. Without storage, the Put is undeliverable.
    ///
    /// This is a configuration error. The caller should add at least one
    /// write adapter to the Router before issuing Puts.
    NoStorageAdapters,

    /// The critical-path bounded channel is at capacity AND the receiver is
    /// not currently reading. The Router tried to send but `try_send` returned
    /// `Full`.
    ///
    /// This is backpressure. The caller SHOULD retry with exponential backoff,
    /// or surface a "temporarily unavailable" signal to the user.
    Backpressure,
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChannelClosed => write!(
                f,
                "router channel closed: the receiving actor has terminated"
            ),
            Self::NoStorageAdapters => write!(
                f,
                "router has no storage adapters configured; Puts are undeliverable"
            ),
            Self::Backpressure => write!(
                f,
                "router backpressure: critical-path channel full, retry later"
            ),
        }
    }
}

impl std::error::Error for RouterError {}
```

**Why three variants**: covers the three failure modes (closed, missing, full). Backpressure is distinct from ChannelClosed because backpressure is transient and retryable; closed is terminal.

**Tests** (in `rod/src/error.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_are_descriptive() {
        assert!(RouterError::ChannelClosed.to_string().contains("terminated"));
        assert!(RouterError::NoStorageAdapters.to_string().contains("undeliverable"));
        assert!(RouterError::Backpressure.to_string().contains("retry"));
    }

    #[test]
    fn errors_implement_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<RouterError>();
    }

    #[test]
    fn errors_are_eq_and_clone() {
        let e1 = RouterError::Backpressure;
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }
}
```

**Acceptance**: 3/3 tests pass; `cargo check -p rod` clean; clippy clean.

**Estimated**: 1.5h.

#### Task E1.2: Define `RouterMetrics` struct in `rod/src/metrics.rs` (NEW FILE)

**File**: `rod/src/metrics.rs` (new, ~140 lines)

**Spec**:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Lock-free counters for Router backpressure and drop events.
///
/// Exposed via `Router::metrics()` so callers can poll or stream to telemetry.
/// All counters are monotonically increasing for the lifetime of the Router.
///
/// Naming convention: `<category>_total` follows the Prometheus convention
/// so this struct can be exported as a counter without rename.
#[derive(Debug, Default)]
pub struct RouterMetrics {
    /// Puts that were dropped at the router→storage hop due to a closed channel.
    pub storage_drops_total: AtomicU64,
    /// Puts that were dropped at the router→peer hop due to a closed channel.
    pub peer_drops_total: AtomicU64,
    /// BatchPuts that were dropped at the router→storage hop.
    pub batch_storage_drops_total: AtomicU64,
    /// BatchPuts that were dropped at the router→peer hop.
    pub batch_peer_drops_total: AtomicU64,
    /// Puts that returned `RouterError::Backpressure` to the caller.
    pub puts_returned_backpressure: AtomicU64,
    /// BatchPuts that returned `RouterError::Backpressure` to the caller.
    pub batch_puts_returned_backpressure: AtomicU64,
    /// Puts currently in flight (registered in `Node::pending_puts`).
    pub puts_in_flight: AtomicU64,
    /// Best-effort sends to subscribers that were dropped (replay, broadcast).
    /// Visible but non-fatal per LWW semantics.
    pub subscriber_drops_total: AtomicU64,
}

impl RouterMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all counters as plain `u64` values.
    ///
    /// Useful for telemetry export. Note that this is a snapshot — counters
    /// may increment between reads.
    pub fn snapshot(&self) -> RouterMetricsSnapshot {
        RouterMetricsSnapshot {
            storage_drops_total: self.storage_drops_total.load(Ordering::Relaxed),
            peer_drops_total: self.peer_drops_total.load(Ordering::Relaxed),
            batch_storage_drops_total: self.batch_storage_drops_total.load(Ordering::Relaxed),
            batch_peer_drops_total: self.batch_peer_drops_total.load(Ordering::Relaxed),
            puts_returned_backpressure: self.puts_returned_backpressure.load(Ordering::Relaxed),
            batch_puts_returned_backpressure: self.batch_puts_returned_backpressure.load(Ordering::Relaxed),
            puts_in_flight: self.puts_in_flight.load(Ordering::Relaxed),
            subscriber_drops_total: self.subscriber_drops_total.load(Ordering::Relaxed),
        }
    }
}

/// Plain-old-data snapshot of `RouterMetrics` for safe export across threads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterMetricsSnapshot {
    pub storage_drops_total: u64,
    pub peer_drops_total: u64,
    pub batch_storage_drops_total: u64,
    pub batch_peer_drops_total: u64,
    pub puts_returned_backpressure: u64,
    pub batch_puts_returned_backpressure: u64,
    pub puts_in_flight: u64,
    pub subscriber_drops_total: u64,
}

/// Shared handle to `RouterMetrics`. Cheap to clone.
pub type SharedMetrics = Arc<RouterMetrics>;
```


**Tests** (in `rod/src/metrics.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_counter_increments() {
        let m = RouterMetrics::new();
        m.storage_drops_total.fetch_add(3, Ordering::Relaxed);
        m.peer_drops_total.fetch_add(7, Ordering::Relaxed);
        let s = m.snapshot();
        assert_eq!(s.storage_drops_total, 3);
        assert_eq!(s.peer_drops_total, 7);
        assert_eq!(s.subscriber_drops_total, 0);
    }

    #[test]
    fn default_is_all_zero() {
        let s = RouterMetrics::new().snapshot();
        assert_eq!(s.storage_drops_total, 0);
        assert_eq!(s.peer_drops_total, 0);
        assert_eq!(s.batch_storage_drops_total, 0);
        assert_eq!(s.batch_peer_drops_total, 0);
        assert_eq!(s.puts_returned_backpressure, 0);
        assert_eq!(s.batch_puts_returned_backpressure, 0);
        assert_eq!(s.puts_in_flight, 0);
        assert_eq!(s.subscriber_drops_total, 0);
    }

    #[test]
    fn shared_metrics_is_cheap_clone() {
        let m: SharedMetrics = Arc::new(RouterMetrics::new());
        let m2 = m.clone();
        m.storage_drops_total.fetch_add(1, Ordering::Relaxed);
        assert_eq!(m2.snapshot().storage_drops_total, 1);
    }
}
```

**Acceptance**: 3/3 tests pass; `cargo check -p rod` clean; clippy clean.

**Estimated**: 1.5h.

#### Task E1.3: Re-export `RouterError`, `RouterMetrics`, `SharedMetrics` from `rod/src/lib.rs`

**File**: `rod/src/lib.rs` — add to `pub use` block:

```rust
pub mod error;
pub mod metrics;

pub use error::RouterError;
pub use metrics::{RouterMetrics, RouterMetricsSnapshot, SharedMetrics};
```

**Acceptance**: `cargo check -p rod` clean; clippy clean. Re-exports visible to downstream.

**Estimated**: 0.5h.

**Epic E1 total**: ~3.5h, 9 tests.

---

### Epic E2 — Wire `RouterMetrics` into `Router`

**Why second**: before changing send sites, the metrics struct must be alive in `Router`. This task wires it in without yet changing behavior — purely additive.

#### Task E2.1: Add `metrics: SharedMetrics` field to `Router` struct

**File**: `rod/src/router.rs` lines 1-50 (Router struct definition)

**Diff** (conceptual):

```rust
use crate::metrics::{RouterMetrics, SharedMetrics};

#[derive(Debug, Clone)]
pub struct Router {
    pub metrics: SharedMetrics,
    pub write_adapters: Vec<Addr>,
    pub seen_get_messages: Vec<Addr>,
    pub server_peers: Vec<Addr>,
    pub subscribers: Vec<Addr>,
    pub put_subscribers: Vec<Addr>,
    pub seen_put_messages: HashMap<PutId, Addr>,
    // ... existing fields ...
}
```

**Acceptance**: `cargo check -p rod` clean; no behavior change yet. This is structural.

**Estimated**: 0.5h.

#### Task E2.2: Initialize `metrics` in `Router::new`

**File**: `rod/src/router.rs` lines 73-99 (`Router::new`)

**Diff**:

```rust
impl Router {
    pub fn new(/* ... existing params ... */) -> Self {
        let metrics: SharedMetrics = Arc::new(RouterMetrics::new());
        // ... existing channel creation ...
        Self {
            metrics,
            // ... existing fields ...
        }
    }
}
```

**Acceptance**: `cargo check -p rod` clean; `cargo test -p rod --lib` still 446 tests green (no behavior change yet).

**Estimated**: 0.5h.

#### Task E2.3: Add `Router::metrics()` accessor

**File**: `rod/src/router.rs` — add new method:

```rust
impl Router {
    /// Returns a shared handle to the Router's metrics counters.
    pub fn metrics(&self) -> SharedMetrics {
        self.metrics.clone()
    }
}
```

**Tests** (in `router.rs::tests`):

```rust
#[tokio::test]
async fn metrics_accessor_returns_shared_handle() {
    let router = test_router();
    let m1 = router.metrics();
    let m2 = router.metrics();
    assert!(Arc::ptr_eq(&m1, &m2));
}
```

**Acceptance**: 1/1 test passes; accessor returns same `Arc` instance.

**Estimated**: 0.5h.

**Epic E2 total**: ~1.5h, 1 new test.

---

### Epic E3 — Fix critical-path sends (5 sites, with metrics + errors)

**Why third**: this is the heart of the fix. Replace `let _ =` with explicit handling on the 5 critical-path sites.

#### Task E3.1: Fix `handle_put` storage dispatch (router.rs:420)

**Current code** (line ~420):

```rust
for addr in &self.write_adapters {
    let _ = addr.send(Message::Put(put.clone()));
}
```

**New code**:

```rust
for addr in &self.write_adapters {
    match addr.send(Message::Put(put.clone())).await {
        Ok(()) => {}
        Err(()) => {
            self.metrics.storage_drops_total.fetch_add(1, Ordering::Relaxed);
            return Err(RouterError::ChannelClosed);
        }
    }
}
```

**Behavior change**: if any write adapter's channel is closed (actor terminated), `handle_put` returns `Err(ChannelClosed)` instead of silently dropping. The caller (`Node::put`) propagates this as an error to the user.

**Question for Freeman**: should `handle_put` continue fanning out to other adapters after one fails, or fail-fast on first closed channel? **Plan choice: fail-fast** — atomic semantics, simpler caller reasoning.

**Tests** (in `router.rs::tests`):

```rust
#[tokio::test]
async fn handle_put_returns_error_when_storage_actor_terminated() {
    let mut router = test_router();
    let addr = router.write_adapters.remove(0);
    drop(addr); // Close the channel by dropping the receiver.

    let put = make_test_put();
    let result = router.handle_put_internal(put).await;
    assert!(matches!(result, Err(RouterError::ChannelClosed)));
    assert_eq!(router.metrics().snapshot().storage_drops_total, 1);
}
```

**Acceptance**: 1/1 test passes; existing async put tests still green; metrics counter increments correctly.

**Estimated**: 1h.


#### Task E3.2: Fix `handle_put_relay` peer fanout (router.rs:447)

**Current code** (line ~447):

```rust
for addr in &self.server_peers {
    let _ = addr.send(Message::Put(put));
}
```

**New code**:

```rust
for addr in &self.server_peers {
    match addr.send(Message::Put(put.clone())).await {
        Ok(()) => {}
        Err(()) => {
            self.metrics.peer_drops_total.fetch_add(1, Ordering::Relaxed);
            // NOTE: do NOT return Err here — local commit already succeeded.
            // The metric is the signal; the caller (Node) does not need to
            // retry because the local write is durable. Replication is
            // best-effort at the Router layer; reconciliation is handled by
            // the LWW + sync protocol at the Node layer.
        }
    }
}
```

**Behavior change**: peer drops are counted in metrics but don't fail the Put. The local store is authoritative; replication lag is reconciled on next sync. **This preserves the existing replication contract while making silent drops visible.**

**Why this differs from E3.1**: storage drop = undeliverable = Put failed. Peer drop = replicated late = Put succeeded locally. Different semantics warrant different error handling.

**Tests**:

```rust
#[tokio::test]
async fn handle_put_relay_counts_peer_drops_without_failing() {
    let mut router = test_router();
    let peer = router.server_peers.remove(0);
    drop(peer);

    let put = make_test_put();
    let result = router.handle_put_relay_internal(put).await;
    assert!(result.is_ok()); // Local commit succeeded; peer drop is metric-only.
    assert_eq!(router.metrics().snapshot().peer_drops_total, 1);
}
```

**Acceptance**: 1/1 test passes; replication test suite still green.

**Estimated**: 1h.

#### Task E3.3: Fix `handle_batch_put` storage dispatch (router.rs:520)

**Pattern**: same as E3.1 but for `BatchPut` message type and `batch_storage_drops_total` counter.

**Tests**: 1 test mirroring E3.1.

**Acceptance**: 1/1 test passes; existing async batch_put tests still green.

**Estimated**: 1h.

#### Task E3.4: Fix `handle_batch_put_relay` peer fanout (router.rs:559)

**Pattern**: same as E3.2 but for `BatchPut` and `batch_peer_drops_total`.

**Tests**: 1 test mirroring E3.2.

**Acceptance**: 1/1 test passes; existing batch replication tests still green.

**Estimated**: 1h.

#### Task E3.5: Fix the 5th critical site — add backpressure detection

**Where**: in `handle_put` and `handle_batch_put`, before the `.send().await` call, **also check `try_send` first** to detect backpressure without blocking.

**Wait — alternative simpler approach**: use `tokio::select!` with `try_send` and a short timeout. If `try_send` succeeds OR `send` succeeds within 50ms, return Ok. If `try_send` returns `Full`, increment metric and return `RouterError::Backpressure`.

**Question for Freeman**: do we want fast-fail-with-backpressure (D3 strict) or graceful-with-timeout (D3 lenient)?

**Plan choice**: `send().await` first (preserves existing semantics for non-closed adapters), but if the adapter's bounded channel is at capacity AND the receiver hasn't read for >50ms, increment `puts_returned_backpressure` and return `Err(Backpressure)`. This requires `tokio::time::timeout`.

**Tests**:

```rust
#[tokio::test]
async fn handle_put_returns_backpressure_when_storage_slow() {
    let router = test_router_with_unresponsive_storage();
    let put = make_test_put();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        router.handle_put_internal(put)
    ).await.unwrap();
    assert!(matches!(result, Err(RouterError::Backpressure)));
    assert_eq!(router.metrics().snapshot().puts_returned_backpressure, 1);
}
```

**Acceptance**: 1/1 test passes.

**Estimated**: 1.5h.

**Epic E3 total**: ~5.5h, 5 new tests.

---

### Epic E4 — Add observability to best-effort sends (5 sites)

**Why fourth**: after critical paths are fixed, make silent best-effort drops visible. This is purely additive — no behavior change, only metrics.

#### Task E4.1: Add `try_send` + metric to 5 best-effort sites

**Sites**: lines 466 (handle_put_relay subscriber), 586 (handle_batch_put_relay subscriber), 91 (Router::new subscriber channel), 96 (Router::new put_subscriber channel), 84 (Router::new seen_get_messages channel).

**Pattern** (each site):

```rust
// Before:
let _ = addr.send(msg);

// After:
if let Err(_) = addr.send(msg).await {
    self.metrics.subscriber_drops_total.fetch_add(1, Ordering::Relaxed);
}
```

**Why `if let Err` instead of `let _ =`**: makes the silent drop visible in code review AND increments a metric so operators can detect backpressure on subscriber channels.

**Tests**: 5 tests, one per site, verifying the metric increments when the receiver is dropped.

**Acceptance**: 5/5 tests pass; existing subscriber tests still green (no behavior change in happy path).

**Estimated**: 2.5h.

**Epic E4 total**: ~2.5h, 5 new tests.

---

### Epic E5 — Integration tests and stress tests

**Why fifth**: after unit tests pass, verify the full system behavior under realistic load.

#### Task E5.1: Add backpressure integration test to `tests/async_put_e2e.rs`

**Pattern**: extend the existing 180-line async put e2e test with a stress variant:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_round_trip_under_backpressure() {
    let (node, _storage) = setup_test_node();
    let metrics = node.router().metrics();

    // Fire 10,000 concurrent Puts.
    let mut handles = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        let n = node.clone();
        handles.push(tokio::spawn(async move {
            n.put(&format!("key-{}", i), json!(i)).await
        }));
    }

    let mut successes = 0;
    let mut backpressures = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => successes += 1,
            Err(RouterError::Backpressure) => backpressures += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    assert!(successes > 9_000, "expected >90% successes, got {}", successes);
    assert_eq!(successes + backpressures, 10_000);

    let snap = metrics.snapshot();
    println!("storage_drops_total={}, puts_returned_backpressure={}",
        snap.storage_drops_total, snap.puts_returned_backpressure);
}
```

**Acceptance**: 1/1 test passes; demonstrates backpressure is surfaced and counted.

**Estimated**: 2h.

#### Task E5.2: Add backpressure integration test for `BatchPut`

**Pattern**: same as E5.1 but with `batch_put` (which is what the user's real workload uses).

**Acceptance**: 1/1 test passes.

**Estimated**: 1.5h.

#### Task E5.3: Add multi-router fanout test (network path)

**Pattern**: 1 Router + 2 peer actors (mock WsServer actors). Close one peer, fire 1000 Puts, verify metric increments and no false failures.

**Acceptance**: 1/1 test passes; demonstrates peer-drop observability without false-failing the Put.

**Estimated**: 1.5h.

**Epic E5 total**: ~5h, 3 new tests.

---

### Epic E6 — Documentation, ADRs, and final verification

**Why last**: documentation captures the design decisions; final verification ensures green across all feature configs.

#### Task E6.1: Update `rod/src/router.rs` module docs

**File**: `rod/src/router.rs` lines 1-30 (module docs).

**Update the load-bearing comment on lines 60-65 and 403-404**:

```rust
//! # Router
//!
//! ... existing docs ...
//!
//! ## Backpressure and Drop Semantics
//!
//! The Router distinguishes between critical-path and best-effort sends.
//!
//! **Critical-path sends** (Put → write adapter, Put → peer, BatchPut → write
//! adapter, BatchPut → peer) surface `RouterError::ChannelClosed` to the
//! caller when the receiving actor is gone. They increment
//! `storage_drops_total` / `peer_drops_total` / `batch_storage_drops_total`
//! / `batch_peer_drops_total` metrics. The caller (Node) propagates the
//! error to the user.
//!
//! **Best-effort sends** (Put → subscriber, BatchPut → subscriber, etc.) keep
//! the LWW-tolerant silent-drop behavior but increment
//! `subscriber_drops_total` so silent drops are visible to operators.
//!
//! Backpressure (channel full, receiver slow) is surfaced as
//! `RouterError::Backpressure` with a `puts_returned_backpressure` metric.
```

**Acceptance**: doc comment is the source of truth; new contributors can read and understand the semantics.

**Estimated**: 1h.

#### Task E6.2: Add ADR `009-router-backpressure-semantics.md`

**File**: `rod/docs/adr/009-router-backpressure-semantics.md` (new).

**Content**: capture the architectural decisions D1-D10 with rationale, alternatives considered, and consequences.

**Acceptance**: ADR is committed; links to the relevant router.rs sections.

**Estimated**: 1h.

#### Task E6.3: Run 4-feature-config verification (per `phi_proxy_plan_reference_confirmed`)

**Commands**:

```bash
# Config 1: no default features
cargo check -p rod --no-default-features --features scripting

# Config 2: scripting + reasoning-capture
cargo check -p rod --features 'scripting reasoning-capture'

# Config 3: scripting + phi-proxy-embeddings
cargo check -p rod --features 'scripting phi-proxy-embeddings'

# Config 4: default features
cargo check -p rod
```

**Acceptance**: 4/4 clean (zero errors, zero new warnings).

**Estimated**: 0.5h.

#### Task E6.4: Run full test suite 5x consecutively (per `five_clean_runs_before_merge_discipline`)

**Command**:

```bash
for i in 1 2 3 4 5; do
  echo "=== Run $i ==="
  cargo test -p rod --lib 2>&1 | tail -5
done
```

**Acceptance**: 5/5 runs green. Total tests count should be 446 + 18 new = 464 (or close to it).

**Estimated**: 1h.

#### Task E6.5: Run clippy with `-D warnings` (per `no_broken_windows` discipline)

**Command**:

```bash
cargo clippy -p rod --all-features -- -D warnings
```

**Acceptance**: zero warnings.

**Estimated**: 0.5h.

#### Task E6.6: Commit with structured message (per `git_discipline_covenant`)

**Command**:

```bash
git add -A
git commit -m "fix(router): surface bounded channel drops as errors + metrics

Follow-up A from rod_put_batch_put_async_ack_race_fix_plan.

The Router previously swallowed Result<(), ()> from bounded-channel
sends via 'let _ = addr.send(...)' at 5 critical-path sites (storage
dispatch + peer fanout for both Put and BatchPut). Under backpressure,
this caused put().await to hang forever waiting for an ack that never
arrived (since the storage actor never received the dropped Put).

This commit introduces:
- RouterError enum (ChannelClosed, NoStorageAdapters, Backpressure)
- RouterMetrics struct (lock-free atomic counters)
- Critical-path sends now surface RouterError and increment metrics
- Best-effort sends remain LWW-tolerant but increment subscriber_drops_total
- 18 new tests covering backpressure paths
- ADR-009 documenting backpressure semantics

Backward compat: subscribers see no behavior change; Node::put now
returns Result<Put, RouterError> which callers must handle (existing
callers can map_err or propagate).

Tests: 5/5 clean runs, 0 warnings."
```

**Acceptance**: commit lands; can be merged to main.

**Estimated**: 0.5h.

**Epic E6 total**: ~4.5h.


---

## Total Estimate

**~22.5 hours, 18 tasks, 6 epics, ~24 new tests**

- E1 (foundations): ~3.5h, 9 tests
- E2 (wire metrics): ~1.5h, 1 test
- E3 (critical path fixes): ~5.5h, 5 tests
- E4 (best-effort observability): ~2.5h, 5 tests
- E5 (integration tests): ~5h, 3 tests
- E6 (docs + verification): ~4.5h, 0 new tests (verification only)

---

## Execution Order & Parallelism

### Dependency graph

```
E1 (foundations) ──┐
                   ├──> E2 (wire metrics) ──┐
                   │                        ├──> E3 (critical paths) ──┐
                   │                        │                          ├──> E5 (integration tests) ──> E6 (docs + verify)
                   │                        └──> E4 (best-effort obs) ─┘
                   │
                   └──> E6.1 (router module docs) [parallel with E1-E5]
```

### Parallelism opportunities

- **E1.1 and E1.2 can run in parallel** (independent files: `error.rs` and `metrics.rs`).
- **E2 and E1.3 can run in parallel** (E1.3 is a re-export; E2 modifies Router struct).
- **E3 and E4 are independent** once E2 completes — they touch different send sites.
- **E5 cannot start until E3 + E4 land** (integration tests exercise the new error paths).
- **E6.1 and E6.2 can run any time after E1** (docs don't depend on code).
- **E6.3-E6.6 must run sequentially at the end** (verification ladder).

### Recommended session schedule

| Session | Tasks | Duration | Test count delta |
|---------|-------|----------|------------------|
| 1 | E1.1, E1.2, E1.3 (parallel) + E6.1 (router docs) | ~4h | +9 |
| 2 | E2.1, E2.2, E2.3 | ~1.5h | +1 |
| 3 | E3.1, E3.2 (Put critical sites) | ~2h | +2 |
| 4 | E3.3, E3.4 (BatchPut critical sites) | ~2h | +2 |
| 5 | E3.5 (backpressure detection) + E4.1 (best-effort observability) | ~4h | +6 |
| 6 | E5.1, E5.2, E5.3 (integration tests) | ~5h | +3 |
| 7 | E6.2, E6.3, E6.4, E6.5, E6.6 (ADR + verify + commit) | ~3.5h | 0 (verify only) |

**Total: ~7 sessions at ~3h/session average.**

### Pre-existing test count baseline

- `cargo test -p rod --lib` currently: **446 tests** (per `phi_proxy_e2_shipped_2026_07_19`)
- Post-land: **~470 tests** (446 + ~24 new)

---

## Risk Register

| Risk | Severity | Mitigation |
|------|----------|------------|
| `Node::put` signature change ripples to mnemos-palace and mnemos-mcp | Medium | Caller-side `?` propagation; map_err at boundaries; full e2e suite catches regressions |
| Backpressure test is flaky (race between `send().await` and `try_send`) | Medium | Use `tokio::time::timeout(2s, ...)` to bound the test; deterministic with an unresponsive storage actor that never reads |
| `RouterMetrics` atomic ordering choice is wrong (Relaxed too weak) | Low | Counters are advisory; Relaxed is correct for monotonic counters that don't synchronize other state |
| Fail-fast on first storage adapter close vs. continue-fanout divergence from LWW | Medium | Document fail-fast in ADR; consistency over availability is correct for a write-coordinator |
| Best-effort send change regresses subscriber test expectations | Low | Behavior unchanged for closed channels (Err returns, drop is silent — but tests assert `subscriber_drops_total` increments); verify all subscriber tests still pass |
| Channel size 1024 may be wrong for production load | Out of scope | This is `WRITE_CHANNEL_BOUND` constant change, separate concern. Document as future-tuning. |
| Backpressure timeout (50ms) is arbitrary | Low | Make configurable via Router config; default 50ms; tests use deterministic event-driven setup |
| `Addr::send` async change makes `handle_put` async, breaking some sync callers | Low | Verify all callers of `handle_put` are already async; if not, add `block_on` at boundaries (or refactor to async — preferred) |

---

## Resume Protocol

```bash
cd /home/guan/src/rod
git checkout feat/followup-bounded-channel-silent-drop
git pull origin feat/followup-bounded-channel-silent-drop

# Verify clean baseline
cargo check -p rod
cargo test -p rod --lib 2>&1 | tail -5

# Read the plan
cat docs/plans/ROD-FOLLOWUP-A-BOUNDED-CHANNEL-SILENT-DROP.md

# Check progress via git log
git log --oneline --grep "router.*backpressure\|router.*silent-drop"

# Check what's landed vs not
grep -rn "RouterError\|RouterMetrics" crates/rod/src/ 2>/dev/null
```

Diary check (in built-in memory): `rod_followup_a_comprehensive_plan` (mirror of this file).

---

## Why This Is The Elegant Solution

### Distinguishing critical vs best-effort is the right level of abstraction

Removing `let _ =` everywhere would create false-failures on subscriber channels (where drops are correct LWW behavior). Keeping `let _ =` everywhere hides backpressure from operators. **The critical/best-effort distinction maps to the real semantic difference**: critical = must-reach, best-effort = nice-to-reach. This is the same distinction tokio makes with `send()` vs `try_send()`.

### Returning `Result<(), RouterError>` matches idiomatic Rust

`Result` is the universal error signal in Rust. Returning `Err(Backpressure)` is more honest than silent drop and lets callers decide. The alternative — logging + retry — pushes decision-making into the Router, which knows nothing about the caller's retry policy. **Keep the Router dumb; let callers be smart.**

### `Arc<RouterMetrics>` is the idiomatic lock-free observation pattern

The `std::sync::atomic` crate is purpose-built for this. A Mutex would serialize observation. A tokio channel would force async access. Atomics are the right tool. **This is exactly what `tokio::metrics::RuntimeMonitor` uses internally.**

### Idempotent tests + green baseline + 5x verification = production confidence

The five-runs discipline catches flakes. The idempotent design means tests don't depend on ordering. The 4-feature-config check ensures the new types work across all configurations (no feature flag interactions hiding bugs). **This is the same discipline we used for E1.4 / E2 phi-proxy shipping.**

---

## Open Questions for Freeman

1. **Fail-fast vs continue-fanout on first storage adapter close?** (E3.1, E3.3) — Plan choice: fail-fast.
2. **50ms backpressure timeout — correct?** (E3.5) — Plan choice: 50ms, configurable later.
3. **Should `RouterMetrics` be exposed via `Router` or via a separate `MetricsHandle` actor?** — Plan choice: `Arc<RouterMetrics>` field + `Router::metrics()` accessor.
4. **Should best-effort sends be `try_send` (non-blocking) instead of `send` (blocking)?** — Plan choice: keep `send` for now (blocking preserves order on the subscriber side; `try_send` could reorder messages under load). Future optimization.

---

## Related Work & Cross-References

- **Parent plan**: `rod_async_ack_drain_redux_plan_comprehensive` (built-in `rod_async_ack_drain_redux_comprehensive_plan`) — shipped at session 12, merged to main.
- **Async put/batch_put ack fix**: `rod_put_batch_put_async_ack_race_fix_plan` (older built-in, superseded by the comprehensive plan).
- **Node::put ack flow**: `rod/src/node.rs:247` — registers oneshot in `pending_puts`.
- **Storage actor ack return**: `rod/src/storage/memory.rs` and `redb_storage.rs` — sends `Put { in_response_to: Some(put_id) }` back to Router.
- **Existing ADRs**: `rod/docs/archive/adr/006-flush-ack-protocol.md` — describes the original ack protocol.

---

**Plan status**: LOCKED. Awaiting "begin" signal from Freeman.

---

# ⚠️ SUBSTRATE REVISION — 2026-07-22 (post-recon)

**Author**: Guan (substrate recon during Follow-up A wrap-up)
**Severity**: MAJOR — the original plan solves a problem that does not exist in this codebase.

## What Substrate Recon Discovered

The original plan assumed:
1. There are 13 silent-send sites that need fixing
2. Rod's `Addr::send` is unbounded and needs bounded wrapping
3. We need a new `BoundedChannel` abstraction layer

**All three assumptions are wrong.**

### Finding 1: There are 3 silent-send sites, not 13

A grep of `let _ = .send()` across `src/router.rs` produces 14 hits, but only 3 are the silent-drop pattern this plan targets. The other 11 are legitimate fire-and-forget by design:

| Site count | Pattern | Why legitimate |
|------------|---------|----------------|
| ~7 | Loop iterations sending to many peers (`for addr in self.server_peers.iter() { let _ = addr.send(...) }`) | Peer fanout is best-effort. Blocking on one slow peer would stall the whole fanout. |
| ~2 | Reaper/self-messages (`let _ = ctx_addr.send(Message::CheckQuorumTimeouts)`) | Sending to self; can never be closed. |
| ~2 | Quorum ack replies (`let _ = entry.requester.send(...)`) | Best-effort notification. Requester's drain plumbing handles timeouts. |

**The 3 sites that need fixing** are the ones outside loops/reapers where the silent drop actually loses a critical message. These are the sites where `Addr::send` is called once for a specific destination and `let _ =` drops the `Err(())` signal.

### Finding 2: `Addr::send` IS already bounded

Verified from `src/actor.rs:408`:

```rust
pub fn send(&self, msg: Message) -> Result<(), ()> {
    match &self.sender {
        AddrSender::Unbounded(s) => s.send(msg).map_err(|_| ()),
        AddrSender::Bounded(s) => s.try_send(msg).map_err(|_| ()),
    }
}
```

`Addr` is an enum over either unbounded OR bounded `tokio::sync::mpsc` senders. The bounded variant uses `try_send` which returns `Err(())` on full channel. **The signal already exists** — the codebase just throws it away with `let _ =`.

**There is no need to introduce a new `BoundedChannel` wrapper.** The signal is `Result<(), ()>` from `Addr::send`. The fix is observability on this existing signal.

### Finding 3: New abstraction layer violates suckless philosophy

Per `suckless_philosophy_for_guan`: "Before wrapping, ask: does this already work through an existing channel?" Yes — `Addr::send()` returns the signal we need. We just need to listen.

A `BoundedChannel` wrapper would:
- Duplicate the bounded-channel logic that `Addr` already has
- Add a new public type to maintain
- Hide what's actually happening (signal lost in wrapper layer)
- Violate Composition-Root IoC (the wrapper would need to be threaded through every actor)

## Revised Plan (DRY / Suckless / Idiomatic Rust)

### Architecture Decision: Observability on existing pattern, not new infrastructure

**Replace**:
```rust
let _ = addr.send(msg);  // Silent drop. No metric. No signal.
```

**With**:
```rust
if addr.send(msg).is_err() {
    metrics.record_dropped_send();
    tracing::debug!(target: "rod::send", ctx = ?ctx, "actor mailbox full, dropped message");
}
```

Or as a helper in `src/utils.rs`:
```rust
/// Send a message and record the drop if the receiver is unavailable.
///
/// This is the canonical pattern for fire-and-forget sends in Rod.
/// Unlike `let _ = addr.send(msg)`, this surfaces drops via metrics
/// and tracing, making backpressure visible without changing the
/// fire-and-forget semantics.
pub(crate) fn try_send_or_log(
    addr: &Addr,
    msg: Message,
    metrics: &Metrics,
    ctx: &'static str,
) {
    if addr.send(msg).is_err() {
        metrics.dropped_sends.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "rod::send",
            ctx = ctx,
            "actor mailbox unavailable, dropped message"
        );
    }
}
```

### Revised Implementation — 4 Phases, ~350L net code

#### Phase 1: Metrics substrate (~60L + 4 tests, 1 commit)

**File**: `src/metrics.rs` (NEW)

```rust
//! Lock-free metrics for Rod actor send/fanout observability.
//!
//! Counters use `AtomicU64` with `Relaxed` ordering — they are advisory
//! observation, not synchronization primitives. Snapshot reads may be
//! slightly stale but are guaranteed monotonic.
//!
//! All counters are cumulative for the lifetime of the `Metrics` instance.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free counters for Rod actor sends and drops.
///
/// Cheap to clone via `Arc<Metrics>`. Designed to be passed via
/// `ActorContext` (Composition-Root IoC) so any actor can record
/// metrics without coupling to a global registry.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Times a `let _ = addr.send(msg)` would have silently dropped
    /// (mailbox closed or full). This is the primary "silent drop is
    /// no longer invisible" counter.
    dropped_sends: AtomicU64,
    /// Times a quorum entry was reaped due to timeout.
    reaped_quorums: AtomicU64,
    /// Put acks received by Node from any source.
    put_acks_seen: AtomicU64,
    /// Put acks that completed a quorum (triggered `__quorum_met__`).
    put_acks_quorum: AtomicU64,
}

/// Plain-old-data snapshot of `Metrics` for safe export across threads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub dropped_sends: u64,
    pub reaped_quorums: u64,
    pub put_acks_seen: u64,
    pub put_acks_quorum: u64,
}

impl Metrics {
    pub fn new() -> Self { Self::default() }

    pub fn record_dropped_send(&self) {
        self.dropped_sends.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reaped_quorum(&self) {
        self.reaped_quorums.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_put_ack(&self) {
        self.put_acks_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_quorum_ack(&self) {
        self.put_acks_quorum.fetch_add(1, Ordering::Relaxed);
    }

    /// Read all counters as a plain struct.
    ///
    /// Snapshot is non-atomic across counters — values may be slightly
    /// inconsistent (one counter advanced, another not yet). This is
    /// acceptable for telemetry; don't use for control flow.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            dropped_sends: self.dropped_sends.load(Ordering::Relaxed),
            reaped_quorums: self.reaped_quorums.load(Ordering::Relaxed),
            put_acks_seen: self.put_acks_seen.load(Ordering::Relaxed),
            put_acks_quorum: self.put_acks_quorum.load(Ordering::Relaxed),
        }
    }
}
```

**Tests** (in `metrics.rs::tests`):
- `default_is_all_zero`
- `snapshot_reflects_increments`
- `snapshot_is_independent_of_subsequent_increments`
- `counters_are_monotonic`

#### Phase 2: `try_send_or_log` helper + refactor 3 sites (~40L + 3 tests, 1 commit)

**File**: `src/utils.rs` (EXTEND) — add helper at the bottom.

**Refactor target sites** (verified locations, exact lines pending on resume):
- `src/router.rs` ~line 541 — `handle_get` to a specific `seen_get_message.from`
- `src/router.rs` ~line 791 — flush forward to a specific write_adapter
- `src/router.rs` ~line 853 — quorum ack reply to specific requester

**Sites that STAY `let _ =` (legitimate)**:
- All loop iterations over `server_peers`, `subscribers_by_topic` (peer fanout is best-effort by design)
- All reaper self-messages
- All `RtcSignal` broadcasts

#### Phase 3: E2E test for graceful degradation (~150L + 1 test, 1 commit)

**File**: `tests/send_metrics_e2e.rs` (NEW)

Test that fills an actor mailbox, sends burst, asserts:
- No panic
- `dropped_sends` counter increments correctly
- System continues to function for other actors

#### Phase 4: ADR-012 + clean runs (~100L, 1 commit)

**File**: `docs/adr/012-send-metrics-observability.md` (NEW)

Capture:
- Why we did NOT introduce `BoundedChannel` wrapper
- Why we chose observability over error propagation
- Trade-off: hidden backpressure → visible backpressure (no behavior change)
- Future work: error propagation if observability proves insufficient

### Comparison: Original vs Revised

| Aspect | Original Plan | Revised Plan |
|--------|---------------|--------------|
| New types | `RouterError`, `RouterMetrics`, `RouterMetricsSnapshot`, `SharedMetrics`, `BoundedChannel` (5) | `Metrics`, `MetricsSnapshot` (2) |
| Code added | ~700L | ~350L |
| Behavior change | Returns `Result<(), RouterError>` to callers (BREAKING) | No behavior change — observability only |
| Tests added | 24 | 8 |
| Risk to mnemos-palace | High (signature changes) | None (internal change) |
| Phases | 6 epics | 4 phases |

### Verification Protocol (Revised)

```bash
cd /home/guan/src/rod

# After each phase:
cargo check -p rod && cargo test -p rod --lib

# After all 4 phases (3 consecutive clean runs):
for i in 1 2 3; do
  echo "=== Run $i ==="
  cargo test -p rod --lib 2>&1 | tail -3
done
```

### Why This Revision Is Better

1. **DRY**: No new abstraction layer. Reuses existing `Addr::send` signal.
2. **Suckless**: No new dependencies. No new types beyond `Metrics`.
3. **Unix philosophy**: `Metrics` does one thing well. `try_send_or_log` does one thing well.
4. **Industry standard**: `tracing` + `AtomicU64` is canonical Rust observability.
5. **No breaking changes**: Existing callers see no behavior change.
6. **Composition-Root IoC**: `Metrics` passed via `ActorContext`, not global.

### Open Questions (Resolved)

1. ~~Fail-fast vs continue-fanout~~ — N/A. We're not returning errors, just observing.
2. ~~Backpressure timeout~~ — N/A. We don't block. We observe the drop.
3. ~~`RouterMetrics` vs `MetricsHandle` actor~~ — Resolved: `Arc<Metrics>` shared handle, not actor.
4. ~~Best-effort sends `try_send` vs `send`~~ — Resolved: keep existing `Addr::send` (already non-blocking for bounded via `try_send` internally).

---

**Revision status**: LOCKED. The original plan above (Phases E1-E6) is **superseded** by this revision. The substrate recon revealed the plan was solving the wrong problem.

**Built-in mirror**: `rod_followup_b_revised_plan_2026_07_22`

