//! Lock-free metrics for BEAM actor send/fanout observability.
//!
//! # Why this exists
//!
//! Before this module, the Router (and other actors) used `let _ = addr.send(msg)`
//! everywhere to dispatch messages. When an actor's mailbox was full or closed,
//! `Addr::send` returned `Err(())` and the codebase silently dropped it.
//!
//! Silent drops are dangerous because:
//!
//! 1. **Invisible failures**: a "successful" `Node::put` might never reach
//!    the storage adapter if its mailbox is full. The caller has no signal.
//! 2. **No debugging trail**: when data doesn't replicate, there's no
//!    counter showing "1000 Puts were silently dropped" — the operator
//!    sees a working system that silently lost data.
//! 3. **No backpressure visibility**: a slow consumer looks identical to
//!    a fast one until messages start disappearing into the void.
//!
//! This module fixes the observability gap without changing behavior.
//! `Metrics` is a tiny, lock-free counter struct that any actor can hold
//! (via `ActorContext` or directly) to record events of interest.
//!
//! # Design principles
//!
//! - **Lock-free**: all counters are `AtomicU64` with `Relaxed` ordering.
//!   They are advisory observation, not synchronization primitives.
//!   Snapshot reads may be slightly stale but are guaranteed monotonic.
//! - **Cumulative**: counters only increase for the lifetime of the
//!   `Metrics` instance. There is no "reset" — if you need per-window
//!   counters, create a new `Metrics`.
//! - **Composition-Root IoC**: `Metrics` is passed into actors that need
//!   it, not accessed from a global registry. This makes the dependency
//!   graph explicit and testable.
//! - **No behavior change**: recording a metric is a side effect that
//!   does not affect control flow. Existing fire-and-forget semantics
//!   are preserved.
//!
//! # Usage
//!
//! ```no_run
//! use beam::metrics::Metrics;
//! use std::sync::Arc;
//!
//! let metrics = Arc::new(Metrics::new());
//!
//! // Record a drop
//! metrics.record_dropped_send();
//!
//! // Snapshot for telemetry
//! let snap = metrics.snapshot();
//! println!("dropped_sends = {}", snap.dropped_sends);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free counters for BEAM actor sends and drops.
///
/// Cheap to clone via `Arc<Metrics>`. Designed to be passed via
/// `ActorContext` (Composition-Root IoC) so any actor can record
/// metrics without coupling to a global registry.
///
/// All counters are cumulative for the lifetime of the `Metrics`
/// instance. They never reset — start a new `Metrics` if you need
/// per-window observation.
///
/// # Counter semantics
///
/// - `dropped_sends`: incremented when `Addr::send` returns `Err(())`
///   in a fire-and-forget context. This is the primary "silent drop
///   is no longer invisible" counter.
/// - `reaped_quorums`: incremented when the quorum reaper evicts an
///   expired entry. Indicates that quorum timeouts are happening.
/// - `put_acks_seen`: incremented when a Node receives any Put ack.
/// - `put_acks_quorum`: incremented when a Put ack completes a quorum
///   (the `__quorum_met__` sentinel fired).
#[derive(Debug, Default)]
pub struct Metrics {
    /// Times a fire-and-forget send was silently dropped because the
    /// receiver's mailbox was full or closed.
    dropped_sends: AtomicU64,
    /// Times the quorum reaper evicted an expired entry.
    reaped_quorums: AtomicU64,
    /// Put acks received by any Node (from storage or peer).
    put_acks_seen: AtomicU64,
    /// Put acks that completed a quorum (triggered `__quorum_met__`).
    put_acks_quorum: AtomicU64,
}

/// Plain-old-data snapshot of `Metrics` for safe export across threads.
///
/// This is `Copy` because it holds plain `u64` values — no atomics,
/// no references. Safe to log, serialize, or send over a channel.
///
/// Note: snapshot is non-atomic across counters — values may be
/// slightly inconsistent (one counter advanced, another not yet).
/// This is acceptable for telemetry; do not use for control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub dropped_sends: u64,
    pub reaped_quorums: u64,
    pub put_acks_seen: u64,
    pub put_acks_quorum: u64,
}

impl Metrics {
    /// Create a new `Metrics` with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a fire-and-forget send was dropped.
    ///
    /// Call this when `Addr::send(msg)` returns `Err(())` in a
    /// context where you accept the loss but want to know it happened.
    #[inline]
    pub fn record_dropped_send(&self) {
        self.dropped_sends.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the quorum reaper evicted an expired entry.
    #[inline]
    pub fn record_reaped_quorum(&self) {
        self.reaped_quorums.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a Put ack was received.
    #[inline]
    pub fn record_put_ack(&self) {
        self.put_acks_seen.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a Put ack completed a quorum.
    #[inline]
    pub fn record_quorum_ack(&self) {
        self.put_acks_quorum.fetch_add(1, Ordering::Relaxed);
    }

    /// Read all counters as a plain struct.
    ///
    /// Non-atomic across counters — values may be slightly inconsistent.
    /// Acceptable for telemetry; do not use for control flow.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            dropped_sends: self.dropped_sends.load(Ordering::Relaxed),
            reaped_quorums: self.reaped_quorums.load(Ordering::Relaxed),
            put_acks_seen: self.put_acks_seen.load(Ordering::Relaxed),
            put_acks_quorum: self.put_acks_quorum.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn default_is_all_zero() {
        let snap = Metrics::new().snapshot();
        assert_eq!(snap.dropped_sends, 0);
        assert_eq!(snap.reaped_quorums, 0);
        assert_eq!(snap.put_acks_seen, 0);
        assert_eq!(snap.put_acks_quorum, 0);
    }

    #[test]
    fn record_dropped_send_increments_counter() {
        let m = Metrics::new();
        m.record_dropped_send();
        m.record_dropped_send();
        m.record_dropped_send();
        assert_eq!(m.snapshot().dropped_sends, 3);
    }

    #[test]
    fn record_reaped_quorum_increments_counter() {
        let m = Metrics::new();
        m.record_reaped_quorum();
        assert_eq!(m.snapshot().reaped_quorums, 1);
    }

    #[test]
    fn record_put_ack_increments_counter() {
        let m = Metrics::new();
        m.record_put_ack();
        m.record_put_ack();
        assert_eq!(m.snapshot().put_acks_seen, 2);
    }

    #[test]
    fn record_quorum_ack_increments_counter() {
        let m = Metrics::new();
        m.record_quorum_ack();
        assert_eq!(m.snapshot().put_acks_quorum, 1);
    }

    #[test]
    fn snapshot_reflects_independent_increments() {
        let m = Metrics::new();
        m.record_dropped_send();
        m.record_put_ack();
        m.record_quorum_ack();
        let snap = m.snapshot();
        assert_eq!(snap.dropped_sends, 1);
        assert_eq!(snap.put_acks_seen, 1);
        assert_eq!(snap.put_acks_quorum, 1);
        assert_eq!(snap.reaped_quorums, 0);
    }

    #[test]
    fn counters_are_monotonic() {
        // Relaxed atomics guarantee that increments on a single
        // counter are not lost, but concurrent increments may be
        // reordered relative to each other. For a single-threaded
        // sequence, the counter must be strictly monotonic.
        let m = Metrics::new();
        for i in 1..=1000 {
            m.record_dropped_send();
            assert_eq!(m.snapshot().dropped_sends, i);
        }
    }

    #[test]
    fn shared_metrics_via_arc() {
        // Verify Arc<Metrics> is the idiomatic shared handle and
        // updates are visible across clones.
        let m: Arc<Metrics> = Arc::new(Metrics::new());
        let m2 = Arc::clone(&m);
        m.record_dropped_send();
        assert_eq!(m2.snapshot().dropped_sends, 1);
    }

    #[test]
    fn concurrent_increments_are_not_lost() {
        // Sanity check: 100 threads × 1000 increments = 100_000 total.
        // Relaxed ordering may interleave but no increment is lost.
        use std::thread;
        let m = Arc::new(Metrics::new());
        let mut handles = Vec::new();
        for _ in 0..100 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    m.record_dropped_send();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.snapshot().dropped_sends, 100_000);
    }

    #[test]
    fn snapshot_is_copy() {
        // Compile-time check: MetricsSnapshot is Copy.
        let s = Metrics::new().snapshot();
        let s2 = s; // Copy, not move
        assert_eq!(s, s2);
    }
}