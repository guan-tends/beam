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
//! # Hot-path instrumentation
//!
//! In addition to the original drop/ack counters, this module now tracks
//! the **relay hot path** — the sequence every message traverses from
//! WebSocket receive to WebSocket send. These counters let us identify
//! the load-bearing component when throughput is bottlenecked:
//!
//! 1. `messages_parsed` — JSON parse entry (`Message::try_from`)
//! 2. `messages_dropped_dup` — dedup gate hit (`Dup::check` returned true)
//! 3. `messages_relayed` — successful relay fan-out (`handle_put_relay`)
//! 4. `subscriber_fanout_total` — total subscriber deliveries across all relays
//! 5. `serialization_calls` — wire-format serialization (`Message::to_string`)
//! 6. `ws_messages_received` — inbound WebSocket frames
//! 7. `ws_messages_sent` — outbound WebSocket frames
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
//! - **Negligible cost**: `Relaxed` atomic increments are a single
//!   `LOCK` instruction on x86 (~1-2 ns). At 100k TPS the total overhead
//!   is ~0.2 ms/s — well within the noise floor of any benchmark.
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
//! println!("messages_parsed = {}", snap.messages_parsed);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free counters for BEAM actor sends, drops, and hot-path throughput.
///
/// Cheap to share via `Arc<Metrics>`. Designed to be passed via
/// `ActorContext` (Composition-Root IoC) so any actor can record
/// metrics without coupling to a global registry.
///
/// All counters are cumulative for the lifetime of the `Metrics`
/// instance. They never reset — start a new `Metrics` if you need
/// per-window observation.
///
/// # Counter semantics
///
/// ## Drop & quorum counters (original)
///
/// - `dropped_sends`: incremented when `Addr::send` returns `Err(())`
///   in a fire-and-forget context. The primary "silent drop is no longer
///   invisible" counter.
/// - `reaped_quorums`: incremented when the quorum reaper evicts an
///   expired entry. Indicates that quorum timeouts are happening.
/// - `put_acks_seen`: incremented when a Node receives any Put ack.
/// - `put_acks_quorum`: incremented when a Put ack completes a quorum
///   (the `__quorum_met__` sentinel fired).
///
/// ## Hot-path counters (v0.11.0)
///
/// These trace the relay hot path: WebSocket → parse → router → serialize → WebSocket.
/// Under load, the ratio between these counters reveals the bottleneck:
///
/// - If `messages_parsed` >> `messages_relayed` → dedup is dropping most messages
/// - If `messages_relayed` >> `ws_messages_sent` → serialization or I/O is the bottleneck
/// - If `ws_messages_received` ≈ `messages_parsed` → parse is keeping up
/// - `subscriber_fanout_total / messages_relayed` → average fanout ratio
#[derive(Debug, Default)]
pub struct Metrics {
    // ── Drop & quorum counters (original) ───────────────────────────
    /// Times a fire-and-forget send was silently dropped because the
    /// receiver's mailbox was full or closed.
    dropped_sends: AtomicU64,
    /// Times the quorum reaper evicted an expired entry.
    reaped_quorums: AtomicU64,
    /// Put acks received by any Node (from storage or peer).
    put_acks_seen: AtomicU64,
    /// Put acks that completed a quorum (triggered `__quorum_met__`).
    put_acks_quorum: AtomicU64,

    // ── Hot-path counters (v0.11.0) ─────────────────────────────────
    /// Times a wire message was parsed from JSON into a `Message` struct.
    ///
    /// Incremented in `Message::try_from` — the entry point of every
    /// inbound message. Under steady state, this should track
    /// `ws_messages_received` closely.
    messages_parsed: AtomicU64,

    /// Times a Put was relayed to peers/subscribers (successful fan-out).
    ///
    /// Incremented in `Router::handle_put_relay` after the relay
    /// completes. The delta between `messages_parsed` and
    /// `messages_relayed` includes dedup drops, quorum acks, and
    /// Get-response routing.
    messages_relayed: AtomicU64,

    /// Times a message was dropped because the dedup gate hit.
    ///
    /// Incremented in `Router::handle_put` when `Dup::check` returns
    /// true. A high ratio of `messages_dropped_dup / messages_parsed`
    /// means peers are redundantly relaying the same messages.
    messages_dropped_dup: AtomicU64,

    /// Times `Message::to_string` / `Put::to_string` was called for
    /// wire-format serialization.
    ///
    /// Incremented on every outbound serialization. Under steady
    /// state, this should track `ws_messages_sent` closely. A
    /// discrepancy means serializations are being called for internal
    /// purposes (logging, debugging) without reaching the wire.
    serialization_calls: AtomicU64,

    /// Total subscriber deliveries across all relay fan-outs.
    ///
    /// Incremented once per subscriber in `handle_put_relay`. The
    /// ratio `subscriber_fanout_total / messages_relayed` gives the
    /// average fanout ratio — how many subscribers receive each
    /// relayed message.
    subscriber_fanout_total: AtomicU64,

    /// Inbound WebSocket message frames received by all WsConn actors.
    ///
    /// Incremented in `WsConn::handle` on each incoming Text/Binary
    /// frame. Under steady state, this is the raw inbound rate before
    /// any processing.
    ws_messages_received: AtomicU64,

    /// Outbound WebSocket message frames sent by all WsConn actors.
    ///
    /// Incremented on each successful `WsConn` send. Under steady
    /// state with no dedup, this should be approximately
    /// `messages_relayed * subscriber_fanout_ratio`.
    ws_messages_sent: AtomicU64,
}

/// Plain-old-data snapshot of `Metrics` for safe export across threads.
///
/// This is `Copy` because it holds plain `u64` values — no atomics,
/// no references. Safe to log, serialize, or send over a channel.
///
/// Note: snapshot is non-atomic across counters — values may be
/// slightly inconsistent (one counter advanced, another not yet).
/// This is acceptable for telemetry; do not use for control flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct MetricsSnapshot {
    // Drop & quorum counters (original)
    pub dropped_sends: u64,
    pub reaped_quorums: u64,
    pub put_acks_seen: u64,
    pub put_acks_quorum: u64,
    // Hot-path counters (v0.11.0)
    pub messages_parsed: u64,
    pub messages_relayed: u64,
    pub messages_dropped_dup: u64,
    pub serialization_calls: u64,
    pub subscriber_fanout_total: u64,
    pub ws_messages_received: u64,
    pub ws_messages_sent: u64,
}

impl Metrics {
    /// Create a new `Metrics` with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Drop & quorum recording methods (original) ──────────────────

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

    // ── Hot-path recording methods (v0.11.0) ────────────────────────

    /// Record that a wire message was parsed from JSON into a `Message` struct.
    ///
    /// Called at the entry point of every inbound message —
    /// `Message::try_from`. This is the first counter in the hot path.
    #[inline]
    pub fn record_parsed(&self) {
        self.messages_parsed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a Put was successfully relayed to peers/subscribers.
    ///
    /// Called in `Router::handle_put_relay` after fan-out completes.
    #[inline]
    pub fn record_relayed(&self) {
        self.messages_relayed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a message was dropped by the dedup gate.
    ///
    /// Called in `Router::handle_put` when `Dup::check` returns true.
    #[inline]
    pub fn record_dropped_dup(&self) {
        self.messages_dropped_dup.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a wire-format serialization call.
    ///
    /// Called in `Message::to_string` / `Put::to_string`.
    #[inline]
    pub fn record_serialization(&self) {
        self.serialization_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record subscriber deliveries from a relay fan-out.
    ///
    /// Called in `Router::handle_put_relay` with the number of
    /// subscribers that received this message. Pass 0 if no
    /// subscribers were present (the relay still happened, just
    /// nobody was listening).
    #[inline]
    pub fn record_subscriber_fanout(&self, count: u64) {
        self.subscriber_fanout_total
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record an inbound WebSocket message frame.
    ///
    /// Called in `WsConn::handle` on each incoming Text or Binary frame.
    #[inline]
    pub fn record_ws_received(&self) {
        self.ws_messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an outbound WebSocket message frame.
    ///
    /// Called on each successful `WsConn` send.
    #[inline]
    pub fn record_ws_sent(&self) {
        self.ws_messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot ────────────────────────────────────────────────────

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
            messages_parsed: self.messages_parsed.load(Ordering::Relaxed),
            messages_relayed: self.messages_relayed.load(Ordering::Relaxed),
            messages_dropped_dup: self.messages_dropped_dup.load(Ordering::Relaxed),
            serialization_calls: self.serialization_calls.load(Ordering::Relaxed),
            subscriber_fanout_total: self.subscriber_fanout_total.load(Ordering::Relaxed),
            ws_messages_received: self.ws_messages_received.load(Ordering::Relaxed),
            ws_messages_sent: self.ws_messages_sent.load(Ordering::Relaxed),
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
        // Hot-path counters
        assert_eq!(snap.messages_parsed, 0);
        assert_eq!(snap.messages_relayed, 0);
        assert_eq!(snap.messages_dropped_dup, 0);
        assert_eq!(snap.serialization_calls, 0);
        assert_eq!(snap.subscriber_fanout_total, 0);
        assert_eq!(snap.ws_messages_received, 0);
        assert_eq!(snap.ws_messages_sent, 0);
    }

    // ── Original counter tests ──────────────────────────────────────

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

    // ── Hot-path counter tests ──────────────────────────────────────

    #[test]
    fn record_parsed_increments_counter() {
        let m = Metrics::new();
        m.record_parsed();
        m.record_parsed();
        assert_eq!(m.snapshot().messages_parsed, 2);
    }

    #[test]
    fn record_relayed_increments_counter() {
        let m = Metrics::new();
        m.record_relayed();
        assert_eq!(m.snapshot().messages_relayed, 1);
    }

    #[test]
    fn record_dropped_dup_increments_counter() {
        let m = Metrics::new();
        m.record_dropped_dup();
        m.record_dropped_dup();
        m.record_dropped_dup();
        assert_eq!(m.snapshot().messages_dropped_dup, 3);
    }

    #[test]
    fn record_serialization_increments_counter() {
        let m = Metrics::new();
        m.record_serialization();
        assert_eq!(m.snapshot().serialization_calls, 1);
    }

    #[test]
    fn record_subscriber_fanout_accumulates() {
        let m = Metrics::new();
        m.record_subscriber_fanout(5);
        m.record_subscriber_fanout(3);
        m.record_subscriber_fanout(0); // no subscribers — still relayed
        assert_eq!(m.snapshot().subscriber_fanout_total, 8);
    }

    #[test]
    fn record_ws_received_increments_counter() {
        let m = Metrics::new();
        for _ in 0..500 {
            m.record_ws_received();
        }
        assert_eq!(m.snapshot().ws_messages_received, 500);
    }

    #[test]
    fn record_ws_sent_increments_counter() {
        let m = Metrics::new();
        m.record_ws_sent();
        m.record_ws_sent();
        assert_eq!(m.snapshot().ws_messages_sent, 2);
    }

    // ── Cross-counter & invariant tests ─────────────────────────────

    #[test]
    fn snapshot_reflects_independent_increments() {
        let m = Metrics::new();
        m.record_dropped_send();
        m.record_put_ack();
        m.record_quorum_ack();
        m.record_parsed();
        m.record_relayed();
        m.record_serialization();
        let snap = m.snapshot();
        assert_eq!(snap.dropped_sends, 1);
        assert_eq!(snap.put_acks_seen, 1);
        assert_eq!(snap.put_acks_quorum, 1);
        assert_eq!(snap.reaped_quorums, 0);
        assert_eq!(snap.messages_parsed, 1);
        assert_eq!(snap.messages_relayed, 1);
        assert_eq!(snap.messages_dropped_dup, 0);
        assert_eq!(snap.serialization_calls, 1);
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
        m.record_parsed();
        assert_eq!(m2.snapshot().dropped_sends, 1);
        assert_eq!(m2.snapshot().messages_parsed, 1);
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
    fn concurrent_hot_path_increments_are_not_lost() {
        // Same concurrent test for a hot-path counter.
        use std::thread;
        let m = Arc::new(Metrics::new());
        let mut handles = Vec::new();
        for _ in 0..50 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for _ in 0..2000 {
                    m.record_parsed();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.snapshot().messages_parsed, 100_000);
    }

    #[test]
    fn snapshot_is_copy() {
        // Compile-time check: MetricsSnapshot is Copy.
        let s = Metrics::new().snapshot();
        let s2 = s; // Copy, not move
        assert_eq!(s, s2);
    }

    #[test]
    fn snapshot_serializes_to_json() {
        // Verify MetricsSnapshot can be serialized to JSON (for the
        // /metrics HTTP endpoint).
        let snap = Metrics::new().snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("dropped_sends"));
        assert!(json.contains("messages_parsed"));
        assert!(json.contains("ws_messages_sent"));
    }
}
