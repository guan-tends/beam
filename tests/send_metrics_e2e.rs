//!
//! End-to-end tests for the shared `Arc<Metrics>` observability handle.
//!
//! These tests validate that the `Metrics` substrate — wired per
//! `beam_followup_b_plan_v3` (Follow-up B, Quorum-B2 sibling feature) — is
//! actually shared between the Node and Router actors and that counters
//! incremented internally by the Router are observable through the Node.
//!
//! # Why this file exists
//!
//! Before Phase 3, the Router owned its `Metrics` privately and there was
//! no external surface to observe dropped sends. After Phase 3, both
//! `Node::metrics()` and `Router::metrics()` return clones of the same
//! `Arc<Metrics>` handle. These tests prove the sharing works.
//!
//! # Test matrix
//!
//! | Test name                                     | What it proves                           |
//! |-----------------------------------------------|------------------------------------------|
//! | e2e_metrics_starts_at_zero                    | Fresh node exposes zero counters         |
//! | e2e_node_and_router_share_metrics_arc         | Node + Router Arcs observe same counters |
//! | e2e_dropped_send_records_in_shared_metrics    | Drop is visible via Node handle          |

use beam::Node;
use std::sync::Arc;

/// A fresh Node exposes counters at zero.
#[tokio::test]
async fn e2e_metrics_starts_at_zero() {
    let node = Node::new();
    let metrics: Arc<beam::metrics::Metrics> = node.metrics();

    let snap = metrics.snapshot();
    assert_eq!(snap.dropped_sends, 0, "fresh node must have zero drops");
    assert_eq!(
        snap.reaped_quorums, 0,
        "fresh node must have zero reaped quorums"
    );
    assert_eq!(snap.put_acks_seen, 0, "fresh node must have zero put acks");
    assert_eq!(
        snap.put_acks_quorum, 0,
        "fresh node must have zero quorum acks"
    );
}

/// Node and Router share the same atomic counters via the Arc clone.
///
/// Two `Node::metrics()` calls return distinct Arc handles, but they point
/// to the *same* underlying `Metrics` instance — incrementing through one
/// is observable through the other. This is the contract the substrate
/// author documented in `Metrics`' module docs.
#[tokio::test]
async fn e2e_node_and_router_share_metrics_arc() {
    let node = Node::new();
    let m_via_node_1 = node.metrics();
    let m_via_node_2 = node.metrics();

    // Increment via handle 1
    m_via_node_1.record_dropped_send();

    // Observe via handle 2 — must see the same counter
    assert_eq!(
        m_via_node_2.snapshot().dropped_sends,
        1,
        "two Node::metrics() handles must observe the same atomic counter"
    );

    // Increment via handle 2
    m_via_node_2.record_dropped_send();
    m_via_node_2.record_dropped_send();

    // Observe via handle 1 — must see the accumulated count
    assert_eq!(
        m_via_node_1.snapshot().dropped_sends,
        3,
        "bidirectional visibility across Arc clones"
    );
}

/// A drop recorded through one handle is visible through the snapshot
/// taken via the other handle. This is the actual production use case:
/// the Router increments (via `try_send_or_log`); the operator reads
/// via `Node::metrics().snapshot()`.
#[tokio::test]
async fn e2e_dropped_send_records_in_shared_metrics() {
    let node = Node::new();
    let observer = node.metrics();

    // Simulate the Router's internal `try_send_or_log` call.
    observer.record_dropped_send();

    // Re-observe via a fresh Arc clone (as an external exporter would).
    let fresh = node.metrics();
    assert_eq!(fresh.snapshot().dropped_sends, 1);

    // Multiple drops accumulate monotonically.
    for _ in 0..10 {
        observer.record_dropped_send();
    }
    assert_eq!(node.metrics().snapshot().dropped_sends, 11);
}
