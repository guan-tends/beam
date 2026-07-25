//! Network fanout acknowledgement policy and result types.
//!
//! Implements Gun.js ask-pattern semantics for multi-peer replication: a
//! requester sends a [`Message::Put`](crate::message::Message::Put) to the
//! router, the router registers the put as a quorum-tracked write via
//! [`Message::RegisterQuorum`](crate::message::Message::RegisterQuorum),
//! the router fans the put out to peers, and tracks the per-peer acks
//! until an [`AckPolicy`] quorum is satisfied.
//!
//! # Wire format (Gun.js compatible)
//!
//! Peer acks reuse the existing `Put { in_response_to: Some(put_id), .. }`
//! wire — the `@` field in serialized JSON. Quorum completion is signalled
//! by a **`__quorum_met__` sentinel** in the reply's `updated_nodes`, with
//! the ack count as the value. This mirrors the existing `_ack`/`_err`
//! sentinel convention used by [`Node::put`](crate::Node::put) and
//! [`Node::batch_put`](crate::Node::batch_put), so callers can use the
//! same drain plumbing.
//!
//! # Reserved sentinel prefix
//!
//! `__rod__` is the reserved prefix for wire-level sentinels. Existing:
//! - `__rod_replay_complete__` — emitted by `map()` replay to signal drain complete
//!
//! New:
//! - `__quorum_met__` — emitted by Router when quorum threshold is reached
//!
//! The `__` prefix is filtered during normal data iteration (see
//! `Node::handle_put` reserved-prefix handling), so these sentinels never
//! collide with user data.
//!
//! # Lifecycle
//!
//! ```text
//!   Node::put_quorum(value, policy)
//!     ├── build Put, register oneshot in pending_puts
//!     ├── send Message::RegisterQuorum { put_id, requester, policy } to Router
//!     ├── send Message::Put(put) to Router
//!     │     ↓
//!     │   Router creates internal QuorumEntry for put_id
//!     │   Router::handle_put_relay fans out to peers (same as fire-and-forget)
//!     │     ↓
//!     │   Each peer eventually replies with Put { @: put_id, .. }
//!     │     ↓
//!     │   Router::handle_put sees Put.@, finds QuorumEntry, increments counter
//!     │   When counter >= policy.quorum → Router sends reply back to requester:
//!     │     Put { @: put_id, updated_nodes: { "__quorum_met__": ack_count } }
//!     └── requester's oneshot resolves with ReplicationStatus
//! ```
//!
//! # Why a sentinel, not a new Result variant
//!
//! The codebase has converged on sentinel-drain as the canonical ack
//! pattern (see `feat/beam-redux-async-ack-and-drain` branch):
//!
//! - `_ack`/`_err` sentinels for storage commit confirmation
//! - `__rod_replay_complete__` sentinel for replay drain
//!
//! Adding `__quorum_met__` keeps the drain plumbing DRY — the same
//! `pending_puts: Arc<RwLock<HashMap<String, oneshot::Sender<...>>>>` map
//! and `tokio::time::timeout` envelope that [`Node::put`](crate::Node::put)
//! uses are reused for [`Node::put_quorum`](crate::Node::put_quorum).
//! Only the *decoder* differs: instead of looking for `_ack`/`_err`,
//! `put_quorum` looks for `__quorum_met__`.
//!
//! # Quorum policies
//!
//! - [`AckPolicy::any`] — first ack wins (Gun.js default, fastest)
//! - [`AckPolicy::for_peer_count`] — ⌈N/2⌉ majority (Raft/Dynamo style)
//! - [`AckPolicy::all`] — every fan-out target must ack
//!
//! # Timeout
//!
//! Default timeout matches Gun.js `lack = 9000ms`. Configurable via
//! [`AckPolicy::with_timeout`]. On timeout the requester's oneshot resolves
//! with `Err("put_quorum timed out")` and the Router's internal
//! `QuorumEntry` is reaped lazily (removed on next access by another put
//! for the same id, or by the Router's periodic cleanup if added later).
//!
//! # Design constraints
//!
//! - **No new Message variant for the ack wire** — reuses
//!   `Put { in_response_to, .. }` for ack routing
//! - **One new Message variant: `RegisterQuorum`** — minimal struct with
//!   `(put_id, requester_addr, policy)`; used by the Router to create the
//!   `QuorumEntry` before fan-out
//! - **Sentinel-driven completion** — `__quorum_met__` in `updated_nodes`,
//!   matching the `_ack`/`_err` convention
//! - **DRY** — `Node::put_quorum` extracts a `put_internal` helper shared
//!   with `Node::put`, so the ack-drain plumbing is not duplicated
//! - **No new dependency** — uses existing `std`, `tokio`

use std::time::Duration;

/// Reserved sentinel key emitted by the Router when quorum threshold is met.
///
/// Stored in the reply Put's `updated_nodes` map with the ack count as the
/// value, e.g. `{"__quorum_met__": 3}` means 3 peers acked.
///
/// The `__` prefix is filtered during normal data iteration so this
/// sentinel never collides with user data.
pub const QUORUM_MET_SENTINEL: &str = "__quorum_met__";

/// Default timeout for quorum requests, matching Gun.js `lack = 9000ms`.
///
/// Gun.js's original ask pattern uses a 9-second lack to bound how long a
/// requester waits for an ack. We adopt the same default for wire-level
/// compatibility, but callers can override via
/// [`AckPolicy::with_timeout`].
pub const DEFAULT_QUORUM_TIMEOUT: Duration = Duration::from_millis(9000);

/// Policy controlling how many peer acks satisfy a `put_quorum` request.
///
/// Construct via the associated functions ([`any`](Self::any),
/// [`for_peer_count`](Self::for_peer_count), [`all`](Self::all)) which
/// pick sensible default timeouts. Override the timeout via
/// [`with_timeout`](Self::with_timeout).
///
/// # Examples
///
/// ```ignore
/// // Gun.js default — first ack wins, 9s timeout
/// let p = AckPolicy::any();
///
/// // Majority of 5 peers — 3 acks needed, 9s timeout
/// let p = AckPolicy::for_peer_count(5);
///
/// // All fanned-out peers must ack
/// let p = AckPolicy::all();
///
/// // Any ack, but with a tighter 2s deadline
/// let p = AckPolicy::any().with_timeout(Duration::from_secs(2));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckPolicy {
    /// Number of peer acks required to satisfy the policy.
    ///
    /// `1` = any, `usize::MAX` = all (effective bound: fanned-out peer count).
    pub quorum: usize,
    /// Maximum time to wait before the request resolves with `Err`.
    pub timeout: Duration,
}

impl Default for AckPolicy {
    /// Defaults to [`AckPolicy::any`] — first ack wins, Gun.js compatible.
    fn default() -> Self {
        Self::any()
    }
}

impl AckPolicy {
    /// "First ack wins" policy with the Gun.js default 9-second timeout.
    ///
    /// This matches the behaviour of Gun.js's `ask` when no quorum is
    /// specified — the requester resolves as soon as any peer or the
    /// local storage commits the put.
    pub fn any() -> Self {
        Self {
            quorum: 1,
            timeout: DEFAULT_QUORUM_TIMEOUT,
        }
    }

    /// Majority quorum: ⌈N/2⌉ of `peer_count` peers must ack.
    ///
    /// Uses the Raft/Dynamo-style majority to balance availability against
    /// consistency. For `peer_count == 0` or `1`, falls back to
    /// [`any`](Self::any) since majority is undefined for trivial peer sets.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(AckPolicy::for_peer_count(3).quorum, 2); // 2 of 3
    /// assert_eq!(AckPolicy::for_peer_count(5).quorum, 3); // 3 of 5
    /// assert_eq!(AckPolicy::for_peer_count(0).quorum, 1); // → any
    /// ```
    pub fn for_peer_count(peer_count: usize) -> Self {
        let quorum = match peer_count {
            0 | 1 => 1,
            n => n.div_ceil(2),
        };
        Self {
            quorum,
            timeout: DEFAULT_QUORUM_TIMEOUT,
        }
    }

    /// "Every fanned-out peer must ack" policy with the default timeout.
    ///
    /// Strictest consistency guarantee — the put is only considered durable
    /// once every target has confirmed. Useful for critical writes where
    /// partial replication is unacceptable.
    pub fn all() -> Self {
        Self {
            quorum: usize::MAX,
            timeout: DEFAULT_QUORUM_TIMEOUT,
        }
    }

    /// Returns a new policy with the given timeout (other fields preserved).
    ///
    /// Useful when callers want a faster-failing deadline than the Gun.js
    /// default, or a longer grace period for high-latency networks.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        Self { ..self }
    }

    /// Returns a new policy with the given quorum requirement.
    ///
    /// Clamped to `>= 1` (a quorum of 0 would resolve immediately, which
    /// is almost certainly not what the caller intends).
    pub fn with_quorum(mut self, quorum: usize) -> Self {
        self.quorum = quorum.max(1);
        Self { ..self }
    }
}

/// Result of a successful `put_quorum` request.
///
/// Reports how many peers acked, whether the policy was satisfied, and
/// how long the request took to resolve. Returned in the `Ok` arm of
/// `put_quorum`'s `Result`.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    /// The id of the originating Put (matches `Put.id`).
    pub put_id: String,
    /// Number of peer acks observed before quorum was satisfied.
    pub acked_by: usize,
    /// Whether the quorum threshold was met.
    ///
    /// Always `true` when returned in the `Ok` arm of `put_quorum` —
    /// included for symmetry with future APIs that may report partial
    /// replication status.
    pub quorum_met: bool,
    /// Wall-clock duration between put submission and quorum satisfaction.
    pub elapsed: Duration,
}

#[cfg(test)]
mod tests {
    //! Unit tests for ack policy math.
    //!
    //! These tests verify the public policy constructors and their
    //! invariants. State-tracking tests for the Router's internal
    //! `QuorumEntry` live in `src/router.rs::tests` since that type is
    //! private to the router module.

    use super::*;
    use std::time::Duration;

    #[test]
    fn policy_any_quorum_is_one() {
        assert_eq!(AckPolicy::any().quorum, 1);
        assert_eq!(AckPolicy::any().timeout, DEFAULT_QUORUM_TIMEOUT);
    }

    #[test]
    fn policy_default_is_any() {
        assert_eq!(AckPolicy::default(), AckPolicy::any());
    }

    #[test]
    fn policy_for_peer_count_majority() {
        // Standard Raft/Dynamo majority math.
        assert_eq!(AckPolicy::for_peer_count(2).quorum, 1); // ⌈2/2⌉ = 1
        assert_eq!(AckPolicy::for_peer_count(3).quorum, 2); // ⌈3/2⌉ = 2
        assert_eq!(AckPolicy::for_peer_count(5).quorum, 3); // ⌈5/2⌉ = 3
        assert_eq!(AckPolicy::for_peer_count(7).quorum, 4); // ⌈7/2⌉ = 4
    }

    #[test]
    fn policy_for_peer_count_trivial_falls_back_to_any() {
        // 0 peers or 1 peer — majority is undefined, fall back to any.
        assert_eq!(AckPolicy::for_peer_count(0).quorum, 1);
        assert_eq!(AckPolicy::for_peer_count(1).quorum, 1);
    }

    #[test]
    fn policy_all_quorum_is_max() {
        assert_eq!(AckPolicy::all().quorum, usize::MAX);
    }

    #[test]
    fn policy_with_timeout_overrides_default() {
        let p = AckPolicy::any().with_timeout(Duration::from_secs(2));
        assert_eq!(p.timeout, Duration::from_secs(2));
        assert_eq!(p.quorum, 1); // other fields preserved
    }

    #[test]
    fn policy_with_quorum_clamps_to_one() {
        // Quorum of 0 would resolve immediately — almost never intended.
        let p = AckPolicy::any().with_quorum(0);
        assert_eq!(p.quorum, 1, "quorum 0 must clamp to 1");

        let p = AckPolicy::any().with_quorum(5);
        assert_eq!(p.quorum, 5);
    }

    #[test]
    fn sentinel_constant_is_stable() {
        // The wire-format string is load-bearing — any change would
        // break interop with existing Rod nodes. Lock it down.
        assert_eq!(QUORUM_MET_SENTINEL, "__quorum_met__");
    }

    #[test]
    fn default_timeout_matches_gun_js_lack() {
        // Gun.js `lack = 9000ms` is the canonical default. Changing
        // this would surprise Gun.js interop users.
        assert_eq!(DEFAULT_QUORUM_TIMEOUT, Duration::from_millis(9000));
    }
}