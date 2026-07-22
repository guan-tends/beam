# Rod Follow-up A — Network Fanout Ack (Gun.js ask-pattern, Quorum-B2)

**Date**: 2026-07-21
**Branch**: `feat/followup-a-network-fanout-ack` (created from `main`)
**Plan file**: `/home/guan/src/rod/docs/plans/ROD-FOLLOWUP-A-NETWORK-FANOUT-ACK.md`
**Built-in mirror**: `rod_followup_a_network_fanout_ack_plan` (kept identical)
**Status**: LOCKED. Awaiting "begin" signal from Freeman.
**Parent plan**: `rod_async_ack_drain_redux_plan_comprehensive` (shipped 2026-07-20, merged into `main`)
**Gun.js reference**: `gun-js/src/ask.js` — `lack` timeout (default 9000ms), `#` request-id, `@` ack-id
**Sibling plan**: `ROD-FOLLOWUP-B-BOUNDED-CHANNEL-SILENT-DROP.md` (future work, separate fix)

---

## Executive Summary

When `handle_put_relay` (`rod/src/router.rs:435`) sends a Put to `server_peers`, it uses **unbounded channels with no ack tracking** — the caller's `put()` returns Ok locally while remote peers may have received nothing (network partition, peer offline, slow consumer, websocket close). This is **silent fanout failure with no signal**.

**Rod already has the wire-format mechanism** for the fix: `Put.in_response_to: Option<String>` serializes as Gun.js's `@` field (`message.rs:103`), and the deserializer parses `@` correctly (`message.rs:391-393`). What's missing is **the router-level routing that turns a peer-received Put-with-`@` into a reply Put back to the original requester** through the same Put channel. And **the originator-side tracker that registers a pending ack, awaits ⌈N/2⌉ peer acks (or timeout), and surfaces the result to the caller**.

This plan implements the **B2 Quorum Ack** shape from the palace entry `wing_rod/room_async_race_fix/follow-up B`. B2 was chosen over B1 (per-peer ack count) and B3 (background reconciliation) for these reasons:

- **mnemos's actual deployment** is single-user + small peer fleet (loom-net plans show typically <5 peers per zone per palace `wing_code/loom-engine` docs). Quorum ⌈N/2⌉ reads as "1 of 1" for solo, "2 of 3" for typical fleet, "3 of 5" for distributed — natural fit.
- **Single-user-write locality**: the user wants fast feedback that "the write landed somewhere durable", not that "the write landed on every peer". Quorum gives "most peers saw it" without waiting for the slowest.
- **B1 (per-peer ack) is too strict**: 5-of-5 acks required means one slow peer blocks the write. Unacceptable UX.
- **B3 (background reconciliation) preserves the bug**: caller still can't tell if the write replicated. We're trying to FIX this, not preserve it.

**Result**: `Node::put_quorum(value, ack_policy)` returns `Result<ReplicationStatus, AckError>` where `ReplicationStatus` reports `(acked_by: Vec<PeerAddr>, pending: Vec<PeerAddr>, timed_out: Vec<PeerAddr>)`. The existing fire-and-forget `Node::put()` is preserved (LWW-correct behavior unchanged for non-ack callers).

---

## Substrate Recon (Verified 2026-07-21)

### 1. Gun.js ask-pattern precedent

**File**: `/home/guan/src/gun-js/src/ask.js`

```javascript
var lack = (this.opt||{}).lack || 9000;  // 9 second default timeout
var id = (as && as['#']) || random(9);   // request id
if(!cb){ return id }
var to = this.on(id, cb, as);             // subscribe on the request id
to.err = to.err || setTimeout(function(){ to.off();
    to.next({err: "Error: No ACK yet.", lack: true});  // timeout → "no ack" error
}, lack);
```

**Wire format** (verified by reading Gun.js source + Rod message.rs):
- Request: `Put { "#": msg_id, ...payload }`
- Reply: `Put { "@": msg_id, ...payload }` — the `@` field names which request id is being acked

**Rod already speaks this wire format**. No protocol change needed. The plan only changes **router-level routing semantics**.

### 2. Current Put routing in `router.rs`

Verified by reading `rod/src/router.rs`:

| Line | Function | What it does | Ack path |
|------|----------|--------------|----------|
| 382 | `handle_put` | Top-level Put dispatch | Branches on `in_response_to` |
| 402-411 | `handle_put` ack branch | Routes Put-reply back to Get-requester via `seen_get_messages.get_mut(in_response_to).from.send(...)` | **Get-reply only** |
| 412-417 | `handle_put` forward branch | Forwards to write_adapters + `handle_put_relay` | No ack |
| 435 | `handle_put_relay` | Sends to server_peers + subscribers via unbounded channels | **No ack** ← the bug |
| 530-545 | `handle_batch_put` ack branch | Mirrors handle_put ack routing for BatchPut | Get-reply only |

**The gap**: `handle_put_relay` sends to peers with no `in_response_to` tracking on the originator side and no Put-reply routing for peer-acks on the receiver side. The Put-reply path exists only for Get→reply (line 402-411).

### 3. Current wire-format usage of `in_response_to`

**`message.rs:61`**: `pub in_response_to: Option<String>` — field exists.

**`message.rs:102-103`**: serialize as `"@"`:
```rust
if let Some(in_response_to) = &self.in_response_to {
    json["@"] = json!(in_response_to);
}
```

**`message.rs:391-399`**: deserialize from `"@"`:
```rust
let in_response_to = match json.get("@") {
    Some(in_response_to) => match in_response_to.as_str() {
        Some(in_response_to) => Some(in_response_to.to_string()),
        _ => return Err("message @ field was not a string"),
    },
    _ => None,
};
```

**Confirmed**: wire format is already Gun.js-ask-compatible. Plan only modifies router-level routing.


### 4. Why this is Gun.js-compatible, not a Rod-only invention

Gun.js's `ask` module is **the** Gun.js precedent for request/response on top of the Put gossip protocol. The wire-format tags `#` (request id) and `@` (ack id) are Gun.js's documented wire format. Rod already uses these tags via the existing `in_response_to` field. **Rod's existing Get→reply path (router.rs:402-411) is already an instance of the Gun.js ask pattern** — just scoped to local Get-requester only, not extended to network peers.

This plan extends the same Gun.js ask pattern to network fanout. **No new wire format. No new message type. Only a new routing rule**: when a peer-received Put has `in_response_to` set AND its `from` field identifies it as coming from a peer actor (not from local Node or storage), route the ack Put back to the requester through the existing Put channel.

---

## Architectural Decisions (LOCKED)

| ID | Decision |
|----|----------|
| D1 | **Use Gun.js ask-pattern semantics** (`#` request id, `@` ack id, configurable `lack` timeout). Wire format unchanged — `Put.in_response_to` already serializes/deserializes correctly. |
| D2 | **Default timeout = 9000ms** (matches Gun.js's `lack` default). Configurable per-call via `AckPolicy.timeout`. |
| D3 | **Default quorum = ⌈N/2⌉** for N≥2, "any" for N=1. Configurable per-call via `AckPolicy.quorum`. |
| D4 | **New type `AckPolicy`** with fields `timeout: Duration`, `quorum: QuorumPolicy`, `required_peers: Option<Vec<Addr>>` (None = all known peers). |
| D5 | **New type `ReplicationStatus`** with fields `acked_by: Vec<Addr>`, `pending: Vec<Addr>`, `timed_out: Vec<Addr>`, `policy: AckPolicy`, `put_id: String`. |
| D6 | **New type `AckError`** (using `thiserror`): `AckError::Timeout { put_id, timed_out, acked_by, policy }`, `AckError::NoPeers { put_id }`, `AckError::LocalCommitFailed { source: RouterError }`. |
| D7 | **New type `QuorumPolicy`**: enum `Any`, `All`, `Quorum(usize)` where `Quorum(n)` means n-of-N. `AckPolicy::quorum: QuorumPolicy`. |
| D8 | **New `Router` field**: `pending_acks: HashMap<String, PendingAck>` keyed by put_id, holding `(policy: AckPolicy, requester: NodeAddr, peers: Vec<Addr>, deadline: Instant)`. |
| D9 | **New `Router` method**: `register_pending_ack(put_id, policy, requester, peers)` — called by Node before sending. `complete_pending_ack(put_id, peer_addr)` — called when an ack arrives. |
| D10 | **Modified `handle_put_relay`**: when sending to each peer, attach an `ack_token` (random 8-char string) as `Put.in_response_to`. The originator's put_id is sent as the `id`. The peer's reply Put will carry `in_response_to = put_id` — the existing `seen_get_messages` map gets a new sibling `pending_acks` for this purpose. |
| D11 | **Modified `handle_put` ack branch (line 402)**: extend to route peer-acks through `pending_acks.get_mut(in_response_to).requester.send(...)` AND continue routing Get-replies through `seen_get_messages`. Both paths coexist — same `in_response_to` field, different routing tables. |
| D12 | **New `Node` method**: `pub async fn put_quorum(&mut self, key: &str, value: Value, policy: AckPolicy) -> Result<ReplicationStatus, AckError>`. Existing `Node::put()` unchanged (still fire-and-forget for callers who don't need ack). |
| D13 | **No new crates, no new dependencies.** `thiserror` is already in the workspace (`redb_storage.rs` uses it). `tokio::sync::mpsc` and `Instant` are stdlib. |
| D14 | **Mnemos caller impact**: zero, unless they explicitly opt-in via `put_quorum`. The existing `put()` API is preserved. |
| D15 | **Wire compatibility**: existing Rod nodes on the wire remain compatible. They will see `Put { "@": ..., ... }` and route it as a peer-ack (if `pending_acks` knows about it) OR drop it as unknown (if not). No protocol break. |

### Why D11 (extend, don't replace) is correct

The existing `seen_get_messages` table handles Get→reply routing. We could either:

(a) **Replace** `seen_get_messages` with a unified `pending_replies` table — risky, mixes Get and Put semantics, breaks existing tests.

(b) **Extend** the `handle_put` ack branch to check BOTH `seen_get_messages` AND `pending_acks`. A Put with `@` either matches a Get-requester (existing path) OR matches a peer-ack tracker (new path). They never collide because a given `@` value is either a Get-reply correlation OR a peer-ack correlation, never both.

(b) is the suckless choice: add a sibling map, route based on which map has the entry, fail-safe (drop if neither matches). No code in the Get-ack path changes. No tests break. The new functionality is purely additive.

### Why D12 (new method, not changed signature) is correct

Changing `Node::put()` to return `Result<ReplicationStatus, AckError>` would break every caller. Adding `Node::put_quorum()` is opt-in. mnemos's existing code (palace, mcp, cli) uses `Node::put()` and continues to. New callers who need ack semantics use `put_quorum()`. **No migration burden.**

### Why D14 (configurable per-call, not per-Node) is correct

A single Node can be used in different contexts: the cli might want fire-and-forget for routine writes but quorum for critical writes. Per-call policy is more flexible than per-Node default. The cost is one extra parameter at the call site — minimal.


---

## New Type Definitions (rod/src/ack.rs)

**File**: `rod/src/ack.rs` (NEW, ~220 lines)

### `QuorumPolicy`

```rust
use std::time::Duration;

/// How many peer acks are required to consider a Put successfully replicated.
///
/// Models the three consistency modes from distributed systems literature:
/// - `Any`     — fastest, weakest (Cassandra ONE)
/// - `Quorum(n)`— majority (Raft/Dynamo quorum, Cassandra QUORUM)
/// - `All`     — strictest (Cassandra ALL, Paxos)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumPolicy {
    /// Wait for the first ack from any peer. Fastest. Weakest.
    Any,
    /// Wait for `n` acks out of N total peers. Default for `AckPolicy::default()`.
    /// For N=1 (no peers), `Quorum(1)` is satisfied immediately with no acks needed.
    Quorum(usize),
    /// Wait for every peer to ack. Strictest. Slowest.
    All,
}

impl Default for QuorumPolicy {
    fn default() -> Self {
        // Matches Gun.js's "ask" default: any ack is enough.
        // We override per-call to ⌈N/2⌉ in `AckPolicy::for_peer_count()`.
        QuorumPolicy::Any
    }
}

impl QuorumPolicy {
    /// Returns the number of acks required for a given total peer count.
    ///
    /// - `Any` → 1 ack (but for 0 peers, 0 — local commit alone satisfies)
    /// - `Quorum(n)` → min(n, peer_count)
    /// - `All` → peer_count
    pub fn required_acks(&self, peer_count: usize) -> usize {
        match self {
            Self::Any => 1.min(peer_count),
            Self::Quorum(n) => (*n).min(peer_count),
            Self::All => peer_count,
        }
    }
}
```

### `AckPolicy`

```rust
/// Caller-supplied policy for `Node::put_quorum`. Controls how many peer acks
/// must arrive before the call returns Ok, and how long to wait.
#[derive(Debug, Clone)]
pub struct AckPolicy {
    /// How many peer acks are required.
    pub quorum: QuorumPolicy,
    /// Maximum time to wait for acks before returning `AckError::Timeout`.
    /// Default: 9000ms (matches Gun.js's `lack`).
    pub timeout: Duration,
    /// If `Some`, only wait for acks from these specific peers. If `None`,
    /// wait for acks from all known peers in the Router's `server_peers` set.
    pub required_peers: Option<Vec<Addr>>,
}

impl Default for AckPolicy {
    fn default() -> Self {
        Self {
            quorum: QuorumPolicy::Any,
            timeout: Duration::from_millis(9000),
            required_peers: None,
        }
    }
}

impl AckPolicy {
    /// Construct a policy that requires ⌈N/2⌉ acks for N peers, with the default
    /// 9-second timeout. This is the recommended default for mnemos's typical
    /// 1-5 peer deployment.
    pub fn quorum_default() -> Self {
        Self::default()
    }

    /// Construct a policy for a specific peer count, with ⌈N/2⌉ quorum.
    pub fn for_peer_count(peer_count: usize) -> Self {
        let n = if peer_count == 0 { 1 } else { (peer_count + 1) / 2 };
        Self {
            quorum: QuorumPolicy::Quorum(n),
            timeout: Duration::from_millis(9000),
            required_peers: None,
        }
    }

    /// Builder: set timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Builder: set required peers.
    pub fn with_required_peers(mut self, peers: Vec<Addr>) -> Self {
        self.required_peers = Some(peers);
        self
    }
}
```

### `ReplicationStatus`

```rust
/// Returned by `Node::put_quorum` on success. Tells the caller which peers
/// acked, which are still pending, and which timed out.
///
/// Even on partial success (some acks arrived, some timed out), this struct
/// is returned and the caller decides whether the partial replication is
/// sufficient. On full timeout (no acks in the window), `AckError::Timeout`
/// is returned instead, but the same `ReplicationStatus` is included as
/// `error.acked_by` and `error.timed_out` for inspection.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    /// put_id this status reports on (matches the originating Put's `id`).
    pub put_id: String,
    /// Peers that sent an ack Put within the timeout window.
    pub acked_by: Vec<Addr>,
    /// Peers that were sent the Put but did NOT ack within the timeout.
    /// For `QuorumPolicy::Any` success, `pending` may be non-empty — call
    /// succeeded with the first ack but other peers are still pending.
    pub pending: Vec<Addr>,
    /// Peers that did NOT ack AND have been waited on for the full timeout.
    /// Always empty on success unless `quorum` was `Quorum(0)` (which is
    /// meaningless and rejected at construction).
    pub timed_out: Vec<Addr>,
    /// The policy that was used. Useful for logging/telemetry.
    pub policy: AckPolicy,
}
```

### `AckError`

```rust
use thiserror::Error;

/// Returned by `Node::put_quorum` on failure. Distinguishes:
/// - `NoPeers`: the caller requested ack but no peers are connected
/// - `Timeout`: local commit succeeded but insufficient peer acks arrived
/// - `LocalCommitFailed`: the local write itself failed (storage dropped,
///   storage actor terminated). Caller must retry.
#[derive(Debug, Error)]
pub enum AckError {
    #[error("no peers connected; cannot satisfy ack policy for put_id={put_id}")]
    NoPeers { put_id: String },

    #[error(
        "ack timeout for put_id={put_id}: required={required}, acked={acked_count}, \
        timed_out={timed_out_count}, after {:?}",
        policy.timeout
    )]
    Timeout {
        put_id: String,
        acked_by: Vec<Addr>,
        timed_out: Vec<Addr>,
        required: usize,
        policy: AckPolicy,
    },

    #[error("local commit failed for put_id={put_id}: {source}")]
    LocalCommitFailed {
        put_id: String,
        #[source]
        source: Box<crate::error::RouterError>,
    },
}
```

### `PendingAck` (Router-internal state)

```rust
use std::time::Instant;
use crate::actor::Addr;

/// Router's internal tracker for an in-flight put_quorum call.
///
/// Lives in `Router::pending_acks` from `Node::put_quorum` invocation until
/// either (a) quorum is reached (entry removed, ack sent to requester),
/// (b) timeout fires (entry removed, AckError::Timeout sent to requester),
/// or (c) requester cancels (entry removed, no signal sent).
#[derive(Debug)]
pub struct PendingAck {
    pub policy: AckPolicy,
    pub requester: Addr,
    pub peers: Vec<Addr>,
    pub acked_by: Vec<Addr>,
    pub deadline: Instant,
    pub put_id: String,
}


### Tests for `rod/src/ack.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_required_acks_for_peer_count() {
        assert_eq!(QuorumPolicy::Any.required_acks(0), 0);
        assert_eq!(QuorumPolicy::Any.required_acks(1), 1);
        assert_eq!(QuorumPolicy::Any.required_acks(5), 1);
        assert_eq!(QuorumPolicy::Quorum(2).required_acks(5), 2);
        assert_eq!(QuorumPolicy::Quorum(10).required_acks(5), 5); // capped
        assert_eq!(QuorumPolicy::All.required_acks(3), 3);
    }

    #[test]
    fn ack_policy_default_matches_gunjs() {
        let p = AckPolicy::default();
        assert_eq!(p.timeout, Duration::from_millis(9000));
        assert!(matches!(p.quorum, QuorumPolicy::Any));
    }

    #[test]
    fn ack_policy_for_peer_count_uses_majority() {
        let p1 = AckPolicy::for_peer_count(0);
        assert!(matches!(p1.quorum, QuorumPolicy::Quorum(1)));
        let p3 = AckPolicy::for_peer_count(3);
        assert!(matches!(p3.quorum, QuorumPolicy::Quorum(2)));
        let p5 = AckPolicy::for_peer_count(5);
        assert!(matches!(p5.quorum, QuorumPolicy::Quorum(3)));
        let p7 = AckPolicy::for_peer_count(7);
        assert!(matches!(p7.quorum, QuorumPolicy::Quorum(4)));
    }

    #[test]
    fn ack_policy_builder_chain() {
        let p = AckPolicy::default()
            .with_timeout(Duration::from_secs(30))
            .with_required_peers(vec![]);
        assert_eq!(p.timeout, Duration::from_secs(30));
        assert!(p.required_peers.is_some());
    }

    #[test]
    fn replication_status_constructor() {
        let s = ReplicationStatus {
            put_id: "abc".into(),
            acked_by: vec![],
            pending: vec![],
            timed_out: vec![],
            policy: AckPolicy::default(),
        };
        assert_eq!(s.put_id, "abc");
        assert!(s.acked_by.is_empty());
    }

    #[test]
    fn ack_error_display_messages_are_descriptive() {
        let e = AckError::NoPeers { put_id: "x".into() };
        assert!(e.to_string().contains("no peers"));
        let e = AckError::LocalCommitFailed {
            put_id: "y".into(),
            source: Box::new(crate::error::RouterError::NoStorageAdapters),
        };
        assert!(e.to_string().contains("local commit"));
    }
}
```

**Acceptance**: 6/6 tests pass; `cargo check -p rod` clean.

---

## Implementation Plan — 5 Epics, 16 Tasks, ~14-19h

### Epic E1 — Foundations: ack types and Router state

**Why first**: every other change depends on `ack.rs` types and `pending_acks` field. Lock them in.

#### Task E1.1: Create `rod/src/ack.rs` with `QuorumPolicy`, `AckPolicy`, `ReplicationStatus`, `AckError`, `PendingAck`

**File**: `rod/src/ack.rs` (new, ~280 lines including tests)

**Spec**: as above (QuorumPolicy, AckPolicy, ReplicationStatus, AckError, PendingAck + 6 unit tests).

**Acceptance**: 6/6 tests pass; `cargo check -p rod` clean.

**Estimated**: 1.5h.

#### Task E1.2: Re-export from `rod/src/lib.rs`

**File**: `rod/src/lib.rs`

**Diff**:

```rust
pub mod ack;
pub mod error;

pub use ack::{AckError, AckPolicy, PendingAck, QuorumPolicy, ReplicationStatus};
pub use error::RouterError;
```

**Acceptance**: `cargo check -p rod` clean; types visible to downstream.

**Estimated**: 0.5h.

#### Task E1.3: Add `pending_acks: HashMap<String, PendingAck>` field to `Router`

**File**: `rod/src/router.rs` lines 100-130 (Router struct).

**Diff**:

```rust
use crate::ack::PendingAck;
use std::collections::HashMap;

pub struct Router {
    // ... existing fields ...
    pub pending_acks: HashMap<String, PendingAck>,
}
```

**Initial value in `Router::new`**: `pending_acks: HashMap::new()`. No behavior change.

**Acceptance**: `cargo check -p rod` clean; all 446 existing tests pass.

**Estimated**: 0.5h.

**Epic E1 total**: ~2.5h, 6 new tests.

---

### Epic E2 — Router ack routing (extend, don't replace)

**Why second**: wire up the router-level routing that turns peer-acks back to the requester. Pure router change.

#### Task E2.1: Extend `handle_put` ack branch (router.rs:402) to route peer-acks via `pending_acks`

**File**: `rod/src/router.rs` lines 402-411.

**Current code**:

```rust
match &put.in_response_to {
    Some(in_response_to) => {
        if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
            if put.checksum.is_some()
                && put.checksum == seen_get_message.last_reply_checksum
            {
                debug!("same reply already sent");
                return;
            }
            seen_get_message.last_reply_checksum = put.checksum;
            let _ = seen_get_message.from.send(Message::Put(put));
        }
    }
    // ...
}
```

**New code**:

```rust
match &put.in_response_to {
    Some(in_response_to) => {
        // Try peer-ack routing first (new path).
        if let Some(pending) = self.pending_acks.get_mut(in_response_to) {
            // Record this peer's ack.
            pending.acked_by.push(put.from.clone());
            debug!(
                "peer ack received for put_id={} from {} (acked_by={}/required={})",
                in_response_to, put.from, pending.acked_by.len(),
                pending.policy.quorum.required_acks(pending.peers.len())
            );
            // Check if quorum reached.
            let required = pending.policy.quorum.required_acks(pending.peers.len());
            if pending.acked_by.len() >= required {
                let mut status = ReplicationStatus {
                    put_id: in_response_to.clone(),
                    acked_by: std::mem::take(&mut pending.acked_by),
                    pending: pending.peers.iter()
                        .filter(|p| !pending.acked_by.contains(p))
                        .cloned()
                        .collect(),
                    timed_out: vec![],
                    policy: pending.policy.clone(),
                };
                // Remove the tracker.
                let ack_pending = self.pending_acks.remove(in_response_to).unwrap();
                status.acked_by = ack_pending.acked_by;
                // Send ReplicationStatus back to requester.
                let _ = ack_pending.requester.send(Message::ReplicationStatus(status));
            }
            return;
        }
        // Fall through to Get-reply routing (existing path).
        if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
            if put.checksum.is_some()
                && put.checksum == seen_get_message.last_reply_checksum
            {
                debug!("same reply already sent");
                return;
            }
            seen_get_message.last_reply_checksum = put.checksum;
            let _ = seen_get_message.from.send(Message::Put(put));
        }
    }
    // ...
}
```

**Note**: This introduces a new `Message::ReplicationStatus` variant — see E2.2.

**Acceptance**: existing Get-reply tests still pass; new peer-ack routing works (verified in E2.4 tests).

**Estimated**: 1.5h.

#### Task E2.2: Add `Message::ReplicationStatus(ReplicationStatus)` variant

**File**: `rod/src/message.rs` lines 230-260 (Message enum).

**Diff**:

```rust
#[derive(Clone, Debug)]
pub enum Message {
    Get(Get),
    Put(Put),
    Ack { id: String, recipient: Addr },
    ReplicationStatus(ReplicationStatus),  // NEW
    Flush { id: String, recipient: Addr },
    // ... existing variants ...
}
```

**Acceptance**: enum compiles; existing message-handling code matches all other variants unchanged.

**Estimated**: 0.5h.

#### Task E2.3: Modify `handle_put_relay` (router.rs:435) to attach `in_response_to` only when ack requested

**File**: `rod/src/router.rs` lines 435-475.

**Decision**: Two paths now exist in `handle_put_relay`:
- If `pending_acks.contains_key(&put.id)` → send with `in_response_to: Some(put.id.clone())` so peer can ack.
- Otherwise → send without `in_response_to` (current behavior, fire-and-forget).

**Diff**:

```rust
fn handle_put_relay(&mut self, put: &Put) {
    // ... existing hop list setup ...
    
    let wants_ack = self.pending_acks.contains_key(&put.id);
    
    for addr in self.server_peers.iter() {
        if put.from == *addr || hops.contains(&addr.to_string()) {
            continue;
        }
        let mut put = put.clone();
        put.peer_hop_list = Some(hops.clone());
        if wants_ack && put.in_response_to.is_none() {
            put.in_response_to = Some(put.id.clone());
        }
        let _ = addr.send(Message::Put(put));
        already_sent_to.insert(addr.clone());
    }
    // ... subscriber relay unchanged ...
}
```

**Behavior change**: when a peer-ack is pending, each peer receives a Put with `@` field set to the originator's put_id. The peer's `handle_put` will route the ack back via E2.1's new path. Without a pending ack, behavior is identical to today.

**Acceptance**: existing replication tests pass; new ack-aware tests pass (E2.4).

**Estimated**: 1h.

#### Task E2.4: Unit tests for E2.1-E2.3 in `router.rs::tests`

```rust
#[tokio::test]
async fn peer_ack_routes_via_pending_acks() {
    let mut router = test_router_with_peer();
    let requester = test_node_addr();
    let peer = router.server_peers.iter().next().unwrap().clone();

    // Register pending ack for a hypothetical put.
    let policy = AckPolicy::default();
    let put_id = "test-put-1".to_string();
    router.pending_acks.insert(put_id.clone(), PendingAck {
        policy: policy.clone(),
        requester: requester.clone(),
        peers: vec![peer.clone()],
        acked_by: vec![],
        deadline: Instant::now() + Duration::from_secs(10),
        put_id: put_id.clone(),
    });

    // Simulate peer sending ack Put with @=put_id.
    let ack_put = Put {
        id: random_string(8),
        from: peer.clone(),
        in_response_to: Some(put_id.clone()),
        updated_nodes: BTreeMap::new(),
        checksum: None,
        json_str: None,
        peer_hop_list: None,
        recipients: None,
    };

    router.handle_put(ack_put);

    // Verify pending_acks entry was removed.
    assert!(router.pending_acks.is_empty());
}

#[tokio::test]
async fn peer_ack_get_reply_still_works() {
    // Verify the existing Get-reply path is unaffected.
    // (This is essentially a re-run of an existing test with new assertion that
    //  pending_acks is NOT used.)
}

#[tokio::test]
async fn handle_put_relay_attaches_in_response_to_when_ack_pending() {
    // Setup: pending_acks contains put_id.
    // Setup: handle_put_relay called with Put matching that id.
    // Assert: peer receives Put with in_response_to == Some(put_id).
}

#[tokio::test]
async fn handle_put_relay_no_ack_when_not_pending() {
    // Setup: pending_acks is empty.
    // Setup: handle_put_relay called.
    // Assert: peer receives Put with in_response_to == None (fire-and-forget).
}
```

**Acceptance**: 4/4 tests pass; existing replication tests still green.

**Estimated**: 1.5h.

**Epic E2 total**: ~4.5h, 4 new tests.


---

### Epic E3 — Node::put_quorum API

**Why third**: the user-facing API. After router is wired, expose the typed entry point on Node.

#### Task E3.1: Add `Node::put_quorum` method

**File**: `rod/src/node.rs` lines 480-540 (near existing `Node::put`).

**Spec**:

```rust
/// Write a value with a quorum ack policy. Returns when the local commit
/// has succeeded AND enough peer acks have arrived (per `policy`).
///
/// Use this when the caller needs to know whether the write replicated to
/// peers, not just whether it committed locally. For fire-and-forget writes,
/// use `Node::put()` instead.
pub async fn put_quorum(
    &mut self,
    key: &str,
    value: Value,
    policy: AckPolicy,
) -> Result<ReplicationStatus, AckError> {
    let put_id = random_string(8);
    let router = self.router.as_ref()
        .ok_or_else(|| AckError::LocalCommitFailed {
            put_id: put_id.clone(),
            source: Box::new(RouterError::NoStorageAdapters),
        })?
        .clone();
    let my_addr = self.addr.as_ref()
        .ok_or_else(|| AckError::LocalCommitFailed {
            put_id: put_id.clone(),
            source: Box::new(RouterError::ChannelClosed),
        })?
        .clone();

    // Register pending ack BEFORE sending (so handle_put_relay sees it).
    let peers: Vec<Addr> = router.server_peers.iter().cloned().collect();
    if peers.is_empty() {
        return Err(AckError::NoPeers { put_id: put_id.clone() });
    }
    let required = policy.quorum.required_acks(peers.len());
    let deadline = Instant::now() + policy.timeout;
    let (tx, rx) = tokio::sync::oneshot::channel::<ReplicationStatus>();
    router.register_pending_ack(PendingAck {
        policy: policy.clone(),
        requester: my_addr.clone(),
        peers: peers.clone(),
        acked_by: vec![],
        deadline,
        put_id: put_id.clone(),
    });

    // Build the Put and send.
    let mut children = BTreeMap::new();
    children.insert(
        key.to_string(),
        NodeData { updated_at: now_millis(), value },
    );
    let mut updated_nodes = BTreeMap::new();
    updated_nodes.insert(self.uid(), children);
    let put = Put {
        id: put_id.clone(),
        from: my_addr.clone(),
        recipients: None,
        in_response_to: None,
        updated_nodes,
        checksum: None,
        json_str: None,
        peer_hop_list: None,
    };

    // Await either ack or timeout.
    match tokio::time::timeout(policy.timeout, rx).await {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(_canceled)) => Err(AckError::Timeout {
            put_id,
            acked_by: vec![],
            timed_out: peers,
            required,
            policy,
        }),
        Err(_elapsed) => Err(AckError::Timeout {
            put_id,
            acked_by: vec![],
            timed_out: peers,
            required,
            policy,
        }),
    }
}
```

**Wait — there's an ordering issue**: we register the pending_ack AND send the Put through `router.send(Message::Put(put))` which goes through `handle_put` → `handle_put_relay`. The current `put_quorum` above builds the Put and sends it but I cut the send call. Let me restate:

```rust
    // ... build put ...
    
    // Send the Put through the router.
    router.send(Message::Put(put)).await
        .map_err(|_| AckError::LocalCommitFailed {
            put_id: put_id.clone(),
            source: Box::new(RouterError::ChannelClosed),
        })?;

    // Await either ack or timeout.
    tokio::time::timeout(policy.timeout, rx).await
        .map_err(|_| AckError::Timeout { put_id, acked_by: vec![], timed_out: peers, required, policy })?
        .map_err(|_| AckError::LocalCommitFailed { put_id, source: Box::new(RouterError::ChannelClosed) })
```

**Acceptance**: existing `Node::put` tests still green; new `put_quorum` tests pass.

**Estimated**: 2h.

#### Task E3.2: Add `Node::pending_put_quorum` (oneshot reply channel)

**File**: `rod/src/node.rs` — add to `Node` struct fields:

```rust
pub struct Node {
    // ... existing fields ...
    pub pending_put_quorum: HashMap<String, oneshot::Sender<ReplicationStatus>>,
}
```

**Why**: The router sends `Message::ReplicationStatus(status)` to `requester` (the Node's own actor address). The Node actor loop receives this, looks up the pending oneshot by put_id, and forwards the status.

**Wait — actually, the simpler design**: send `Message::Put` with `in_response_to` instead of a new variant. The Node can route the ack Put back to the put_quorum caller using the same `pending_puts` mechanism the existing async put uses. This avoids a new Message variant.

**Decision (D-Revised)**: drop E2.2's `Message::ReplicationStatus` variant. Instead, the router's ack routing sends a Put with `in_response_to = Some(put.id)` to the requester, and Node's existing `pending_puts` map handles the rest. **No new Message variant. Zero new wire types.**

**Acceptance**: rework E2.1 to send a Put-with-`@` to the requester instead of ReplicationStatus. Node handles this in its `handle_put` via existing pending_puts routing.

**Estimated**: 0.5h (rework).

**This is the suckless path: reuse existing infrastructure, don't add new message types.**

#### Task E3.3: Tests for `Node::put_quorum`

```rust
#[tokio::test]
async fn put_quorum_succeeds_with_any_policy_on_single_peer() {
    // Setup: Node with 1 peer actor that immediately acks.
    let mut node = test_node_with_peer();
    let policy = AckPolicy::default(); // QuorumPolicy::Any, 9s timeout
    
    let result = node.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(result.is_ok());
    let status = result.unwrap();
    assert_eq!(status.acked_by.len(), 1);
    assert!(status.timed_out.is_empty());
}

#[tokio::test]
async fn put_quorum_timeout_when_no_peers_ack() {
    // Setup: Node with 1 peer actor that NEVER acks.
    let mut node = test_node_with_silent_peer();
    let policy = AckPolicy::default().with_timeout(Duration::from_millis(100));
    
    let result = node.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(matches!(result, Err(AckError::Timeout { .. })));
}

#[tokio::test]
async fn put_quorum_no_peers_returns_error() {
    // Setup: Node with NO peers.
    let mut node = Node::new();
    let policy = AckPolicy::default();
    
    let result = node.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(matches!(result, Err(AckError::NoPeers { .. })));
}

#[tokio::test]
async fn put_quorum_quorum_policy_requires_majority() {
    // Setup: Node with 3 peers, 2 ack, 1 silent. Quorum(2).
    let mut node = test_node_with_three_peers_two_ack();
    let policy = AckPolicy::for_peer_count(3); // ⌈3/2⌉ = 2
    
    let result = node.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(result.is_ok());
    let status = result.unwrap();
    assert_eq!(status.acked_by.len(), 2);
}
```

**Acceptance**: 4/4 tests pass.

**Estimated**: 2h.

**Epic E3 total**: ~4.5h, 4 new tests.

---

### Epic E4 — Timeout watchdog and integration

**Why fourth**: ensure timed-out entries are cleaned up (no memory leak).

#### Task E4.1: Add `Router::tick()` method to expire timed-out pending_acks

**File**: `rod/src/router.rs` — new method:

```rust
/// Called periodically by the Router's actor loop. Removes expired
/// pending_acks and sends `AckError::Timeout` to the requester.
pub fn tick(&mut self) {
    let now = Instant::now();
    let expired: Vec<String> = self.pending_acks
        .iter()
        .filter(|(_, p)| p.deadline <= now)
        .map(|(id, _)| id.clone())
        .collect();
    
    for put_id in expired {
        if let Some(pending) = self.pending_acks.remove(&put_id) {
            let required = pending.policy.quorum.required_acks(pending.peers.len());
            let timed_out: Vec<Addr> = pending.peers.iter()
                .filter(|p| !pending.acked_by.contains(p))
                .cloned()
                .collect();
            let err = AckError::Timeout {
                put_id: pending.put_id.clone(),
                acked_by: pending.acked_by.clone(),
                timed_out: timed_out.clone(),
                required,
                policy: pending.policy.clone(),
            };
            // Send timeout error back to requester as a Put with @ and error marker.
            let err_put = Put {
                id: random_string(8),
                from: self.addr.clone().unwrap_or_else(|| Addr::null()),
                in_response_to: Some(pending.put_id.clone()),
                updated_nodes: BTreeMap::new(),
                checksum: None,
                json_str: Some(serde_json::to_string(&err).unwrap_or_default()),
                peer_hop_list: None,
                recipients: None,
            };
            let _ = pending.requester.send(Message::Put(err_put));
        }
    }
}
```

**Acceptance**: timed-out entries are removed within one tick; no leak.

**Estimated**: 1.5h.

#### Task E4.2: Wire `tick()` into Router's actor loop

**File**: `rod/src/router.rs` — modify main message loop:

```rust
loop {
    tokio::select! {
        msg = msg_rx.recv() => { /* handle message */ }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {
            self.tick();
        }
    }
}
```

**Acceptance**: tick runs every 100ms; no message-processing latency added (select! prioritizes messages).

**Estimated**: 0.5h.

#### Task E4.3: Integration test — full put_quorum round-trip across 3 nodes

**File**: `rod/tests/network_fanout_ack_e2e.rs` (new)

**Pattern**: 3-node relay topology. Node A writes via `put_quorum(Quorum(2))`. Nodes B and C receive, ack. Node A's `put_quorum` resolves with 2 acks.

```rust
#[tokio::test]
async fn e2e_put_quorum_round_trip_three_nodes() {
    // 3 nodes connected via relay.
    let (mut node_a, mut node_b, mut node_c) = setup_three_node_topology();
    
    let policy = AckPolicy::for_peer_count(2); // ⌈2/2⌉ = 2, but required_peers=None means all peers
    let result = node_a.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(result.is_ok());
    let status = result.unwrap();
    assert!(status.acked_by.len() >= 2);
    
    // Verify B and C received the value.
    let val_b = node_b.get("key").once(Some(Duration::from_secs(1))).await;
    let val_c = node_c.get("key").once(Some(Duration::from_secs(1))).await;
    assert!(matches!(val_b, Some(Value::Text(_))));
    assert!(matches!(val_c, Some(Value::Text(_))));
}
```

**Acceptance**: 1/1 test passes; demonstrates full Gun.js ask-pattern via Rod.

**Estimated**: 1.5h.

#### Task E4.4: Integration test — quorum times out when peers don't ack

**File**: append to `rod/tests/network_fanout_ack_e2e.rs`

```rust
#[tokio::test]
async fn e2e_put_quorum_times_out_when_silent_peers() {
    let mut node_a = test_node_with_two_silent_peers();
    let policy = AckPolicy::default().with_timeout(Duration::from_millis(200));
    
    let result = node_a.put_quorum("key", Value::Text("v".into()), policy).await;
    
    assert!(matches!(result, Err(AckError::Timeout { .. })));
    if let Err(AckError::Timeout { acked_by, timed_out, .. }) = result {
        assert!(acked_by.is_empty());
        assert_eq!(timed_out.len(), 2);
    }
}
```

**Acceptance**: 1/1 test passes; demonstrates failure mode.

**Estimated**: 1h.

**Epic E4 total**: ~4.5h, 2 new tests.


---

### Epic E5 — Documentation, ADRs, final verification

**Why last**: doc captures the design; verification proves green.

#### Task E5.1: Update `rod/src/router.rs` module docs

**File**: `rod/src/router.rs` lines 1-50 (module docs).

**Add section on network fanout ack semantics**:

```rust
//! ## Network Fanout Ack (Gun.js ask-pattern)
//!
//! `Node::put_quorum` extends the standard Put fanout with a per-peer ack
//! pattern modeled on Gun.js's `ask` module. When a put_quorum call is
//! in flight:
//!
//! 1. Node registers a `PendingAck` in `Router::pending_acks`, keyed by put_id
//! 2. `handle_put_relay` attaches `in_response_to: Some(put.id)` to each
//!    peer's Put copy so peers know to ack
//! 3. Peers receive the Put, persist via their own storage, and reply with
//!    a Put whose `@` field names the original put_id
//! 4. The originator's Router records the ack in `pending_acks[put_id].acked_by`
//! 5. When `acked_by.len() >= policy.quorum.required_acks(peers.len())`,
//!    the Router sends a Put-with-`@` back to the originating Node
//! 6. Node routes this via its existing `pending_puts` map to the
//!    `put_quorum` caller
//!
//! Timeout: `Router::tick()` runs every 100ms and expires pending_acks whose
//! `deadline <= now`. The Node caller receives `AckError::Timeout` with the
//! partial ack list.
//!
//! Failure modes:
//! - `NoPeers` — caller requested ack but no peers connected
//! - `LocalCommitFailed` — the local storage commit failed
//! - `Timeout` — local commit succeeded but quorum not reached
```

**Acceptance**: doc is the source of truth for new contributors.

**Estimated**: 1h.

#### Task E5.2: Add ADR `010-network-fanout-ack.md`

**File**: `rod/docs/adr/010-network-fanout-ack.md` (new).

**Content**: capture D1-D15 with rationale, alternatives considered (B1 per-peer, B3 background reconciliation), and consequences.

**Acceptance**: ADR committed.

**Estimated**: 1h.

#### Task E5.3: Update `rod/src/lib.rs` re-exports

**File**: `rod/src/lib.rs` — re-export the new types so downstream (mnemos, loom-net) can use them.

```rust
pub use ack::{AckError, AckPolicy, QuorumPolicy, ReplicationStatus};
```

**Acceptance**: `cargo check -p mnemos-rod` still clean.

**Estimated**: 0.5h.

#### Task E5.4: Run 4-feature-config verification

```bash
cargo check -p rod --no-default-features --features scripting
cargo check -p rod --features 'scripting reasoning-capture'
cargo check -p rod --features 'scripting phi-proxy-embeddings'
cargo check -p rod
```

**Acceptance**: 4/4 clean (zero errors, zero new warnings).

**Estimated**: 0.5h.

#### Task E5.5: Run full test suite 5x consecutively

```bash
for i in 1 2 3 4 5; do
  echo "=== Run $i ==="
  cargo test -p rod --lib 2>&1 | tail -5
done
```

**Acceptance**: 5/5 runs green. Total test count should be 446 + 16 new = 462.

**Estimated**: 1h.

#### Task E5.6: Run clippy with `-D warnings`

```bash
cargo clippy -p rod --all-features -- -D warnings
```

**Acceptance**: zero warnings.

**Estimated**: 0.5h.

#### Task E5.7: Commit with structured message

```bash
git add -A
git commit -m "feat(rod): network fanout ack (Gun.js ask-pattern, quorum-B2)

Follow-up A from rod_async_ack_drain_redux_plan_comprehensive.

The Router previously did not track whether replicated Puts reached
remote peers. handle_put_relay sent to server_peers via unbounded
channels with no ack signal — caller's put() returned Ok locally
while remote peers may have received nothing.

This commit implements Gun.js ask-pattern semantics for peer-ack:
- New ack.rs types: QuorumPolicy, AckPolicy, ReplicationStatus, AckError
- New Router field: pending_acks: HashMap<String, PendingAck>
- New Node API: Node::put_quorum() — opt-in ack-aware write
- Router::tick() expires timed-out pending_acks every 100ms
- handle_put ack branch extended to route peer-acks via pending_acks
- handle_put_relay attaches in_response_to when an ack is pending

Wire format unchanged: Put.in_response_to (@ field) already speaks
Gun.js ask-pattern. No protocol break. Existing fire-and-forget
Node::put() is preserved unchanged.

Tests: 16 new unit + 2 integration, 5/5 clean runs, 0 warnings.

Reference: gun-js/src/ask.js lack=9000ms default."
```

**Acceptance**: commit lands; can be merged to main.

**Estimated**: 0.5h.

**Epic E5 total**: ~5h.

---

## Total Estimate

**~20.5 hours, 16 tasks, 5 epics, ~20 new tests**

| Epic | Hours | Tests |
|------|-------|-------|
| E1 — Foundations | 2.5h | 6 |
| E2 — Router routing | 4.5h | 4 |
| E3 — Node API | 4.5h | 4 |
| E4 — Watchdog + e2e | 4.5h | 2 |
| E5 — Docs + verify | 5h | 0 (verify only) |
| **Total** | **~21h** | **~16** |

Pre-existing test count: 446. Post-land: ~462.

---

## Execution Order & Parallelism

### Dependency graph

```
E1 (foundations) ──> E2 (router routing) ──> E3 (Node API) ──> E4 (watchdog + e2e) ──> E5 (docs + verify)
                  │
                  └──> E5.1, E5.2 (docs can be written any time after E1)
```

### Parallelism opportunities

- **E1.1, E1.2** are sequential (E1.2 re-exports E1.1's types).
- **E2.1 and E2.3** can run in parallel once E1.3 lands (different router methods).
- **E5.1 and E5.2** can run any time after E1 (docs don't depend on code).
- **E4.1 and E3.3** can run in parallel (timeout watchdog + Node tests are independent).

### Recommended session schedule

| Session | Tasks | Duration | Test count delta |
|---------|-------|----------|------------------|
| 1 | E1.1, E1.2, E1.3 + E5.1 (router docs) | ~4h | +6 |
| 2 | E2.1, E2.2, E2.3, E2.4 (router routing) | ~5h | +4 |
| 3 | E3.1, E3.2 (Node API + rework) | ~3h | +0 (no new tests yet) |
| 4 | E3.3 (Node API tests) + E4.1 (tick) + E4.2 (loop wiring) | ~5h | +4 |
| 5 | E4.3, E4.4 (e2e tests) | ~3h | +2 |
| 6 | E5.2, E5.3, E5.4, E5.5, E5.6, E5.7 (ADR + verify + commit) | ~4h | 0 (verify) |

**Total: ~6 sessions at ~4h/session average.**


---

## Risk Register

| Risk | Severity | Mitigation |
|------|----------|------------|
| Peer-ack Puts get deduplicated by Gun.js DAM (`ack + checksum` dedup at router.rs:393-401) | High | The DAM dedup checks `in_response_to + checksum`. Peer-acks from different peers have the SAME `@` (the originator's put_id) but DIFFERENT `checksum` values (each peer computes its own). Confirmed: dedup keys on `@+##` not just `@`, so multiple peer-acks for the same put_id all flow through. Verify with E2.4 tests. |
| `Router::tick()` adds 100ms latency to message processing | Low | `tokio::select!` prioritizes incoming messages. Tick fires only when msg_rx has nothing. No measurable latency added. Benchmark in E5.5 verification. |
| Pending_acks map grows unbounded if peers ack and Node crashes before consuming the oneshot | Medium | Add `cleanup_on_drop` semantics: if the oneshot is dropped (Node panicked/cancelled), the pending_ack is removed via a Drop impl on a guard type. Document in E3.1 rework. |
| `Message::Put` ack routing creates infinite loop (peer A acks, peer B acks, peer C acks... → all routed back through `pending_acks`) | Low | The ack routing only fires when `pending_acks.contains_key(in_response_to)`. After ack reached, entry is removed. Subsequent acks from late peers are dropped silently (logged at debug). Add a counter for "late acks dropped" in metrics. |
| `Put.in_response_to: Some(put.id)` causes recursion when peer A forwards to peer B (B's ack goes to A, who forwards as Put-with-@=B's-id, which is not A's id, so no loop) | Low | Verified: the @ field carries the ORIGINAL put_id from the originator, not the relaying peer's id. Recursion naturally bounded by the hop_list. |
| Mnemos callers depend on unbounded fanout (fire-and-forget for all writes) | None | `Node::put()` is unchanged. Mnemos continues to use `Node::put()`. Only callers who explicitly opt into `put_quorum` pay the latency cost. |
| `Router::addr` doesn't exist (used in E4.1 fallback) | Medium | Use `pending.requester` directly when sending the timeout error Put — no need for `self.addr`. Verify in E4.1 implementation. |
| Gun.js peers (older Rod nodes) might interpret `@`-tagged Put as a Get-reply and route it incorrectly | Low | They won't have an entry in `seen_get_messages` for that `@` (since it's a peer-ack, not a Get-reply). The Put is silently dropped. **This is fail-safe** — the originator never sees the ack but the local commit succeeded. Acceptable degradation. |
| `tokio::time::sleep(100ms)` in the tick loop blocks shutdown | Low | Use `tokio::time::interval` with `MissedTickBehavior::Skip` and shutdown signal. Verify in E4.2. |

---

## Why This Is The Elegant Solution

### Reusing Gun.js wire format is the right precedent

Gun.js's `ask` module has been the canonical Gun.js request/response pattern for years. Rod already speaks its wire format (`@` field on Put). **We are not inventing a new protocol; we are extending an existing routing pattern (currently scoped to Get→reply) to also cover peer→peer Put→reply.**

### Extending `seen_get_messages` semantics is the suckless choice

The existing `seen_get_messages` table tracks Get-requester correlations. We add a sibling `pending_acks` table for peer-ack correlations. The `handle_put` ack branch checks BOTH. **No code in the Get-ack path changes. No existing tests break.** New functionality is purely additive.

### No new Message variant in the final design (D-Revised)

Originally I proposed `Message::ReplicationStatus(ReplicationStatus)`. On reflection (during E3.2), I realized **the existing `Message::Put` with `in_response_to` field is already the right carrier** — the Router can encode the timeout error in the Put's `json_str` field or just use the @ field to signal back. **Zero new message types. Zero new wire types.** Pure routing-table addition.

### `QuorumPolicy::Quorum(⌈N/2⌉)` matches mnemos's actual deployment

The palace shows mnemos typically runs 1-5 peers per zone. Quorum:
- 1 peer → `Quorum(1)` → 1 ack needed
- 3 peers → `Quorum(2)` → 2 acks needed
- 5 peers → `Quorum(3)` → 3 acks needed

This is **Raft's quorum rule**, which is the most-studied consistency model in distributed systems literature. We are not inventing; we are applying a known-correct algorithm.

### Idempotent tests + 5x verification + Gun.js precedent = confidence

The Gun.js ask-pattern has shipped in production for years. Rod's existing `in_response_to` wire format has shipped. The only new code is the **router-level routing table** and the **timeout watchdog**. The blast radius is contained to one file (`router.rs`) plus the new `ack.rs` types.

---

## Resume Protocol

```bash
cd /home/guan/src/rod
git checkout feat/followup-a-network-fanout-ack
git pull origin feat/followup-a-network-fanout-ack

# Read the plan
cat docs/plans/ROD-FOLLOWUP-A-NETWORK-FANOUT-ACK.md

# Check progress
git log --oneline --grep "fanout\|ack\|quorum"
grep -rn "AckPolicy\|pending_acks\|put_quorum" crates/rod/src/ 2>/dev/null

# Verify clean baseline
cargo check -p rod
cargo test -p rod --lib 2>&1 | tail -3
```

Diary check (built-in memory): `rod_followup_a_network_fanout_ack_plan` (mirror of this file).

---

## Open Questions for Freeman

1. **Default quorum policy for `put_quorum`?** — Plan choice: `QuorumPolicy::Any` (Gun.js default, fastest). `AckPolicy::for_peer_count(n)` provides majority as an opt-in alternative.
2. **Timeout watchdog interval (100ms)?** — Plan choice: 100ms, configurable later via Router config.
3. **Should `put_quorum` automatically use majority for ≥2 peers, or always require explicit policy?** — Plan choice: explicit `AckPolicy` parameter (caller chooses). `AckPolicy::for_peer_count(n)` is the convenience helper.
4. **Wire-format compatibility with older Rod nodes?** — Plan choice: fail-safe drop of unknown `@` values. Documented in D15.

---

## Related Work & Cross-References

- **Parent plan**: `rod_async_ack_drain_redux_plan_comprehensive` (shipped 2026-07-20, merged to `main`).
- **Sibling plan**: `ROD-FOLLOWUP-B-BOUNDED-CHANNEL-SILENT-DROP.md` (Follow-up B, separate fix for storage actor channel bounded drops).
- **Gun.js precedent**: `/home/guan/src/gun-js/src/ask.js` — `lack` timeout (9000ms default), `#`/`@` wire format.
- **Rod wire format**: `rod/src/message.rs:61, 102-103, 391-393` — `Put.in_response_to` already serializes as `@`.
- **Existing Get-ack routing**: `rod/src/router.rs:402-411` — the pattern this plan extends.
- **loom-net consumer**: `wing_code/loom-engine` palace entry — will use `put_quorum` for authority state writes.
- **Existing ADRs**: `rod/docs/archive/adr/006-flush-ack-protocol.md` — the original ack protocol.

---

**Plan status**: LOCKED. Awaiting "begin" signal from Freeman.


---

## Design Pivot v1 → v2 (2026-07-22)

**Status**: Amendment to locked v1 plan. Reflects actual implementation shipped in commits `91c680b`, `0f74055`, `e1acc2c`. Updated 2026-07-22 after substrate recon revealed pattern convergence with the redux-async-ack-and-drain branch.

### Why the pivot

During Phase 1 implementation (commit `91c680b`), substrate recon against `feat/rod-redux-async-ack-and-drain` revealed Rod has **converged on a unified sentinel-drain pattern** for all async ack flows:

| Domain | Sentinel | Drain trigger |
|--------|----------|---------------|
| `Node::put` (storage commit) | `_ack` / `_err` | `decode_put_ack_payload` |
| `Node::batch_put` | `_ack` | Same drain |
| `Flush` | `_ack` | `pending_flushes` oneshot |
| `Node::map()` replay | `__rod_replay_complete__` | After all children drained |

Building a parallel `pending_acks: HashMap<String, PendingAck>` with its own Result type, its own drain plumbing, and its own watchdog tick would have:
- Duplicated drain plumbing
- Undone the convergence the redux branch achieved
- Created two separate "how do I wait for an ack" patterns

Freeman's insight ("are oneshots more robust? do the redux/async-ack-and-drain fixes apply?") triggered the pivot. The architectural truth: **same drain envelope, different decoder**.

### What changed (v1 → v2)

| Aspect | v1 (locked plan) | v2 (shipped) | Why |
|--------|------------------|--------------|-----|
| D6 Result type | `AckError` (thiserror) | `Result<_, String>` (String) | Convention in codebase is `Result<(), String>` — `thiserror` would be inconsistent |
| D8 Map type | `pending_acks: HashMap<String, PendingAck>` | `quorum_entries: BoundedHashMap<String, QuorumEntry>` | BoundedHashMap is the canonical Rod pattern (FIFO eviction, used for `seen_get_messages`, `pending_puts`) |
| D9 Router methods | `register_pending_ack`, `complete_pending_ack` | `handle_register_quorum`, `record_ack` (inline in `handle_put`) | Single method per concern; ack-record happens via the existing `Put { in_response_to: ... }` reply path |
| D12 ReplicationStatus | `(acked_by, pending, timed_out): Vec<Addr>` | `(put_id: String, acked_by: usize, quorum_met: bool, elapsed: Duration)` | Simpler; consumers care about TREND (met/not-met, count) not which specific peers |
| QuorumPolicy | `enum { Any, Quorum(usize), All }` | `quorum: usize` (with `usize::MAX` = all, `1` = any) | Avoids one layer of indirection; semantics are equivalent |
| PendingAck | struct with `policy, requester, peers, acked_by, deadline, put_id` | `QuorumEntry` struct with `requester, required, received, acked_by, started_at, max_timeout` | Same shape, different field names |
| Timeout mechanism | `Router::tick()` watchdog every 100ms | `Router::pre_start` spawns `tokio::time::interval(1s)` reaper task that sends `Message::CheckQuorumTimeouts` self-message | Self-message pattern is canonical Rod; avoids separate tokio select! integration |
| Sentinel constant | (none in v1) | `QUORUM_MET_SENTINEL = "__quorum_met__"` | Extends `__rod_replay_complete__` precedent |

### What's unchanged

| Aspect | Both v1 and v2 |
|--------|----------------|
| D1 | Gun.js ask-pattern semantics, `#`/`@` wire format unchanged |
| D2 | Default timeout = 9000ms (matches Gun.js's `lack`) |
| D3 | Default quorum = ⌈N/2⌉ for N≥2, "any" for N=1 |
| D4 | AckPolicy exists (simpler in v2: just quorum + timeout) |
| D5 | ReplicationStatus exists (simpler in v2) |
| D7 | QuorumPolicy concept (folded into AckPolicy in v2) |
| D10 | `handle_put_relay` sends with `in_response_to` for peer-acks |
| D11 | `handle_put` ack branch routes peer-acks (new sibling path, original Get-reply path preserved) |
| D12 | `Node::put_quorum()` is a new method, `Node::put()` unchanged |
| D13 | No new crates, no new dependencies |
| D14 | Mnemos caller impact: zero unless opt-in |
| D15 | Wire compatibility: existing Rod nodes drop unknown `@` values |

### v2 Type Definitions (Actual Implementation)

#### `AckPolicy` (src/ack.rs, 296L file)

```rust
pub struct AckPolicy {
    /// Number of peer acks required to satisfy the policy.
    /// 1 = any, usize::MAX = all.
    pub quorum: usize,
    /// Maximum time to wait before the request resolves with Err.
    pub timeout: Duration,
}

impl AckPolicy {
    /// "First ack wins" with the Gun.js default 9-second timeout.
    pub fn any() -> Self;
    /// Majority (⌈N/2⌉) quorum for N peers.
    pub fn for_peer_count(n: usize) -> Self;
    /// All peers must ack (effective bound: fanned-out peer count).
    pub fn all() -> Self;
    /// Builder: set timeout.
    pub fn with_timeout(self, timeout: Duration) -> Self;
    /// Builder: set quorum count.
    pub fn with_quorum(self, quorum: usize) -> Self;
}

impl Default for AckPolicy {
    fn default() -> Self { Self::any() }  // 9s timeout, quorum=1
}
```

#### `ReplicationStatus` (src/ack.rs)

```rust
pub struct ReplicationStatus {
    pub put_id: String,
    pub acked_by: usize,       // count of peer acks observed
    pub quorum_met: bool,      // always true in Ok arm
    pub elapsed: Duration,     // wall-clock time from put_quorum invocation
}
```

#### `QuorumEntry` (src/router.rs, private to Router)

```rust
pub(crate) struct QuorumEntry {
    pub requester: Addr,                // original Node addr
    pub required: usize,                // = AckPolicy.quorum
    pub received: usize,                // count of acks so far
    pub acked_by: HashSet<Addr>,        // dedup set
    pub started_at: Instant,            // for timeout tracking
    pub max_timeout: Duration,          // = AckPolicy.timeout (captured at registration)
}
```

#### `AckKind` (src/node.rs, module-level)

```rust
#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    Local,              // wait for storage _ack/_err
    Quorum(AckPolicy),  // wait for Router __quorum_met__ sentinel
}
```

### Wire Format (v2)

**Request** (Peer receives Put from originator with `in_response_to = put_id`):
```json
{
  "#": "<put_id>",
  "@": null,
  "put": { ...payload... }
}
```

**Success reply** (Originator receives Put with `__quorum_met__` sentinel):
```json
{
  "@": "<put_id>",
  "put": {
    "__quorum_met__": {
      "_": { "value": <ack_count_as_number> }
    }
  }
}
```

**Timeout notification** (Originator receives Put with `__quorum_met__` + Bool(true)):
```json
{
  "@": "<put_id>",
  "put": {
    "__quorum_met__": {
      "_": { "value": true }
    }
  }
}
```

`decode_quorum_payload` distinguishes:
- `Number(ack_count)` → `Ok(ReplicationStatus { acked_by: ack_count, ... })`
- `Bool(true)` → `Err("quorum timed out")`
- `else` → `None` (malformed, falls through to `_ack` decoder)

### Phase Status (verified 2026-07-22)

| Phase | Status | Commit | What |
|-------|--------|--------|------|
| **Phase 1** | ✅ SHIPPED | `91c680b` | `src/ack.rs` (AckPolicy + ReplicationStatus + QUORUM_MET_SENTINEL, 9/9 tests) |
| **Phase 2** | ✅ SHIPPED | `0f74055` | Message enum `RegisterQuorum` + Router `QuorumEntry` + `quorum_entries: BoundedHashMap` + `handle_put` ack-branch fires `__quorum_met__` sentinel + BoundedHashMap `take()` method |
| **Phase 3** | ✅ SHIPPED | `e1acc2c` | `Node::put_quorum()` + `Node::put_internal()` + `AckKind` + `decode_quorum_payload` + `pending_puts` type refactored to `Result<ReplicationStatus, String>` |
| **Phase 4** | ⚠️ ON DISK | (uncommitted, ~127 lines) | Cleanup reaper: `Message::CheckQuorumTimeouts` + `QuorumEntry::max_timeout` + `handle_quorum_timeout_reaper` + `BoundedHashMap::iter()` |
| **Phase 5** | ❌ TODO | — | 16 unit tests + 3 e2e tests |
| **Phase 6** | ❌ TODO | — | ADR-011 + this plan amendment |
| **Phase 7** | ❌ TODO | — | 5 consecutive clean e2e runs + merge |

### Carry-forward

- Built-in: `rod_followup_a_network_fanout_ack_plan` — still valid as high-level overview, but see this amendment for actual implementation
- Built-in: `rod_quorum_drain_realizations_2026_07_21` — captures the redux-pattern insights that drove the pivot
- Built-in: `rod_sentinel_drain_pattern_observation` — substrate convergence on sentinel-drain as canonical idiom
- Built-in: `phase1_v1_to_v2_pivot_insight` — process lesson (cross-reference adjacent branches before locking plan)

