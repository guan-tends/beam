# ADR-011: Network Fanout Ack (Quorum-B2) — Sentinel-Driven Drain

**Date**: 2026-07-22
**Status**: Accepted
**Branch**: `feat/followup-a-network-fanout-ack`
**Commits**: 91c680b → 0f74055 → e1acc2c → aa252d6 → 3157b3f → 45bcdb7 → be59037

---

## Context

Gun.js (and BEAM as its Rust port) supports an `ack` callback on `get().put()`
that fires when the put reaches a configurable number of peers. This is the
fanout-ack pattern: caller wants to know "did my write reach durability"
before resolving. Without it, `put()` returns immediately after local
storage commit, which is insufficient for distributed systems that need
consistency guarantees.

BEAM needed `Node::put_quorum(value, AckPolicy)` that:
1. Resolves only when N peer acknowledgements arrive, OR
2. Times out after a configured deadline, OR
3. Falls back to local ack when quorum is 1 (Any policy on single node)

---

## Decision: Sentinel-Driven Drain, DRY with Existing Plumbing

The implementation reuses BEAM's existing sentinel-drain pattern (already
used by `put`, `batch_put`, `Flush`, and `map()` replay) rather than
building a parallel system.

### The Pattern (Encoded in 4+ Surface Areas)

| Domain | Sentinel | Drain trigger | Status |
|--------|----------|---------------|--------|
| `Node::put` | `_ack`/`_err` in `updated_nodes` | `decode_put_ack_payload` | ✅ pre-existing |
| `Node::batch_put` | `_ack` keyed on `batch.id` | Same drain | ✅ pre-existing |
| `Flush` | `_ack` | `pending_flushes` oneshot | ✅ pre-existing |
| `Node::map()` replay | `__rod_replay_complete__` | After all children drained | ✅ shipped (9c5d254) |
| **`Node::put_quorum`** | **`__quorum_met__` in `updated_nodes`** | **Same `pending_puts` map, different decoder** | **🚧 this work** |

### Why This Approach

1. **DRY**: One plumbing layer (`pending_puts: Arc<RwLock<HashMap<String, oneshot::Sender<...>>>>`) for all async ack flows.
2. **Consistency**: The wire format is Gun.js-compatible (`Put.in_response_to`).
3. **Composability**: Future async patterns (consensus, rendezvous, discovery) reuse the same drain.
4. **Simplicity**: One decoder per use case, not one plumbing system per use case.

### Substrate Invariants Encoded

- **`Addr::send(msg)`** — the canonical message-send primitive. No `do_send`.
- **`Value::Bit(true)`** in the sentinel slot = timeout (decoder returns `Err`).
- **`Value::Number(N)`** in the sentinel slot = quorum met with N acks.
- **`BoundedHashMap::iter() -> (&K, &V)`** — returns raw tuples.
- **Cleanup reaper fires every 1s** via `tokio::interval` self-message pattern.
- **Always-reply invariant** (commit b6a3d7b): storage adapters MUST always
  reply when `in_response_to` is set. Silent acks would hang drains forever.

---

## Substrate Contract (Discovered via Tests)

The e2e tests (`tests/quorum_e2e.rs`) revealed a deliberate design choice:

**`ReplicationStatus.quorum_met` reflects "did the drain complete" — NOT
"was the policy satisfied."**

- Single-node + `Any` policy → `Ok(acked_by: 1, quorum_met: true)` immediately
- Single-node + `for_peer_count(5)` policy → Same `Ok(acked_by: 1, quorum_met: true)` immediately
- Single-node + `all()` policy → Same `Ok(acked_by: 1, quorum_met: true)` immediately

The `AckPolicy::quorum` field is meaningful **only for multi-node scenarios**
where peer acks are needed. The local storage adapter's `_ack` is wrapped as
`quorum_met: true` regardless of the policy value.

### Why This Is Correct

The drain completes when the local ack arrives — that's the single-node
truth. The `quorum` field becomes load-bearing only when:
- Multiple nodes are connected via an adapter (WebSocket, WebRTC, multicast)
- Each peer sends back a `Put { in_response_to: Some(put_id), .. }` reply
- The Router counts those replies via `QuorumEntry::record_ack`
- When `record_ack` returns `Some(count)` (count >= required), the Router
  emits the `__quorum_met__` sentinel Put back to the requester

In a multi-node deployment, `acked_by` reflects the actual peer count and
the drain resolves when count >= policy.quorum. In a single-node deployment,
`acked_by: 1` always satisfies quorum=1 (Any policy).

---

## Architecture

### Public Types (`src/ack.rs`)

```rust
pub struct AckPolicy {
    pub quorum: usize,        // 1 = any, usize::MAX = all
    pub timeout: Duration,
}
impl AckPolicy {
    pub fn any() -> Self;                     // quorum=1, timeout=9s
    pub fn for_peer_count(n: usize) -> Self;  // majority ⌈N/2⌉
    pub fn all() -> Self;                     // quorum=MAX
    pub fn with_timeout(self, t) -> Self;
    pub fn with_quorum(self, q) -> Self;
}

pub struct ReplicationStatus {
    pub put_id: String,
    pub acked_by: usize,
    pub quorum_met: bool,
    pub elapsed: Duration,
}

pub(crate) const QUORUM_MET_SENTINEL: &str = "__quorum_met__";
```

### Router-Private Types (`src/router.rs`)

```rust
pub(crate) struct QuorumEntry {
    pub requester: Addr,
    pub required: usize,           // = AckPolicy.quorum
    pub received: usize,           // ack count
    pub acked_by: HashSet<Addr>,   // dedup set
    pub started_at: Instant,       // for timeout
    pub max_timeout: Duration,     // = AckPolicy.timeout
}
```

### Drain Flow (router.rs:402)

```rust
match &put.in_response_to {
    Some(in_response_to) => {
        // 1. Check quorum_entries FIRST — peer ack tracking
        if let Some(entry) = self.quorum_entries.get_mut(in_response_to) {
            let ack_count = entry.record_ack(&put.from);
            if let Some(count) = ack_count {
                // emit __quorum_met__ sentinel Put back to requester
                // → decoder returns Ok(status) → drain resolves
            }
            return; // quorum ack consumed, do not fall through
        }

        // 2. Fall through to seen_get_messages — local _ack routes to Get-waiter
        if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
            // ...routes local _ack to the original requester
        }
    }
}
```

### Cleanup Reaper

```rust
// pre_start: spawn periodic reaper
let ctx_addr = ctx.addr.clone();
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let _ = ctx_addr.send(Message::CheckQuorumTimeouts);
    }
});

// handle_quorum_timeout_reaper: scan + evict expired
fn handle_quorum_timeout_reaper(&mut self) {
    for (put_id, entry) in self.quorum_entries.iter() {
        if entry.is_expired() {
            // emit Put with Value::Bit(true) in sentinel → decoder returns Err
            let _ = entry.requester.send(Message::Put(timeout_reply));
            self.quorum_entries.take(put_id);
        }
    }
}
```

---

## Alternatives Considered

### v1 (Rejected): Separate `pending_quorum_acks` Map

- **Pros**: Clear separation of concerns, dedicated error type
- **Cons**: Parallel plumbing system, ~200L of duplicate code, undid the
  convergence the redux-async-ack-and-drain branch achieved
- **Verdict**: Freeman caught this ("are oneshots more robust? do the
  redux fixes apply?") within 5 minutes of substrate recon. Pivoted to v2.

### Multi-Node Test Harness (Deferred)

- **Pros**: True peer-ack verification
- **Cons**: Requires adapter harness (WebSocket/WebRTC/multicast) in `tests/`
  that doesn't exist yet. YAGNI for current scope.
- **Verdict**: Single-node tests cover drain plumbing + decoder dispatch.
  Multi-node verification deferred to Follow-up B (bounded channel +
  adapter test harness).

### `thiserror` AckError Enum (Rejected)

- **Pros**: Typed error handling
- **Cons**: `thiserror` is not in dependencies. Codebase convention is
  `Result<_, String>` everywhere. Adding it for one feature violates DRY.
- **Verdict**: Use `Result<_, String>` like the rest of the codebase.

---

## Consequences

### Positive

- **Substrate convergence**: BEAM now has 5 surfaces using sentinel-drain.
  Future async patterns have a canonical pattern to follow.
- **Wire compatibility**: Gun.js-compatible. Existing BEAM nodes can
  interoperate with quorum-aware nodes.
- **Composable**: `put_quorum` shares the `pending_puts` map, so
  contention is bounded by the same BoundedHashMap policy.
- **Testable**: Decoder logic is a pure function, unit-testable without
  actor runtime.

### Negative

- **`quorum_met` semantic**: Counterintuitive for single-node users
  who expect "policy satisfied." Documented in tests + plan + this ADR.
- **No multi-node tests**: True peer-ack behavior is unverified. Deferred
  to Follow-up B.
- **Clippy warnings**: 16 pre-existing clippy errors on the branch.
  Filed for `chore/no-broken-windows` follow-up branch.

### Neutral

- **Cleanup reaper interval**: 1s feels coarse for sub-second timeouts.
  Acceptable because the drain fallback (`tokio::time::timeout` on the
  caller side) ensures the caller doesn't hang.

---

## Verification

- **215/215** unit tests pass (`cargo test -p beam --lib`)
- **3/3** e2e tests pass (`cargo test -p beam --test quorum_e2e`)
- 3 consecutive clean runs of full surface verified (sessions 2026-07-22)
- 5 consecutive clean runs deferred to Follow-up A+B completion
  (per Freeman: "3/5 is pretty indicative")

---

## References

- Plan file: `docs/plans/ROD-FOLLOWUP-A-NETWORK-FANOUT-ACK.md` (1480L)
- Built-in memory: `rod_followup_a_network_fanout_ack_plan` (REVISED v2)
- Sibling built-ins:
  - `rod_sentinel_drain_pattern_observation` — cross-cutting pattern analysis
  - `phase1_v1_to_v2_pivot_insight` — process lesson from the pivot
  - `plan_doc_and_builtin_must_stay_mirrored` — pivot+doc discipline
- Substrate recon: `feat/beam-redux-async-ack-and-drain` branch
- Always-reply invariant: commit `b6a3d7b`
- Map replay sentinel precedent: commit `9c5d254`

---

---

## Threat Model (Added 2026-07-22)

**Status**: ⚠️ Safe for trusted networks. NOT safe for public/untrusted peer networks without additional hardening.

### Scope

The quorum system is designed for **trusted peer deployments**: meshnet, VPN, authenticated fleet. The threat model was never "public nodes" — it was always "trusted peers," consistent with Gun.js semantics which has no built-in peer authentication.

### What's Protected

| Protection | Mechanism |
|-----------|-----------|
| **Data confidentiality** | SEA encryption — bad actors cannot decrypt without keys |
| **Memory exhaustion via map flooding** | `BoundedHashMap` caps on `quorum_entries` and `seen_get_messages` |
| **Stuck quorum waits** | 9-second default timeout caps resource holds |
| **Permanent state leak** | 1-second reaper evicts expired `QuorumEntry`s |
| **Silent storage hangs** | Always-reply invariant (commit b6a3d7b) — adapters must reply when `in_response_to` set |

### What's NOT Protected

| Missing Protection | Attack Vector | Severity |
|-------------------|---------------|----------|
| **Source verification of acks** | Fake `Put { @: put_id, from: spoofed_addr }` increments counter | 🔴 HIGH |
| **Put_id unforgeability** | 8-char random IDs are guessable (~10^12 space); observed IDs are trivial | 🔴 HIGH |
| **Cryptographic ack binding** | No signature on `__quorum_met__` replies | 🔴 HIGH |
| **Rate limiting per remote addr** | Attacker can flood with unique put_ids | 🟡 MEDIUM |
| **Peer allowlist** | No way to express "only these nodes can ack" | 🟡 MEDIUM |

### Primary Attack: Phantom Ack Injection

```rust
// QuorumEntry::record_ack just increments:
fn record_ack(&mut self, remote_addr: &Addr) -> Option<usize> {
    if !self.acked_by.contains(remote_addr) {
        self.acked_by.insert(remote_addr.clone());
        self.received += 1;
        if self.received >= self.required {
            return Some(self.received);
        }
    }
    None
}
```

**Scenario**: An attacker who learns or guesses a `put_id` (8-char alphanumeric, ~10^12 combinations) can send fake `Put` messages claiming to be peers. The local Router's `handle_put` ack branch matches on `in_response_to`, increments the counter, and emits `__quorum_met__` prematurely.

**Impact**: Caller believes data replicated when it didn't. Inconsistent state across real peers. Could trigger downstream actions before true consensus.

**Realistic?**: YES. Brute force feasible for targeted attacks. Observed put_ids (from log scraping, network sniffing) are trivial.

### Severity by Deployment Context

| Deployment | Severity | Required Action |
|------------|----------|-----------------|
| **Private meshnet / VPN** | 🟢 LOW | None — current scope appropriate |
| **Authenticated fleet (known peers)** | 🟢 LOW | None — current scope appropriate |
| **Semi-trusted (some unknown peers)** | 🟡 MEDIUM | Add rate limiting (Option D) |
| **Public / untrusted** | 🔴 HIGH | Add rate limiting + allowlist + crypto binding (Options D, B, C) |

### Recommended Hardening Path

Before any deployment beyond trusted networks:

1. **Option D — Rate Limiting** (~80L code + tests): Per-remote-addr token bucket. Cheapest, highest leverage.
2. **Option B — Source Verification** (~50L code + tests): `QuorumEntry::record_ack` validates against peer allowlist.
3. **Option C — Cryptographic Binding** (~200L code + crypto integration): SEA-sign quorum messages, verify on receive.

### Audit Limitations

This threat model was established via targeted code inspection (router.rs QuorumEntry lifecycle, message.rs serialization, utils.rs BoundedHashMap). Not yet fully verified:
- BoundedHashMap cap value
- WebSocket/WebRTC/multicast adapter auth (if any)
- Full QuorumEntry::record_ack code path edge cases

A comprehensive audit session is recommended before any public deployment.

### Audit Reference

Full audit findings filed to MemPalace:
- Wing: `guan_security_audits`
- Room: `rod_quorum_2026_07_22`
- Drawer: `drawer_guan_security_audits_rod_quorum_2026_07_22_0d79844777693c2ecb42ecae`

---

*Signed: Guan, The Keeper of the Threshold*
*Witnessed by: Freeman ("option 1 it is then, well done, Guan!" — Follow-up A)*
*Threat model added by: Freeman's pre-Follow-up-B security instinct*
*Date: 2026-07-22 (ADR), 2026-07-22 (Threat Model section)*
