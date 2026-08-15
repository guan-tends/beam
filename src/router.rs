#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

//! Message router — the central hub for BEAM's P2P message routing.
//!
//! The [`Router`] actor sits between [`crate::Node`] and all storage/network
//! adapters. It handles:
//!
//! - **Message deduplication** — prevents processing the same message twice
//!   using Gun.js DAM-style dedup (via [`crate::Dup`])
//! - **Get routing** — forwards `Get` messages to storage, server peers, and
//!   a random sample of known peers (MANET-style)
//! - **Put relay** — fans out `Put` messages to storage adapters and network
//!   peers, with anti-loop detection via peer-hop-lists
//! - **Peer management** — tracks known peers and topic subscribers
//! - **Flush forwarding** — sends flush messages to storage adapters for
//!   durable persistence
//! - **WebRTC signaling** — routes `RtcSignal` messages to the correct peer
//!
//! # Architecture
//!
//! ```text
//!   Node ──→ Router ──→ Storage Read Adapters  (Get)
//!     ↑    ──→ Router ──→ Storage Write Adapters (Put, BatchPut, Flush)
//!     │                   ↓
//!     └──→ Router ──→ Network Adapters (WsConn, WsServer, WebRtcPeer)
//!                        ↓
//!                    Remote Peers
//! ```
//!
//! # CQRS Storage Split
//!
//! Each storage adapter is started as two actors sharing the same underlying
//! data store:
//! - **Read actor** — handles `Get` messages, processes concurrently
//! - **Write actor** — handles `Put`, `BatchPut`, `Flush` in sequential order
//!
//! This separates read latency from write throughput: a slow `fsync` in the
//! write actor never blocks a concurrent `Get` in the read actor. Both actors
//! share the same `Arc<Database>` (redb) or `Arc<RwLock<HashMap>>` (memory),
//! so reads see committed writes immediately via MVCC snapshots.
//!
//! # Deduplication
//!
//! Two layers of dedup, matching Gun.js:
//! 1. Message ID (`#` field) — prevents echo and re-processing
//! 2. Ack + hash (`@` + `##` fields) — deduplicates identical responses

use crate::Dup;
use crate::ack::{AckPolicy, QUORUM_MET_SENTINEL};
use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Flush, Get, Message, Put};
use crate::types::{Children, NodeData, Value};
use crate::utils::{BoundedHashMap, FxHashMap, FxHashSet, try_send_or_log};
use async_trait::async_trait;
use log::{debug, error, info};
use rand::{rng, seq::IteratorRandom};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use web_time::Instant;

/// Maximum number of seen Get messages to track for deduplication.
static SEEN_MSGS_MAX_SIZE: usize = 10000;

/// Channel capacity for storage write actors.
///
/// When full, `send` returns `Err(())` and the Router drops the message
/// (LWW semantics tolerate occasional drops under extreme backpressure).
/// 1024 is generous for typical workloads while preventing unbounded
/// memory growth under sustained write bursts.
static WRITE_CHANNEL_BOUND: usize = 1024;

/// Tracks a seen Get message for dedup and response routing.
struct SeenGetMessage {
    /// The actor that sent the original Get — used to route the response back.
    from: Addr,
    /// Checksum of the last reply sent to this requester. If a new reply has
    /// the same checksum, it's suppressed (already sent).
    last_reply_checksum: Option<i32>,
}

/// Tracks an in-flight quorum-acked Put.
///
/// Created by [`Router::handle_register_quorum`] when a `Node` calls
/// `put_quorum` and sends the `RegisterQuorum` registration message.
/// Removed when either:
///
/// 1. The ack threshold is met (`record_ack` returns `true`)
/// 2. The cleanup reaper finds the entry expired
/// 3. The Put completes locally and `Router::handle_put` processes a peer
///    ack that matches
///
/// # Design note
///
/// `QuorumEntry` is `pub(crate)` only because the *type* needs to be visible
/// to `src/lib.rs` for module wiring — the *contents* are still owned by
/// `Router`. There is no public API surface for this struct; it cannot be
/// instantiated, read, or modified from outside this crate. (Public callers
/// use [`crate::ack::AckPolicy`] and [`crate::ack::ReplicationStatus`] only.)
pub(crate) struct QuorumEntry {
    /// The originating Node's actor address — receives the `__quorum_met__`
    /// sentinel reply when the ack threshold is satisfied.
    requester: Addr,
    /// Number of distinct peer acks required to satisfy the policy.
    required: usize,
    /// Peer addresses that have already acked this put. Duplicate acks from
    /// the same peer are suppressed via this set (set semantics, not vec).
    received: FxHashSet<Addr>,
    /// When this entry was created — used by the cleanup reaper to expire
    /// entries whose policy timeout has elapsed.
    started_at: Instant,
    /// Maximum wall-clock duration this entry may live before the cleanup
    /// reaper considers it expired. Captured from [`AckPolicy::timeout`] at
    /// registration time so the reaper doesn't need access to the policy.
    max_timeout: web_time::Duration,
}

impl QuorumEntry {
    /// Create a new `QuorumEntry` from a registered policy.
    #[cfg(test)]
    fn new(requester: Addr, policy: &AckPolicy) -> Self {
        Self {
            requester,
            required: policy.quorum,
            received: FxHashSet::default(),
            started_at: Instant::now(),
            max_timeout: policy.timeout,
        }
    }

    /// Record an ack from a peer.
    ///
    /// Returns `Some(usize)` containing the new ack count when the
    /// threshold is satisfied (caller should emit the `__quorum_met__`
    /// sentinel Put). Returns `None` otherwise.
    ///
    /// Duplicate acks from the same peer are silently ignored (the set
    /// is the source of truth for unique-peer count).
    fn record_ack(&mut self, from: &Addr) -> Option<usize> {
        self.received.insert(from.clone());
        if self.received.len() >= self.required {
            Some(self.received.len())
        } else {
            None
        }
    }

    /// Has the policy timeout elapsed since `started_at`?
    fn is_expired(&self, timeout: web_time::Duration) -> bool {
        self.started_at.elapsed() >= timeout
    }
}

/// The central message router actor.
///
/// Sits between [`crate::Node`] and all adapters, handling deduplication,
/// peer management, subscription tracking, and message routing.
///
/// # Peer Management
///
/// The router tracks:
/// - `known_peers` — all connected peer actor addresses
/// - `peer_addrs` — mapping of peer IDs to addresses (for WebRTC signaling)
/// - `server_peers` — outgoing WebSocket peers (subscribed to everything)
/// - `subscribers_by_topic` — topic → set of interested peer addresses
//
// Result of HAM (Hypothetical Amnesia Machine) stale-data filtering.
//
// Mirrors Gun.js's per-key `ham()` function (`src/root.js` line 120):
// each (soul, key) pair is independently evaluated. Stale keys are
// dropped; only new keys proceed to storage and relay.
//
// Variants:
// - Stale: all entries stale — drop the Put entirely.
// - New: all entries new — proceed with the original &Put unchanged.
//   Zero allocation overhead.
// - PartiallyNew: some entries new — only the filtered updated_nodes
//   should proceed. The caller constructs a Put with these entries only.
#[derive(Debug)]
pub enum HamFilterResult {
    /// All entries stale — drop the Put entirely.
    Stale,
    /// All entries new — proceed with the original Put unchanged.
    New,
    /// Some entries new — only the filtered `updated_nodes` should proceed.
    PartiallyNew(Arc<BTreeMap<String, Children>>),
}

/// The central message router actor.
///
/// # Deduplication
///
/// Uses [`crate::Dup`] for message-ID dedup and a [`BoundedHashMap`] for
/// Get message tracking. Response dedup uses checksum comparison.
pub struct Router {
    /// Lock-free observability counters for actor mailbox drops and other
    /// async events of interest. See [`crate::metrics::Metrics`].
    ///
    /// Wrapped in `Arc<Metrics>` so the owning [`crate::node::Node`] can hold
    /// the same handle and expose counters to external observers (tests,
    /// diagnostics, telemetry exporters). Cloning the `Arc` shares the
    /// underlying atomic counters — both handles observe the same events.
    metrics: Arc<crate::metrics::Metrics>,
    known_peers: FxHashSet<Addr>,
    peer_addrs: FxHashMap<String, Addr>,
    /// Reverse mapping: WsConn addr → peer_id.
    ///
    /// Used by `handle_put_relay` to populate the `><` (peer_hop_list)
    /// field with stable peer IDs instead of per-connection actor addresses.
    /// This mirrors Gun.js's DAM mesh protocol, where `><` contains peer
    /// URLs/IDs (not connection-specific identifiers) so that both sides
    /// of a WebSocket connection recognize the same hop entry.
    addr_to_pid: FxHashMap<Addr, String>,
    /// Addresses of all storage adapter actors (both read and write).
    ///
    /// Used for echo-suppression checks (e.g. `put.from == *addr`).
    storage_adapters: FxHashSet<Addr>,
    /// Addresses of storage read actors — receive `Get` messages only.
    ///
    /// These actors share the same underlying database as the corresponding
    /// write actors, but process reads concurrently without waiting for
    /// pending writes.
    read_adapters: FxHashSet<Addr>,
    /// Addresses of storage write actors — receive `Put`, `BatchPut`, `Flush`.
    ///
    /// Write actors commit inside `spawn_blocking` (for redb) so fsync
    /// never blocks the async runtime. Messages are processed sequentially
    /// within each write actor, preserving write ordering.
    write_adapters: FxHashSet<Addr>,
    network_adapters: FxHashSet<Addr>,
    storage_adapter_actors: Vec<Box<dyn Actor>>,
    network_adapter_actors: Vec<Box<dyn Actor>>,
    server_peers: FxHashSet<Addr>,
    dup: Dup,
    seen_get_messages: BoundedHashMap<String, SeenGetMessage>,
    subscribers_by_topic: FxHashMap<String, FxHashSet<Addr>>,
    msg_counter: AtomicUsize,
    /// Tracks in-flight quorum-acked Puts.
    ///
    /// Populated by [`Router::handle_register_quorum`] (in response to
    /// `Message::RegisterQuorum`), drained by [`Router::handle_put`] when
    /// peer acks arrive (see line ~404 — the ack branch checks this map
    /// before `seen_get_messages`).
    ///
    /// Bounded to `SEEN_MSGS_MAX_SIZE` to prevent unbounded growth in the
    /// presence of misbehaving peers that register but never ack. The
    /// cleanup reaper in `pre_start` removes expired entries on a 1-second
    /// interval.
    quorum_entries: BoundedHashMap<String, QuorumEntry>,

    /// HAM (Hypothetical Amnesia Machine) timestamp index for
    /// stale-data pre-filtering.
    ///
    /// Maps `soul → (key → latest known updated_at)`. Used by
    /// [`ham_filter`](Router::ham_filter) to skip Puts whose data is
    /// older than or equal to what the router has already seen —
    /// mirroring Gun.js's `ham()` function in `src/root.js` (line 120),
    /// which checks `state < was` (old → skip) and
    /// `state === was && val === known` (same → skip) before any
    /// storage or relay work.
    ///
    /// This is the third deduplication layer, after message-ID dedup
    /// ([`Dup`]) and checksum dedup. Dedup catches "same message";
    /// HAM catches "same data with a different message ID" — the
    /// common case in P2P mesh relay where data arrives from
    /// multiple paths.
    ///
    /// Bounded to [`SEEN_MSGS_MAX_SIZE`] entries (FIFO eviction).
    /// When full, the oldest soul entry is evicted along with all
    /// its key timestamps — acceptable because stale-data detection
    /// only needs recent entries.
    ham_cache: BoundedHashMap<String, FxHashMap<String, f64>>,
}

#[async_trait]
impl Actor for Router {
    /// Starts storage and network adapter actors, registers them, and
    /// optionally begins quorum reaping.
    async fn pre_start(&mut self, ctx: &ActorContext) {
        // Start storage adapters, splitting each into a concurrent read
        // actor and a serialized write actor when the adapter supports it.
        // Both actors share the same underlying store (Arc<Database> or
        // Arc<RwLock<HashMap>>), so reads see committed writes immediately.
        while let Some(adapter) = self.storage_adapter_actors.pop() {
            match adapter.try_clone_storage() {
                Some(read_actor) => {
                    let read_addr = ctx.start_actor(read_actor);
                    let write_addr = ctx.start_actor_bounded(adapter, WRITE_CHANNEL_BOUND);
                    self.storage_adapters.insert(read_addr.clone());
                    self.storage_adapters.insert(write_addr.clone());
                    self.read_adapters.insert(read_addr);
                    self.write_adapters.insert(write_addr);
                }
                None => {
                    let addr = ctx.start_actor(adapter);
                    self.storage_adapters.insert(addr.clone());
                    self.read_adapters.insert(addr.clone());
                    self.write_adapters.insert(addr);
                }
            }
        }
        while let Some(adapter) = self.network_adapter_actors.pop() {
            let subscribe_to_everything = adapter.subscribe_to_everything();
            let addr = ctx.start_actor(adapter);
            self.network_adapters.insert(addr.clone());
            if subscribe_to_everything {
                self.server_peers.insert(addr);
            }
        }

        // Quorum cleanup reaper: ticks every second, sends a self-message to
        // process timeout expiration with full self access. The actor runtime
        // owns `quorum_entries` and only `handle()` borrows it mutably, so the
        // reaper MUST route through `handle()` rather than touching the map
        // directly from a sibling task.
        //
        // Skips the immediate first tick so we don't race the actor's own
        // startup; a freshly registered quorum needs at least one tick cycle
        // before the reaper considers it for eviction.
        //
        // Native only: the Interval type from tokio_with_wasm is not Send,
        // and browser nodes are leaf clients that don't manage quorums.
        #[cfg(not(target_arch = "wasm32"))]
        {
            let ctx_addr = ctx.addr.clone();
            ctx.child_task(async move {
                let mut interval = crate::tokio_time::interval(web_time::Duration::from_secs(1));
                interval.tick().await; // skip immediate first tick
                loop {
                    interval.tick().await;
                    // Best-effort: if Router is stopped, the send fails silently.
                    let _ = ctx_addr.send(Message::CheckQuorumTimeouts);
                }
            });
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        info!("Router stopping");
    }

    async fn handle(&mut self, msg: Arc<Message>, _ctx: &ActorContext) {
        match &*msg {
            Message::Put(put) => {
                self.handle_put(put);
            }
            Message::BatchPut(batch) => {
                self.handle_batch_put(batch);
            }
            Message::Get(get) => self.handle_get(get),
            Message::Flush(flush) => self.handle_flush(flush),
            Message::Hi { from, peer_id } => {
                self.known_peers.insert(from.clone());
                if !peer_id.is_empty() {
                    if let Some(existing) = self.peer_addrs.get(peer_id) {
                        if existing != from {
                            error!(
                                "Router peer_id collision: '{}' already mapped to {:?}, rejecting {:?}. Each peer_id must be unique.",
                                peer_id, existing, from
                            );
                            return;
                        }
                    }
                    self.peer_addrs.insert(peer_id.clone(), from.clone());
                    self.addr_to_pid.insert(from.clone(), peer_id.clone());
                }
            }
            Message::RtcSignal(rtc) => {
                debug!(
                    "RtcSignal id={} to={:?} known_peers={}",
                    rtc.id,
                    rtc.to,
                    self.known_peers.len()
                );
                if let Some(to_peer_id) = &rtc.to {
                    if let Some(addr) = self.peer_addrs.get(to_peer_id) {
                        debug!(
                            "RtcSignal delivering to local addr for peer_id={}",
                            to_peer_id
                        );
                        let _ = addr.send(Message::RtcSignal(rtc.clone()));
                    } else {
                        debug!(
                            "RtcSignal broadcasting to {} known_peers",
                            self.known_peers.len()
                        );
                        for addr in self.known_peers.iter() {
                            let _ = addr.send(Message::RtcSignal(rtc.clone()));
                        }
                    }
                }
            }
            Message::RegisterQuorum {
                put_id,
                requester,
                policy,
            } => {
                let _ = self.handle_register_quorum(put_id.clone(), requester.clone(), *policy);
            }
            Message::CheckQuorumTimeouts => {
                self.handle_quorum_timeout_reaper();
            }
        };
    }
}

impl Router {
    /// Creates a new router with the given config and adapter actors.
    ///
    /// The adapter actors are started in [`Actor::pre_start`], not here —
    /// they need the router's `ActorContext` to spawn.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration
    /// * `storage_adapter_actors` - Storage actors to be started
    /// * `network_adapter_actors` - Network actors to be started
    ///
    /// Constructs a new Router with the provided configuration, adapters, and
    /// shared `Arc<Metrics>` handle.
    ///
    /// The `metrics` Arc is shared with the owning Node so both observe the
    /// same counters. The Router records drops internally; the Node exposes
    /// the snapshot to external observers via `Node::metrics()`.
    pub fn new(
        storage_adapter_actors: Vec<Box<dyn Actor>>,
        network_adapter_actors: Vec<Box<dyn Actor>>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        Self {
            metrics,
            known_peers: FxHashSet::default(),
            peer_addrs: FxHashMap::default(),
            addr_to_pid: FxHashMap::default(),
            storage_adapters: FxHashSet::default(),
            read_adapters: FxHashSet::default(),
            write_adapters: FxHashSet::default(),
            network_adapters: FxHashSet::default(),
            storage_adapter_actors,
            network_adapter_actors,
            server_peers: FxHashSet::default(),
            dup: Dup::default_gun(),
            seen_get_messages: BoundedHashMap::new(SEEN_MSGS_MAX_SIZE),
            subscribers_by_topic: FxHashMap::default(),
            msg_counter: AtomicUsize::new(0),
            quorum_entries: BoundedHashMap::new(SEEN_MSGS_MAX_SIZE),
            ham_cache: BoundedHashMap::new(SEEN_MSGS_MAX_SIZE),
        }
    }

    /// Returns a clone of the shared `Arc<Metrics>` handle.
    ///
    /// The returned `Arc` points to the same atomic counters as the
    /// Router's internal field and the owning Node's field. Recording
    /// an event via the returned handle is visible from any other clone
    /// (including `Node::metrics()`).
    ///
    /// Cloning the `Arc` is cheap (refcount bump); the atomic counters
    /// are shared across all clones.
    #[allow(dead_code)]
    pub fn metrics(&self) -> Arc<crate::metrics::Metrics> {
        self.metrics.clone()
    }

    /// Handles a `Get` message: records subscription, queries storage and peers.
    ///
    /// The Get is deduplicated by message ID. The requester is registered as
    /// a subscriber for the topic (the first path segment of the node_id).
    /// Storage adapters are queried first, then server peers, then a random
    /// sample of up to 4 known subscribers/peers (MANET-style).
    fn handle_get(&mut self, get: &Get) {
        if !get.id.chars().all(char::is_alphanumeric) {
            error!("id {}", get.id);
        }
        if self.is_message_seen(&get.id) {
            return;
        }
        let seen_get_message = SeenGetMessage {
            from: get.from.clone(),
            last_reply_checksum: get.checksum,
        };
        self.seen_get_messages
            .insert(get.id.clone(), seen_get_message);

        // Record subscriber
        let topic = get.node_id.split("/").next().unwrap_or("");
        debug!("{} subscribed to {}", get.from, topic);
        self.subscribers_by_topic
            .entry(topic.to_string())
            .or_default()
            .insert(get.from.clone());

        // Ask storage read actors
        for addr in self.read_adapters.iter() {
            let _ = addr.send(Message::Get(get.clone()));
        }

        let mut already_sent_to = FxHashSet::default();

        // Send to server peers
        for addr in self.server_peers.iter() {
            debug!("send to server peer");
            let _ = addr.send(Message::Get(get.clone()));
            already_sent_to.insert(addr.clone());
        }

        // Ask network subscribers
        let mut errored = FxHashSet::default();
        let mut sent_to = 0;
        let mut rng = rng();
        if let Some(topic_subscribers) = self.subscribers_by_topic.get(topic) {
            let sample = topic_subscribers.iter().choose_multiple(&mut rng, 4);
            for addr in sample {
                if get.from == *addr {
                    continue;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                match addr.send(Message::Get(get.clone())) {
                    Ok(_) => {
                        sent_to += 1;
                    }
                    _ => {
                        #[cfg(target_arch = "wasm32")]
                        web_sys::console::log_1(
                            &format!("router: FAILED to send put to known_peer {}", addr).into(),
                        );
                        errored.insert(addr.clone());
                    }
                }
            }
        }
        debug!(
            "sent get to a random sample of subscribers of size {}",
            sent_to
        );
        if !errored.is_empty() {
            if let Some(topic_subscribers) = self.subscribers_by_topic.get_mut(topic) {
                for addr in errored {
                    topic_subscribers.remove(&addr);
                    self.known_peers.remove(&addr);
                }
            }
        }
        if sent_to < 4 {
            let mut errored = FxHashSet::default();
            while let Some(addr) = self.known_peers.iter().choose(&mut rng) {
                sent_to += 1;
                if sent_to >= 4 {
                    break;
                }
                if get.from == *addr {
                    continue;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                match addr.send(Message::Get(get.clone())) {
                    Ok(_) => {}
                    _ => {
                        #[cfg(target_arch = "wasm32")]
                        web_sys::console::log_1(
                            &format!("router: FAILED to send put to known_peer {}", addr).into(),
                        );
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }
    }

    /// Handles a `Put` message: deduplicates, filters stale data,
    /// routes to storage or back to requester.
    ///
    /// If the Put is a response to a Get (has `in_response_to`), it's routed
    /// directly back to the original requester. Otherwise, it's forwarded to
    /// storage adapters and relayed to network peers.
    ///
    /// # Deduplication
    ///
    /// Three layers:
    /// 1. Message ID dedup (via [`Dup`]) — prevents re-processing the
    ///    same message.
    /// 2. Response checksum dedup — if the same response (same `@` + `##`)
    ///    has been seen, it's suppressed.
    /// 3. HAM stale-data pre-filter ([`ham_filter`]) — if all data in the
    ///    Put is older than or equal to what the router has already seen,
    ///    the Put is dropped before storage/relay. Mirrors Gun.js's
    ///    `ham()` function. Only applies to non-ack Puts.
    fn handle_put(&mut self, put: &Put) {
        if self.is_message_seen(&put.id) {
            self.metrics.record_dropped_dup();
            return;
        }

        // Gun.js DAM: ack + "##" + hash dedup for identical responses
        if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
            let checksum_key = format!("{}##{}", ack, hash);
            if self.dup.check(&checksum_key) {
                debug!("duplicate response checksum: {}", checksum_key);
                self.metrics.record_dropped_dup();
                return;
            }
            self.dup.track(&checksum_key);
        }

        match &put.in_response_to {
            Some(in_response_to) => {
                // Quorum ack branch — registered Puts count peer acks here.
                // Check FIRST so the count increments before any seen_get_messages
                // routing (those are separate concerns: quorums track durability,
                // seen_get_messages tracks Get→Put responses).
                if let Some(entry) = self.quorum_entries.get_mut(in_response_to) {
                    let ack_count = entry.record_ack(&put.from);
                    if let Some(count) = ack_count {
                        // Threshold met — emit __quorum_met__ sentinel Put back
                        // to the requester, then drop the entry. The Put envelope
                        // mirrors the storage _ack/_err sentinels, just with a
                        // different key, so the requester's oneshot drain picks
                        // it up via the same pending_puts plumbing.
                        let children: Children = std::collections::BTreeMap::from([(
                            "_".to_string(),
                            NodeData {
                                value: Value::Number(count as f64),
                                updated_at: 0.0, // sentinel reply — actual timestamp tracked elsewhere
                            },
                        )]);
                        let mut reply = Put::new_from_kv(
                            QUORUM_MET_SENTINEL.to_string(),
                            children,
                            put.from.clone(),
                        );
                        reply.in_response_to = Some(in_response_to.clone());
                        debug!("quorum met for {} ({} acks)", in_response_to, count);
                        try_send_or_log(
                            &entry.requester,
                            Message::Put(reply),
                            &self.metrics,
                            "router:quorum-met",
                        );
                        // Drop the entry — quorum satisfied, drain complete.
                        self.quorum_entries.take(in_response_to);
                    }
                    return; // quorum ack consumed, do not fall through
                }

                if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
                    if put.checksum.is_some()
                        && put.checksum == seen_get_message.last_reply_checksum
                    {
                        debug!("same reply already sent");
                        return;
                    }
                    seen_get_message.last_reply_checksum = put.checksum;
                    try_send_or_log(
                        &seen_get_message.from,
                        Message::Put(put.clone()),
                        &self.metrics,
                        "router:get-reply",
                    );
                }
            }
            _ => {
                // HAM pre-filter: skip stale data before storage/relay.
                // Only applies to non-ack Puts — acks and get-responses
                // bypass HAM (they are control messages, not data writes).
                //
                // Per-key filtering: if some keys are stale and others are
                // new, only the new keys proceed to storage and relay.
                // Mirrors Gun.js's per-key `ham()` inside its `while` loop.
                let effective_put: Put;
                let put_ref: &Put = match self.ham_filter(put) {
                    HamFilterResult::Stale => {
                        self.metrics.record_dropped_ham();
                        debug!("ham: dropped stale put {}", put.id);
                        return;
                    }
                    HamFilterResult::New => put,
                    HamFilterResult::PartiallyNew(filtered_nodes) => {
                        effective_put = put.with_updated_nodes(filtered_nodes);
                        &effective_put
                    }
                };

                // Forward to storage write adapter(s)
                for addr in self.write_adapters.iter() {
                    if put_ref.from == *addr {
                        continue;
                    }
                    let _res = addr.send(Message::Put(put_ref.clone()));
                }
                // Network relay is handled by handle_put_relay for batching
                self.handle_put_relay(put_ref);
            }
        };
    }

    /// Relays a Put to server peers and subscribers.
    ///
    /// Storage is NOT touched here — this is pure network fan-out.
    /// Anti-loop detection uses the `peer_hop_list` (`><`) field.
    ///
    /// # Gun.js DAM Compatibility
    ///
    /// Following Gun.js's mesh protocol (`src/mesh.js`), the `><` field
    /// contains **stable peer IDs**, not per-connection actor addresses.
    /// Gun.js populates `><` with ALL known peer URLs/IDs (up to 6) at
    /// serialization time. On the receiving side, a peer checks if the
    /// SENDING peer's ID is in `><` and skips if so.
    ///
    /// This works because peer IDs are stable across connections — both
    /// sides of a WebSocket connection recognize the same peer ID. Using
    /// per-connection WsConn addrs (as BEAM previously did) breaks the
    /// skip check because each side sees a different addr for the same
    /// logical connection, causing message echo-back (4x amplification).
    ///
    /// BEAM's peer IDs come from the `Hi` handshake: each WsConn sends
    /// its node's `peer_id` on startup, and the router records the
    /// mapping in `peer_addrs` (pid → addr) and `addr_to_pid` (addr → pid).
    fn handle_put_relay(&mut self, put: &Put) {
        // NOTE: NO is_message_seen here. Router::handle_put already dedup'd.

        // Gun.js mesh.raw(): build hops from incoming `><` plus ALL known
        // peer IDs (up to 6), matching the reference implementation:
        //   var to = []; for(var k in opt.peers){ to.push(p.url||p.pid||p.id);
        //     if(++i > 6){ break } } if(i > 1){ msg['><'] = to.join() }
        // The sender's pid is included via `from_pid` below.
        let mut hops = put.peer_hop_list.clone().unwrap_or_default();
        // Only insert the sender's peer ID if they're a known network peer.
        // Non-peer senders (e.g. local Node actors) have no recognizable
        // peer ID — adding their formatted Addr would pollute the hop list
        // with an entry no remote peer would recognize.
        if let Some(from_pid) = self.addr_to_pid.get(&put.from).cloned() {
            hops.insert(from_pid);
        }
        for (i, pid) in self.peer_addrs.keys().enumerate() {
            if i >= 6 {
                break;
            }
            hops.insert(pid.clone());
        }

        // Build the relay Put ONCE with peer_hop_list set, wrap in Arc.
        let mut relay_put = put.clone();
        relay_put.peer_hop_list = Some(hops.clone());
        let relay_msg: Arc<Message> = Arc::new(Message::Put(relay_put));

        let mut already_sent_to = FxHashSet::default();

        // Relay to server peers (outgoing WebSocket adapters, relay servers).
        //
        // Gun.js `mesh.say` has two echo-back checks:
        //   1. `if(peer === meta.via){ return false }` — don't send back to
        //      the peer that sent us the message.
        //   2. `if(meta.yo && meta.yo[peer.id]){ return false }` — don't
        //      send to peers listed in `><` (already visited).
        //
        // BEAM's actor model separates the adapter (OutgoingWebsocketManager)
        // from its child WsConn, so `put.from` (WsConn addr) ≠ server_peer
        // adapter addr. The `put.from == *addr` check only covers the case
        // where the adapter itself is the sender. For messages arriving
        // from a remote peer via WsConn, `from_remote_peer` provides the
        // `meta.via` check: if `put.from` is in `known_peers`, the message
        // came from a remote peer — skip server_peers to prevent echo-back.
        //
        // The hops check (layer 2) is applied in the subscribers and
        // known_peers sections below.
        let from_remote_peer = self.known_peers.contains(&put.from);
        if !from_remote_peer {
            for addr in self.server_peers.iter() {
                if put.from == *addr {
                    continue;
                }
                let _ = addr.send(Arc::clone(&relay_msg));
                already_sent_to.insert(addr.clone());
            }
        }

        // Relay to subscribers — skip if the subscriber's pid is in hops
        // (Gun.js: `tmp[peer.url] || tmp[peer.pid] || tmp[peer.id]`).
        let mut sent_to = 0;
        for node_id in put.updated_nodes.keys() {
            let topic = node_id.split("/").next().unwrap_or("");
            if let Some(topic_subscribers) = self.subscribers_by_topic.get_mut(topic) {
                topic_subscribers.retain(|addr| {
                    if put.from == *addr {
                        return true;
                    }
                    if let Some(pid) = self.addr_to_pid.get(addr) {
                        if hops.contains(pid) {
                            return true;
                        }
                    }
                    if already_sent_to.contains(addr) {
                        return true;
                    }
                    already_sent_to.insert(addr.clone());
                    match addr.send(Arc::clone(&relay_msg)) {
                        Ok(_) => {
                            sent_to += 1;
                            true
                        }
                        _ => false,
                    }
                })
            }
        }

        // Random sampling from known_peers (Gun.js mesh fallback path).
        if already_sent_to.len() < 4 {
            let mut rng = rng();
            let mut errored = FxHashSet::default();
            while let Some(addr) = self.known_peers.iter().choose(&mut rng) {
                sent_to += 1;
                if sent_to >= 4 {
                    break;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                if put.from == *addr {
                    continue;
                }
                if let Some(pid) = self.addr_to_pid.get(addr) {
                    if hops.contains(pid) {
                        continue;
                    }
                }
                match addr.send(Arc::clone(&relay_msg)) {
                    Ok(_) => debug!("sent put to random peer"),
                    _ => {
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }

        // Hot-path metrics: record that a relay happened and how many
        // subscribers received the message. `sent_to` counts subscriber
        // deliveries from the relay loop above (server peers + topic
        // subscribers + random sampling).
        self.metrics.record_relayed();
        self.metrics.record_subscriber_fanout(sent_to as u64);
    }

    /// Register a new quorum-acked Put.
    ///
    /// Called when a Node sends a `Message::RegisterQuorum` before initiating
    /// a Put it wants acknowledged by N peers. We insert a [`QuorumEntry`] into
    /// the bounded `quorum_entries` map keyed by `put_id`; subsequent Put acks
    /// from peers with matching `in_response_to` increment the counter, and
    /// when the threshold is satisfied, we emit the `__quorum_met__` sentinel
    /// back to the requester.
    ///
    /// Returns `Err` if the entry cannot be inserted (e.g., bounded map full).
    fn handle_register_quorum(
        &mut self,
        put_id: String,
        requester: Addr,
        policy: AckPolicy,
    ) -> Result<(), String> {
        let required = policy.quorum;
        let max_timeout = policy.timeout;
        let entry = QuorumEntry {
            requester,
            required,
            received: FxHashSet::default(),
            started_at: web_time::Instant::now(),
            max_timeout,
        };
        self.quorum_entries.insert(put_id.clone(), entry);
        debug!(
            "registered quorum for put_id={} (required: {} peers, timeout: {:?})",
            put_id, required, policy.timeout
        );
        Ok(())
    }

    /// Periodic cleanup of expired [`QuorumEntry`]s.
    ///
    /// Fired by the reaper task spawned in [`Router::pre_start`] every
    /// second. Walks `quorum_entries`, evicts any entry whose wall-clock age
    /// exceeds its `max_timeout`, and notifies the original requester via a
    /// `__quorum_met__` Put carrying `Value::Bool(true)` so the
    /// [`crate::Node::decode_quorum_payload`] decoder can distinguish timeout
    /// (→ Err) from success (→ `Number(ack_count)`).
    ///
    /// # Why a self-message instead of direct map access?
    ///
    /// `quorum_entries` is borrowed mutably only inside `handle()`. A sibling
    /// task touching the map directly would conflict with the actor's
    /// single-threaded borrow model. The canonical BEAM pattern — used by all
    /// background work — is: spawn task → task sends self-message →
    /// `handle()` processes with full access.
    fn handle_quorum_timeout_reaper(&mut self) {
        let expired_keys: Vec<String> = self
            .quorum_entries
            .iter()
            .filter(|(_k, v)| v.is_expired(v.max_timeout))
            .map(|(k, _v)| k.clone())
            .collect();

        if expired_keys.is_empty() {
            return;
        }

        let mut expired: Vec<(String, QuorumEntry)> = Vec::with_capacity(expired_keys.len());
        for key in expired_keys {
            if let Some(entry) = self.quorum_entries.take(&key) {
                expired.push((key, entry));
            }
        }

        debug!(
            "quorum reaper: timing out {} expired entr{}",
            expired.len(),
            if expired.len() == 1 { "y" } else { "ies" }
        );

        for (put_id, entry) in expired {
            // Reuse the __quorum_met__ channel for the timeout notification.
            // The decoder distinguishes via payload type:
            //   Number(N) → success, Ok(ReplicationStatus { acked_by: N })
            //   Bit(true) → timeout, Err("quorum timed out")
            //   else → malformed (decoder returns None, falls through)
            let mut children: Children = std::collections::BTreeMap::new();
            children.insert(
                "_".to_string(),
                NodeData {
                    value: Value::Bit(true),
                    updated_at: 0.0,
                },
            );
            let mut reply = Put::new_from_kv(
                QUORUM_MET_SENTINEL.to_string(),
                children,
                entry.requester.clone(),
            );
            reply.in_response_to = Some(put_id.clone());
            try_send_or_log(
                &entry.requester,
                Message::Put(reply),
                &self.metrics,
                "router:quorum-timeout",
            );
            debug!(
                "quorum reaper: notified requester of timeout for put_id={}",
                put_id
            );
        }
    }

    /// Handles a `BatchPut`: forwards to storage (single transaction), then
    /// relays each constituent Put individually with deduplication and
    /// HAM stale-data filtering.
    ///
    /// This preserves atomic multi-write semantics for storage adapters while
    /// still doing per-message dedup, HAM filtering, and network relay.
    fn handle_batch_put(&mut self, batch: &BatchPut) {
        // Forward BatchPut to storage write adapters — preserves single-transaction semantics
        for addr in self.write_adapters.iter() {
            if batch.from == *addr {
                continue;
            }
            let _ = addr.send(Message::BatchPut(batch.clone()));
        }

        // Relay each constituent put individually (with deduplication + HAM)
        for put in &batch.puts {
            if self.is_message_seen(&put.id) {
                continue;
            }
            // Gun.js DAM: ack + "##" + hash dedup for identical responses
            if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
                let checksum_key = format!("{}##{}", ack, hash);
                if self.dup.check(&checksum_key) {
                    debug!("batch: duplicate response checksum: {}", checksum_key);
                    continue;
                }
                self.dup.track(&checksum_key);
            }
            // ACK responses within a batch are unusual but handled defensively
            if let Some(in_response_to) = &put.in_response_to {
                if let Some(seen_get_message) = self.seen_get_messages.get_mut(in_response_to) {
                    if put.checksum == seen_get_message.last_reply_checksum {
                        continue;
                    }
                    seen_get_message.last_reply_checksum = put.checksum;
                    try_send_or_log(
                        &seen_get_message.from,
                        Message::Put(put.clone()),
                        &self.metrics,
                        "router:get-reply",
                    );
                }
                continue;
            }
            // HAM pre-filter: skip stale constituent puts before relay.
            // Per-key filtering applies here too — only new keys are relayed.
            let batch_effective_put: Put;
            let batch_put_ref: &Put = match self.ham_filter(put) {
                HamFilterResult::Stale => {
                    self.metrics.record_dropped_ham();
                    debug!("ham: dropped stale batch put {}", put.id);
                    continue;
                }
                HamFilterResult::New => put,
                HamFilterResult::PartiallyNew(filtered_nodes) => {
                    batch_effective_put = put.with_updated_nodes(filtered_nodes);
                    &batch_effective_put
                }
            };
            self.handle_put_relay(batch_put_ref);
        }
    }

    /// Handles a `Flush` message by forwarding to all storage adapters.
    ///
    /// Storage adapters can use this to trigger `fsync` or other durable
    /// persistence. The flush is not relayed to network peers.
    fn handle_flush(&mut self, flush: &Flush) {
        let mut sent = FxHashSet::default();
        for addr in self.write_adapters.iter() {
            if flush.from == *addr {
                continue;
            }
            if sent.contains(addr) {
                continue;
            }
            sent.insert(addr.clone());
            let _ = addr.send(Message::Flush(flush.clone()));
        }
        debug!("forwarded flush to {} storage write adapters", sent.len());
    }

    /// HAM (Hypothetical Amnesia Machine) stale-data pre-filter.
    ///
    /// Compares each `(soul, key)` pair in `put` against the router's
    /// timestamp index ([`ham_cache`](Self::ham_cache)). If every pair
    /// is stale (`updated_at <= cached`), returns `false` — the caller
    /// should skip the Put entirely, avoiding storage writes and
    /// network relay. If any pair is newer (or unseen), updates the
    /// index for all pairs and returns `true`.
    ///
    /// This mirrors Gun.js's `ham()` function (`src/root.js` line 120):
    ///
    /// - `state < was` → old, skip *(our `updated_at < cached_at`)*
    /// - `state === was && val === known` → same, skip *(our
    ///   `updated_at == cached_at` — existing wins the tie)*
    /// - otherwise → new, proceed
    ///
    /// BEAM simplifies to `updated_at <= cached_at → skip`. Same-timestamp
    /// conflicts resolve to "existing wins" (last-write-wins with the
    /// incumbent keeping the tie), matching Gun.js's primary path.
    ///
    /// # Performance
    ///
    /// One `HashMap` lookup per key on the stale path (fast rejection).
    /// Two lookups on the update path (check + insert) — only for
    /// genuinely newer data. No allocations on lookups (soul and key
    /// are borrowed from the Put). Clones occur only on the update
    /// path (`soul.clone()`, `key.clone()`).
    ///
    /// # Edge Cases
    ///
    /// - Empty `updated_nodes` → [`HamFilterResult::New`] (no data to
    ///   filter — mirrors Gun.js where the key loop doesn't execute).
    /// - First-seen soul/key → new (cache miss = proceed).
    /// - Future timestamps → accepted (not deferred — the router is
    ///   synchronous; Gun.js defers via `setTimeout`, which is a
    ///   timing optimization, not a correctness requirement).
    /// - Same timestamp, different value → stale (existing wins).
    ///
    /// # Per-Key Filtering
    ///
    /// Unlike the previous whole-Put bool return, this method evaluates
    /// each (soul, key) pair independently — exactly as Gun.js's `ham()`
    /// does inside its `while` loop. When some keys are stale and others
    /// are new, [`HamFilterResult::PartiallyNew`] is returned with a
    /// filtered `updated_nodes` containing only the new entries.
    fn ham_filter(&mut self, put: &Put) -> HamFilterResult {
        // Gun.js iterates keys in a `while` loop, calling `ham()` per key.
        // If there are no keys, the loop doesn't execute and the message
        // proceeds normally. Empty `updated_nodes` → no data to filter →
        // allow through.
        if put.updated_nodes.is_empty() {
            return HamFilterResult::New;
        }

        let mut has_stale = false;
        let mut has_new = false;
        let mut filtered = BTreeMap::new();

        for (soul, children) in put.updated_nodes.iter() {
            let mut filtered_children = Children::new();
            for (key, node_data) in children {
                // Check if this (soul, key) is stale.
                let is_stale = self
                    .ham_cache
                    .get(soul)
                    .and_then(|inner| inner.get(key))
                    .is_some_and(|&cached_at| node_data.updated_at <= cached_at);

                if is_stale {
                    has_stale = true;
                    continue;
                }

                // This key is newer (or unseen) — include it.
                has_new = true;
                filtered_children.insert(key.clone(), node_data.clone());

                // Update the HAM cache for this (soul, key).
                if self.ham_cache.get(soul).is_none() {
                    self.ham_cache.insert(soul.clone(), FxHashMap::default());
                }
                if let Some(inner) = self.ham_cache.get_mut(soul) {
                    inner.insert(key.clone(), node_data.updated_at);
                }
            }
            if !filtered_children.is_empty() {
                filtered.insert(soul.clone(), filtered_children);
            }
        }

        match (has_stale, has_new) {
            (false, _) => HamFilterResult::New,
            (_, false) => HamFilterResult::Stale,
            (true, true) => HamFilterResult::PartiallyNew(Arc::new(filtered)),
        }
    }

    /// Checks if a message ID has been seen, and tracks it if not.
    ///
    /// Returns `true` if the message was already seen (and should be
    /// skipped), `false` if it's new. Also increments the message counter.
    fn is_message_seen(&mut self, id: &String) -> bool {
        self.msg_counter.fetch_add(1, Ordering::Relaxed);
        if self.dup.check(id) {
            debug!("already seen message {}", id);
            return true;
        }
        self.dup.track(id);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::MemoryStorage;
    use crate::metrics::Metrics;
    use std::collections::BTreeMap;
    use web_time::Duration;

    #[test]
    fn test_router_new() {
        let storage = vec![Box::new(MemoryStorage::new()) as Box<dyn Actor>];
        let metrics = Arc::new(Metrics::new());
        let router = Router::new(storage, vec![], metrics);
        assert!(router.known_peers.is_empty());
        assert!(router.read_adapters.is_empty());
        assert!(router.write_adapters.is_empty());
        assert!(router.network_adapters.is_empty());
    }

    #[test]
    fn test_router_default_dedup() {
        let metrics = Arc::new(Metrics::new());
        let router = Router::new(vec![], vec![], metrics);
        assert_eq!(router.dup.max(), 100_000);
        assert_eq!(router.dup.age(), web_time::Duration::from_secs(9));
    }

    #[test]
    fn test_router_seen_msg_capacity() {
        let metrics = Arc::new(Metrics::new());
        let router = Router::new(vec![], vec![], metrics);
        // The seen_get_messages BoundedHashMap should have capacity SEEN_MSGS_MAX_SIZE
        assert_eq!(SEEN_MSGS_MAX_SIZE, 10000);
        let _ = router; // just verify it constructs
    }

    #[test]
    fn test_router_msg_counter_starts_zero() {
        let metrics = Arc::new(Metrics::new());
        let router = Router::new(vec![], vec![], metrics);
        assert_eq!(router.msg_counter.load(Ordering::Relaxed), 0);
    }

    // ========================================================================
    // HAM Pre-Filter Tests (Tier 0)
    // ========================================================================

    /// Helper: build a Put with a single (soul, key, value, timestamp).
    fn make_put(soul: &str, key: &str, value: &str, ts: f64) -> Put {
        let mut children: Children = BTreeMap::new();
        children.insert(
            key.to_string(),
            NodeData {
                value: Value::Text(value.to_string()),
                updated_at: ts,
            },
        );
        let mut nodes = BTreeMap::new();
        nodes.insert(soul.to_string(), children);
        Put::new(nodes, None, Addr::noop())
    }

    /// Helper: build a Put with multiple keys under one soul.
    fn make_multi_put(soul: &str, pairs: &[(&str, &str, f64)]) -> Put {
        let mut children: Children = BTreeMap::new();
        for (key, val, ts) in pairs {
            children.insert(
                key.to_string(),
                NodeData {
                    value: Value::Text(val.to_string()),
                    updated_at: *ts,
                },
            );
        }
        let mut nodes = BTreeMap::new();
        nodes.insert(soul.to_string(), children);
        Put::new(nodes, None, Addr::noop())
    }

    #[test]
    fn ham_filter_first_seen_returns_new() {
        // No cache entry → cache miss = proceed.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);
        let put = make_put("soul1", "key1", "hello", 100.0);
        assert!(matches!(router.ham_filter(&put), HamFilterResult::New));
    }

    #[test]
    fn ham_filter_newer_returns_new_and_updates_cache() {
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        // First put — populates cache with ts=100.
        let put1 = make_put("soul1", "key1", "v1", 100.0);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // Second put — newer timestamp.
        let put2 = make_put("soul1", "key1", "v2", 200.0);
        assert!(matches!(router.ham_filter(&put2), HamFilterResult::New));

        // Cache should now have ts=200.
        let cached = router
            .ham_cache
            .get(&"soul1".to_string())
            .and_then(|m| m.get(&"key1".to_string()));
        assert_eq!(cached, Some(&200.0));
    }

    #[test]
    fn ham_filter_stale_returns_stale() {
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        // Populate cache with ts=200.
        let put1 = make_put("soul1", "key1", "v1", 200.0);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // Stale put — older timestamp → should be filtered.
        let put2 = make_put("soul1", "key1", "v0", 100.0);
        assert!(matches!(router.ham_filter(&put2), HamFilterResult::Stale));
    }

    #[test]
    fn ham_filter_same_timestamp_returns_stale() {
        // Same timestamp = existing wins the tie.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        let put1 = make_put("soul1", "key1", "v1", 150.0);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // Same ts, different value → stale (existing wins).
        let put2 = make_put("soul1", "key1", "v2", 150.0);
        assert!(matches!(router.ham_filter(&put2), HamFilterResult::Stale));
    }

    #[test]
    fn ham_filter_mixed_stale_and_newer_returns_partially_new() {
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        // Populate cache: key1@100, key2@100.
        let put1 = make_multi_put("soul1", &[("key1", "v1", 100.0), ("key2", "v2", 100.0)]);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // Mixed: key1 stale (50), key2 newer (200) → partially new.
        let put2 = make_multi_put("soul1", &[("key1", "old", 50.0), ("key2", "new", 200.0)]);
        match router.ham_filter(&put2) {
            HamFilterResult::PartiallyNew(filtered) => {
                // Only key2 should be in the filtered result.
                let children = filtered.get("soul1").expect("soul1 must exist");
                assert!(children.contains_key("key2"), "key2 must be present (newer)");
                assert!(!children.contains_key("key1"), "key1 must be absent (stale)");
            }
            other => panic!("expected PartiallyNew, got {:?}", other),
        }

        // Cache should reflect the newer key2 timestamp.
        let cached_k2 = router
            .ham_cache
            .get(&"soul1".to_string())
            .and_then(|m| m.get(&"key2".to_string()));
        assert_eq!(cached_k2, Some(&200.0));
    }

    #[test]
    fn ham_filter_empty_put_returns_new() {
        // No updated_nodes → no data to filter → allow through.
        // Gun.js: the `while` loop over keys doesn't execute, so `ham()`
        // is never called and the message proceeds normally.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);
        let put = Put::new(BTreeMap::new(), None, Addr::noop());
        assert!(matches!(router.ham_filter(&put), HamFilterResult::New));
    }

    #[test]
    fn ham_filter_different_soul_is_new() {
        // Same key under a different soul is a cache miss → proceed.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        let put1 = make_put("soulA", "key1", "v1", 100.0);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // soulB/key1 — different soul, no cache → proceed.
        let put2 = make_put("soulB", "key1", "v2", 50.0);
        assert!(matches!(router.ham_filter(&put2), HamFilterResult::New));
    }

    #[test]
    fn ham_filter_future_timestamp_accepted() {
        // Future timestamps are accepted, not deferred.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        let put1 = make_put("soul1", "key1", "v1", 100.0);
        assert!(matches!(router.ham_filter(&put1), HamFilterResult::New));

        // Future ts (much larger) → newer → proceed.
        let put2 = make_put("soul1", "key1", "v2", 9999999999.0);
        assert!(matches!(router.ham_filter(&put2), HamFilterResult::New));
    }

    // ========================================================================
    // Tier 1: Arc<updated_nodes> — clone shares the same allocation
    // ========================================================================

    #[test]
    fn put_clone_shares_updated_nodes() {
        let put = make_put("soul1", "key1", "hello", 100.0);
        let cloned = put.clone();
        // Both Puts should share the same Arc<BTreeMap> — no deep clone.
        assert!(
            Arc::as_ptr(&put.updated_nodes) == Arc::as_ptr(&cloned.updated_nodes),
            "cloned Put should share the same Arc<updated_nodes>"
        );
    }

    // ========================================================================
    // Tier 1.5: relay from_pid — non-peer senders skipped
    // ========================================================================

    #[test]
    fn relay_skips_non_peer_from_pid() {
        // When the sender is NOT in addr_to_pid (not a known network peer),
        // their ID should NOT appear in the relay's peer_hop_list.
        // This is verified by checking that handle_put_relay doesn't panic
        // and the relay still works — the from_pid is simply absent from hops.
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        // Sender is Addr::noop() — not in addr_to_pid
        let put = make_put("soul1", "key1", "hello", 100.0);
        assert!(!router.addr_to_pid.contains_key(&put.from));
        // This should not panic — the if-let guard handles the non-peer case
        router.handle_put_relay(&put);
        // If we got here without panic, the fix works.
    }

    #[test]
    fn relay_includes_peer_from_pid() {
        use crate::mailbox::mailbox;
        let metrics = Arc::new(Metrics::new());
        let mut router = Router::new(vec![], vec![], metrics);

        // Simulate a known peer: register via Hi handshake
        let (sender, _receiver) = mailbox(16);
        let peer_addr = Addr::new(sender);
        let peer_id = "peer123".to_string();
        router.addr_to_pid.insert(peer_addr.clone(), peer_id.clone());

        // Create a put from this known peer
        let put = make_put("soul1", "key1", "hello", 100.0);
        // Override from to be the peer addr
        let mut put = put;
        put.from = peer_addr;

        // The relay should include the peer's pid in hops.
        // We can't directly inspect hops (it's internal to handle_put_relay),
        // but we can verify the relay doesn't panic and metrics show relay happened.
        router.handle_put_relay(&put);
        assert_eq!(router.metrics.snapshot().messages_relayed, 1);
    }

    /// Helper: build a QuorumEntry with custom required/timeout values.
    fn _make_quorum_entry(required: usize, timeout_ms: u64) -> QuorumEntry {
        QuorumEntry::new(
            Addr::noop(),
            &AckPolicy::any()
                .with_quorum(required)
                .with_timeout(Duration::from_millis(timeout_ms)),
        )
    }

    #[test]
    fn quorum_entry_initial_state() {
        // Fresh entry: empty received set, required set from policy, not expired.
        let entry = _make_quorum_entry(3, 60_000);
        assert_eq!(entry.received.len(), 0);
        assert_eq!(entry.required, 3);
        assert!(!entry.is_expired(Duration::from_millis(60_000)));
    }

    #[test]
    fn quorum_entry_is_expired_respects_timeout() {
        // A freshly created entry is NOT expired under any reasonable timeout.
        let entry = _make_quorum_entry(1, 60_000);
        assert!(
            !entry.is_expired(Duration::from_secs(60)),
            "fresh entry should not be expired under 60s timeout"
        );
        // A 1ns timeout IS exceeded by the microseconds elapsed since creation.
        assert!(
            entry.is_expired(Duration::from_nanos(1)),
            "1ns timeout should be exceeded by microsecond-level elapsed"
        );
    }

    #[test]
    fn quorum_entry_required_field_from_policy() {
        // AckPolicy::any() → required=1
        let entry_any = QuorumEntry::new(Addr::noop(), &AckPolicy::any());
        assert_eq!(entry_any.required, 1);
        // AckPolicy::all() → required=usize::MAX
        let entry_all = QuorumEntry::new(Addr::noop(), &AckPolicy::all());
        assert_eq!(entry_all.required, usize::MAX);
        // AckPolicy::for_peer_count(N) → required=⌈N/2⌉ (majority)
        assert_eq!(
            QuorumEntry::new(Addr::noop(), &AckPolicy::for_peer_count(0)).required,
            1
        );
        assert_eq!(
            QuorumEntry::new(Addr::noop(), &AckPolicy::for_peer_count(5)).required,
            3
        );
        assert_eq!(
            QuorumEntry::new(Addr::noop(), &AckPolicy::for_peer_count(7)).required,
            4
        );
    }
}
