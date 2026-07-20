#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

//! Message router — the central hub for Rod's P2P message routing.
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

use crate::Config;
use crate::Dup;
use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Flush, Get, Message, Put};
use crate::utils::BoundedHashMap;
use async_trait::async_trait;
use log::{debug, error, info};
use rand::{seq::IteratorRandom, thread_rng};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

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
///
/// # Deduplication
///
/// Uses [`crate::Dup`] for message-ID dedup and a [`BoundedHashMap`] for
/// Get message tracking. Response dedup uses checksum comparison.
pub struct Router {
    config: Config,
    known_peers: HashSet<Addr>,
    peer_addrs: HashMap<String, Addr>,
    /// Addresses of all storage adapter actors (both read and write).
    ///
    /// Used for echo-suppression checks (e.g. `put.from == *addr`).
    storage_adapters: HashSet<Addr>,
    /// Addresses of storage read actors — receive `Get` messages only.
    ///
    /// These actors share the same underlying database as the corresponding
    /// write actors, but process reads concurrently without waiting for
    /// pending writes.
    read_adapters: HashSet<Addr>,
    /// Addresses of storage write actors — receive `Put`, `BatchPut`, `Flush`.
    ///
    /// Write actors commit inside `spawn_blocking` (for redb) so fsync
    /// never blocks the async runtime. Messages are processed sequentially
    /// within each write actor, preserving write ordering.
    write_adapters: HashSet<Addr>,
    network_adapters: HashSet<Addr>,
    storage_adapter_actors: Vec<Box<dyn Actor>>,
    network_adapter_actors: Vec<Box<dyn Actor>>,
    server_peers: HashSet<Addr>,
    dup: Dup,
    seen_get_messages: BoundedHashMap<String, SeenGetMessage>,
    subscribers_by_topic: HashMap<String, HashSet<Addr>>,
    msg_counter: AtomicUsize,
}

#[async_trait]
impl Actor for Router {
    /// Starts storage and network adapter actors, registers them, and
    /// optionally begins stats reporting.
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

        // Stats collection is currently unimplemented (see update_stats)
        if self.config.stats {
            self.update_stats();
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        info!("Router stopping");
    }

    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        debug!("incoming message {}", msg.clone().to_string());
        match msg {
            Message::Put(put) => self.handle_put(put),
            Message::BatchPut(batch) => {
                self.handle_batch_put(batch);
            }
            Message::Get(get) => self.handle_get(get),
            Message::Flush(flush) => self.handle_flush(flush),
            Message::Hi { from, peer_id } => {
                self.known_peers.insert(from.clone());
                if !peer_id.is_empty() {
                    if let Some(existing) = self.peer_addrs.get(&peer_id) {
                        if existing != &from {
                            error!(
                                "Router peer_id collision: '{}' already mapped to {:?}, rejecting {:?}. Each peer_id must be unique.",
                                peer_id, existing, from
                            );
                            return;
                        }
                    }
                    self.peer_addrs.insert(peer_id, from);
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
                        let _ = addr.send(Message::RtcSignal(rtc));
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
    pub fn new(
        config: Config,
        storage_adapter_actors: Vec<Box<dyn Actor>>,
        network_adapter_actors: Vec<Box<dyn Actor>>,
    ) -> Self {
        Self {
            config,
            known_peers: HashSet::new(),
            peer_addrs: HashMap::new(),
            storage_adapters: HashSet::new(),
            read_adapters: HashSet::new(),
            write_adapters: HashSet::new(),
            network_adapters: HashSet::new(),
            storage_adapter_actors,
            network_adapter_actors,
            server_peers: HashSet::new(),
            dup: Dup::default_gun(),
            seen_get_messages: BoundedHashMap::new(SEEN_MSGS_MAX_SIZE),
            subscribers_by_topic: HashMap::new(),
            msg_counter: AtomicUsize::new(0),
        }
    }

    /// Stats reporting placeholder.
    ///
    /// Stats collection is not yet implemented. The original implementation
    /// used `sysinfo` to report memory/CPU/uptime but was removed due to
    /// dependency weight. A future implementation could use lightweight
    /// metrics via the `msg_counter` atomic.
    fn update_stats(&self) {
        // Stats collection not yet implemented.
        // TODO: implement lightweight stats via msg_counter and broadcast channel.
    }

    /// Handles a `Get` message: records subscription, queries storage and peers.
    ///
    /// The Get is deduplicated by message ID. The requester is registered as
    /// a subscriber for the topic (the first path segment of the node_id).
    /// Storage adapters are queried first, then server peers, then a random
    /// sample of up to 4 known subscribers/peers (MANET-style).
    fn handle_get(&mut self, get: Get) {
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

        let mut already_sent_to = HashSet::new();

        // Send to server peers
        for addr in self.server_peers.iter() {
            debug!("send to server peer");
            let _ = addr.send(Message::Get(get.clone()));
            already_sent_to.insert(addr.clone());
        }

        // Ask network subscribers
        let mut errored = HashSet::new();
        let mut sent_to = 0;
        let mut rng = thread_rng();
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
            let mut errored = HashSet::new();
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
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }
    }

    /// Handles a `Put` message: deduplicates, routes to storage or back to requester.
    ///
    /// If the Put is a response to a Get (has `in_response_to`), it's routed
    /// directly back to the original requester. Otherwise, it's forwarded to
    /// storage adapters and relayed to network peers.
    ///
    /// # Deduplication
    ///
    /// Two layers:
    /// 1. Message ID dedup (via [`Dup`])
    /// 2. Response checksum dedup — if the same response (same `@` + `##`) has
    ///    been seen, it's suppressed
    fn handle_put(&mut self, put: Put) {
        if self.is_message_seen(&put.id) {
            return;
        }

        // Gun.js DAM: ack + "##" + hash dedup for identical responses
        if let (Some(ack), Some(hash)) = (&put.in_response_to, put.checksum) {
            let checksum_key = format!("{}##{}", ack, hash);
            if self.dup.check(&checksum_key) {
                debug!("duplicate response checksum: {}", checksum_key);
                return;
            }
            self.dup.track(&checksum_key);
        }

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
            _ => {
                // Forward to storage write adapter(s)
                for addr in self.write_adapters.iter() {
                    if put.from == *addr {
                        continue;
                    }
                    let _ = addr.send(Message::Put(put.clone()));
                    debug!("sent to write adapter {}", addr);
                }
                // Network relay is handled by handle_put_relay for batching
                self.handle_put_relay(&put);
            }
        };
    }

    /// Relays a Put to server peers and subscribers.
    ///
    /// Storage is NOT touched here — this is pure network fan-out.
    /// Anti-loop detection uses the `peer_hop_list` field: peers already
    /// in the hop list are skipped.
    fn handle_put_relay(&mut self, put: &Put) {
        // NOTE: NO is_message_seen here. Router::handle_put already dedup'd.
        let mut hops = put.peer_hop_list.clone().unwrap_or_default();
        hops.insert(put.from.to_string());
        let mut already_sent_to = HashSet::new();

        // Send to server peers
        for addr in self.server_peers.iter() {
            if put.from == *addr || hops.contains(&addr.to_string()) {
                continue;
            }
            let mut put = put.clone();
            put.peer_hop_list = Some(hops.clone());
            let _ = addr.send(Message::Put(put));
            already_sent_to.insert(addr.clone());
        }

        // Relay to subscribers
        let mut sent_to = 0;
        for node_id in put.clone().updated_nodes.keys() {
            let topic = node_id.split("/").next().unwrap_or("");
            if let Some(topic_subscribers) = self.subscribers_by_topic.get_mut(topic) {
                topic_subscribers.retain(|addr| {
                    if put.from == *addr || hops.contains(&addr.to_string()) {
                        return true;
                    }
                    if already_sent_to.contains(addr) {
                        return true;
                    }
                    already_sent_to.insert(addr.clone());
                    let mut put = put.clone();
                    put.peer_hop_list = Some(hops.clone());
                    match addr.send(Message::Put(put)) {
                        Ok(_) => {
                            sent_to += 1;
                            true
                        }
                        _ => false,
                    }
                })
            }
        }
        debug!("sent put to {} subscribers", already_sent_to.len());
        if already_sent_to.len() < 4 {
            let mut rng = thread_rng();
            let mut errored = HashSet::new();
            while let Some(addr) = self.known_peers.iter().choose(&mut rng) {
                sent_to += 1;
                if sent_to >= 4 {
                    break;
                }
                if already_sent_to.contains(addr) {
                    continue;
                }
                already_sent_to.insert(addr.clone());
                if put.from == *addr || hops.contains(&addr.to_string()) {
                    continue;
                }
                let mut put = put.clone();
                put.peer_hop_list = Some(hops.clone());
                match addr.send(Message::Put(put)) {
                    Ok(_) => {
                        debug!("sent put to random peer");
                    }
                    _ => {
                        errored.insert(addr.clone());
                    }
                }
            }
            for addr in errored {
                self.known_peers.remove(&addr);
            }
        }
    }

    /// Handles a `BatchPut`: forwards to storage (single transaction), then
    /// relays each constituent Put individually with deduplication.
    ///
    /// This preserves atomic multi-write semantics for storage adapters while
    /// still doing per-message dedup and network relay.
    fn handle_batch_put(&mut self, batch: BatchPut) {
        // Forward BatchPut to storage write adapters — preserves single-transaction semantics
        for addr in self.write_adapters.iter() {
            if batch.from == *addr {
                continue;
            }
            let _ = addr.send(Message::BatchPut(batch.clone()));
        }

        // Relay each constituent put individually (with deduplication)
        for put in batch.puts {
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
                    let _ = seen_get_message.from.send(Message::Put(put));
                }
                continue;
            }
            self.handle_put_relay(&put);
        }
    }

    /// Handles a `Flush` message by forwarding to all storage adapters.
    ///
    /// Storage adapters can use this to trigger `fsync` or other durable
    /// persistence. The flush is not relayed to network peers.
    fn handle_flush(&mut self, flush: Flush) {
        let mut sent = HashSet::new();
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

    #[test]
    fn test_router_new() {
        let config = Config::default();
        let storage = vec![Box::new(MemoryStorage::new()) as Box<dyn Actor>];
        let router = Router::new(config, storage, vec![]);
        assert!(router.known_peers.is_empty());
        assert!(router.read_adapters.is_empty());
        assert!(router.write_adapters.is_empty());
        assert!(router.network_adapters.is_empty());
    }

    #[test]
    fn test_router_default_dedup() {
        let router = Router::new(Config::default(), vec![], vec![]);
        assert_eq!(router.dup.max(), 999);
        assert_eq!(router.dup.age(), std::time::Duration::from_secs(9));
    }

    #[test]
    fn test_router_seen_msg_capacity() {
        let router = Router::new(Config::default(), vec![], vec![]);
        // The seen_get_messages BoundedHashMap should have capacity SEEN_MSGS_MAX_SIZE
        assert_eq!(SEEN_MSGS_MAX_SIZE, 10000);
        let _ = router; // just verify it constructs
    }

    #[test]
    fn test_router_msg_counter_starts_zero() {
        let router = Router::new(Config::default(), vec![], vec![]);
        assert_eq!(router.msg_counter.load(Ordering::Relaxed), 0);
    }
}
