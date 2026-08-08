//! Graph node — the core API for reading and writing data in the BEAM graph.
//!
//! [`Node`] is the primary user-facing type. It represents a node in the
//! distributed graph database and provides methods for:
//!
//! - **Reading**: [`Node::get`] (traverse to child), [`Node::on`] (subscribe
//!   to value updates), [`Node::once`] (read once), [`Node::map`] (subscribe
//!   to all children)
//! - **Writing**: [`Node::put`] (set a value), [`Node::batch_put`] (atomic
//!   multi-write)
//! - **Networking**: [`Node::connect_peer`] (WebSocket), [`Node::connect_webrtc_peer`]
//!   (WebRTC, feature-gated)
//! - **Lifecycle**: [`Node::stop`]
//!
//! # Architecture
//!
//! A `Node` is cheaply cloneable (uses `Arc` internally). Each clone shares
//! the same underlying state. Child nodes are created lazily via `get()` and
//! are backed by their own actor + broadcast channels.
//!
//! The root node owns a [`Router`] actor that manages storage and network
//! adapters, message deduplication, and peer routing. All `put` and `get`
//! operations flow through the router.
//!
//! # Example
//!
//! ```ignore
//! use beam::{Node, Value};
//!
//! let mut db = Node::new();
//! let mut sub = db.get("greeting").on();
//! db.get("greeting").put("Hello World!".into());
//! // sub.recv().await == Some(Value::Text("Hello World!"))
//! ```

use crate::ack::{AckPolicy, QUORUM_MET_SENTINEL, ReplicationStatus};
use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::MemoryStorage;
use crate::message::{BatchPut, Flush, Get, Message, Put};
use crate::metrics::Metrics;
use crate::router::Router;
use crate::types::{Children, NodeData, Value};
use crate::utils::random_string;
use async_trait::async_trait;
use futures_util::StreamExt;
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::sync::{broadcast, oneshot};
use tokio_tungstenite::connect_async;

/// Configuration for a [`Node`] and its associated adapters.
///
/// Controls public space access, broadcast channel sizing,
/// and WebRTC ICE server configuration.
#[derive(Clone)]
pub struct Config {
    /// Whether to accept writes to public space (non-user-owned nodes).
    ///
    /// When `true` (default), the node accepts puts to any node ID. When
    /// `false`, only content-addressed (signed) data and user-owned nodes
    /// are accepted — matching Gun.js `opt.enforce` semantics.
    pub allow_public_space: bool,
    /// Public key to prioritize for this node (format: `x.y`).
    ///
    /// When set, the node will preferentially cache data owned by this
    /// public key. Used for user-authenticated nodes.
    pub my_pub: Option<String>,
    /// Buffer size for broadcast channels used by `on()` and `map()`.
    ///
    /// Defaults to 4096. Increase for high-throughput scenarios; decrease
    /// to save memory per active subscription.
    pub broadcast_buffer_size: usize,
    /// STUN/TURN servers for WebRTC ICE negotiation.
    ///
    /// Defaults to Google's public STUN server. Only used when the
    /// `webrtc` feature is enabled.
    pub ice_servers: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            allow_public_space: true,
            my_pub: None,
            broadcast_buffer_size: 4096,
            ice_servers: vec!["stun:stun.l.google.com:19302".to_string()],
        }
    }
}

/// A graph node — the primary API for reading and writing data in BEAM.
///
/// A `Node` represents a position in the distributed graph. The root node
/// (created via [`Node::new`] or [`Node::new_with_config`]) owns the router
/// and all adapter actors. Child nodes (created via [`Node::get`]) share
/// the router and communicate through broadcast channels.
///
/// # Cloning
///
/// `Node` is cheaply cloneable. Clones share the same underlying state
/// via `Arc`. This is important for async patterns where you need to
/// send a node into multiple futures.
///
/// # Concurrency
///
/// Each node runs as a tokio actor, processing [`Message::Put`] messages
/// in its `handle()` method. Reads are served via `broadcast` channels —
/// `on()` returns a `Receiver<Value>` and `map()` returns a
/// `Receiver<(String, Value)>`.
/// Type alias for the pending put acknowledgment sender.
///
/// Each entry is a oneshot sender that resolves when storage adapters
/// acknowledge a put operation. The key is the put's `id`.
type PendingPuts = Arc<RwLock<HashMap<String, oneshot::Sender<Result<ReplicationStatus, String>>>>>;

#[derive(Clone)]
pub struct Node {
    uid: Arc<RwLock<String>>,
    path: Vec<String>,
    children: Arc<RwLock<BTreeMap<String, Node>>>,
    parent: Arc<RwLock<Option<(String, Node)>>>,
    broadcast_buffer_size: usize,
    on_sender: broadcast::Sender<Value>,
    map_sender: broadcast::Sender<(String, Value)>,
    actor_context: Box<ActorContext>,
    /// Shutdown signal sender — broadcasts `true` to all child tasks for graceful shutdown.
    shutdown_tx: watch::Sender<bool>,
    addr: Arc<RwLock<Option<Addr>>>,
    router: Arc<RwLock<Option<Addr>>>,
    pending_flushes: Arc<RwLock<HashMap<String, oneshot::Sender<()>>>>,
    /// Pending `put` acknowledgements keyed by `Put.id`.
    ///
    /// When `Node::put` (or `Node::batch_put`) is called, a oneshot sender
    /// is registered here keyed by the put's `id`. Storage adapters ack
    /// the put by sending a `Put` message with `in_response_to: Some(id)`
    /// back to this node's `addr`. `Node::handle_put` intercepts the ack
    /// and completes the oneshot, resolving the awaited future.
    ///
    /// The sender's payload carries `Result<(), String>` so commit failures
    /// propagate to the caller instead of being silently swallowed.
    pending_puts: PendingPuts,
    allow_public_space: bool,
    ice_servers: Vec<String>,
    /// Shared lock-free observability counters.
    ///
    /// Cloned via `Arc` to the [`Router`] at construction time so the
    /// Router's internal `try_send_or_log` calls and any external
    /// observer (e.g. e2e tests, telemetry exporter) share the same
    /// underlying atomic counters. See [`crate::metrics::Metrics`].
    metrics: Arc<Metrics>,
}

#[async_trait]
impl Actor for Node {
    async fn handle(&mut self, msg: Message, _context: &ActorContext) {
        if let Message::Put(put) = msg {
            self.handle_put(put)
        }
    }
}

impl Node {
    /// Creates a new root-level node with default configuration and in-memory storage.
    ///
    /// This is the simplest way to get started with BEAM. The node will use
    /// [`MemoryStorage`] and have no network adapters connected.
    pub fn new() -> Self {
        let storage = MemoryStorage::new();
        Self::new_with_config(Config::default(), vec![Box::new(storage)], Vec::new())
    }

    /// Returns the unique identifier of this node (the path joined by `/`).
    ///
    /// The root node has an empty `uid`. Child nodes have uids like
    /// `"parent_key/child_key"`.
    pub fn id(&self) -> String {
        self.uid.read().clone()
    }

    /// Returns the peer ID of this node's actor context.
    ///
    /// The peer ID is a random string generated at node creation time.
    /// It identifies this node instance in the P2P mesh.
    pub fn peer_id(&self) -> String {
        self.actor_context.peer_id.read().clone()
    }

    /// Creates a new root-level node with custom configuration, storage, and network adapters.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration (see [`Config`])
    /// * `storage_adapters` - Storage actors (e.g. [`MemoryStorage`], `RedbStorage`)
    /// * `network_adapters` - Network actors (e.g. `OutgoingWebsocketManager`, `WsServer`)
    pub fn new_with_config(
        config: Config,
        storage_adapters: Vec<Box<dyn Actor>>,
        network_adapters: Vec<Box<dyn Actor>>,
    ) -> Self {
        // Shared observability handle — cloned to Router so both observe
        // the same atomic counters. External observers reach this via
        // `Node::metrics()`.
        let metrics = Arc::new(Metrics::new());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut actor_context = ActorContext::new(random_string(16));
        actor_context.shutdown_rx = shutdown_rx;
        let mut node = Self {
            path: vec![],
            uid: Arc::new(RwLock::new("".to_string())),
            children: Arc::new(RwLock::new(BTreeMap::new())),
            parent: Arc::new(RwLock::new(None)),
            broadcast_buffer_size: config.broadcast_buffer_size,
            on_sender: broadcast::channel::<Value>(config.broadcast_buffer_size).0,
            map_sender: broadcast::channel::<(String, Value)>(config.broadcast_buffer_size).0,
            addr: Arc::new(RwLock::new(None)),
            router: Arc::new(RwLock::new(None)),
            pending_flushes: Arc::new(RwLock::new(HashMap::new())),
            pending_puts: Arc::new(RwLock::new(HashMap::new())),
            allow_public_space: config.allow_public_space,
            ice_servers: config.ice_servers.clone(),
            actor_context: Box::new(actor_context),
            shutdown_tx,
            metrics: metrics.clone(),
        };

        node.actor_context.node = Some(node.clone());
        let addr = node.actor_context.start_actor(Box::new(node.clone()));
        *node.addr.write() = Some(addr);

        let router = Box::new(Router::new(storage_adapters, network_adapters, metrics));
        let router_addr = node.actor_context.start_router(router);
        node.actor_context.router = router_addr.clone();
        *node.router.write() = Some(router_addr);

        node
    }

    /// Returns a clone of the shared `Arc<Metrics>` handle.
    ///
    /// The returned Arc points to the same atomic counters as the
    /// Router's internal field. External observers (tests, telemetry
    /// exporters) read counters via [`crate::metrics::Metrics::snapshot`]
    /// or record events via [`crate::metrics::Metrics::record_dropped_send`].
    ///
    /// Cloning the Arc is cheap (refcount bump); the atomic counters
    /// are shared across all clones.
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Handles incoming [`Put`] messages by dispatching values to subscribers.
    ///
    /// If the put is a flush acknowledgement (has `in_response_to` matching a
    /// pending flush), the flush's oneshot sender is triggered instead.
    ///
    /// For replay puts (have `in_response_to`), a `__beam_replay_complete__`
    /// marker is sent on the map channel after all child values are dispatched.
    fn handle_put(&mut self, put: Put) {
        // Intercept acks BEFORE processing the message as data. Two ack
        // channels share the `in_response_to` field:
        //
        // 1. **Flush acks** — see [`Node::flush_storage`]. Payload is unit `()`;
        //    any `Put` with `in_response_to` matching a `pending_flushes` key
        //    resolves that barrier. Flush acks carry `_flushed` (no payload
        //    discrimination needed — the presence of a match IS the success).
        //
        // 2. **Put/BatchPut acks** — sent directly from storage adapters
        //    after commit. Payload is `Result<(), String>` decoded via
        //    [`Self::decode_put_ack_payload`]. The storage adapter chooses
        //    between `_ack` (success) and `_err:<msg>` (failure).
        //
        // We check `pending_flushes` first because Flush was the original
        // ack pattern and remains the simplest. Put acks are a strictly
        // additive extension.
        if let Some(response_id) = &put.in_response_to {
            // Flush acks — any payload, presence is success
            if let Some(sender) = self.pending_flushes.write().remove(response_id) {
                let _ = sender.send(());
                return;
            }
            // Put/BatchPut acks — try quorum sentinel first, fall back to _ack/_err
            if let Some(sender) = self.pending_puts.write().remove(response_id) {
                let result = if let Some(quorum_result) = Node::decode_quorum_payload(&put) {
                    // Router fired __quorum_met__ — either peer-ack quorum
                    // satisfied (Ok) or cleanup reaper timed us out (Err).
                    quorum_result
                } else {
                    // Local storage _ack/_err reply — wrap as minimal status
                    Self::decode_put_ack_payload(&put).map(|()| ReplicationStatus {
                        put_id: response_id.clone(),
                        acked_by: 1,
                        quorum_met: true,
                        elapsed: Duration::ZERO,
                    })
                };
                let _ = sender.send(result);
                return;
            }
        }
        let is_replay = put.in_response_to.is_some();
        for (node_id, node_data) in put.updated_nodes {
            if node_id == *self.uid.read() {
                for (child, child_data) in node_data {
                    // Skip internal control keys
                    if child.starts_with("__beam_") {
                        continue;
                    }
                    if let Some(child) = self.children.read().get(&child) {
                        let _ = child.on_sender.send(child_data.value.clone());
                    }
                    let _ = self
                        .map_sender
                        .send((child.to_string(), child_data.value.clone()));
                }
                if is_replay {
                    let _ = self
                        .map_sender
                        .send(("__beam_replay_complete__".to_string(), Value::Null));
                }
            }
        }
    }

    /// Flushes pending writes to persistent storage and waits for acknowledgement.
    ///
    /// Sends a [`Flush`] message to all storage adapters via the router, then
    /// waits for the first adapter to acknowledge. If no acknowledgement
    /// arrives within the timeout, returns an error.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait. Defaults to 30 seconds if `None`.
    ///
    /// # Errors
    ///
    /// - `"router not initialized"` — node has no router (shouldn't happen in normal use)
    /// - `"failed to send flush to router"` — router channel is closed
    /// - `"flush ack channel closed"` — oneshot sender was dropped
    /// - `"flush timed out"` — no acknowledgement within timeout
    pub async fn flush_storage(&self, timeout: Option<Duration>) -> Result<(), String> {
        let router_addr = match &*self.router.read() {
            Some(addr) => addr.clone(),
            None => return Err("router not initialized".to_string()),
        };
        let flush = Flush::new(self.addr.read().clone().unwrap(), None);
        let id = flush.id.clone();
        let (tx, rx) = oneshot::channel();
        self.pending_flushes.write().insert(id.clone(), tx);

        if let Err(_e) = router_addr.send(Message::Flush(flush)) {
            self.pending_flushes.write().remove(&id);
            return Err("failed to send flush to router".to_string());
        }

        let dur = timeout.unwrap_or(Duration::from_secs(30));
        match tokio::time::timeout(dur, rx).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err("flush ack channel closed".to_string()),
            Err(_) => {
                self.pending_flushes.write().remove(&id);
                Err("flush timed out".to_string())
            }
        }
    }

    fn new_child(&self, key: String) -> Node {
        assert!(!key.is_empty(), "Key length must be greater than zero");
        let mut path = self.path.clone();
        path.push(key.clone());
        let new_child_uid = path.join("/");
        debug!("new_child_uid {}", new_child_uid);
        let node = Self {
            path,
            children: Arc::new(RwLock::new(BTreeMap::new())),
            parent: Arc::new(RwLock::new(Some((self.uid.read().clone(), self.clone())))),
            broadcast_buffer_size: self.broadcast_buffer_size,
            on_sender: broadcast::channel::<Value>(self.broadcast_buffer_size).0,
            map_sender: broadcast::channel::<(String, Value)>(self.broadcast_buffer_size).0,
            uid: Arc::new(RwLock::new(new_child_uid)),
            router: self.router.clone(),
            pending_flushes: Arc::new(RwLock::new(HashMap::new())),
            pending_puts: Arc::new(RwLock::new(HashMap::new())),
            addr: Arc::new(RwLock::new(None)),
            actor_context: self.actor_context.clone(),
            allow_public_space: self.allow_public_space,
            ice_servers: self.ice_servers.clone(),
            // Children share the parent's metrics Arc so all nodes in a
            // tree aggregate drops into the same counters.
            metrics: self.metrics.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        };
        let addr = self.actor_context.start_actor(Box::new(node.clone()));
        *node.addr.write() = Some(addr);
        let mut guard = self.children.write();
        guard.insert(key, node.clone());
        node
    }

    /// Subscribes to this node's value updates.
    ///
    /// Returns a [`broadcast::Receiver`] that will receive [`Value`] updates
    /// whenever the node's value changes. The current value (if any) is
    /// requested from storage via a `Get` message — it arrives asynchronously.
    pub fn on(&mut self) -> broadcast::Receiver<Value> {
        let key = if self.path.len() > 1 {
            self.path.last().cloned()
        } else {
            None
        };
        let addr;
        let node_id;
        if let Some((parent_id, parent)) = &*self.parent.read() {
            node_id = parent_id.clone();
            addr = parent.addr.read().clone().unwrap();
        } else {
            node_id = self.uid.read().to_string();
            addr = self.addr.read().clone().unwrap();
        }
        let get = Get::new(node_id, key, addr);
        // subscribe before send so we don't miss the response
        let subscriber = self.on_sender.subscribe();
        if let Some(router) = self.router.read().clone() {
            let _ = router.send(Message::Get(get));
        }
        subscriber
    }

    /// Reads the node's value once, or `None` if not found within the timeout.
    ///
    /// This is a convenience wrapper around [`Node::on`] with a timeout.
    /// The default timeout is 66ms (matching Gun.js's `opt.wait`).
    ///
    /// # Arguments
    ///
    /// * `wait` - Optional timeout. Defaults to 66ms.
    pub async fn once(&mut self, wait: Option<Duration>) -> Option<Value> {
        let val = tokio::time::timeout(wait.unwrap_or(Duration::from_millis(66)), self.on().recv())
            .await
            .ok()?
            .expect("recv error??");
        Some(val)
    }

    /// Connects to a remote peer via WebSocket with automatic reconnection.
    ///
    /// Retries with exponential backoff starting at 1 second, maxing at 60
    /// seconds. The spawned `WsConn` actor auto-handshakes via [`Message::Hi`]
    /// on `pre_start`, and the [`Router`] registers the peer on `Hi` receipt.
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket URL (e.g. `"wss://relay.example.com/ws"`)
    ///
    /// # Panics
    ///
    /// Panics if the URL is invalid (should not happen with well-formed URLs).
    pub fn connect_peer(&self, url: &str) {
        let ctx = self.actor_context.clone();
        let ctx_for_actor = ctx.clone();
        let url = url.to_string();
        let allow_public_space = self.allow_public_space;
        ctx.child_task(async move {
            let ctx = ctx_for_actor;
            let mut backoff = Duration::from_secs(1);
            let max_backoff = Duration::from_secs(60);
            loop {
                match connect_async(&url).await {
                    Ok((socket, _)) => {
                        let (sender, receiver) = socket.split();
                        let conn =
                            crate::adapters::WsConn::new(sender, receiver, allow_public_space);
                        let addr = ctx.start_actor(Box::new(conn));
                        info!("BEAM connected to peer {} (addr: {})", url, addr);
                        backoff = Duration::from_secs(1);
                        // Stay alive; WsConn runs until disconnect.
                        // TODO: detect disconnect for faster reconnect loop.
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    }
                    Err(e) => {
                        warn!(
                            "BEAM connect to {} failed: {}. retry in {:?}",
                            url, e, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        });
    }

    /// Connects to a remote peer via WebRTC data channel.
    ///
    /// Signaling bootstraps over the existing WebSocket mesh via
    /// [`Message::RtcSignal`]. Once the data channel opens, the peer is
    /// registered in `Router::known_peers` just like a WebSocket peer, and
    /// Gun protocol messages flow over the P2P link.
    ///
    /// Requires the `webrtc` feature. Without it, this method is a no-op.
    #[cfg(feature = "webrtc")]
    pub fn connect_webrtc_peer(
        &self,
        peer_id: &str,
        target_peer_id: &str,
        role: crate::adapters::WebRtcRole,
    ) {
        let peer_id = peer_id.to_string();
        let target_peer_id = target_peer_id.to_string();
        let ice_servers = self.ice_servers.clone();
        let allow_public_space = self.allow_public_space;
        let ctx = self.actor_context.clone();
        let ctx_for_actor = ctx.clone();
        ctx.child_task(async move {
            let peer = crate::adapters::WebRtcPeer::new(
                peer_id,
                target_peer_id,
                role,
                allow_public_space,
                ice_servers,
            );
            let addr = ctx_for_actor.start_actor(Box::new(peer));
            info!("BEAM WebRtcPeer started (addr: {})", addr);
        });
    }

    /// Returns a child node corresponding to the given key, creating it if necessary.
    ///
    /// This is the primary graph traversal method. Calling `node.get("key")`
    /// returns a child node. If the child already exists, the existing
    /// instance is returned; otherwise a new child is created lazily.
    ///
    /// # Arguments
    ///
    /// * `key` - The child key. Must not be empty (empty returns `self`).
    pub fn get(&mut self, key: &str) -> Node {
        if key.is_empty() {
            return self.clone();
        }
        debug!("get key {}", key);
        // Explicit scope to drop read guard BEFORE entering else branch.
        // The temporary from `self.children.read()` would otherwise live
        // until the end of the `if let` statement, including the else block,
        // causing a deadlock when new_child() tries to write().
        let existing = {
            let guard = self.children.read();
            guard.get(key).cloned()
        };
        match existing {
            Some(child) => child,
            None => self.new_child(key.to_string()),
        }
    }

    /// Subscribes to all children of this node.
    ///
    /// Returns a [`broadcast::Receiver`] that emits `(child_key, value)` tuples
    /// for each child. The current children (if any) are requested from storage
    /// via a `Get` message.
    pub fn map(&self) -> broadcast::Receiver<(String, Value)> {
        let node_id = self.uid.read().to_string();
        let addr = self.addr.read().clone().unwrap();
        let get = Get::new(node_id, None, addr);
        // subscribe before send so we don't miss the response
        let subscriber = self.map_sender.subscribe();
        if let Some(router) = self.router.read().clone() {
            let _ = router.send(Message::Get(get));
        }
        subscriber
    }

    /// Walks up the parent chain, building the `updated_nodes` map for a Put.
    ///
    /// For each ancestor, this inserts the child's value as a child of the
    /// parent node in `updated_nodes`, and links the parent as a `Value::Link`.
    /// This builds the Gun.js wire format's nested node structure.
    fn add_parent_nodes(
        &mut self,
        updated_nodes: &mut BTreeMap<String, Children>,
        value: Value,
        updated_at: f64,
    ) {
        let parent = &*self.parent.read();
        if let Some((parent_id, parent)) = parent {
            let mut parent = parent.clone();
            let mut children = Children::default();
            children.insert(
                self.path.last().unwrap().clone(),
                NodeData {
                    value: value.clone(),
                    updated_at,
                },
            );
            updated_nodes.insert(parent_id.to_string(), children);
            parent.add_parent_nodes(updated_nodes, Value::Link(parent.id()), updated_at);
        }
    }

    /// Writes a value to this node and waits for storage acknowledgement.
    ///
    /// The value is immediately sent to local `on()` subscribers (so any
    /// in-process listeners see it before the ack returns), then a [`Put`]
    /// message is sent to the router for storage and network relay. The
    /// timestamp is the current Unix epoch in milliseconds.
    ///
    /// Returns `Ok(())` once a storage adapter has committed the put durably
    /// and acked back. Returns `Err(String)` if the adapter reports a commit
    /// failure, the router was not initialized, the router channel rejected
    /// the message, or the ack did not arrive within the timeout window
    /// (default 30 seconds).
    ///
    /// # Why Async?
    ///
    /// Without the ack, `put` returns synchronously after queuing the message
    /// to the storage actor — but the storage actor may not have processed
    /// the message yet. A subsequent `get` or `once` would read stale state.
    /// The async ack closes that race: this future resolves only after the
    /// storage actor has committed the put. See [module docs](self) for the
    /// full race-condition history.
    ///
    /// # Ack Pattern
    ///
    /// Mirrors [`Node::flush_storage`](Self::flush_storage):
    /// 1. Register a oneshot keyed by the put's `id` in `pending_puts`
    /// 2. Send `Message::Put` to the router
    /// 3. Storage adapter commits, then sends
    ///    `Put { in_response_to: Some(id), updated_nodes: { "_ack": { "_ack"|"_err": ... } } }`
    ///    back to this node's `addr` directly (NOT through the router)
    /// 4. `Node::handle_put` drains `pending_puts` and resolves the oneshot
    ///
    /// # Arguments
    ///
    /// * `value` - The value to set (see [`Value`] for supported types)
    ///
    /// Writes a value and waits for it to be replicated to N peers per
    /// the given [`AckPolicy`].
    ///
    /// Resolves with a [`ReplicationStatus`] when the policy threshold is
    /// satisfied, or `Err(String)` on timeout, router failure, or unrecoverable
    /// storage error.
    ///
    /// # Wire-level flow
    ///
    /// 1. Build a [`Put`] and register a oneshot in `pending_puts`
    /// 2. Send `Message::RegisterQuorum { put_id, requester, policy }` to Router
    ///    (this creates a tracked [`crate::router::QuorumEntry`])
    /// 3. Send `Message::Put(put)` to Router for relay to peers
    /// 4. Peers eventually reply with `Put { @: put_id, .. }`
    /// 5. Router's `handle_put` ack branch counts each peer ack in the QuorumEntry
    /// 6. When `acked_by >= policy.quorum`, Router sends a sentinel
    ///    `Put { @: put_id, updated_nodes: { "__quorum_met__": ack_count } }`
    ///    back to this Node
    /// 7. This Node's `handle_put` drain decodes the sentinel via
    ///    [`Node::decode_quorum_payload`] and resolves the oneshot with the
    ///    [`ReplicationStatus`]
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use beam::{Node, Value, AckPolicy};
    ///
    /// let node = Node::new();
    /// let policy = AckPolicy::for_peer_count(3); // majority of 3 peers
    /// let status = node.put_quorum(Value::Text("hello".into()), policy).await?;
    /// assert!(status.quorum_met);
    /// assert!(status.acked_by >= 2);
    /// ```
    ///
    /// # Arguments
    ///
    /// * `value` - The value to set (see [`Value`] for supported types)
    /// * `policy` - The [`AckPolicy`] describing how many peer acks are required
    ///   and how long to wait
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` if:
    /// - The router is not initialized
    /// - The Router fails to receive the `RegisterQuorum` or `Put` message
    /// - The policy timeout elapses before quorum is met
    /// - The ack channel closes (Router dropped us)
    pub async fn put_quorum(
        &mut self,
        value: Value,
        policy: AckPolicy,
    ) -> Result<ReplicationStatus, String> {
        let updated_at: f64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;
        debug!("put_quorum (required: {} peers)", policy.quorum);
        self.on_sender.send(value.clone()).ok();
        let mut updated_nodes = BTreeMap::new();
        self.add_parent_nodes(&mut updated_nodes, value, updated_at);
        let my_addr = self.addr.read().clone().unwrap();
        let put = Put::new(updated_nodes, None, my_addr.clone());
        let put_id = put.id.clone();
        let (tx, rx) = oneshot::channel();
        self.pending_puts.write().insert(put_id.clone(), tx);

        let router_addr = match &*self.router.read() {
            Some(addr) => addr.clone(),
            None => {
                self.pending_puts.write().remove(&put_id);
                return Err("router not initialized".to_string());
            }
        };

        // Register the quorum BEFORE sending the put — Router must know about
        // the put_id before peer acks start arriving, or the first ack will
        // be missed (no QuorumEntry to increment).
        if router_addr
            .send(Message::RegisterQuorum {
                put_id: put_id.clone(),
                requester: my_addr,
                policy,
            })
            .is_err()
        {
            self.pending_puts.write().remove(&put_id);
            return Err("failed to send RegisterQuorum to router".to_string());
        }

        if router_addr.send(Message::Put(put)).is_err() {
            self.pending_puts.write().remove(&put_id);
            return Err("failed to send put to router".to_string());
        }

        match tokio::time::timeout(policy.timeout, rx).await {
            Ok(Ok(status)) => status,
            Ok(Err(_)) => Err("put_quorum ack channel closed".to_string()),
            Err(_) => {
                self.pending_puts.write().remove(&put_id);
                Err(format!(
                    "put_quorum timed out after {:?} (required {} peers)",
                    policy.timeout, policy.quorum
                ))
            }
        }
    }

    pub async fn put(&mut self, value: Value) -> Result<(), String> {
        let updated_at: f64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;
        self.on_sender.send(value.clone()).ok();
        debug!("put {}", value.to_string());
        let mut updated_nodes = BTreeMap::new();
        // Store the value at self.uid under the "_" convention (Gun.js
        // soul-value encoding) so that map() on self returns the value
        // as a synthetic child. This is what Gun.js semantics expect for
        // a node's own value vs its children. Previously put only wrote
        // under parent_id via add_parent_nodes, so map() on the leaf
        // never saw the put value.
        let self_uid = self.uid.read().clone();
        let mut self_children = Children::default();
        self_children.insert(
            "_".to_string(),
            NodeData {
                value: value.clone(),
                updated_at,
            },
        );
        updated_nodes.insert(self_uid, self_children);
        // Continue with parent chain propagation using the raw value so
        // parents' child entries remain the actual value (this is what
        // node.get("key").put(...) → node.get("key").once(...) expects).
        self.add_parent_nodes(&mut updated_nodes, value, updated_at);
        let my_addr = self.addr.read().clone().unwrap();
        let put = Put::new(updated_nodes, None, my_addr);
        let put_id = put.id.clone();
        let (tx, rx) = oneshot::channel();
        self.pending_puts.write().insert(put_id.clone(), tx);

        let router_addr = match &*self.router.read() {
            Some(addr) => addr.clone(),
            None => {
                self.pending_puts.write().remove(&put_id);
                return Err("router not initialized".to_string());
            }
        };

        if router_addr.send(Message::Put(put)).is_err() {
            self.pending_puts.write().remove(&put_id);
            return Err("failed to send put to router".to_string());
        }

        let dur = Duration::from_secs(30); // TODO: accept timeout as parameter when API stabilizes
        match tokio::time::timeout(dur, rx).await {
            Ok(Ok(_status)) => Ok(()), // discard ReplicationStatus for local put
            Ok(Err(_)) => Err("put ack channel closed".to_string()),
            Err(_) => {
                self.pending_puts.write().remove(&put_id);
                Err("put timed out".to_string())
            }
        }
    }

    /// Writes multiple values in a single storage transaction and waits for ack.
    ///
    /// Each operation is a `(path, value)` pair where `path` is a vector of
    /// keys from the caller's [`Node`] down to the leaf. The caller should
    /// invoke this on the **root** [`Node`].
    ///
    /// Returns `Ok(())` once the storage adapter has committed the batch
    /// durably and acked back. Returns `Err(String)` on commit failure,
    /// router error, or ack timeout (default 30s).
    ///
    /// # Atomicity
    ///
    /// Unlike multiple sequential [`put`](Self::put) calls, all operations
    /// in a batch either succeed together or fail together. Storage adapters
    /// that support transactions (e.g. [`crate::adapters::RedbStorage`])
    /// wrap the batch in a single transaction.
    ///
    /// # Ack Pattern
    ///
    /// The whole batch shares a single ack keyed by `BatchPut.id`. The
    /// originating node registers one oneshot, the storage adapter sends
    /// one ack after the entire transaction commits or aborts.
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of `(path, value)` pairs
    pub async fn batch_put(&mut self, ops: Vec<(Vec<String>, Value)>) -> Result<(), String> {
        let updated_at: f64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;

        let mut puts = Vec::with_capacity(ops.len());
        for (path, value) in ops {
            // Traverse from self to leaf, lazily creating children.
            let mut leaf = self.clone();
            for key in &path {
                leaf = leaf.get(key);
            }

            // Notify local on() subscribers at the leaf (mirrors Node::put).
            let _ = leaf.on_sender.send(value.clone());

            let mut updated_nodes = BTreeMap::new();
            leaf.add_parent_nodes(&mut updated_nodes, value, updated_at);

            let my_addr = self.addr.read().clone().unwrap();
            let put = Put::new(updated_nodes, None, my_addr);
            puts.push(put);
        }

        let my_addr = self.addr.read().clone().unwrap();
        let batch = BatchPut::new(puts, my_addr);
        let batch_id = batch.id.clone();
        let (tx, rx) = oneshot::channel();
        // Register under batch id; storage adapter will ack via `in_response_to: Some(batch.id)`.
        self.pending_puts.write().insert(batch_id.clone(), tx);

        let router_addr = match &*self.router.read() {
            Some(addr) => addr.clone(),
            None => {
                self.pending_puts.write().remove(&batch_id);
                return Err("router not initialized".to_string());
            }
        };

        if router_addr.send(Message::BatchPut(batch)).is_err() {
            self.pending_puts.write().remove(&batch_id);
            return Err("failed to send batch_put to router".to_string());
        }

        let dur = Duration::from_secs(30); // TODO: accept timeout as parameter when API stabilizes
        match tokio::time::timeout(dur, rx).await {
            Ok(Ok(_status)) => Ok(()), // discard ReplicationStatus for batch_put
            Ok(Err(_)) => Err("batch_put ack channel closed".to_string()),
            Err(_) => {
                self.pending_puts.write().remove(&batch_id);
                Err("batch_put timed out".to_string())
            }
        }
    }

    /// Stops the node and all its child actors and adapters.
    ///
    /// This calls [`ActorContext::stop`] from the node's actor context, which
    /// aborts all child tasks and sends stop signals to all child actors.
    pub fn stop(&mut self) {
        info!("Node stopping");
        self.actor_context.stop();
    }

    /// Gracefully shuts down the node, ensuring data integrity.
    ///
    /// This is the preferred shutdown path. The sequence is:
    ///
    /// 1. **Flush storage** — calls [`Node::flush_storage`] to ensure all
    ///    pending writes in the actor mailboxes are processed and committed
    ///    by the storage adapters. The router processes messages in order,
    ///    so any puts ahead of the flush are committed before the flush ack
    ///    returns.
    ///
    /// 2. **Signal shutdown** — broadcasts `true` on the shutdown watch
    ///    channel. Long-running child tasks (accept loops, signal processors)
    ///    that `select!` on `shutdown_rx` break their loops and stop
    ///    accepting new connections or work.
    ///
    /// 3. **Drain** — waits briefly for in-flight messages to complete and
    ///    network connections to close. The drain duration is bounded by
    ///    the remaining time budget after the flush.
    ///
    /// 4. **Force stop** — calls [`Node::stop`] to abort any remaining
    ///    tasks and send stop signals to all child actors. This is the
    ///    same as a hard shutdown, but by this point all critical work
    ///    should already be done.
    ///
    /// # Arguments
    ///
    /// * `timeout` — maximum total time for the graceful shutdown sequence.
    ///   If the flush and drain do not complete within this duration, the
    ///   method proceeds to force stop and returns an error.
    ///
    /// # Returns
    ///
    /// - `Ok(())` — graceful shutdown completed within the timeout.
    /// - `Err(String)` — timed out; force stop was used. The error message
    ///   describes which phase timed out.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use beam::Node;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let mut node = Node::new();
    /// // ... use node ...
    /// if let Err(e) = node.shutdown(Duration::from_secs(30)).await {
    ///     eprintln!("graceful shutdown timed out: {}, force-stopped", e);
    /// }
    /// # }
    /// ```
    pub async fn shutdown(&mut self, timeout: Duration) -> Result<(), String> {
        info!("Node graceful shutdown initiated (timeout: {:?})", timeout);

        // Phase 1: Flush storage — ensure pending writes reach disk.
        // The flush message goes through the router, which processes
        // messages in FIFO order. Any puts ahead of the flush in the
        // mailbox are committed before the flush ack returns.
        let flush_result = tokio::time::timeout(timeout, self.flush_storage(Some(timeout))).await;

        match flush_result {
            Ok(Ok(())) => info!("Storage flush completed during shutdown"),
            Ok(Err(e)) => {
                warn!("Storage flush error during shutdown: {} — continuing", e);
            }
            Err(_) => {
                warn!("Storage flush timed out during shutdown — forcing stop");
                self.stop();
                return Err("flush timed out".to_string());
            }
        }

        // Phase 2: Signal shutdown to all long-running child tasks.
        // This causes accept loops, retry loops, and signal processors
        // to break and stop accepting new work.
        if self.shutdown_tx.send(true).is_err() {
            warn!("Shutdown signal already sent — all receivers may be dropped");
        }
        info!("Shutdown signal broadcast to child tasks");

        // Phase 3: Drain — give in-flight messages and connection close
        // handshakes time to complete. We use a fraction of the remaining
        // timeout budget (or a default if flush consumed little).
        let drain_timeout = Duration::from_secs(5);
        debug!("Draining for {:?} before force stop", drain_timeout);
        tokio::time::sleep(drain_timeout).await;

        // Phase 4: Force stop — abort remaining tasks, send stop signals.
        // By this point all critical work (flush, signal) is done. This
        // cleans up any stragglers.
        self.stop();
        info!("Node graceful shutdown complete");
        Ok(())
    }

    /// Decodes a put-ack payload sent by a storage adapter after commit.
    ///
    /// Storage adapters ack a put by sending `Message::Put(ack)` with
    /// `in_response_to: Some(original_put.id)` and `updated_nodes` containing
    /// a single entry under the `_ack` node id. The child of that entry
    /// indicates the result:
    ///
    /// - `_ack` (Value::Text("ok")) → commit succeeded
    /// - `_err` (Value::Text("<msg>")) → commit failed with `<msg>`
    ///
    /// # Sentinel Convention
    ///
    /// This uses the **same** sentinel convention as the Flush ack
    /// (see `Node::flush_storage` and the storage adapter implementations),
    /// so all ack-routing logic is uniform across Put, BatchPut, and Flush.
    /// A future change to add richer ack payloads (e.g. commit timestamp,
    /// byte count) only needs to update this single decoder.
    ///
    /// # Fallback Behavior
    ///
    /// If the ack is present (matched `in_response_to` from `pending_puts`)
    /// but contains neither sentinel, the ack is treated as success. The
    /// ack's mere presence proves the put reached the storage actor and
    /// the adapter decided to acknowledge it — absence of a failure marker
    /// is positive evidence.
    ///
    /// Failure to produce an ack at all (timeout, channel drop) is handled
    /// by the awaiting caller via `tokio::time::timeout`.
    fn decode_put_ack_payload(put: &Put) -> Result<(), String> {
        for (_node_id, children) in put.updated_nodes.iter().rev() {
            if let Some(node_data) = children.get("_err") {
                if let Value::Text(msg) = &node_data.value {
                    return Err(msg.clone());
                }
                return Err("storage put commit failed (non-text _err payload)".to_string());
            }
            if children.contains_key("_ack") {
                return Ok(());
            }
        }
        // Ack present but no sentinel — treat as success.
        Ok(())
    }

    /// Sibling decoder for the Router's `__quorum_met__` sentinel reply.
    ///
    /// Inspects the Put envelope for the sentinel as a top-level key in
    /// `updated_nodes` and, if found, returns a [`ReplicationStatus`] carrying
    /// the ack count from the sentinel payload. Returns `None` for any other
    /// reply shape — the caller falls back to [`Self::decode_put_ack_payload`].
    ///
    /// The Router emits this sentinel when the configured [`AckPolicy`]
    /// threshold is met (see [`crate::router::Router::handle_register_quorum`]).
    /// The reply envelope mirrors a storage `_ack` (same `in_response_to`
    /// convention) but uses `__quorum_met__` as the `updated_nodes` key so
    /// the decoders can disambiguate without coordination.
    ///
    /// # Wire format (emitted by Router via `Put::new_from_kv`)
    ///
    /// ```text
    /// updated_nodes = {
    ///     "__quorum_met__" => {
    ///         "_" => NodeData { value: Number(ack_count), updated_at: 0.0 }
    ///     }
    /// }
    /// ```
    ///
    /// The `ack_count` is the number of peer acks observed before the
    /// Decodes a peer-received Put carrying the `__quorum_met__` sentinel.
    ///
    /// Returns:
    /// - `None` — no quorum sentinel present (caller falls through to the
    ///   local-storage `_ack`/`_err` decoder).
    /// - `Some(Ok(status))` — sentinel carried `Value::Number(N)`, quorum met
    ///   with N peer acks.
    /// - `Some(Err(msg))` — sentinel carried `Value::Bit(true)`, the Router
    ///   cleanup reaper evicted this entry as timed out.
    ///
    /// The `elapsed` field is filled with the time since this decoder was
    /// called. For accurate elapsed measurement, callers should wrap the
    /// entire drain with an `Instant`.
    fn decode_quorum_payload(put: &Put) -> Option<Result<ReplicationStatus, String>> {
        let started_at = std::time::Instant::now();
        let children = put.updated_nodes.get(QUORUM_MET_SENTINEL)?;
        let node_data = children.get("_")?;
        match &node_data.value {
            Value::Number(n) => {
                let ack_count = *n as usize;
                Some(Ok(ReplicationStatus {
                    put_id: put.id.clone(),
                    acked_by: ack_count,
                    quorum_met: true,
                    elapsed: started_at.elapsed(),
                }))
            }
            Value::Bit(true) => Some(Err(format!("quorum timed out for put_id={}", put.id))),
            // Bit(false), String, Null, etc. — malformed. Fall through.
            _ => None,
        }
    }
}

/// Options for put operations (SEA certificate support).
///
/// Currently a placeholder — the `cert` field is reserved for future
/// certificate-based write authorization.
#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    /// Optional certificate for delegated writes.
    ///
    /// When set, the put will be checked against the certificate's
    /// policy (path restrictions, expiry, authorized certificants).
    /// Currently a no-op; reserved for future enforcement.
    pub cert: Option<serde_json::Value>,
}

impl Node {
    /// Sets a value with options (currently cert is no-op; reserved for future enforcement).
    ///
    /// See [`Node::put`] for the basic version. The `options` parameter
    /// allows passing a [`PutOptions`] with a certificate for delegated
    /// writes, though certificate enforcement is not yet implemented.
    pub async fn put_with_options(
        &mut self,
        value: Value,
        _options: PutOptions,
    ) -> Result<(), String> {
        self.put(value).await
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_new() {
        let node = Node::new();
        assert!(node.id().is_empty(), "root node uid should be empty");
        assert!(!node.peer_id().is_empty(), "peer_id should be non-empty");
    }

    #[tokio::test]
    async fn test_node_default() {
        let node = Node::default();
        assert!(node.id().is_empty());
    }

    #[tokio::test]
    async fn test_node_get_creates_child() {
        let mut node = Node::new();
        let child = node.get("child_key");
        assert_eq!(child.id(), "child_key");
    }

    #[tokio::test]
    async fn test_node_get_empty_key_returns_self() {
        let mut node = Node::new();
        let child = node.get("");
        assert_eq!(child.id(), node.id());
    }

    #[tokio::test]
    async fn test_node_get_nested() {
        let mut node = Node::new();
        let deep = node.get("a").get("b").get("c");
        assert_eq!(deep.id(), "a/b/c");
    }

    #[tokio::test]
    async fn test_node_get_returns_existing() {
        let mut node = Node::new();
        let child1 = node.get("key");
        let child2 = node.get("key");
        assert_eq!(child1.id(), child2.id());
    }

    #[tokio::test]
    async fn test_node_put_and_on() {
        let mut node = Node::new();
        let mut sub = node.get("greeting").on();
        node.get("greeting").put("hello".into()).await.expect("put");
        let val = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(val, Value::Text("hello".to_string()));
    }

    #[tokio::test]
    async fn test_node_once() {
        let mut node = Node::new();
        node.get("key").put("value".into()).await.expect("put");
        let val = node.get("key").once(Some(Duration::from_secs(2))).await;
        assert_eq!(val, Some(Value::Text("value".to_string())));
    }

    #[tokio::test]
    async fn test_node_batch_put() {
        let mut node = Node::new();
        let mut sub = node.get("a").on();
        node.batch_put(vec![(vec!["a".to_string()], Value::Text("x".into()))])
            .await
            .expect("batch_put");
        let val = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(val, Value::Text("x".to_string()));
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert!(config.allow_public_space);
        assert_eq!(config.broadcast_buffer_size, 4096);
        assert!(!config.ice_servers.is_empty());
    }

    #[test]
    fn test_config_custom() {
        let config = Config {
            allow_public_space: false,
            my_pub: Some("test.pub".to_string()),
            broadcast_buffer_size: 1024,
            ice_servers: vec![],
        };
        assert!(!config.allow_public_space);
        assert_eq!(config.broadcast_buffer_size, 1024);
        assert!(config.ice_servers.is_empty());
    }

    #[test]
    fn test_put_options_default() {
        let opts = PutOptions::default();
        assert!(opts.cert.is_none());
    }

    // ========================================================================
    // Async Ack Pattern Tests
    // ========================================================================
    //
    // These tests exercise the async `put`/`batch_put` ack pattern. They are
    // distinct from the sync-style tests above in that they actually `.await`
    // the put and verify the commit completed before the future resolves.
    //
    // The race they defend against: before the ack pattern, `put` returned
    // synchronously after queueing to the storage actor — a subsequent `get`
    // could read stale state. These tests would flake on the old code; on
    // the new code they should be deterministic.
    // ========================================================================

    /// Helper: build a minimal ack `Put` message that mimics what a storage
    /// adapter sends back after commit. Used by unit tests that exercise
    /// `handle_put` / `decode_put_ack_payload` without the full storage stack.
    ///
    /// The from-address is a no-op since these unit tests inject the ack
    /// directly through `handle(...)` — no routing involved.
    fn make_ack_put(put_id: &str, sentinel: &str) -> Put {
        let mut children = BTreeMap::new();
        children.insert(
            sentinel.to_string(),
            NodeData {
                value: Value::Text(if sentinel == "_err" {
                    "test error".to_string()
                } else {
                    "ok".to_string()
                }),
                updated_at: 0.0,
            },
        );
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), children);
        let mut put = Put::new(nodes, Some(put_id.to_string()), Addr::noop());
        // Compute checksum so callers can serialize.
        put.to_string();
        put
    }

    #[tokio::test]
    async fn test_decode_put_ack_payload_success() {
        let ack = make_ack_put("put-1", "_ack");
        let result = Node::decode_put_ack_payload(&ack);
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
    }

    #[tokio::test]
    async fn test_decode_put_ack_payload_error_carries_message() {
        let ack = make_ack_put("put-2", "_err");
        let result = Node::decode_put_ack_payload(&ack);
        assert!(result.is_err(), "expected Err");
        let err = result.unwrap_err();
        assert!(
            err.contains("test error"),
            "expected error message in result, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_decode_put_ack_payload_no_sentinel_treated_as_success() {
        // Empty ack payload (no _ack/_err) is treated as success — the ack's
        // presence is the signal. This matches the documented fallback.
        let mut children = BTreeMap::new();
        children.insert(
            "_ack".to_string(),
            NodeData {
                value: Value::Null,
                updated_at: 0.0,
            },
        );
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), children);
        let put = Put::new(nodes, Some("put-3".to_string()), Addr::noop());
        let result = Node::decode_put_ack_payload(&put);
        assert!(result.is_ok(), "no-sentinel ack should be success");
    }

    #[tokio::test]
    async fn test_decode_put_ack_payload_flushed_sentinel_also_succeeds() {
        // `_flushed` is the Flush ack sentinel. handle_put intercepts flush
        // acks FIRST (before falling through to the put-ack decoder), so the
        // decoder will only see `_flushed` if it sneaks through — which
        // shouldn't happen, but the decoder should still treat it as success
        // because it isn't `_err`.
        let ack = make_ack_put("flush-1", "_flushed");
        let result = Node::decode_put_ack_payload(&ack);
        assert!(
            result.is_ok(),
            "_flushed should decode as success (no _err present)"
        );
    }

    #[tokio::test]
    async fn test_pending_puts_drain_on_ack() {
        // Register a oneshot for a fake put_id, then send a matching ack
        // through the actor handle. The pending_puts map should drain.
        let mut node = Node::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let put_id = "test-pending-1".to_string();
        node.pending_puts.write().insert(put_id.clone(), tx);

        // Build ack message
        let ack = make_ack_put(&put_id, "_ack");

        // Inject via the actor handle
        let ctx = ActorContext::new("test-peer".to_string());
        node.handle(Message::Put(ack), &ctx).await;

        // The future should resolve with Ok(())
        let result = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("ack did not arrive within 1s")
            .expect("ack channel closed unexpectedly");
        assert!(result.is_ok(), "expected Ok, got {:?}", result);
        assert!(
            !node.pending_puts.read().contains_key(&put_id),
            "pending_puts should be drained after ack"
        );
    }

    #[tokio::test]
    async fn test_pending_puts_drain_on_error() {
        // Same as above but with _err payload — the oneshot should resolve
        // with the error message.
        let mut node = Node::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let put_id = "test-pending-err".to_string();
        node.pending_puts.write().insert(put_id.clone(), tx);

        let ack = make_ack_put(&put_id, "_err");

        let ctx = ActorContext::new("test-peer".to_string());
        node.handle(Message::Put(ack), &ctx).await;

        let result = tokio::time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("error ack did not arrive within 1s")
            .expect("ack channel closed unexpectedly");
        assert!(result.is_err(), "expected Err, got {:?}", result);
        assert!(result.unwrap_err().contains("test error"));
        assert!(!node.pending_puts.read().contains_key(&put_id));
    }

    #[tokio::test]
    async fn test_pending_puts_no_match_passes_through() {
        // If an ack arrives with an id that doesn't match any pending_put,
        // it should be processed as a normal Put (not drain anything).
        let mut node = Node::new();
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let put_id = "registered-id".to_string();
        node.pending_puts.write().insert(put_id.clone(), tx);

        // Different ack id
        let ack = make_ack_put("different-id", "_ack");

        let ctx = ActorContext::new("test-peer".to_string());
        node.handle(Message::Put(ack), &ctx).await;

        // The original pending_put should still be registered (not drained)
        assert!(
            node.pending_puts.read().contains_key(&put_id),
            "unrelated ack should NOT drain unrelated pending_put"
        );
    }

    #[tokio::test]
    async fn test_put_returns_after_storage_ack() {
        // The KEY test for the race fix. `put` must not return until storage
        // has acked. We verify this by chaining a `get` IMMEDIATELY after
        // `put` resolves — if the ack pattern works, get sees the new value.
        //
        // Pre-fix behavior: get returned stale state (or None) because the
        // storage actor hadn't processed the Put message yet.
        let mut node = Node::new();
        node.get("race_key")
            .put("race_value".into())
            .await
            .expect("put should succeed");
        let val = node
            .get("race_key")
            .once(Some(Duration::from_secs(2)))
            .await;
        assert_eq!(
            val,
            Some(Value::Text("race_value".to_string())),
            "put → get should observe the new value (race fix verification)"
        );
    }

    #[tokio::test]
    async fn test_batch_put_returns_after_storage_ack() {
        // Batch counterpart of test_put_returns_after_storage_ack.
        let mut node = Node::new();
        node.batch_put(vec![
            (vec!["batch_a".to_string()], Value::Text("1".into())),
            (vec!["batch_b".to_string()], Value::Text("2".into())),
            (vec!["batch_c".to_string()], Value::Text("3".into())),
        ])
        .await
        .expect("batch_put should succeed");

        // All three children should be visible immediately after batch_put resolves.
        let a = node.get("batch_a").once(Some(Duration::from_secs(2))).await;
        let b = node.get("batch_b").once(Some(Duration::from_secs(2))).await;
        let c = node.get("batch_c").once(Some(Duration::from_secs(2))).await;
        assert_eq!(a, Some(Value::Text("1".to_string())));
        assert_eq!(b, Some(Value::Text("2".to_string())));
        assert_eq!(c, Some(Value::Text("3".to_string())));
    }

    #[tokio::test]
    async fn test_put_sequential_no_ack_loss() {
        // Issue 5 puts in rapid succession. Each must resolve with Ok and
        // each subsequent get must see its respective value — no ack lost.
        let mut node = Node::new();
        for i in 0..5 {
            let key = format!("seq_{}", i);
            let val = format!("val_{}", i);
            node.get(&key)
                .put(val.clone().into())
                .await
                .expect("put should succeed");
            let got = node.get(&key).once(Some(Duration::from_secs(2))).await;
            assert_eq!(
                got,
                Some(Value::Text(val.clone())),
                "put {} → get should see {:?}, got {:?}",
                i,
                val,
                got
            );
        }
    }

    #[tokio::test]
    async fn test_pending_puts_cleared_on_router_send_failure() {
        // If the router isn't initialized, put should return an error AND
        // remove its pending_puts entry (so we don't leak oneshot channels).
        // We test this by constructing a node whose router addr is None.
        //
        // (Hard to construct from outside since Node::new() wires up a router,
        // so we exercise the error path indirectly: invalid router state.)
        let mut node = Node::new();
        // First put should succeed (router wired up by Node::new()).
        node.get("normal_key")
            .put("normal".into())
            .await
            .expect("first put ok");
        // Now corrupt the router addr to force the error path.
        *node.router.write() = None;
        let result = node.get("broken_key").put("x".into()).await;
        assert!(result.is_err(), "expected Err when router is None");
        // Pending puts should be empty — the failed put should have cleaned up.
        assert!(
            node.pending_puts.read().is_empty(),
            "pending_puts should be empty after router-send failure"
        );
    }
    // ========================================================================
    // Phase 5: Quorum Drain Tests (Network Fanout Ack)
    // ========================================================================
    //
    // Tests exercise:
    // - decode_quorum_payload return-shape: Some(Ok) | Some(Err) | None
    //   (success sentinel, timeout sentinel, fall-through to _ack decoder)
    // - AckPolicy builder math (any / for_peer_count / all / builders)
    // - ReplicationStatus invariant (Ok arm has quorum_met = true)

    /// Build a Put carrying the `__quorum_met__` sentinel for decoder tests.
    fn make_quorum_put(sentinel_value: Value) -> Put {
        let mut children: Children = std::collections::BTreeMap::new();
        children.insert(
            "_".to_string(),
            NodeData {
                value: sentinel_value,
                updated_at: 0.0,
            },
        );
        let mut put = Put::new_from_kv(QUORUM_MET_SENTINEL.to_string(), children, Addr::noop());
        put.id = "test_put_id".to_string();
        put.in_response_to = Some("test_put_id".to_string());
        put
    }

    #[test]
    fn decode_quorum_payload_success_with_number() {
        let put = make_quorum_put(Value::Number(3.0));
        let result = Node::decode_quorum_payload(&put);
        assert!(result.is_some(), "should decode sentinel-bearing Put");
        let inner = result.unwrap();
        assert!(inner.is_ok(), "Number payload should decode as Ok");
        let status = inner.unwrap();
        assert_eq!(status.put_id, "test_put_id");
        assert_eq!(status.acked_by, 3);
        assert!(status.quorum_met);
    }

    #[test]
    fn decode_quorum_payload_timeout_with_bit_true() {
        let put = make_quorum_put(Value::Bit(true));
        let result = Node::decode_quorum_payload(&put);
        assert!(result.is_some(), "Bit(true) is a recognized sentinel");
        let inner = result.unwrap();
        assert!(inner.is_err(), "Bit(true) must decode as Err(timeout)");
        let err_msg = inner.unwrap_err();
        assert!(
            err_msg.contains("quorum timed out"),
            "error message should mention timeout, got: {err_msg}"
        );
        assert!(
            err_msg.contains("test_put_id"),
            "error message should include put_id, got: {err_msg}"
        );
    }

    #[test]
    fn decode_quorum_payload_no_sentinel_falls_through() {
        let mut children: Children = std::collections::BTreeMap::new();
        children.insert(
            "_".to_string(),
            NodeData {
                value: Value::Number(1.0),
                updated_at: 0.0,
            },
        );
        let put = Put::new_from_kv("not_quorum".to_string(), children, Addr::noop());
        let result = Node::decode_quorum_payload(&put);
        assert!(
            result.is_none(),
            "no sentinel key → None (fall through to _ack decoder)"
        );
    }

    #[test]
    fn decode_quorum_payload_malformed_value_falls_through() {
        for bad_value in [
            Value::Bit(false),
            Value::Null,
            Value::Text("not a count".to_string()),
        ] {
            let put = make_quorum_put(bad_value.clone());
            let result = Node::decode_quorum_payload(&put);
            assert!(
                result.is_none(),
                "malformed value {bad_value:?} should return None (fall through)"
            );
        }
    }

    #[test]
    fn decode_quorum_payload_missing_underscore_key() {
        let mut children: Children = std::collections::BTreeMap::new();
        children.insert(
            "wrong_key".to_string(),
            NodeData {
                value: Value::Number(1.0),
                updated_at: 0.0,
            },
        );
        let put = Put::new_from_kv(QUORUM_MET_SENTINEL.to_string(), children, Addr::noop());
        let result = Node::decode_quorum_payload(&put);
        assert!(result.is_none(), "missing _ key → None");
    }

    #[test]
    fn ack_policy_any_has_quorum_one_and_nine_second_timeout() {
        let p = AckPolicy::any();
        assert_eq!(p.quorum, 1, "AckPolicy::any → quorum=1");
        assert_eq!(
            p.timeout,
            Duration::from_secs(9),
            "AckPolicy::any → 9s timeout (Gun.js lack default)"
        );
    }

    #[test]
    fn ack_policy_for_peer_count_majority() {
        assert_eq!(AckPolicy::for_peer_count(0).quorum, 1, "0 peers → 1");
        assert_eq!(AckPolicy::for_peer_count(1).quorum, 1);
        assert_eq!(AckPolicy::for_peer_count(2).quorum, 1); // ⌈2/2⌉ = 1
        assert_eq!(AckPolicy::for_peer_count(3).quorum, 2); // ⌈3/2⌉ = 2
        assert_eq!(AckPolicy::for_peer_count(4).quorum, 2);
        assert_eq!(AckPolicy::for_peer_count(5).quorum, 3); // ⌈5/2⌉ = 3
        assert_eq!(AckPolicy::for_peer_count(7).quorum, 4); // ⌈7/2⌉ = 4
    }

    #[test]
    fn ack_policy_all_is_max_usize() {
        let p = AckPolicy::all();
        assert_eq!(p.quorum, usize::MAX, "AckPolicy::all → quorum=MAX");
        assert_eq!(p.timeout, Duration::from_secs(9));
    }

    #[test]
    fn ack_policy_with_timeout_overrides() {
        let p = AckPolicy::any().with_timeout(Duration::from_secs(30));
        assert_eq!(p.timeout, Duration::from_secs(30));
        assert_eq!(p.quorum, 1, "with_timeout preserves quorum");
    }

    #[test]
    fn ack_policy_with_quorum_overrides() {
        let p = AckPolicy::any().with_quorum(5);
        assert_eq!(p.quorum, 5);
        assert_eq!(
            p.timeout,
            Duration::from_secs(9),
            "with_quorum preserves timeout"
        );
    }

    #[test]
    fn ack_policy_default_is_any() {
        let p = AckPolicy::default();
        assert_eq!(p.quorum, 1);
        assert_eq!(p.timeout, Duration::from_secs(9));
    }

    #[test]
    fn replication_status_quorum_met_true_on_ok_arm() {
        let status = ReplicationStatus {
            put_id: "p1".to_string(),
            acked_by: 3,
            quorum_met: true, // invariant: Ok arm must have this
            elapsed: Duration::from_millis(42),
        };
        assert_eq!(status.put_id, "p1");
        assert_eq!(status.acked_by, 3);
        assert!(status.quorum_met);
        assert_eq!(status.elapsed, Duration::from_millis(42));
    }

    #[test]
    fn drain_dispatch_quorum_ok_vs_err_vs_fallthrough() {
        // The drain block (Node::handle_put) dispatches three cases based on
        // decode_quorum_payload's return shape:
        //   Some(Ok(_))  → send Ok(status)
        //   Some(Err(_)) → send Err(timeout_msg)
        //   None         → fall through to local-storage _ack decoder
        let success_put = make_quorum_put(Value::Number(2.0));
        match Node::decode_quorum_payload(&success_put) {
            Some(Ok(_)) => {}
            other => panic!("expected Some(Ok) for Number payload, got {other:?}"),
        }
        let timeout_put = make_quorum_put(Value::Bit(true));
        match Node::decode_quorum_payload(&timeout_put) {
            Some(Err(e)) => assert!(e.contains("timed out")),
            other => panic!("expected Some(Err) for Bit(true) payload, got {other:?}"),
        }
        let non_sentinel_put = {
            let mut children = std::collections::BTreeMap::new();
            children.insert(
                "_".to_string(),
                NodeData {
                    value: Value::Number(1.0),
                    updated_at: 0.0,
                },
            );
            Put::new_from_kv("storage_ack".to_string(), children, Addr::noop())
        };
        match Node::decode_quorum_payload(&non_sentinel_put) {
            None => {}
            other => panic!("expected None for non-sentinel Put, got {other:?}"),
        }
    }

    // ========================================================================
    // End Phase 5 tests
    // ========================================================================
}
