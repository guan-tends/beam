//! Graph node — the core API for reading and writing data in the Rod graph.
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
//! use rod::{Node, Value};
//!
//! let mut db = Node::new();
//! let mut sub = db.get("greeting").on();
//! db.get("greeting").put("Hello World!".into());
//! // sub.recv().await == Some(Value::Text("Hello World!"))
//! ```

use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::MemoryStorage;
use crate::message::{BatchPut, Flush, Get, Message, Put};
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
use tokio::sync::{broadcast, oneshot};
use tokio_tungstenite::connect_async;
use url::Url;

/// Configuration for a [`Node`] and its associated adapters.
///
/// Controls public space access, stats reporting, broadcast channel sizing,
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
    /// Whether to expose node stats at the `/stats` endpoint.
    ///
    /// Currently a no-op placeholder — stats collection is not implemented.
    pub stats: bool,
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
            stats: true,
            my_pub: None,
            broadcast_buffer_size: 4096,
            ice_servers: vec!["stun:stun.l.google.com:19302".to_string()],
        }
    }
}

/// A graph node — the primary API for reading and writing data in Rod.
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
    addr: Arc<RwLock<Option<Addr>>>,
    router: Arc<RwLock<Option<Addr>>>,
    pending_flushes: Arc<RwLock<HashMap<String, oneshot::Sender<()>>>>,
    allow_public_space: bool,
    ice_servers: Vec<String>,
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
    /// This is the simplest way to get started with Rod. The node will use
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
        let actor_context = ActorContext::new(random_string(16));
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
            allow_public_space: config.allow_public_space,
            ice_servers: config.ice_servers.clone(),
            actor_context: Box::new(actor_context),
        };

        node.actor_context.node = Some(node.clone());
        let addr = node.actor_context.start_actor(Box::new(node.clone()));
        *node.addr.write() = Some(addr);

        let router = Box::new(Router::new(config, storage_adapters, network_adapters));
        let router_addr = node.actor_context.start_router(router);
        node.actor_context.router = router_addr.clone();
        *node.router.write() = Some(router_addr);

        node
    }

    /// Handles incoming [`Put`] messages by dispatching values to subscribers.
    ///
    /// If the put is a flush acknowledgement (has `in_response_to` matching a
    /// pending flush), the flush's oneshot sender is triggered instead.
    ///
    /// For replay puts (have `in_response_to`), a `__rod_replay_complete__`
    /// marker is sent on the map channel after all child values are dispatched.
    fn handle_put(&mut self, put: Put) {
        // Intercept flush acks before processing as normal data
        if let Some(response_id) = &put.in_response_to {
            if let Some(sender) = self.pending_flushes.write().remove(response_id) {
                let _ = sender.send(());
                return;
            }
        }
        let is_replay = put.in_response_to.is_some();
        for (node_id, node_data) in put.updated_nodes {
            if node_id == *self.uid.read() {
                for (child, child_data) in node_data {
                    // Skip internal control keys
                    if child.starts_with("__rod_") {
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
                        .send(("__rod_replay_complete__".to_string(), Value::Null));
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
            addr: Arc::new(RwLock::new(None)),
            actor_context: self.actor_context.clone(),
            allow_public_space: self.allow_public_space,
            ice_servers: self.ice_servers.clone(),
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
                match connect_async(Url::parse(&url).expect("valid URL")).await {
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

    /// Sets a value on this node and propagates it through the graph.
    ///
    /// The value is immediately sent to local `on()` subscribers, then a
    /// [`Put`] message is sent to the router for storage and network relay.
    /// The timestamp is the current Unix epoch in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to set (see [`Value`] for supported types)
    pub fn put(&mut self, value: Value) {
        let updated_at: f64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;
        self.on_sender.send(value.clone()).ok();
        debug!("put {}", value.to_string());
        let mut updated_nodes = BTreeMap::new();
        self.add_parent_nodes(&mut updated_nodes, value, updated_at);
        let my_addr = self.addr.read().clone().unwrap();
        let put = Put::new(updated_nodes, None, my_addr);
        if let Some(router) = &*self.router.read() {
            let _ = router.send(Message::Put(put));
        }
    }

    /// Writes multiple values in a single storage transaction.
    ///
    /// Each operation is a `(path, value)` pair where `path` is a vector of
    /// keys from the caller's [`Node`] down to the leaf. The caller should
    /// invoke this on the **root** [`Node`].
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of `(path, value)` pairs
    pub fn batch_put(&mut self, ops: Vec<(Vec<String>, Value)>) {
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
        if let Some(router) = &*self.router.read() {
            let _ = router.send(Message::BatchPut(batch));
        }
    }

    /// Stops the node and all its child actors and adapters.
    ///
    /// This calls [`ActorContext::stop`] on the node's actor context, which
    /// aborts all child tasks and sends stop signals to all child actors.
    pub fn stop(&mut self) {
        info!("Node stopping");
        self.actor_context.stop();
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
    pub fn put_with_options(&mut self, value: Value, _options: PutOptions) {
        self.put(value);
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
        node.get("greeting").put("hello".into());
        let val = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("timeout")
            .expect("recv error");
        assert_eq!(val, Value::Text("hello".to_string()));
    }

    #[tokio::test]
    async fn test_node_once() {
        let mut node = Node::new();
        node.get("key").put("value".into());
        let val = node.get("key").once(Some(Duration::from_secs(2))).await;
        assert_eq!(val, Some(Value::Text("value".to_string())));
    }

    #[tokio::test]
    async fn test_node_batch_put() {
        let mut node = Node::new();
        let mut sub = node.get("a").on();
        node.batch_put(vec![(vec!["a".to_string()], Value::Text("x".into()))]);
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
        assert!(config.stats);
        assert_eq!(config.broadcast_buffer_size, 4096);
        assert!(!config.ice_servers.is_empty());
    }

    #[test]
    fn test_config_custom() {
        let config = Config {
            allow_public_space: false,
            stats: false,
            my_pub: Some("test.pub".to_string()),
            broadcast_buffer_size: 1024,
            ice_servers: vec![],
        };
        assert!(!config.allow_public_space);
        assert!(!config.stats);
        assert_eq!(config.broadcast_buffer_size, 1024);
        assert!(config.ice_servers.is_empty());
    }

    #[test]
    fn test_put_options_default() {
        let opts = PutOptions::default();
        assert!(opts.cert.is_none());
    }
}
