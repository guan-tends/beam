use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Flush, Get, Message, Put};
use crate::router::Router;
use crate::types::{Children, NodeData, Value};
use crate::utils::random_string;
use crate::adapters::MemoryStorage;
use async_trait::async_trait;
use log::{debug, info, warn};
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use url::Url;
use std::collections::BTreeMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{SystemTime, Duration}; // TODO get time from ActorContext
use tokio::sync::{broadcast, oneshot};
use std::collections::HashMap; // TODO replace with generics: Sender and Receiver traits?

// TODO proper automatic tests
// Node { node: Arc<RwLock<NodeInner>> } instead of Arc<RwLock> for each member? compare performance
// TODO connections don't seem to be closed / timeouted properly when client has disconnected
// TODO should use async RwLock everywhere?

// TODO: separate configs for each adapter?
/// [Node] configuration object.
#[derive(Clone)]
pub struct Config {
    pub allow_public_space: bool,
    /// Prioritize data storage for this public key. Format: x.y where x and y are base64 encoded ECDSA public key coordinates.
    /// Example: hyECQHwSo7fgr2MVfPyakvayPeixxsaAWVtZ-vbaiSc.TXIp8MnCtrnW6n2MrYquWPcc-DTmZzMBmc2yaGv9gIU
    pub my_pub: Option<String>,
    /// Show node stats at /stats?
    pub stats: bool,
    /// Buffer size for broadcast channels used by `on()` and `map()`.
    /// Defaults to 4096. Increase for high-throughput scenarios; decrease
    /// to save memory per active subscription.
    pub broadcast_buffer_size: usize,
    /// STUN/TURN servers for WebRTC ICE negotiation.
    /// Defaults to Google's public STUN server.
    /// Only used when the `webrtc` feature is enabled.
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

/// A Graph Node that provides an API for graph traversal.
/// Sends, processes and relays Put & Get messages between storage and transport adapters.
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
        match msg {
            Message::Put(put) => self.handle_put(put),
            _ => {}
        }
    }
}

impl Node {
    /// Create a new root-level Node using default configuration. No network or storage adapters are started.
    pub fn new() -> Self {
        // Use MemoryStorage by default
        let storage = MemoryStorage::new();
        Self::new_with_config(Config::default(), vec![Box::new(storage)], Vec::new())
    }

    pub fn id(&self) -> String {
        self.uid.read().clone()
    }

    pub fn peer_id(&self) -> String {
        self.actor_context.peer_id.read().clone()
    }

    /// Create a new root-level Node using custom configuration. Starts the default or configured network and storage adapters.
    ///
    /// # Examples
    ///
    /// ```
    /// tokio_test::block_on(async {
    ///
    ///     use rod::{Node, Config, Value};
    ///     use rod::adapters::{MemoryStorage, OutgoingWebsocketManager};
    ///
    ///     let config = Config::default();
    ///     let memory_storage = Box::new(MemoryStorage::new());
    ///     let ws_client = Box::new(OutgoingWebsocketManager::new(config.clone(), vec!["wss://some-rod-server.com/ws".to_string()]));
    ///     let mut db = Node::new_with_config(config.clone(), vec![memory_storage], vec![ws_client]);
    ///     let mut sub = db.get("greeting").on();
    ///     db.get("greeting").put("Hello World!".into());
    ///     if let Value::Text(str) = sub.recv().await.unwrap() {
    ///         assert_eq!(&str, "Hello World!");
    ///     }
    ///
    /// })
    /// ```
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

        let router = Box::new(Router::new(config, storage_adapters, network_adapters)); // actually, we should communicate with
                                                                                        // MemoryStorage (or sled), which has a special role in maintaining our version of the current state?
                                                                                        // MemoryStorage can then communicate with router as needed.
        let router_addr = node.actor_context.start_router(router);
        node.actor_context.router = router_addr.clone();
        *node.router.write() = Some(router_addr);

        node
    }

    fn handle_put(&mut self, put: Put) {
        // Intercept flush acks before processing as normal data
        if let Some(response_id) = &put.in_response_to {
            if let Some(sender) = self.pending_flushes.write().remove(response_id) {
                let _ = sender.send(());
                return;
            }
        }
        // TODO accept puts only from our memory/sled adapter, which is supposed to serve the latest version.
        // Or store latest NodeData in Node? Would eat up memory though.
        let is_replay = put.in_response_to.is_some();
        for (node_id, node_data) in put.updated_nodes {
            if node_id == *self.uid.read() {
                for (child, child_data) in node_data {
                    if child.starts_with("__rod_") { continue; }
                    if let Some(child) = self.children.read().get(&child) {
                        let _ = child.on_sender.send(child_data.value.clone());
                    }
                    let _ = self
                        .map_sender
                        .send((child.to_string(), child_data.value.clone()));
                }
                if is_replay {
                    let _ = self.map_sender.send(("__rod_replay_complete__".to_string(), Value::Null));
                }
            }
        }
    }

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
        assert!(key.len() > 0, "Key length must be greater than zero");
        let mut path = self.path.clone();
        path.push(key.clone());
        let new_child_uid = path.join("/");
        debug!("new_child_uid {}", new_child_uid);
        let node = Self {
            path,
            children: Arc::new(RwLock::new(BTreeMap::new())),
            parent: Arc::new(RwLock::new(Some((
                self.uid.read().clone(),
                self.clone(),
            )))),
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

    /// Subscribe to the Node's value.
    pub fn on(&mut self) -> broadcast::Receiver<Value> {
        let key;
        if self.path.len() > 1 {
            key = self.path.iter().nth(self.path.len() - 1).cloned();
        } else {
            key = None;
        }
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
        // subscribe before send
        let subscriber = self.on_sender.subscribe();
        if let Some(router) = self.router.read().clone() {
            let _ = router.send(Message::Get(get));
        }
        subscriber
    }
    /// Get back the value only once, or None when not found.
    pub async fn once(&mut self, wait: Option<Duration>) -> Option<Value> {
        let val = tokio::time::timeout(
            wait.unwrap_or(Duration::from_millis(66)),
            self.on().recv(),
        ).await.ok()?.expect("recv error??");
        Some(val)
    }

    /// Connect to a remote peer at `url` via WebSocket with automatic reconnection.
    /// Retries with exponential backoff (starting at 1s, max 60s).
    /// The spawned WsConn actor auto-handshakes via Message::Hi on pre_start,
    /// and Router::handle_adds_peer handles the Hi to register the peer.
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
                        let conn = crate::adapters::WsConn::new(
                            sender,
                            receiver,
                            allow_public_space,
                        );
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

    /// Connect to a remote peer via WebRTC data channel.
    /// Signaling bootstraps over the existing WebSocket mesh via `Message::RtcSignal`.
    /// Once the data channel opens, the peer is registered in `Router::known_peers`
    /// just like a WebSocket peer, and Gun protocol messages flow over the P2P link.
    ///
    /// Requires the `webrtc` feature. Without it, this method is a no-op.
    #[cfg(feature = "webrtc")]
    pub fn connect_webrtc_peer(&self, peer_id: &str, target_peer_id: &str, role: crate::adapters::WebRtcRole) {
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


    // TODO: optionally specify which adapters to ask
    /// Return a child Node corresponding to the given key.
    pub fn get(&mut self, key: &str) -> Node {
        if key == "" {
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

    /// Subscribe to all children of this Node.
    pub fn map(&self) -> broadcast::Receiver<(String, Value)> {
        let node_id = self.uid.read().to_string();
        let addr = self.addr.read().clone().unwrap();
        let get = Get::new(node_id, None, addr);
        // subscribe before send
        let subscriber = self.map_sender.subscribe();
        if let Some(router) = self.router.read().clone() {
            let _ = router.send(Message::Get(get));
        }
        subscriber
    }

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

    /// Set a Value for the Node.
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

    /// Write multiple values in a single storage transaction.
    ///
    /// Each operation is a `(path, value)` pair where `path` is a vector of
    /// keys from the caller's Node down to the leaf. The caller should
    /// invoke this on the **root** Node.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rod::Node;
    /// let mut root = Node::new();
    /// root.batch_put(vec![
    ///     (vec!["users", "alice"], "hi".into()),
    ///     (vec!["users", "bob"], "hey".into()),
    /// ]);
    /// ```
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

    pub fn stop(&mut self) {
        info!("Node stopping");
        self.actor_context.stop();
    }
}

/// Options for put operations (SEA cert support)
#[derive(Clone, Debug, Default)]
pub struct PutOptions {
    /// Optional certificate for delegated writes
    pub cert: Option<serde_json::Value>,
}

impl Node {
    /// Set a Value with options (currently cert is no-op; reserved for future enforcement)
    pub fn put_with_options(&mut self, value: Value, _options: PutOptions) {
        self.put(value);
    }
}
