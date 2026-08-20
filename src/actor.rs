#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

//! Actor framework — a lightweight actor model built on [`Mailbox`].
//!
//! This module provides a minimal actor system where actors communicate via
//! [`Arc<Message>`] over pre-allocated ring-buffer mailboxes. The mailbox
//! replaces tokio's `mpsc` channels, eliminating per-message allocation and
//! reducing scheduler overhead through batch draining.
//!
//! # Architecture
//!
//! - [`Actor`] trait — defines the message handling interface
//! - [`ActorContext`] — per-actor context with peer ID, router address, and
//!   child actor management
//! - [`Addr`] — a clonable, hashable address for sending messages to an actor
//!
//! # Mailbox
//!
//! All actors use [`crate::mailbox::mailbox`] — a bounded `VecDeque` protected
//! by `parking_lot::Mutex` with `tokio::sync::Notify` for wakeups. Messages
//! are wrapped in [`Arc<Message>`] so fanout is a refcount bump (~2 ns),
//! not a deep clone.
//!
//! - **Default capacity** (65536) — [`ActorContext::start_actor`] creates an
//!   actor with a generous mailbox. Backpressure only under extreme load.
//! - **Bounded** — [`ActorContext::start_actor_bounded`] creates an actor with
//!   a smaller capacity. When full, [`Addr::send`] returns `Err(())`,
//!   applying backpressure. Used for storage write actors.
//!
//! # Message Flow
//!
//! ```text
//! Sender → Addr.send(msg) → Mailbox (bounded VecDeque<Arc<Message>>)
//!                                ↓
//!                      Actor.handle(Arc<Message>, ctx)
//!                                ↓
//!                          Actor can:
//!                          - spawn child actors
//!                          - send to router
//!                          - spawn child tasks
//! ```
//!
//! # `Arc<Message>` Semantics
//!
//! [`Addr::send`] accepts `impl Into<Arc<Message>>`, so existing call sites
//! that pass `Message::Put(put)` still compile — the message is wrapped in
//! `Arc` inside `send`. For fanout paths (e.g. relay), callers can pass
//! `Arc::clone(&msg)` to skip the allocation entirely — all subscribers
//! share the same `Arc`.
//!
//! # Shutdown
//!
//! Actors are stopped via a stop signal channel. When the context's `stop()`
//! method is called, all child tasks are aborted and stop signals are sent
//! to all child actors.

use crate::Node;
use crate::mailbox::{self, MailboxReceiver, MailboxSender};
use crate::message::Message;
use crate::metrics::Metrics;
use crate::tokio_spawn::JoinHandle;
use crate::utils::FxHashMap;
use crate::utils::random_string;
use async_trait::async_trait;
use futures_util::Future;
use parking_lot::RwLock;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::Send;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::watch;

/// Default mailbox capacity for unbounded actors.
const DEFAULT_MAILBOX_CAPACITY: usize = 65536;

/// The core actor trait.
///
/// Implementors define how to handle [`Message`] values and optionally
/// configure lifecycle hooks (`pre_start`, `stopping`).
///
/// # Lifecycle
///
/// 1. `pre_start` — called once before the actor begins processing messages
/// 2. `handle` — called for each message received
/// 3. `stopping` — called once after the actor's message loop exits
///
/// # Example
///
/// ```no_run
/// use beam::actor::{Actor, ActorContext};
/// use beam::message::Message;
/// use async_trait::async_trait;
/// use std::sync::Arc;
///
/// struct EchoActor;
///
/// #[async_trait]
/// impl Actor for EchoActor {
///     async fn handle(&mut self, msg: Arc<Message>, _ctx: &ActorContext) {
///         // Process message — &*msg gives &Message
///     }
/// }
/// ```
#[async_trait]
pub trait Actor: Send + Sync + 'static {
    /// Handle an incoming message.
    ///
    /// Messages are wrapped in [`Arc<Message>`] so that fanout paths
    /// (e.g. relay) can share a single allocation across all subscribers.
    /// Use `&*msg` or `msg.as_ref()` to access the inner [`Message`].
    async fn handle(&mut self, message: Arc<Message>, context: &ActorContext);

    /// Handle a batch of messages drained from the mailbox.
    ///
    /// Override to process multiple messages in a single call, enabling
    /// batch optimizations like coalescing WebSocket writes. The default
    /// implementation calls [`handle`](Actor::handle) for each message.
    ///
    /// Implementors that override this **must** drain all messages from
    /// `batch` (e.g. via `batch.drain(..)` or `batch.clear()`).
    async fn handle_batch(&mut self, batch: &mut Vec<Arc<Message>>, context: &ActorContext) {
        for msg in batch.drain(..) {
            self.handle(msg, context).await;
        }
    }

    /// Called once before the actor starts processing messages.
    ///
    /// Override to initialize state, spawn child actors, or establish
    /// connections. Defaults to a no-op.
    async fn pre_start(&mut self, _context: &ActorContext) {}

    /// Called once after the actor's message loop exits.
    ///
    /// Override for cleanup logic. Defaults to a no-op.
    async fn stopping(&mut self, _context: &ActorContext) {}

    /// Whether this actor wants to receive all messages (not just addressed
    /// to it). Used by the Multicast adapter.
    ///
    /// Defaults to `false`.
    fn subscribe_to_everything(&self) -> bool {
        false
    }

    /// Whether this actor is a relay server (WsServer) that accepts
    /// incoming WebSocket connections and fans out to individual WsConn
    /// clients.
    ///
    /// The Router uses this to distinguish WsServer (which handles
    /// per-connection echo-back via `msg.is_from(conn)`) from
    /// OutgoingWebsocketManager (which sends to a single remote relay
    /// and must be skipped on echo-back).
    ///
    /// Defaults to `false`.
    fn is_relay_server(&self) -> bool {
        false
    }

    /// Attempts to produce a clone of this actor for storage read/write
    /// splitting.
    ///
    /// Storage adapters override this to return a boxed clone, enabling the
    /// [`crate::router::Router`] to start separate read and write actors that
    /// share the same underlying database. Non-storage actors return `None`
    /// (the default).
    ///
    /// When the Router receives `Some`, it starts two actors: one registered
    /// in `read_adapters` (receives only `Get`), one in `write_adapters`
    /// (receives `Put`, `BatchPut`, `Flush`). Both share the same underlying
    /// data store via `Arc`, so reads see committed writes immediately.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        None
    }
}

impl dyn Actor {
    /// Internal run loop — receives messages until the stop signal fires
    /// or the mailbox is closed.
    ///
    /// Uses [`MailboxReceiver::recv_batch`] to drain up to 64 messages per
    /// wakeup, amortizing scheduler overhead across batches. This is the
    /// key performance difference from tokio's `mpsc::recv` which wakes
    /// per message.
    async fn run(
        &mut self,
        mut receiver: MailboxReceiver,
        mut stop_receiver: Receiver<()>,
        context: ActorContext,
    ) {
        self.pre_start(&context).await;
        let mut batch: Vec<Arc<Message>> = Vec::with_capacity(64);
        loop {
            tokio::select! {
                _v = stop_receiver.recv() => {
                    context.stop();
                    break;
                },
                count = receiver.recv_batch(&mut batch, 64) => {
                    if count == 0 {
                        // Mailbox closed.
                        break;
                    }
                    self.handle_batch(&mut batch, &context).await;
                }
            }
        }
        self.stopping(&context).await;
    }
}

/// Per-actor context providing access to runtime services.
///
/// Each actor receives an `ActorContext` in `pre_start`, `handle`, and
/// `stopping`. The context is clonable — clones share the same underlying
/// state via `Arc`.
///
/// # Key Fields
///
/// - `peer_id` — this node's peer identifier (shared across all actors)
/// - `router` — the router actor's address (for forwarding messages)
/// - `addr` — this actor's own address
/// - `node` — optional owned [`Node`] (set for the root actor)
#[derive(Clone)]
pub struct ActorContext {
    /// This node's peer ID, shared across all actors.
    pub peer_id: Arc<RwLock<String>>,
    /// The router actor's address for message forwarding.
    /// `Arc<RwLock<…>>` so it can be set after construction (the root node's
    /// router is created after the node itself).
    pub router: Arc<RwLock<Addr>>,
    /// Stop signals for child actors (keyed by child Addr).
    stop_signals: Arc<RwLock<FxHashMap<Addr, Sender<()>>>>,
    /// Join handles for spawned child tasks.
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// This actor's own address.
    pub addr: Addr,
    /// Whether this actor has been stopped.
    pub is_stopped: Arc<RwLock<bool>>,
    /// Shutdown signal receiver — set to `true` when graceful shutdown begins.
    /// Long-running child tasks select on this to break their loops.
    pub shutdown_rx: watch::Receiver<bool>,
    /// Optional owned Node (set for the root actor).
    pub node: Arc<RwLock<Option<Node>>>,
    /// Shared metrics handle — lock-free counters for the relay hot path.
    ///
    /// Cloned from the root Node's `Metrics` so all actors in the tree
    /// observe the same atomic counters. Set in `Node::new_with_config`
    /// and propagated through `child_context`.
    pub metrics: Arc<Metrics>,
}

impl ActorContext {
    /// Creates a new `ActorContext` with the given peer ID.
    ///
    /// The `addr` and `router` fields are initialized to [`Addr::noop()`]
    /// and should be set before use.
    pub fn new(peer_id: String) -> Self {
        Self {
            addr: Addr::noop(),
            stop_signals: Arc::new(RwLock::new(FxHashMap::default())),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            peer_id: Arc::new(RwLock::new(peer_id)),
            router: Arc::new(RwLock::new(Addr::noop())),
            is_stopped: Arc::new(RwLock::new(false)),
            shutdown_rx: watch::channel(false).1,
            node: Arc::new(RwLock::new(None)),
            metrics: Arc::new(Metrics::new()),
        }
    }

    /// Returns the number of child actors spawned by this context.
    pub fn child_actor_count(&self) -> usize {
        self.stop_signals.read().len()
    }

    /// Creates a child context with the given address and stop signal.
    fn child_context(&self, addr: Addr, stop_signal: Sender<()>) -> Self {
        let mut stop_signals = FxHashMap::default();
        stop_signals.insert(addr.clone(), stop_signal);
        Self {
            addr,
            stop_signals: Arc::new(RwLock::new(stop_signals)),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            peer_id: self.peer_id.clone(),
            router: self.router.clone(),
            is_stopped: self.is_stopped.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            node: self.node.clone(),
            metrics: self.metrics.clone(),
        }
    }

    /// Spawns a child actor with an unbounded channel and returns its address.
    ///
    /// The actor runs in a tokio task. Its lifecycle is managed by this
    /// context — calling `stop()` will send a stop signal and abort the task.
    pub fn start_actor(&self, actor: Box<dyn Actor>) -> Addr {
        self.start_actor_or_router(actor, false, None)
    }

    /// Spawns a child actor with a bounded channel and returns its address.
    ///
    /// The `bound` parameter sets the channel capacity. When full, `send`
    /// returns `Err(())`, applying backpressure to senders. Use for
    /// write-heavy actors where unbounded queue growth is undesirable.
    pub fn start_actor_bounded(&self, actor: Box<dyn Actor>, bound: usize) -> Addr {
        self.start_actor_or_router(actor, false, Some(bound))
    }

    /// Spawns a router actor. The router's context will have its `router`
    /// field set to its own address (so messages forwarded to `router`
    /// come back to itself).
    pub fn start_router(&self, actor: Box<dyn Actor>) -> Addr {
        self.start_actor_or_router(actor, true, None)
    }

    /// Spawns a router actor with a bounded mailbox.
    ///
    /// Same as [`start_router`](Self::start_router) but with an explicit
    /// backpressure ceiling. Use when the default `DEFAULT_MAILBOX_CAPACITY`
    /// is not appropriate for the deployment.
    pub fn start_router_bounded(&self, actor: Box<dyn Actor>, bound: usize) -> Addr {
        self.start_actor_or_router(actor, true, Some(bound))
    }

    /// Spawns a child async task (non-blocking).
    ///
    /// The task's `JoinHandle` is tracked so it can be aborted on stop.
    pub fn child_task<T>(&self, task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        let handle = crate::tokio_spawn::spawn(task);
        self.task_handles.write().push(handle);
    }

    /// Spawns a blocking child task via `spawn_blocking`.
    ///
    /// Use for CPU-intensive work that should not block the async runtime.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn blocking_child_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = tokio::task::spawn_blocking(task);
        self.task_handles.write().push(handle);
    }

    fn start_actor_or_router(
        &self,
        mut actor: Box<dyn Actor>,
        is_router: bool,
        bound: Option<usize>,
    ) -> Addr {
        let capacity = bound.unwrap_or(DEFAULT_MAILBOX_CAPACITY);
        let (sender, receiver) = mailbox::mailbox(capacity);
        let addr = Addr::new(sender);
        let (stop_sender, stop_receiver) = channel(1);
        let new_context = self.child_context(addr.clone(), stop_sender.clone());
        if is_router {
            *new_context.router.write() = addr.clone();
        }
        self.stop_signals.write().insert(addr.clone(), stop_sender);
        let stop_signals = self.stop_signals.clone();
        let addr_clone = addr.clone();
        crate::tokio_spawn::spawn(async move {
            actor.run(receiver, stop_receiver, new_context).await;
            stop_signals.write().remove(&addr_clone);
        });
        addr
    }

    /// Stops this actor and all its children.
    ///
    /// Aborts all child tasks and sends stop signals to all child actors.
    /// Sets `is_stopped` to `true`.
    pub fn stop(&self) {
        for handle in self.task_handles.read().iter() {
            handle.abort();
        }
        for signal in self.stop_signals.read().values() {
            let _ = signal.try_send(());
        }
        *self.node.write() = None;
        *self.is_stopped.write() = true;
    }
}

/// A clonable, hashable address for sending messages to an actor.
///
/// `Addr` implements `PartialEq`, `Eq`, and `Hash` based on its `id` field
/// (a random 32-character string), **not** the underlying channel sender.
/// This means two `Addr`s are equal iff they refer to the same actor.
///
/// # Sending Messages
///
/// ```no_run
/// use beam::actor::Addr;
/// use beam::message::Message;
/// use std::sync::Arc;
///
/// // addr.send(Message::Put(put)) — wraps in Arc internally
/// // addr.send(Arc::clone(&msg)) — refcount bump, no allocation
/// // Err(()) means the actor's mailbox is closed (actor stopped)
/// ```
#[derive(Clone, Debug)]
pub struct Addr {
    id: String,
    sender: MailboxSender,
}

impl Addr {
    /// Creates a new address wrapping a [`MailboxSender`].
    pub fn new(sender: MailboxSender) -> Self {
        Self {
            id: random_string(32),
            sender,
        }
    }

    /// Sends a message to this actor.
    ///
    /// Accepts `impl Into<Arc<Message>>` so callers can pass either:
    /// - `Message::Put(put)` — wrapped in `Arc` internally (one allocation)
    /// - `Arc::clone(&msg)` — refcount bump, zero allocation (for fanout)
    ///
    /// Returns `Ok(())` if the message was enqueued, `Err(())` if the
    /// mailbox is full (backpressure) or closed (actor stopped).
    ///
    /// Callers that must not lose messages should retry on `Err`. The
    /// Router's storage dispatch uses `let _ = addr.send(...)` and accepts
    /// occasional drops under extreme backpressure, which is the correct
    /// trade-off for an LWW graph store.
    #[allow(clippy::result_unit_err)] // mailbox-closed/full is unrecoverable; no meaningful error payload
    pub fn send(&self, msg: impl Into<Arc<Message>>) -> Result<(), ()> {
        self.sender.send(msg.into())
    }

    /// Returns the unique identifier of this actor address.
    ///
    /// The id is a random 32-character alphanumeric string, generated at
    /// address creation. Two `Addr`s are equal iff their ids match.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns a no-op address that silently drops all messages.
    ///
    /// Useful as a placeholder before a real address is set.
    pub fn noop() -> Addr {
        Addr::new(MailboxSender::noop())
    }
}

impl PartialEq for Addr {
    fn eq(&self, other: &Addr) -> bool {
        self.id == other.id
    }
}

impl Eq for Addr {}

impl Hash for Addr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor:{}", self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addr_equality() {
        let (s1, _r1) = crate::mailbox::mailbox(16);
        let (s2, _r2) = crate::mailbox::mailbox(16);
        let a1 = Addr::new(s1);
        let a2 = Addr::new(s2);
        assert_ne!(a1, a2, "different addrs are not equal");
        assert_eq!(a1, a1.clone(), "clone is equal");
    }

    #[test]
    fn test_addr_hash() {
        let (s1, _r1) = crate::mailbox::mailbox(16);
        let a1 = Addr::new(s1);
        let a2 = a1.clone();
        let mut set = std::collections::HashSet::new();
        set.insert(a1);
        assert!(set.contains(&a2), "clone should be found in HashSet");
    }

    #[test]
    fn test_addr_display() {
        let (s, _r) = crate::mailbox::mailbox(16);
        let addr = Addr::new(s);
        let display = format!("{}", addr);
        assert!(display.starts_with("actor:"));
        assert_eq!(display.len(), "actor:".len() + 32);
    }

    #[test]
    fn test_addr_noop_sends_silently() {
        let addr = Addr::noop();
        assert_eq!(addr.id.len(), 32);
    }

    #[test]
    fn test_addr_id_accessor() {
        let (s, _r) = crate::mailbox::mailbox(16);
        let addr = Addr::new(s);
        assert_eq!(addr.id().len(), 32);
        assert!(addr.id().chars().all(|c| c.is_ascii_alphanumeric()));
        // Display format is "actor:{id}" — id() should return the raw id without prefix
        assert_ne!(addr.id(), format!("{}", addr));
        assert!(format!("{}", addr).ends_with(addr.id()));
    }

    #[test]
    fn test_addr_id_length() {
        let (s, _r) = crate::mailbox::mailbox(16);
        let addr = Addr::new(s);
        assert_eq!(addr.id.len(), 32);
        assert!(
            addr.id.chars().all(|c| c.is_ascii_alphanumeric()),
            "addr id should be alphanumeric"
        );
    }

    struct TestActor {
        received: Arc<RwLock<Vec<Message>>>,
    }

    #[async_trait]
    impl Actor for TestActor {
        async fn handle(&mut self, message: Arc<Message>, _ctx: &ActorContext) {
            // Clone the inner Message out of the Arc for the test vector.
            self.received.write().push((*message).clone());
        }
    }

    #[tokio::test]
    async fn test_actor_context_new() {
        let ctx = ActorContext::new("peer1".to_string());
        assert_eq!(*ctx.peer_id.read(), "peer1");
        assert_eq!(ctx.child_actor_count(), 0);
        assert!(!*ctx.is_stopped.read());
    }

    #[tokio::test]
    async fn test_actor_start_and_send() {
        let ctx = ActorContext::new("test".to_string());
        let received = Arc::new(RwLock::new(Vec::new()));
        let actor = TestActor {
            received: received.clone(),
        };
        let _addr = ctx.start_actor(Box::new(actor));

        // Give the actor a moment to start
        crate::tokio_time::sleep(web_time::Duration::from_millis(50)).await;

        assert_eq!(ctx.child_actor_count(), 1);

        // Stop the actor
        ctx.stop();
        assert!(*ctx.is_stopped.read());
    }
}

#[test]
fn test_shutdown_signal_default_false() {
    // A new ActorContext should have shutdown_rx initialized to false.
    let ctx = ActorContext::new("test-peer".to_string());
    assert!(
        !*ctx.shutdown_rx.borrow(),
        "shutdown_rx should default to false"
    );
}

#[test]
fn test_shutdown_signal_propagates_to_child() {
    // When the parent sends true on shutdown, child contexts (which
    // share the same watch channel) should observe the change.
    let (tx, rx) = watch::channel(false);
    let mut ctx = ActorContext::new("parent".to_string());
    ctx.shutdown_rx = rx;

    let (stop_tx, _stop_rx) = channel(1);
    let child = ctx.child_context(Addr::noop(), stop_tx);

    // Child starts with false
    assert!(!*child.shutdown_rx.borrow());

    // Parent signals shutdown
    tx.send(true).unwrap();

    // Child observes the change
    assert!(
        *child.shutdown_rx.borrow(),
        "child should see shutdown signal"
    );
}

#[test]
fn test_shutdown_signal_isolated_per_node() {
    // Different nodes create independent watch channels —
    // signaling one should not affect the other.
    let mut ctx_a = ActorContext::new("node-a".to_string());
    let ctx_b = ActorContext::new("node-b".to_string());

    // Both start false
    assert!(!*ctx_a.shutdown_rx.borrow());
    assert!(!*ctx_b.shutdown_rx.borrow());

    // Replace ctx_a's channel with a controllable one
    let (tx_a, rx_a) = watch::channel(false);
    ctx_a.shutdown_rx = rx_a;
    tx_a.send(true).unwrap();

    // ctx_a sees shutdown, ctx_b does not
    assert!(*ctx_a.shutdown_rx.borrow());
    assert!(
        !*ctx_b.shutdown_rx.borrow(),
        "unrelated node should not see signal"
    );
}
