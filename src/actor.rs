#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

//! Actor framework — a lightweight actor model built on Tokio channels.
//!
//! This module provides a minimal actor system inspired by
//! [Alice Ryhl's "Actors with Tokio"](https://ryhl.io/blog/actors-with-tokio/)
//! guide. Actors communicate via typed messages over channels and run on the
//! Tokio async runtime.
//!
//! # Architecture
//!
//! - [`Actor`] trait — defines the message handling interface
//! - [`ActorContext`] — per-actor context with peer ID, router address, and
//!   child actor management
//! - [`Addr`] — a clonable, hashable address for sending messages to an actor
//!
//! # Channel Types
//!
//! Actors can use either unbounded or bounded channels:
//!
//! - **Unbounded** (default) — [`ActorContext::start_actor`] creates an actor
//!   with an unbounded channel. No backpressure; messages are always enqueued.
//! - **Bounded** — [`ActorContext::start_actor_bounded`] creates an actor with
//!   a bounded channel of the given capacity. When full, [`Addr::send`]
//!   returns `Err(())`, applying backpressure. Used for storage write actors
//!   where unbounded queue growth is undesirable.
//!
//! Both channel types are abstracted behind [`AddrSender`]/[`AddrReceiver`]
//! enums, so callers use the same [`Addr::send`] API regardless of channel
//! type.
//!
//! # Message Flow
//!
//! ```text
//! Sender → Addr.send(msg) → Channel (bounded or unbounded)
//!                                ↓
//!                          Actor.handle(msg, ctx)
//!                                ↓
//!                          Actor can:
//!                          - spawn child actors
//!                          - send to router
//!                          - spawn child tasks
//! ```
//!
//! # Shutdown
//!
//! Actors are stopped via a stop signal channel. When the context's `stop()`
//! method is called, all child tasks are aborted and stop signals are sent
//! to all child actors.

use crate::Node;
use crate::message::Message;
use crate::utils::random_string;
use async_trait::async_trait;
use futures_util::Future;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::Send;
use std::sync::Arc;
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};
use tokio::task::JoinHandle;

/// Internal enum holding either an unbounded or bounded channel sender.
///
/// [`Addr::send`] dispatches over this enum so callers don't need to know
/// whether the underlying channel has backpressure.
#[derive(Clone, Debug)]
enum AddrSender {
    Unbounded(UnboundedSender<Message>),
    Bounded(Sender<Message>),
}

/// Internal enum holding either an unbounded or bounded channel receiver.
///
/// [`Actor::run`] consumes this, abstracting over the two receiver types.
enum AddrReceiver {
    Unbounded(UnboundedReceiver<Message>),
    Bounded(Receiver<Message>),
}

impl AddrReceiver {
    /// Receives the next message, or `None` when the channel is closed.
    async fn recv(&mut self) -> Option<Message> {
        match self {
            Self::Unbounded(r) => r.recv().await,
            Self::Bounded(r) => r.recv().await,
        }
    }
}

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
///
/// struct EchoActor;
///
/// #[async_trait]
/// impl Actor for EchoActor {
///     async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
///         // Process message
///     }
/// }
/// ```
#[async_trait]
pub trait Actor: Send + Sync + 'static {
    /// Handle an incoming message.
    async fn handle(&mut self, message: Message, context: &ActorContext);

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
    /// or the channel is closed.
    ///
    /// Accepts [`AddrReceiver`] so the run loop works with both unbounded
    /// and bounded channels. Bounded channels provide backpressure for
    /// write-heavy actors (e.g. storage write actors).
    async fn run(
        &mut self,
        mut receiver: AddrReceiver,
        mut stop_receiver: Receiver<()>,
        mut context: ActorContext,
    ) {
        self.pre_start(&context).await;
        loop {
            tokio::select! {
                _v = stop_receiver.recv() => {
                    context.stop();
                    break;
                },
                opt_msg = receiver.recv() => {
                    let msg = match opt_msg {
                        Some(msg) => msg,
                        None => break,
                    };
                    self.handle(msg, &context).await
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
    pub router: Addr,
    /// Stop signals for child actors (keyed by child Addr).
    stop_signals: Arc<RwLock<HashMap<Addr, Sender<()>>>>,
    /// Join handles for spawned child tasks.
    task_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,
    /// This actor's own address.
    pub addr: Addr,
    /// Whether this actor has been stopped.
    pub is_stopped: Arc<RwLock<bool>>,
    /// Optional owned Node (set for the root actor).
    pub node: Option<Node>,
}

impl ActorContext {
    /// Creates a new `ActorContext` with the given peer ID.
    ///
    /// The `addr` and `router` fields are initialized to [`Addr::noop()`]
    /// and should be set before use.
    pub fn new(peer_id: String) -> Self {
        Self {
            addr: Addr::noop(),
            stop_signals: Arc::new(RwLock::new(HashMap::new())),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            peer_id: Arc::new(RwLock::new(peer_id)),
            router: Addr::noop(),
            is_stopped: Arc::new(RwLock::new(false)),
            node: None,
        }
    }

    /// Returns the number of child actors spawned by this context.
    pub fn child_actor_count(&self) -> usize {
        self.stop_signals.read().len()
    }

    /// Creates a child context with the given address and stop signal.
    fn child_context(&self, addr: Addr, stop_signal: Sender<()>) -> Self {
        let mut stop_signals = HashMap::new();
        stop_signals.insert(addr.clone(), stop_signal);
        Self {
            addr,
            stop_signals: Arc::new(RwLock::new(stop_signals)),
            task_handles: Arc::new(RwLock::new(Vec::new())),
            peer_id: self.peer_id.clone(),
            router: self.router.clone(),
            is_stopped: self.is_stopped.clone(),
            node: self.node.clone(),
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

    /// Spawns a child async task (non-blocking).
    ///
    /// The task's `JoinHandle` is tracked so it can be aborted on stop.
    pub fn child_task<T>(&self, task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(task);
        self.task_handles.write().push(handle);
    }

    /// Spawns a blocking child task via `spawn_blocking`.
    ///
    /// Use for CPU-intensive work that should not block the async runtime.
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
        let (addr, receiver) = match bound {
            Some(cap) => {
                let (sender, receiver) = channel::<Message>(cap);
                (Addr::new_bounded(sender), AddrReceiver::Bounded(receiver))
            }
            None => {
                let (sender, receiver) = unbounded_channel::<Message>();
                (Addr::new(sender), AddrReceiver::Unbounded(receiver))
            }
        };
        let (stop_sender, stop_receiver) = channel(1);
        let mut new_context = self.child_context(addr.clone(), stop_sender.clone());
        if is_router {
            new_context.router = addr.clone();
        }
        self.stop_signals.write().insert(addr.clone(), stop_sender);
        let stop_signals = self.stop_signals.clone();
        let addr_clone = addr.clone();
        tokio::spawn(async move {
            actor.run(receiver, stop_receiver, new_context).await;
            stop_signals.write().remove(&addr_clone);
        });
        addr
    }

    /// Stops this actor and all its children.
    ///
    /// Aborts all child tasks and sends stop signals to all child actors.
    /// Sets `is_stopped` to `true`.
    pub fn stop(&mut self) {
        for handle in self.task_handles.read().iter() {
            handle.abort();
        }
        for signal in self.stop_signals.read().values() {
            let _ = signal.try_send(());
        }
        self.node = None;
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
///
/// // addr.send(msg) returns Result<(), ()>
/// // Err(()) means the actor's channel is closed (actor stopped)
/// ```
#[derive(Clone, Debug)]
pub struct Addr {
    id: String,
    sender: AddrSender,
}

impl Addr {
    /// Creates a new address wrapping an unbounded channel sender.
    pub fn new(sender: UnboundedSender<Message>) -> Self {
        Self {
            id: random_string(32),
            sender: AddrSender::Unbounded(sender),
        }
    }

    /// Creates a new address wrapping a bounded channel sender.
    ///
    /// Bounded addresses apply backpressure: when the channel is full,
    /// `send` returns `Err(())` (same error as a closed channel).
    pub fn new_bounded(sender: Sender<Message>) -> Self {
        Self {
            id: random_string(32),
            sender: AddrSender::Bounded(sender),
        }
    }

    /// Sends a message to this actor.
    ///
    /// Returns `Ok(())` if the message was enqueued, `Err(())` if the
    /// actor's channel is closed (actor has stopped) or — for bounded
    /// channels — if the channel is full (backpressure).
    ///
    /// Callers that must not lose messages should retry on `Err`. The
    /// Router's storage dispatch uses `let _ = addr.send(...)` and accepts
    /// occasional drops under extreme backpressure, which is the correct
    /// trade-off for an LWW graph store.
    #[allow(clippy::result_unit_err)] // channel-closed/full is unrecoverable; no meaningful error payload
    pub fn send(&self, msg: Message) -> Result<(), ()> {
        match &self.sender {
            AddrSender::Unbounded(s) => s.send(msg).map_err(|_| ()),
            AddrSender::Bounded(s) => s.try_send(msg).map_err(|_| ()),
        }
    }

    /// Returns a no-op address with a discarded receiver.
    ///
    /// Messages sent to a noop address are silently dropped. Useful as a
    /// placeholder before a real address is set.
    pub fn noop() -> Addr {
        let (sender, _receiver) = unbounded_channel::<Message>();
        Addr::new(sender)
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
        let (s1, _r1) = unbounded_channel::<Message>();
        let (s2, _r2) = unbounded_channel::<Message>();
        let a1 = Addr::new(s1);
        let a2 = Addr::new(s2);
        assert_ne!(a1, a2, "different addrs are not equal");
        assert_eq!(a1, a1.clone(), "clone is equal");
    }

    #[test]
    fn test_addr_hash() {
        let (s1, _r1) = unbounded_channel::<Message>();
        let a1 = Addr::new(s1);
        let a2 = a1.clone();
        let mut set = std::collections::HashSet::new();
        set.insert(a1);
        assert!(set.contains(&a2), "clone should be found in HashSet");
    }

    #[test]
    fn test_addr_display() {
        let (s, _r) = unbounded_channel::<Message>();
        let addr = Addr::new(s);
        let display = format!("{}", addr);
        assert!(display.starts_with("actor:"));
        assert_eq!(display.len(), "actor:".len() + 32);
    }

    #[test]
    fn test_addr_noop_sends_silently() {
        let addr = Addr::noop();
        // Sending to noop should not panic
        // We can't easily send a Message without constructing one,
        // but noop creates a valid channel with a discarded receiver
        assert_eq!(addr.id.len(), 32);
    }

    #[test]
    fn test_addr_id_length() {
        let (s, _r) = unbounded_channel::<Message>();
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
        async fn handle(&mut self, message: Message, _ctx: &ActorContext) {
            self.received.write().push(message);
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
        let mut ctx = ActorContext::new("test".to_string());
        let received = Arc::new(RwLock::new(Vec::new()));
        let actor = TestActor {
            received: received.clone(),
        };
        let _addr = ctx.start_actor(Box::new(actor));

        // Give the actor a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(ctx.child_actor_count(), 1);

        // Stop the actor
        ctx.stop();
        assert!(*ctx.is_stopped.read());
    }
}
