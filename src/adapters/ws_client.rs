//! Outgoing WebSocket client manager — connects to remote relay servers.
//!
//! [`OutgoingWebsocketManager`] is a network adapter that maintains outbound
//! WebSocket connections to one or more relay servers. It:
//!
//! - Connects to each configured URL on startup (with retry)
//! - Creates a [`WsConn`] actor per connection
//! - Fans out outgoing messages to all connected clients
//! - Marks itself as `subscribe_to_everything` so the router sends all
//!   `Get` and `Put` messages to it (relay behavior)
//!
//! # Relay Semantics
//!
//! Unlike direct P2P connections, relay servers receive all messages
//! (not just topic-matched ones). This makes them suitable as bootstrap
//! nodes for discovering peers and relaying messages when direct
//! connectivity is unavailable.

use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::ws_conn::WsConn;
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, info};
use crate::tokio_time::sleep;
use web_time::Duration;

/// Manages outbound WebSocket connections to relay servers.
///
/// Created with a list of WebSocket URLs. On `pre_start`, connects to each
/// URL (with retry) and spawns a [`WsConn`] actor per connection. All
/// outgoing messages are fanned out to all connected clients.
///
/// The `clients` map is shared via `Arc<RwLock<...>>` so that e2e tests
/// can hold their own clone of this manager and observe connection
/// state via [`Self::connected_count`] while the actor-driven copy
/// (moved into [`crate::Node`]) performs the actual work.
#[derive(Clone)]
pub struct OutgoingWebsocketManager {
    config: Config,
    clients: Arc<RwLock<HashMap<String, Addr>>>,
    urls: Vec<String>,
}

impl OutgoingWebsocketManager {
    /// Creates a new manager for the given URLs.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration (uses `allow_public_space` for connections)
    /// * `urls` - WebSocket URLs to connect to (e.g. `["wss://relay.example.com/ws"]`)
    pub fn new(config: Config, urls: Vec<String>) -> Self {
        OutgoingWebsocketManager {
            urls,
            clients: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Returns the number of remote URLs that have an active WebSocket connection.
    ///
    /// This is a **readiness signal**: it reflects the state of the
    /// `clients` map, which is populated only after `connect_async`
    /// succeeds (see `pre_start`). Once `connected_count() == urls.len()`,
    /// all configured peer connections have completed the WebSocket
    /// handshake and are ready to send/receive messages.
    ///
    /// e2e tests should poll on this instead of blind `sleep(N)`:
    ///
    /// ```ignore
    /// while client.connected_count().await < expected {
    ///     crate::tokio_time::sleep(Duration::from_millis(50)).await;
    /// }
    /// ```
    ///
    /// Returns a snapshot under the read lock; the count is monotonic
    /// (only grows as connections succeed). Callers do not need to
    /// handle rollback — the actor never removes entries from `clients`
    /// during normal operation.
    pub async fn connected_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Returns the configured target URLs. Useful for tests that want to
    /// know how many connections to expect.
    pub fn urls(&self) -> &[String] {
        &self.urls
    }
}

#[async_trait]
impl Actor for OutgoingWebsocketManager {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("OutgoingWebsocketManager starting");
        for url in self.urls.iter() {
            // Retry connection until the websocket is established, or the actor
            // is shut down. Uses a bounded retry interval so transient DNS or
            // network blips don't cause permanent disconnection.
            //
            // NOTE: The loop condition checks `clients` so that if a prior
            // iteration's `start_actor` raced ahead of the `insert`, we don't
            // create a duplicate WsConn for the same URL.
            loop {
                if self.clients.read().await.contains_key(url) {
                    debug!("already connected to {}", url);
                    break;
                }

                debug!("attempting WebSocket connect to {}", url);
                let result = connect_async(url).await;

                if let Ok((socket, _)) = result {
                    let (sender, receiver) = socket.split();
                    let client = WsConn::new(sender, receiver, self.config.allow_public_space);
                    let addr = ctx.start_actor(Box::new(client));
                    self.clients.write().await.insert(url.clone(), addr);
                    debug!("connected to {}", url);
                    break;
                }

                debug!("connect to {} failed, retrying in 200ms", url);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    /// Returns `true` — this adapter subscribes to all messages (relay behavior).
    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn handle(&mut self, message: Message, _ctx: &ActorContext) {
        // Fan out to all connected clients.
        //
        // Snapshot under the read lock so we don't hold the lock while
        // calling `send` (which may briefly contend on the actor's
        // mailbox). `send().is_err()` clients are skipped — `pre_start`
        // will retry the connection on the next loop iteration. We do
        // not evict dead clients from the map here; that is the
        // responsibility of the reconnection loop, which already runs
        // periodically. This keeps `handle` simple and avoids
        // priority inversion under load.
        let snapshot: Vec<Addr> = self.clients.read().await.values().cloned().collect();
        for client in snapshot {
            let _ = client.send(message.clone());
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        let count = self.clients.read().await.len();
        info!(
            "OutgoingWebsocketManager stopping — {} outgoing connections",
            count
        );
        // The WsConn child actors receive stop signals via ActorContext::stop()
        // and send WebSocket Close frames in their own stopping(). Here we
        // clear the map so no further fan-out attempts are made.
        self.clients.write().await.clear();
    }
}
