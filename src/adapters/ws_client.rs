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
use tokio_tungstenite::connect_async;
use url::Url;

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::adapters::ws_conn::WsConn;
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, info};
use tokio::time::{Duration, sleep};

/// Manages outbound WebSocket connections to relay servers.
///
/// Created with a list of WebSocket URLs. On `pre_start`, connects to each
/// URL (with retry) and spawns a [`WsConn`] actor per connection. All
/// outgoing messages are fanned out to all connected clients.
pub struct OutgoingWebsocketManager {
    config: Config,
    clients: HashMap<String, Addr>,
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
            clients: HashMap::new(),
            config,
        }
    }
}

#[async_trait]
impl Actor for OutgoingWebsocketManager {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("OutgoingWebsocketManager starting");
        for url in self.urls.iter() {
            // Retry connection until the websocket is established.
            // TODO: break on actor shutdown signal instead of polling.
            loop {
                sleep(Duration::from_millis(1000)).await;
                if self.clients.contains_key(url) {
                    break; // Already connected — move to next URL
                }
                let result = connect_async(Url::parse(url).expect("Can't connect to URL")).await;
                if let Ok(tuple) = result {
                    let (socket, _) = tuple;
                    debug!("outgoing websocket opened to {}", url);
                    let (sender, receiver) = socket.split();
                    let client = WsConn::new(sender, receiver, self.config.allow_public_space);
                    let addr = ctx.start_actor(Box::new(client));
                    self.clients.insert(url.clone(), addr);
                    break; // Connected — move to next URL
                }
            }
        }
    }

    /// Returns `true` — this adapter subscribes to all messages (relay behavior).
    fn subscribe_to_everything(&self) -> bool {
        true
    }

    async fn handle(&mut self, message: Message, _ctx: &ActorContext) {
        // Fan out to all connected clients, removing any that have died.
        self.clients
            .retain(|_url, client| client.send(message.clone()).is_ok());
    }
}
