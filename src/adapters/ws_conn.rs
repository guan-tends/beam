//! Per-connection WebSocket actor — bridges Gun protocol messages over WebSocket.
//!
//! [`WsConn`] wraps a single WebSocket connection (either inbound via
//! [`crate::adapters::WsServer`] or outbound via
//! [`crate::adapters::OutgoingWebsocketManager`]). It:
//!
//! - Sends outgoing messages as `WsMessage::Text` on the WebSocket
//! - Receives incoming messages, parses them via [`Message::try_from`], and
//!   forwards to the [`crate::router::Router`]
//! - Sends a `Message::Hi` on startup to register with the router
//!
//! # Lifecycle
//!
//! 1. `pre_start`: Send `Hi` message, spawn receive loop
//! 2. `handle`: Forward outgoing messages to WebSocket
//! 3. `stopping`: Log shutdown
//!
//! The receive loop runs as a child task and calls `ctx.stop()` when the
//! WebSocket closes, triggering actor shutdown.

use crate::actor::{Actor, ActorContext};
use crate::message::Message;
use futures_util::SinkExt;
use futures_util::stream::{SplitSink, SplitStream};

use async_trait::async_trait;

use futures_util::{TryStreamExt, future};
use log::{debug, error, info};

use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Type alias for the WebSocket stream over a TLS or plain TCP connection.
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
/// Type alias for the sending half of a split WebSocket stream.
type WsSender = SplitSink<WsStream, WsMessage>;
/// Type alias for the receiving half of a split WebSocket stream.
type WsReceiver = SplitStream<WsStream>;

/// A per-connection WebSocket actor that bridges Gun protocol messages.
///
/// Created by [`crate::adapters::WsServer`] (inbound) or
/// [`crate::adapters::OutgoingWebsocketManager`] (outbound). Each `WsConn`
/// manages a single WebSocket connection and translates between the
/// Gun wire format (text) and [`Message`] enum values.
pub struct WsConn {
    sender: WsSender,
    receiver: Option<WsReceiver>,
    allow_public_space: bool,
}

impl WsConn {
    /// Creates a new `WsConn` from the split halves of a WebSocket stream.
    ///
    /// # Arguments
    ///
    /// * `sender` - The writing half of the WebSocket stream
    /// * `receiver` - The reading half of the WebSocket stream
    /// * `allow_public_space` - Whether to accept public space writes (forwarded
    ///   to `Message::try_from` for inbound message parsing)
    pub fn new(sender: WsSender, receiver: WsReceiver, allow_public_space: bool) -> Self {
        Self {
            sender,
            receiver: Some(receiver),
            allow_public_space,
        }
    }
}

#[async_trait]
impl Actor for WsConn {
    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        let _ = self.sender.send(WsMessage::Text(msg.to_string())).await;
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WsConn starting");
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
        };
        let _ = self.sender.send(WsMessage::Text(hi.to_string())).await;
        let receiver = self.receiver.take().unwrap();
        let mut ctx2 = ctx.clone();
        let allow_public_space = self.allow_public_space;
        ctx.child_task(async move {
            let _ = receiver
                .try_for_each(|msg| {
                    if let Ok(s) = msg.to_text() {
                        debug!("WsConn received: {}", s);
                        if let Ok(msgs) =
                            Message::try_from(s, ctx2.addr.clone(), allow_public_space)
                        {
                            for msg in msgs.into_iter() {
                                if ctx2.router.send(msg).is_err() {
                                    error!("failed to forward incoming message to router");
                                }
                            }
                        }
                    }
                    future::ok(())
                })
                .await;
            ctx2.stop();
        });
    }

    async fn stopping(&mut self, _context: &ActorContext) {
        info!("WsConn stopping");
    }
}
