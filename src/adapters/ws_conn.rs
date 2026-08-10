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
use std::sync::Arc;

use async_trait::async_trait;

use log::{debug, error, info};
use web_time::Duration;

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
    async fn handle(&mut self, msg: Arc<Message>, ctx: &ActorContext) {
        // Serialize to wire format. Internal souls (root "", value
        // "soul/key") are stripped during serialization by Put::to_string.
        let wire = match &*msg {
            Message::Put(put) => put.to_string(),
            _ => msg.to_string(),
        };
        ctx.metrics.record_serialization();
        debug!(
            "[WS→] SENDING {} bytes: {}",
            wire.len(),
            &wire[..wire.len().min(300)]
        );
        let _ = self.sender.send(WsMessage::Text(wire.into())).await;
        ctx.metrics.record_ws_sent();
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WsConn starting");
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
        };
        let _ = self
            .sender
            .send(WsMessage::Text(hi.to_string().into()))
            .await;
        let receiver = self.receiver.take().unwrap();
        let mut ctx2 = ctx.clone();
        let allow_public_space = self.allow_public_space;
        ctx.child_task(async move {
            use futures_util::StreamExt;
            let mut receiver = receiver;
            while let Some(result) = receiver.next().await {
                let ws_msg = match result {
                    Ok(m) => m,
                    Err(e) => {
                        debug!("[WS] recv error: {}", e);
                        break;
                    }
                };
                let text = match ws_msg {
                    WsMessage::Text(t) => t,
                    WsMessage::Binary(_) => {
                        debug!("[WS] binary frame (ignored)");
                        continue;
                    }
                    WsMessage::Ping(_) => {
                        debug!("[WS] ping frame (ignored)");
                        continue;
                    }
                    WsMessage::Pong(_) => {
                        debug!("[WS] pong frame (ignored)");
                        continue;
                    }
                    WsMessage::Close(_) => {
                        debug!("[WS] close frame received from peer");
                        break;
                    }
                    WsMessage::Frame(_) => {
                        debug!("[WS] raw frame (ignored)");
                        continue;
                    }
                };
                if text.is_empty() {
                    debug!("[WS] empty text frame (ignored)");
                    continue;
                }
                ctx2.metrics.record_ws_received();
                debug!(
                    "[WS←] RECV {} bytes: {}",
                    text.len(),
                    &text[..text.len().min(300)]
                );
                match Message::try_from(&text, ctx2.addr.clone(), allow_public_space) {
                    Ok(msgs) => {
                        ctx2.metrics.record_parsed();
                        for msg in msgs {
                            if ctx2.router.send(msg).is_err() {
                                error!("failed to forward incoming message to router");
                            }
                        }
                    }
                    Err(e) => {
                        debug!("[WS] parse error: {} (len={})", e, text.len());
                    }
                }
            }
            debug!("[WS] receive loop ended — stopping actor");
            ctx2.stop();
        });
    }

    async fn stopping(&mut self, _context: &ActorContext) {
        info!("WsConn stopping — sending WebSocket Close frame");
        let close_result =
            crate::tokio_time::timeout(Duration::from_secs(2), self.sender.close()).await;

        match close_result {
            Ok(Ok(())) => debug!("WsConn Close frame acknowledged"),
            Ok(Err(e)) => debug!("WsConn Close error (non-fatal): {}", e),
            Err(_) => debug!("WsConn Close timed out — connection dropped"),
        }
    }
}
