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
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::sync::Arc;

use async_trait::async_trait;

use log::{debug, error, info};
use web_time::Duration;

use tokio_websockets::{Message as WsMessage, WebSocketStream};

/// A per-connection WebSocket actor that bridges Gun protocol messages.
///
/// Created by [`crate::adapters::WsServer`] (inbound) or
/// [`crate::adapters::OutgoingWebsocketManager`] (outbound). Each `WsConn`
/// manages a single WebSocket connection and translates between the
/// Gun wire format (text) and [`Message`] enum values.
/// A per-connection WebSocket actor that bridges Gun protocol messages.
///
/// Generic over the underlying stream type `S` (plain `TcpStream` or
/// `TlsStream<TcpStream>`). Created by [`crate::adapters::WsServer`]
/// (inbound) or [`crate::adapters::OutgoingWebsocketManager`] (outbound).
/// Each `WsConn` manages a single WebSocket connection and translates
/// between the Gun wire format (text) and [`Message`] enum values.
pub struct WsConn<S> {
    /// Write half of the WebSocket (for sending messages in `handle`).
    ws_sink: Option<SplitSink<WebSocketStream<S>, WsMessage>>,
    /// Read half of the WebSocket (moved into receive loop in `pre_start`).
    ws_stream: Option<SplitStream<WebSocketStream<S>>>,
    allow_public_space: bool,
    /// Reusable serialization buffer — eliminates per-message allocation.
    send_buf: Vec<u8>,
}

impl<S> WsConn<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    /// Creates a new `WsConn` from a `WebSocketStream`.
    pub fn new(ws: WebSocketStream<S>, allow_public_space: bool) -> Self {
        let (sink, stream) = futures_util::StreamExt::split(ws);
        Self {
            ws_sink: Some(sink),
            ws_stream: Some(stream),
            allow_public_space,
            send_buf: Vec::with_capacity(512),
        }
    }

    /// Send the current `send_buf` contents as a WS text frame.
    async fn flush_ws(&mut self, ctx: &ActorContext) {
        debug!(
            "[WS→] SENDING {} bytes: {}",
            self.send_buf.len(),
            std::str::from_utf8(&self.send_buf[..self.send_buf.len().min(300)]).unwrap_or("<utf8>")
        );
        if let Some(sink) = &mut self.ws_sink {
            let _ = sink
                .send(WsMessage::text(
                    String::from_utf8(self.send_buf.clone()).expect("wire format is valid UTF-8"),
                ))
                .await;
        }
        ctx.metrics.record_ws_sent();
    }
}

#[async_trait]
impl<S> Actor for WsConn<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    async fn handle(&mut self, msg: Arc<Message>, ctx: &ActorContext) {
        msg.to_writer(&mut self.send_buf);
        ctx.metrics.record_serialization();
        self.flush_ws(ctx).await;
    }

    /// Batch handler — serializes all messages, then flushes once.
    ///
    /// Each message is serialized into `send_buf` and immediately sent
    /// as a separate WS text frame. While we could coalesce into a single
    /// frame, Gun.js peers expect one message per frame. The win here is
    /// amortizing scheduler overhead: we drain the full mailbox batch
    /// without yielding between messages, then the sink's internal
    /// buffer handles the actual I/O coalescing.
    async fn handle_batch(&mut self, batch: &mut Vec<Arc<Message>>, ctx: &ActorContext) {
        if let Some(sink) = &mut self.ws_sink {
            for msg in batch.drain(..) {
                msg.to_writer(&mut self.send_buf);
                ctx.metrics.record_serialization();
                let _ = sink
                    .send(WsMessage::text(
                        String::from_utf8(self.send_buf.clone())
                            .expect("wire format is valid UTF-8"),
                    ))
                    .await;
                ctx.metrics.record_ws_sent();
            }
        } else {
            batch.clear();
        }
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        info!("WsConn starting");

        // Send Hi message to register with the relay.
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
        };
        hi.to_writer(&mut self.send_buf);
        if let Some(sink) = &mut self.ws_sink {
            let _ = sink
                .send(WsMessage::text(
                    String::from_utf8(self.send_buf.clone()).expect("wire format is valid UTF-8"),
                ))
                .await;
        }

        // Move the read half into a child task for the receive loop.
        let reader = self.ws_stream.take().expect("ws_stream already taken");
        let mut ctx2 = ctx.clone();
        let allow_public_space = self.allow_public_space;
        ctx.child_task(async move {
            let mut reader = reader;
            while let Some(result) = reader.next().await {
                let ws_msg = match result {
                    Ok(m) => m,
                    Err(e) => {
                        debug!("[WS] recv error: {}", e);
                        break;
                    }
                };
                if ws_msg.is_text() {
                    let text = ws_msg.as_text().unwrap_or("");
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
                    match Message::try_from(text, ctx2.addr.clone(), allow_public_space) {
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
                } else if ws_msg.is_binary() {
                    debug!("[WS] binary frame (ignored)");
                } else if ws_msg.is_close() {
                    debug!("[WS] close frame received from peer");
                    break;
                } else if ws_msg.is_ping() {
                    debug!("[WS] ping frame (ignored)");
                } else if ws_msg.is_pong() {
                    debug!("[WS] pong frame (ignored)");
                }
            }
            debug!("[WS] receive loop ended — stopping actor");
            ctx2.stop();
        });
    }

    async fn stopping(&mut self, _context: &ActorContext) {
        info!("WsConn stopping — sending WebSocket Close frame");
        if let Some(sink) = &mut self.ws_sink {
            let close_result =
                crate::tokio_time::timeout(Duration::from_secs(2), sink.close()).await;
            match close_result {
                Ok(Ok(())) => debug!("WsConn Close frame acknowledged"),
                Ok(Err(e)) => debug!("WsConn Close error (non-fatal): {}", e),
                Err(_) => debug!("WsConn Close timed out — connection dropped"),
            }
        }
    }
}
