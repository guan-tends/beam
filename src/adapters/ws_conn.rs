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

use log::{debug, info};
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

    /// Serialize a message into `send_buf` and feed it as a WS text frame
    /// into the sink's internal buffer.
    ///
    /// With `flush_threshold(usize::MAX)` configured on the WebSocket
    /// builder, `feed()` never triggers an implicit flush — it simply
    /// queues the frame. The caller is responsible for calling
    /// `flush()` after all messages are queued.
    async fn send_msg(&mut self, msg: &Arc<Message>, ctx: &ActorContext) {
        msg.to_writer(&mut self.send_buf);
        ctx.metrics.record_serialization();
        if let Some(sink) = &mut self.ws_sink {
            // Transfer ownership of send_buf into the WS frame — zero copy.
            // Replace with a fresh buffer for the next message.
            let buf = std::mem::take(&mut self.send_buf);
            let _ = sink
                .feed(WsMessage::text(
                    String::from_utf8(buf).expect("wire format is valid UTF-8"),
                ))
                .await;
        }
        ctx.metrics.record_ws_sent();
    }

    /// Flush the WS sink's internal buffer to the underlying TCP stream.
    async fn flush_sink(&mut self) {
        if let Some(sink) = &mut self.ws_sink {
            let _ = sink.flush().await;
        }
    }
}

#[async_trait]
impl<S> Actor for WsConn<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    /// Fallback single-message handler. Feeds one message then flushes.
    /// Used when the actor runtime calls `handle` instead of `handle_batch`.
    async fn handle(&mut self, msg: Arc<Message>, ctx: &ActorContext) {
        self.send_msg(&msg, ctx).await;
        self.flush_sink().await;
    }

    /// Batch handler — feeds all messages into the WS frame buffer without
    /// flushing. Because `flush_threshold` is set to `usize::MAX`, `feed()`
    /// never triggers an implicit flush and never blocks on I/O.
    ///
    /// Flushing is handled by a dedicated background task spawned in
    /// `pre_start`, which calls `flush()` on a timer. This decouples the
    /// actor's message processing from socket I/O — the actor never
    /// suspends waiting for the TCP buffer to drain.
    ///
    /// # Cooperative Scheduling
    ///
    /// On `current_thread` runtime, `feed()` with `flush_threshold(MAX)`
    /// completes without suspending, so a full batch of 64 messages can
    /// be processed without yielding to other tasks. We call
    /// `yield_now()` every 16 messages to ensure the relay's router and
    /// other actors get scheduled. Without this, on `current_thread`, a
    /// sender's WsConn can starve the relay's receive loop, causing a
    /// deadlock where the relay never processes incoming puts.
    async fn handle_batch(&mut self, batch: &mut Vec<Arc<Message>>, ctx: &ActorContext) {
        if self.ws_sink.is_some() {
            let mut count = 0;
            for msg in batch.drain(..) {
                self.send_msg(&msg, ctx).await;
                count += 1;
                if count % 16 == 0 {
                    crate::tokio_spawn::yield_now().await;
                }
            }
            // Single flush per batch — not per message. This is the key
            // difference from `sink.send()` (which flushes per message).
            // One flush per 64 messages dramatically reduces I/O syscalls
            // while still delivering data to TCP promptly.
            self.flush_sink().await;
        } else {
            batch.clear();
        }
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        // Send Hi message to register with the relay.
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
        };
        hi.to_writer(&mut self.send_buf);
        ctx.metrics.record_serialization();
        if let Some(sink) = &mut self.ws_sink {
            let buf = std::mem::take(&mut self.send_buf);
            let _ = sink
                .feed(WsMessage::text(
                    String::from_utf8(buf).expect("wire format is valid UTF-8"),
                ))
                .await;
        }
        ctx.metrics.record_ws_sent();
        self.flush_sink().await;

        // Move the read half into a child task for the receive loop.
        let reader = self.ws_stream.take().expect("ws_stream already taken");
        let ctx2 = ctx.clone();
        let allow_public_space = self.allow_public_space;
        ctx.child_task(async move {
            let mut reader = reader;
            while let Some(result) = reader.next().await {
                let ws_msg = match result {
                    Ok(m) => m,
                    Err(_e) => {
                        break;
                    }
                };
                if ws_msg.is_text() {
                    let text = ws_msg.as_text().unwrap_or("");
                    if text.is_empty() {
                        continue;
                    }
                    ctx2.metrics.record_ws_received();
                    match Message::try_from(text, ctx2.addr.clone(), allow_public_space) {
                        Ok(msgs) => {
                            ctx2.metrics.record_parsed();
                            for msg in msgs {
                                let _ = ctx2.router.read().send(msg);
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
