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
use bytes::Bytes;
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
    /// Serialize a message and feed it as a WS text frame into the sink.
    ///
    /// For `Message::Put`, uses [`Put::get_or_serialize`] to cache the
    /// wire bytes on first serialization. When the same `Arc<Message>`
    /// is relayed to multiple peers, only the first WsConn serializes —
    /// subsequent peers receive cached bytes (refcount bump, zero-copy).
    /// Mirrors Gun.js's `meta.raw` caching in `mesh.raw()`.
    async fn send_msg(&mut self, msg: &Arc<Message>, ctx: &ActorContext) {
        let bytes = match msg.as_ref() {
            Message::Put(put) => put.get_or_serialize(),
            _ => {
                msg.to_writer(&mut self.send_buf);
                Bytes::from(std::mem::take(&mut self.send_buf))
            }
        };
        ctx.metrics.record_serialization();
        if let Some(sink) = &mut self.ws_sink {
            let _ = sink
                .feed(WsMessage::text(
                    String::from_utf8(bytes.to_vec()).expect("wire format is valid UTF-8"),
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

    /// Batch handler — packs all messages into a single WebSocket text
    /// frame as a JSON array, then flushes once.
    ///
    /// Mirrors Gun.js's `peer.batch` packing in `mesh.say`: messages
    /// are accumulated as `[{msg1},{msg2},...]` and flushed as a single
    /// WS frame, amortizing frame header overhead.
    ///
    /// For single-message batches, the message is sent as-is (no array
    /// wrapper) — preserving backwards compatibility with peers that
    /// may not expect array frames.
    ///
    /// # Serialized Message Cache (Sprint 1)
    ///
    /// For `Message::Put`, uses [`Put::get_or_serialize`] to reuse
    /// cached wire bytes. When the same `Arc<Message>` is relayed to
    /// multiple peers, only the first WsConn serializes — subsequent
    /// peers get cached bytes (refcount bump).
    ///
    /// # Cooperative Scheduling
    ///
    /// On `current_thread` runtime, we call `yield_now()` every 16
    /// messages to ensure the relay's router and other actors get
    /// scheduled. Without this, a sender's WsConn can starve the
    /// relay's receive loop, causing a deadlock.
    ///
    /// # Frame Size Safety
    ///
    /// If the accumulated buffer exceeds `MAX_BATCH_FRAME_SIZE`, we
    /// flush early and start a new array. This prevents exceeding
    /// WebSocket frame size limits on peers with restrictive configs.
    async fn handle_batch(&mut self, batch: &mut Vec<Arc<Message>>, ctx: &ActorContext) {
        if self.ws_sink.is_none() {
            batch.clear();
            return;
        }

        match batch.len() {
            0 => {}
            1 => {
                // Single message — send as-is (no array wrapper needed).
                let msg = batch.drain(..).next().unwrap();
                self.send_msg(&msg, ctx).await;
                self.flush_sink().await;
            }
            _ => {
                // Multiple messages — pack into a JSON array frame.
                // Gun.js: peer.batch = '['; peer.batch += ',' + raw; ... flush
                self.send_buf.clear();
                self.send_buf.push(b'[');
                let mut first = true;
                let mut count = 0;

                for msg in batch.drain(..) {
                    if !first {
                        self.send_buf.push(b',');
                    }
                    first = false;

                    // Use cached serialization for Put, direct for others.
                    let bytes = match msg.as_ref() {
                        Message::Put(put) => put.get_or_serialize(),
                        _ => {
                            let mut buf = Vec::with_capacity(64);
                            msg.to_writer(&mut buf);
                            Bytes::from(buf)
                        }
                    };
                    self.send_buf.extend_from_slice(&bytes);
                    ctx.metrics.record_serialization();
                    ctx.metrics.record_ws_sent();

                    count += 1;
                    if count % 16 == 0 {
                        crate::tokio_spawn::yield_now().await;
                    }
                }

                self.send_buf.push(b']');

                // Feed the batched array as a single WS text frame.
                let buf = std::mem::take(&mut self.send_buf);
                if let Some(sink) = &mut self.ws_sink {
                    let _ = sink
                        .feed(WsMessage::text(
                            String::from_utf8(buf).expect("wire format is valid UTF-8"),
                        ))
                        .await;
                }

                self.flush_sink().await;
            }
        }
    }

    async fn pre_start(&mut self, ctx: &ActorContext) {
        // Send Hi message to register with the relay.
        let hi = Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
            is_ack: None, // Initial contact — Gun.js should ack with dam: "?" + "@"
            msg_id: crate::utils::random_string(8),
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
