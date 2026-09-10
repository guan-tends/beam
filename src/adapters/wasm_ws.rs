//! WASM WebSocket adapter — bridges Gun protocol messages over WebSocket in the browser.
//!
//! Browser BEAM is a client-only node. It connects to relay servers via
//! WebSocket. This adapter:
//!
//! - Opens a WebSocket connection to a relay URL
//! - Sends outgoing [`Message`]s as text frames
//! - Receives incoming text frames, parses via [`Message::try_from`], forwards to router
//! - Registers with both the local router and the relay on connection open
//!
//! # Design
//!
//! The WebSocket API is callback-based — `new()` returns immediately while
//! the connection is still opening. We buffer outgoing messages in a shared
//! [`Arc<Mutex<Vec>>`] until `onopen` fires, then flush. The actor's `pre_start`
//! registers with the local router (using `ctx.addr`); `onopen` registers with
//! the relay (the wire Hi format does not include the actor addr).
//!
//! # Send Strategy
//!
//! We use a "try-send, fallback to buffer" approach in `handle()`. Rather than
//! checking `ready_state()` (which is unreliable in WASM — it may report
//! `CONNECTING` even after `onopen` has fired), we call `send_with_str()`
//! directly. If it returns `Err` (WebSocket still in CONNECTING state), the
//! message is buffered to the outbox. The `onopen` callback flushes the outbox
//! when the connection opens. After `onopen`, `send_with_str()` succeeds
//! because the underlying WebSocket IS open, regardless of what `ready_state()`
//! reports.

use crate::actor::{Actor, ActorContext, Addr};
use crate::message::Message;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{MessageEvent, WebSocket};

/// A WebSocket connection actor for browser/WASM environments.
///
/// Created by [`crate::node::Node::connect_peer_wasm`]. The actor registers
/// itself with the local router in `pre_start` so the router forwards
/// outgoing Puts to this actor for relay delivery.
pub struct WasmWsConn {
    ws: WebSocket,
    /// Shared with the `onopen` callback for flushing.
    outbox: Arc<Mutex<Vec<String>>>,
}

impl WasmWsConn {
    /// Creates a new `WasmWsConn` by opening a WebSocket to the given URL.
    ///
    /// The WebSocket begins connecting immediately. Messages sent via
    /// `handle()` before `onopen` fires are buffered in the outbox and
    /// flushed when the connection opens.
    ///
    /// # Panics
    ///
    /// Panics if the WebSocket cannot be constructed (invalid URL scheme,
    /// non-browser environment). The reconnecting caller
    /// ([`crate::node::Node::connect_peer_wasm`]) uses [`Self::try_new`]
    /// instead, which reports construction failures instead of panicking.
    pub fn new(url: &str, ctx: &ActorContext, allow_public_space: bool) -> Self {
        let ws = WebSocket::new(url).expect("Failed to create WebSocket");
        Self::from_socket(ws, ctx, allow_public_space)
    }

    /// Like [`Self::new`] but returns construction errors instead of
    /// panicking — the reconnect loop treats a failed construction as a
    /// failed connection attempt and backs off.
    pub fn try_new(
        url: &str,
        ctx: &ActorContext,
        allow_public_space: bool,
    ) -> Result<Self, JsValue> {
        let ws = WebSocket::new(url)?;
        Ok(Self::from_socket(ws, ctx, allow_public_space))
    }

    /// Wires an already-constructed WebSocket into a `WasmWsConn` actor.
    ///
    /// Installs the four lifecycle callbacks (`onopen` Hi + outbox flush,
    /// `onmessage` parse-and-forward, `onerror`/`onclose` actor stop) and
    /// returns the actor. Split from [`Self::new`] so the reconnect loop
    /// in `connect_peer_wasm` can feed in a fresh socket per attempt
    /// (closure wiring written once — DRY).
    pub fn from_socket(
        ws: WebSocket,
        ctx: &ActorContext,
        allow_public_space: bool,
    ) -> Self {
        let peer_id = ctx.peer_id.read().clone();
        let router = ctx.router.read().clone();
        let addr = ctx.addr.clone();
        let stop_ctx = ctx.clone();

        let outbox = Arc::new(Mutex::new(Vec::<String>::new()));

        // --- onopen: send Hi to relay, flush outbox ---
        let outbox_for_open = outbox.clone();
        let peer_id_for_open = peer_id.clone();
        let ws_for_open = ws.clone();
        let onopen: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            // Register with the relay via dam:"?" PID exchange handshake.
            // is_ack=None means initial contact — relay should respond with ack.
            let hi = Message::Hi {
                from: Addr::noop(),
                peer_id: peer_id_for_open.clone(),
                is_ack: None,
                msg_id: crate::utils::random_string(8),
            };
            let _ = ws_for_open.send_with_str(&hi.to_string());

            // Flush buffered messages.
            let mut queue = outbox_for_open.lock().unwrap();
            for text in queue.drain(..) {
                let _ = ws_for_open.send_with_str(&text);
            }
        });

        // --- onmessage: parse and forward to router ---
        let router_for_msg = router.clone();
        let addr_for_msg = addr.clone();
        let onmessage: Closure<dyn FnMut(MessageEvent)> =
            Closure::new(move |event: MessageEvent| {
                if let Some(text) = event.data().as_string() {
                    match Message::try_from(&text, addr_for_msg.clone(), allow_public_space) {
                        Ok(msgs) => {
                            for msg in msgs {
                                let _ = router_for_msg.send(msg);
                            }
                        }
                        Err(e) => {
                            web_sys::console::error_1(
                                &format!("WasmWsConn: parse error: {}", e).into(),
                            );
                        }
                    }
                }
            });

        // --- onerror ---
        let stop_ctx_err = stop_ctx.clone();
        let onerror: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            stop_ctx_err.stop();
        });

        // --- onclose: end the actor so the reconnect loop can respawn ---
        //
        // The browser fires `close` after `error` (failed connection) or on
        // its own (server closed / network drop). Stopping the actor ends
        // its run loop, which resolves the lifecycle `JoinHandle` that
        // `connect_peer_wasm`'s reconnect loop awaits — resumption there IS
        // the disconnect signal (same chain as the native `WsConn`:
        // child task ends → ctx.stop() → run breaks → handle resolves).
        // Firing alongside `onerror`'s stop is idempotent (`is_stopped`).
        let onclose: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            stop_ctx.stop();
        });

        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        onopen.forget();
        onmessage.forget();
        onerror.forget();
        onclose.forget();

        Self { ws, outbox }
    }
}

#[async_trait]
impl Actor for WasmWsConn {
    async fn pre_start(&mut self, ctx: &ActorContext) {
        // Register with local router so Puts get forwarded to this actor.
        let _ = ctx.router.read().send(Message::Hi {
            from: ctx.addr.clone(),
            peer_id: ctx.peer_id.read().clone(),
            is_ack: None,
            msg_id: crate::utils::random_string(8),
        });
    }

    async fn handle(&mut self, msg: Arc<Message>, _ctx: &ActorContext) {
        let mut buf = Vec::with_capacity(256);
        msg.to_writer(&mut buf);
        let text = std::str::from_utf8(&buf).unwrap_or("");
        // Try to send directly. If the WebSocket isn't open (CONNECTING state),
        // send_with_str throws InvalidStateError — buffer to outbox instead.
        // The onopen callback flushes the outbox when the connection opens.
        match self.ws.send_with_str(text) {
            Ok(()) => {}
            Err(_) => {
                self.outbox.lock().unwrap().push(text.to_string());
            }
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        let _ = self.ws.close();
    }
}
