//! WASM WebSocket adapter — bridges Gun protocol messages over WebSocket in the browser.
//!
//! This is the browser counterpart to [`WsConn`](crate::adapters::WsConn).
//! Instead of tokio-tungstenite, it uses `web_sys::WebSocket` via `wasm-bindgen`.
//!
//! # Architecture
//!
//! Browser BEAM is a client-only node. It connects to relay servers via
//! WebSocket. This adapter:
//!
//! - Opens a WebSocket connection to a relay URL
//! - Sends outgoing [`Message`]s as text frames
//! - Receives incoming text frames, parses via [`Message::try_from`], forwards to router
//! - Sends `Message::Hi` on connection open to register with the router
//!
//! # Browser Constraints
//!
//! - Single-threaded: all async work runs on the browser's main thread
//! - WebSocket API is callback-based — closures are registered on the WebSocket
//!   and leaked to the JS heap (they live as long as the connection)
//! - The `WasmWsConn` actor only holds the `WebSocket` handle for sending;
//!   incoming messages are forwarded to the router directly from the callback

use crate::actor::{Actor, ActorContext};
use crate::message::Message;
use async_trait::async_trait;
use log::{debug, error, info};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{MessageEvent, WebSocket};

/// A WebSocket connection actor for browser/WASM environments.
///
/// Created by [`Node::connect_peer_wasm`](crate::Node::connect_peer_wasm).
/// Each `WasmWsConn` manages a single WebSocket connection to a relay server
/// and translates between the Gun wire format (text) and [`Message`] enum values.
pub struct WasmWsConn {
    ws: WebSocket,
}

impl WasmWsConn {
    /// Creates a new `WasmWsConn` by opening a WebSocket to the given URL.
    ///
    /// The connection is asynchronous — messages will flow once the WebSocket
    /// fires its `open` event. The `Hi` message is sent on open.
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket URL (e.g. `"wss://relay.example.com/ws"`)
    /// * `ctx` - Actor context for sending Hi and forwarding messages to router
    /// * `allow_public_space` - Whether to accept public space writes
    ///
    /// # Panics
    ///
    /// Panics if the WebSocket cannot be created (invalid URL, etc.)
    pub fn new(url: &str, ctx: &ActorContext, allow_public_space: bool) -> Self {
        let ws = WebSocket::new(url).expect("Failed to create WebSocket");
        let peer_id = ctx.peer_id.read().clone();
        let router = ctx.router.clone();
        let addr = ctx.addr.clone();
        let mut stop_ctx = ctx.clone();

        // On open: send Hi message
        let ws_for_open = ws.clone();
        let peer_id_for_open = peer_id.clone();
        let addr_for_open = addr.clone();
        let onopen: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            info!("WasmWsConn connected to relay");
            let hi = Message::Hi {
                from: addr_for_open.clone(),
                peer_id: peer_id_for_open.clone(),
            };
            let _ = ws_for_open.send_with_str(&hi.to_string());
        });

        // On message: parse and forward to router
        let router_for_msg = router.clone();
        let addr_for_msg = addr.clone();
        let onmessage: Closure<dyn FnMut(MessageEvent)> = Closure::new(move |event: MessageEvent| {
            if let Some(text) = event.data().as_string() {
                debug!("WasmWsConn received: {} bytes", text.len());
                match Message::try_from(&text, addr_for_msg.clone(), allow_public_space) {
                    Ok(msgs) => {
                        for msg in msgs.into_iter() {
                            if router_for_msg.send(msg).is_err() {
                                error!("WasmWsConn: failed to forward message to router");
                            }
                        }
                    }
                    Err(e) => {
                        error!("WasmWsConn: parse error: {}", e);
                    }
                }
            }
        });

        // On error: log and stop
        let onerror: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
            error!("WasmWsConn WebSocket error: {:?}", event);
            stop_ctx.stop();
        });

        // On close: log
        let onclose: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            info!("WasmWsConn disconnected from relay");
        });

        // Register callbacks — leak closures so they live as long as the WebSocket.
        // This is the standard wasm-bindgen pattern for event handlers that
        // should persist for the lifetime of the JS object.
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        // Prevent Rust from dropping the closures (which would unregister them)
        onopen.forget();
        onmessage.forget();
        onerror.forget();
        onclose.forget();

        Self { ws }
    }
}

#[async_trait]
impl Actor for WasmWsConn {
    async fn handle(&mut self, msg: Message, _ctx: &ActorContext) {
        let text = msg.to_string();
        if let Err(e) = self.ws.send_with_str(&text) {
            error!("WasmWsConn: failed to send: {:?}", e);
        }
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        info!("WasmWsConn stopping — closing WebSocket");
        let _ = self.ws.close();
    }
}
