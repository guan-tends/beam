//! JavaScript bindings for BEAM in the browser.
//!
//! Provides a `#[wasm_bindgen]` API exposing BEAM's core graph operations
//! to JavaScript. Async operations (put, get) are bridged via
//! `wasm_bindgen_futures` — `put` is fire-and-forget, `get` returns a
//! `Promise`, and `on` registers a callback for real-time subscriptions.
//!
//! # Usage from JavaScript
//!
//! ```js
//! import init, { Beam } from "./beam.js";
//! await init();
//!
//! const beam = new Beam();
//! beam.connect("wss://relay.example.com/ws");
//!
//! // Write
//! beam.put("chat.123", "Hello!");
//!
//! // Read once
//! const val = await beam.get("chat.123"); // "Hello!"
//!
//! // Subscribe to child updates (Gun.js .on() semantics)
//! beam.on("chat", (value) => console.log("new message:", value));
//! ```

use crate::adapters::WasmIdbStorage;
use crate::node::Node;
use crate::Config;
use crate::types::Value;
use std::sync::OnceLock;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

// ─── Tokio runtime for WASM ───
// On native, `#[tokio::main]` provides the runtime. In the browser, we create
// a current-thread runtime once and enter its context for every JS call.
// The `time` feature is NOT enabled — tokio's timer wheel calls
// `std::time::Instant::now()` which panics on `wasm32-unknown-unknown`.
// Timer functions (sleep, timeout, interval) come from `tokio_with_wasm`
// via the `tokio_time` shim module, backed by JS `setTimeout`/`setInterval`.
static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("failed to create tokio runtime")
    })
}

/// JavaScript-facing BEAM API.
///
/// Wraps a [`Node`] and exposes a simplified interface for browser use.
/// Each `Beam` instance is an independent node in the P2P mesh.
#[wasm_bindgen]
pub struct Beam {
    node: Node,
}

#[wasm_bindgen]
impl Beam {
    /// Creates a new BEAM node with in-memory storage.
    ///
    /// Data is lost when the page reloads. For persistence, use
    /// [`new_persistent()`](Self::new_persistent) instead.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Beam {
        console_error_panic_hook::set_once();
        let _guard = runtime().enter();
        web_sys::console::log_1(&"Beam::new() — creating node".into());
        let beam = Beam {
            node: Node::new(),
        };
        web_sys::console::log_1(&"Beam::new() — node created".into());
        beam
    }

    /// Creates a new BEAM node with IndexedDB persistent storage.
    ///
    /// Data survives page reloads. The IndexedDB database opens
    /// asynchronously — writes are buffered until the DB is ready,
    /// then flushed automatically.
    pub fn new_persistent() -> Beam {
        console_error_panic_hook::set_once();
        let _guard = runtime().enter();
        Beam {
            node: Node::new_with_config(
                Config::default(),
                vec![Box::new(WasmIdbStorage::new())],
                Vec::new(),
            ),
        }
    }

    /// Connects to a relay server via WebSocket.
    ///
    /// The connection is asynchronous — data will start flowing once the
    /// WebSocket handshake completes. You can call `put()` immediately;
    /// messages will be queued and sent once connected.
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket URL (e.g. `"wss://relay.example.com/ws"`)
    pub fn connect(&self, url: &str) {
        let _guard = runtime().enter();
        web_sys::console::log_1(&format!("connect: {}", url).into());
        self.node.connect_peer_wasm(url);
    }

    // ─── Write operations (fire-and-forget) ───
    // `Node::put()` is async (awaits a storage ack). In the browser we
    // fire-and-forget via `spawn_local` — the value is written to the
    // local graph immediately, then replicated to peers when connected.

    /// Writes a string value to the graph at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Dot-separated path (e.g. `"chat.123"`)
    /// * `value` - String to store
    pub fn put(&mut self, path: &str, value: &str) {
        let _guard = runtime().enter();
        web_sys::console::log_1(&format!("put: path={} value={}", path, value).into());
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        let value = value.to_string();
        let path_log = path.to_string();
        spawn_local(async move {
            web_sys::console::log_1(&format!("put.spawn: path={} starting", path_log).into());
            match node.put(Value::Text(value)).await {
                Ok(()) => web_sys::console::log_1(&format!("put.spawn: path={} OK", path_log).into()),
                Err(e) => web_sys::console::log_1(&format!("put.spawn: path={} ERR: {}", path_log, e).into()),
            }
        });
    }

    /// Writes a numeric value to the graph at the given path.
    pub fn put_num(&mut self, path: &str, value: f64) {
        let _guard = runtime().enter();
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        spawn_local(async move {
            let _ = node.put(Value::Number(value)).await;
        });
    }

    /// Writes a boolean value to the graph at the given path.
    pub fn put_bool(&mut self, path: &str, value: bool) {
        let _guard = runtime().enter();
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        spawn_local(async move {
            let _ = node.put(Value::Bit(value)).await;
        });
    }

    /// Writes a null value to the graph at the given path.
    pub fn put_null(&mut self, path: &str) {
        let _guard = runtime().enter();
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        spawn_local(async move {
            let _ = node.put(Value::Null).await;
        });
    }

    // ─── Read operations ───

    /// Reads the value at the given path once.
    ///
    /// Returns a `Promise` that resolves to the value (string) or `null`
    /// if not found within the timeout (default 66ms, matching Gun.js).
    ///
    /// ```js
    /// const val = await beam.get("chat.123");
    /// if (val) console.log("got:", val);
    /// ```
    #[wasm_bindgen(js_name = get)]
    pub fn get(&mut self, path: &str) -> js_sys::Promise {
        let _guard = runtime().enter();
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));

        wasm_bindgen_futures::future_to_promise(async move {
            match node.once(None).await {
                Some(Value::Text(s)) => Ok(JsValue::from(s)),
                Some(Value::Number(n)) => Ok(JsValue::from(n)),
                Some(Value::Bit(b)) => Ok(JsValue::from(b)),
                Some(Value::Link(s)) => Ok(JsValue::from(s)),
                Some(Value::Null) | None => Ok(JsValue::NULL),
            }
        })
    }

    /// Subscribes to child updates at the given path.
    ///
    /// Uses Gun.js `.on()` semantics: the callback fires for each child
    /// value under the path, not just the path's own value. For example,
    /// `beam.on("chat", cb)` fires for each message written to
    /// `chat.<timestamp>`.
    ///
    /// The subscription lives until `stop()` is called or the `Beam`
    /// instance is dropped.
    ///
    /// ```js
    /// beam.on("chat", (value) => {
    ///   console.log("new message:", value);
    /// });
    /// ```
    pub fn on(&mut self, path: &str, callback: js_sys::Function) {
        let _guard = runtime().enter();
        web_sys::console::log_1(&format!("on: path={}", path).into());
        let node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        let mut rx = node.map();
        let path_log = path.to_string();

        spawn_local(async move {
            web_sys::console::log_1(&format!("on.spawn: path={} started, waiting for map() events", path_log).into());
            loop {
                match rx.recv().await {
                    Ok((_key, value)) => {
                        web_sys::console::log_1(&format!("on.spawn: path={} received key={} value={:?}", path_log, _key, value).into());
                        let js_val = match value {
                            Value::Text(s) => JsValue::from(s),
                            Value::Number(n) => JsValue::from(n),
                            Value::Bit(b) => JsValue::from(b),
                            Value::Link(s) => JsValue::from(s),
                            Value::Null => JsValue::NULL,
                        };
                        // Skip internal control keys
                        if js_val.is_null() {
                            continue;
                        }
                        // Fire callback — errors are silently ignored
                        // (callback may have been GC'd or page unloaded).
                        let _ = callback.call1(&JsValue::UNDEFINED, &js_val);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    /// Stops the node and closes all connections.
    pub fn stop(&mut self) {
        let _guard = runtime().enter();
        self.node.stop();
    }
}
