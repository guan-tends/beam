//! JavaScript bindings for BEAM in the browser.
//!
//! This module provides a `#[wasm_bindgen]` API that exposes BEAM's core
//! functionality to JavaScript. It wraps [`Node`] in a JS-friendly interface.
//!
//! # Usage from JavaScript
//!
//! ```js
//! import init, { Beam } from "./beam.js";
//! await init();
//!
//! const beam = Beam.new();              // in-memory (lost on reload)
//! // or: const beam = Beam.new_persistent();  // IndexedDB (survives reload)
//! beam.connect("wss://relay.example.com/ws");
//! beam.put("greeting", "Hello from browser!");
//! const value = beam.get("greeting"); // "Hello from browser!"
//! ```

use crate::adapters::WasmIdbStorage;
use crate::node::Node;
use crate::Config;
use crate::types::Value;
use wasm_bindgen::prelude::*;

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
    ///
    /// Call `connect()` to join a relay mesh, then `put()` / `get()` to read/write.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Beam {
        Beam {
            node: Node::new(),
        }
    }

    /// Creates a new BEAM node with IndexedDB persistent storage.
    ///
    /// Data survives page reloads. The IndexedDB database opens
    /// asynchronously — writes are buffered until the DB is ready,
    /// then flushed automatically.
    ///
    /// ```js
    /// const beam = Beam.new_persistent();
    /// beam.connect("wss://relay.example.com/ws");
    /// beam.put("app.theme", "dark");
    /// // Reload page — "app.theme" is still "dark"
    /// ```
    pub fn new_persistent() -> Beam {
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
        self.node.connect_peer_wasm(url);
    }

    /// Writes a value to the graph at the given path.
    ///
    /// The value is replicated to all connected peers. If no peers are
    /// connected yet, the value is stored locally and will sync when
    /// a connection is established.
    ///
    /// # Arguments
    ///
    /// * `path` - Dot-separated path (e.g. `"users.alice.name"`)
    /// * `value` - Value to store (string, number, boolean, or null)
    pub fn put(&mut self, path: &str, value: &str) {
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        node.put(Value::Text(value.to_string()));
    }

    /// Writes a numeric value to the graph at the given path.
    pub fn put_num(&mut self, path: &str, value: f64) {
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        node.put(Value::Number(value));
    }

    /// Writes a boolean value to the graph at the given path.
    pub fn put_bool(&mut self, path: &str, value: bool) {
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        node.put(Value::Bit(value));
    }

    /// Writes a null value to the graph at the given path.
    pub fn put_null(&mut self, path: &str) {
        let mut node = path.split('.').fold(self.node.clone(), |mut n, key| n.get(key));
        node.put(Value::Null);
    }

    /// Stops the node and closes all connections.
    pub fn stop(&mut self) {
        self.node.stop();
    }
}
