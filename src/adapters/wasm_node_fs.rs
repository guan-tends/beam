//! Node.js filesystem storage adapter — persistent storage for BEAM on Node.js.
//!
//! This is the Node.js counterpart to [`RedbStorage`](crate::adapters::RedbStorage).
//! It uses Node's `node:fs/promises` API via `wasm_bindgen` module imports to
//! persist graph data as postcard-serialized files on disk.
//!
//! # Architecture
//!
//! - **Storage model**: One file per node ID (soul) inside a `beam_data/` directory
//! - **Serialization**: [`postcard`] (binary, consistent with native redb/fjall)
//! - **Cache**: In-memory `HashMap` for synchronous reads of recently written data
//! - **Write strategy**: Write-through — cache updated synchronously, disk write
//!   is fire-and-forget via `wasm_bindgen_futures::spawn_local`
//!
//! # Feature Gate
//!
//! This adapter requires the `node-fs` feature:
//!
//! ```toml
//! [features]
//! node-fs = []
//! ```
//!
//! Build with:
//! ```sh
//! wasm-pack build --target nodejs --features node-fs --no-default-features
//! ```
//!
//! # Usage from JavaScript
//!
//! ```js
//! import { Beam } from "./beam.js";
//!
//! // Persistent storage — data survives process restart
//! const beam = Beam.new_with_node_fs();
//! beam.connect("ws://relay.example.com");
//! beam.put("chat.001", "hello");
//! ```
//!
//! # Conflict Resolution
//!
//! Same as [`MemoryStorage`]: last-write-wins per child, using `updated_at`
//! timestamps. On `Get`, data is read from the cache (fast path) or from disk
//! (slow path). On `Put`, data is written to cache and disk (write-through).

#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

use crate::actor::{Actor, ActorContext};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;
use crate::utils::FxHashMap;
use arena_btreemap::BTreeMap;
use async_trait::async_trait;
use log::{error, info, warn};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

// ─── Node.js fs/promises bindings ─────────────────────────────────────
//
// These extern blocks import async functions from Node's built-in
// `fs/promises` and `path` modules. Each returns a `Promise` that we
// await via `JsFuture`. The function signatures match the Node.js API:
//
// - `readFile(path) → Promise<Buffer>` — reads file contents as a Node Buffer
// - `writeFile(path, data) → Promise<void>` — writes data to a file
// - `mkdir(path, opts) → Promise<void>` — creates a directory (recursive)
// - `join(...paths) → String` — joins path segments (synchronous)

#[wasm_bindgen(module = "node:fs/promises")]
extern "C" {
    /// Reads a file. Returns a Promise that resolves with the file contents
    /// as a Node.js `Buffer`, or rejects if the file doesn't exist.
    fn readFile(path: &str) -> js_sys::Promise;

    /// Writes data to a file, creating or overwriting it. Returns a Promise
    /// that resolves when the write completes.
    fn writeFile(path: &str, data: &js_sys::Uint8Array) -> js_sys::Promise;

    /// Creates a directory. The `{ recursive: true }` option creates parent
    /// directories as needed and doesn't error if the directory already exists.
    fn mkdir(path: &str, options: &JsValue) -> js_sys::Promise;
}

#[wasm_bindgen(module = "node:path")]
extern "C" {
    /// Joins path segments into a single path string. Synchronous.
    fn join(base: &str, name: &str) -> String;
}

/// Node.js filesystem storage adapter for WASM.
///
/// Persists graph data to the filesystem using `node:fs/promises`. Each node
/// (identified by its soul string) is stored as a single file containing
/// postcard-serialized `Children` data.
///
/// Created with [`WasmNodeFsStorage::new`] (default: `./beam_data/`) or
/// [`WasmNodeFsStorage::with_dir`] for a custom directory.
///
/// # Serialization
///
/// Uses [`postcard`] for binary serialization, consistent with native
/// [`RedbStorage`]. This ensures the same data has the same on-disk
/// representation regardless of platform.
///
/// # Async Pattern
///
/// File I/O is asynchronous via `wasm_bindgen_futures::spawn_local`. Writes
/// are fire-and-forget — the cache is updated synchronously (fast path for
/// reads), and the disk write happens in the background. Reads check the
/// cache first; on cache miss, they fall back to an async `readFile`.
pub struct WasmNodeFsStorage {
    /// In-memory write cache for fast reads of recently written data.
    /// File reads are async; the cache provides synchronous reads for
    /// data that was recently written.
    cache: Arc<RwLock<FxHashMap<String, Children>>>,
    /// Base directory for storing node files (default: `./beam_data/`).
    base_dir: String,
    /// Whether the base directory has been created and is ready for I/O.
    /// Uses `Arc<AtomicBool>` so the async mkdir callback can set it.
    db_ready: Arc<AtomicBool>,
}

// Test-only accessors for unit tests in `wasm_tests.rs`.
#[cfg(test)]
impl WasmNodeFsStorage {
    /// Returns the base directory path (test accessor).
    pub(crate) fn base_dir_str(&self) -> &str {
        &self.base_dir
    }

    /// Returns whether the storage is ready for disk I/O (test accessor).
    pub(crate) fn is_ready(&self) -> bool {
        self.db_ready.load(Ordering::SeqCst)
    }
}

impl Default for WasmNodeFsStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmNodeFsStorage {
    /// Creates a new Node.js filesystem storage adapter with the default
    /// directory (`./beam_data/`).
    ///
    /// The directory is created asynchronously during `pre_start`. Until
    /// it's ready, reads fall back to the in-memory cache and writes are
    /// buffered in the cache.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(FxHashMap::default())),
            base_dir: "beam_data".to_string(),
            db_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates a new adapter with a custom base directory.
    ///
    /// Use this when you need to control where data is stored (e.g. for
    /// tests or when integrating with an application that manages its own
    /// data directory).
    pub fn with_dir(dir: &str) -> Self {
        Self {
            cache: Arc::new(RwLock::new(FxHashMap::default())),
            base_dir: dir.to_string(),
            db_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the filesystem path for a given node ID (soul).
    ///
    /// Uses `node:path.join` to ensure correct path separators on all platforms.
    /// Empty node IDs are replaced with `_root` to avoid writing to the
    /// directory itself.
    fn node_path(&self, node_id: &str) -> String {
        let safe_name = if node_id.is_empty() { "_root" } else { node_id };
        join(&self.base_dir, safe_name)
    }

    /// Serializes `Children` to postcard bytes for disk storage.
    ///
    /// This is the same format used by native `RedbStorage` and `FjallStorage`,
    /// ensuring cross-platform on-disk consistency.
    fn serialize_children(children: &Children) -> Vec<u8> {
        postcard::to_allocvec(children).unwrap_or_default()
    }

    /// Deserializes `Children` from postcard bytes.
    ///
    /// Returns an empty `BTreeMap` on error (graceful degradation —
    /// the node is treated as having no children).
    fn deserialize_children(bytes: &[u8]) -> Children {
        if bytes.is_empty() {
            return BTreeMap::default();
        }
        match postcard::from_bytes::<Children>(bytes) {
            Ok(children) => children,
            Err(e) => {
                warn!("WasmNodeFsStorage: deserialize error: {}", e);
                BTreeMap::default()
            }
        }
    }

    /// Handles a `Get` request by checking the cache first, then disk.
    ///
    /// Cache hit → immediate reply (fast path).
    /// Cache miss → async `readFile`, reply when data arrives.
    /// File not found → reply with empty children (so `.map()` listeners
    /// don't hang).
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        // Fast path: cache hit
        if let Some(children) = self.cache.read().get(&get.node_id).cloned() {
            self.reply_get(&get, children, ctx);
            return;
        }

        // Slow path: read from disk asynchronously
        let path = self.node_path(&get.node_id);
        let node_id = get.node_id.clone();
        let from = get.from.clone();
        let get_id = get.id.clone();
        let child_key = get.child_key.clone();
        let my_addr = ctx.addr.clone();
        let cache = self.cache.clone();

        spawn_local(async move {
            let promise = readFile(&path);
            match wasm_bindgen_futures::JsFuture::from(promise).await {
                Ok(result) => {
                    let bytes = js_sys::Uint8Array::from(result);
                    let raw = bytes.to_vec();
                    let children = WasmNodeFsStorage::deserialize_children(&raw);
                    // Cache the result for future reads
                    cache.write().insert(node_id.clone(), children.clone());
                    // Reply with the requested data
                    let reply_children = match &child_key {
                        Some(ck) => match children.get(ck) {
                            Some(cv) => {
                                let mut r = BTreeMap::default();
                                r.insert(ck.clone(), cv.clone());
                                r
                            }
                            None => return,
                        },
                        None => children,
                    };
                    let mut reply_nodes = BTreeMap::default();
                    reply_nodes.insert(node_id, reply_children);
                    let put = Put::new(reply_nodes, Some(get_id), my_addr);
                    let _ = from.send(Message::Put(put));
                }
                Err(_e) => {
                    // File not found — reply with empty children
                    let mut reply_nodes = BTreeMap::default();
                    reply_nodes.insert(node_id, BTreeMap::default());
                    let put = Put::new(reply_nodes, Some(get_id), my_addr);
                    let _ = from.send(Message::Put(put));
                }
            }
        });
    }

    /// Replies to a `Get` with children data from the cache.
    fn reply_get(&self, get: &Get, children: Children, ctx: &ActorContext) {
        let reply_children = match &get.child_key {
            Some(ck) => match children.get(ck) {
                Some(cv) => {
                    let mut r = BTreeMap::default();
                    r.insert(ck.clone(), cv.clone());
                    r
                }
                None => return,
            },
            None => children,
        };
        let mut reply_nodes = BTreeMap::default();
        reply_nodes.insert(get.node_id.clone(), reply_children);
        let put = Put::new(reply_nodes, Some(get.id.clone()), ctx.addr.clone());
        let _ = get.from.send(Message::Put(put));
    }

    /// Handles a `Put` by merging into cache (synchronous) and writing
    /// through to disk (async, fire-and-forget).
    ///
    /// Conflict resolution: last-write-wins per child using `updated_at`.
    fn handle_put(&mut self, put: Put, ctx: &ActorContext) {
        // Apply to cache first (synchronous, fast path)
        for (node_id, update_data) in put.updated_nodes.iter().rev() {
            let mut write = self.cache.write();
            if let Some(children) = write.get_mut(node_id) {
                for (child_id, child_data) in update_data {
                    if let Some(existing) = children.get(child_id) {
                        if child_data.updated_at >= existing.updated_at {
                            children.insert(child_id.clone(), child_data.clone());
                        }
                    } else {
                        children.insert(child_id.clone(), child_data.clone());
                    }
                }
            } else {
                write.insert(node_id.to_string(), update_data.clone());
            }
        }

        // Write through to disk (async, fire-and-forget)
        if self.db_ready.load(Ordering::SeqCst) {
            for (node_id, _update_data) in put.updated_nodes.iter() {
                // `_update_data` is intentionally unused — we read the
                // merged value from the cache rather than the individual
                // update, because the cache contains the full merged state.
                let path = self.node_path(node_id);
                // Read the current cached value (which includes the merge we just did)
                let children = self.cache.read().get(node_id).cloned().unwrap_or_default();
                let bytes = WasmNodeFsStorage::serialize_children(&children);
                let js_bytes = js_sys::Uint8Array::from(&bytes[..]);
                spawn_local(async move {
                    let promise = writeFile(&path, &js_bytes);
                    if let Err(e) = wasm_bindgen_futures::JsFuture::from(promise).await {
                        error!("WasmNodeFsStorage: writeFile error: {:?}", e);
                    }
                });
            }
        }

        // Send ack
        self.send_put_ack(&put, ctx);
    }

    /// Handles a `BatchPut` by applying each put to cache, then writing
    /// all to disk, and sending a single ack.
    fn handle_batch_put(&mut self, batch: BatchPut, ctx: &ActorContext) {
        // Apply all puts to cache
        for put in batch.puts.iter() {
            for (node_id, update_data) in put.updated_nodes.iter().rev() {
                let mut write = self.cache.write();
                if let Some(children) = write.get_mut(node_id) {
                    for (child_id, child_data) in update_data {
                        if let Some(existing) = children.get(child_id) {
                            if child_data.updated_at >= existing.updated_at {
                                children.insert(child_id.clone(), child_data.clone());
                            }
                        } else {
                            children.insert(child_id.clone(), child_data.clone());
                        }
                    }
                } else {
                    write.insert(node_id.to_string(), update_data.clone());
                }
            }
        }

        // Write through to disk
        if self.db_ready.load(Ordering::SeqCst) {
            for put in batch.puts.iter() {
                for (node_id, _update_data) in put.updated_nodes.iter() {
                    // `_update_data` is intentionally unused here — we
                    // read from the cache (which already contains the
                    // merged result) rather than the individual update.
                    let path = self.node_path(node_id);
                    let children = self.cache.read().get(node_id).cloned().unwrap_or_default();
                    let bytes = WasmNodeFsStorage::serialize_children(&children);
                    let js_bytes = js_sys::Uint8Array::from(&bytes[..]);
                    spawn_local(async move {
                        let promise = writeFile(&path, &js_bytes);
                        if let Err(e) = wasm_bindgen_futures::JsFuture::from(promise).await {
                            error!("WasmNodeFsStorage: batch writeFile error: {:?}", e);
                        }
                    });
                }
            }
        }

        // Send batch ack
        let mut ack_children = BTreeMap::default();
        ack_children.insert(
            "_ack".to_string(),
            NodeData {
                value: Value::Text("ok".to_string()),
                updated_at: web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64,
            },
        );
        let mut nodes = BTreeMap::default();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(batch.id.clone()), ctx.addr.clone());
        let _ = batch.from.send(Message::Put(ack));
    }

    /// Sends a put ack back to the originating node.
    fn send_put_ack(&self, put: &Put, ctx: &ActorContext) {
        let mut ack_children = BTreeMap::default();
        ack_children.insert(
            "_ack".to_string(),
            NodeData {
                value: Value::Text("ok".to_string()),
                updated_at: web_time::SystemTime::now()
                    .duration_since(web_time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64,
            },
        );
        let mut nodes = BTreeMap::default();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(put.id.clone()), ctx.addr.clone());
        let _ = put.from.send(Message::Put(ack));
    }
}

#[async_trait]
impl Actor for WasmNodeFsStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        info!(
            "WasmNodeFsStorage adapter starting (dir: {})",
            self.base_dir
        );

        // Create the base directory with { recursive: true } asynchronously.
        // Uses spawn_local because JsFuture is !Send (Rc-based) and cannot
        // be awaited directly in an async_trait fn (which requires Send).
        // The db_ready flag is set when mkdir completes — until then, writes
        // go to cache only and reads fall back to cache.
        let base_dir = self.base_dir.clone();
        let db_ready = self.db_ready.clone();

        spawn_local(async move {
            let opts = js_sys::Object::new();
            js_sys::Reflect::set(&opts, &"recursive".into(), &true.into()).ok();
            let promise = mkdir(&base_dir, &opts);
            match wasm_bindgen_futures::JsFuture::from(promise).await {
                Ok(_) => {
                    db_ready.store(true, Ordering::SeqCst);
                    info!("WasmNodeFsStorage: directory ready: {}", base_dir);
                }
                Err(e) => {
                    error!("WasmNodeFsStorage: mkdir error: {:?}", e);
                }
            }
        });
    }

    async fn handle(&mut self, message: Arc<Message>, ctx: &ActorContext) {
        match &*message {
            Message::Get(get) => self.handle_get(get.clone(), ctx),
            Message::Put(put) => self.handle_put(put.clone(), ctx),
            Message::Flush(flush) => {
                // File writes are fire-and-forget (spawned as microtasks).
                // There's no fsync barrier — we ack immediately so callers
                // don't hang. Data is in the OS page cache after writeFile
                // resolves.
                let mut ack = BTreeMap::default();
                ack.insert(
                    "_flushed".to_string(),
                    NodeData {
                        value: Value::Text("true".to_string()),
                        updated_at: web_time::SystemTime::now()
                            .duration_since(web_time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                );
                let mut nodes = BTreeMap::default();
                nodes.insert("_ack".to_string(), ack);
                let put = Put::new(nodes, Some(flush.id.clone()), ctx.addr.clone());
                let _ = flush.from.send(Message::Put(put));
            }
            Message::BatchPut(batch) => self.handle_batch_put(batch.clone(), ctx),
            _ => {}
        }
    }

    /// WASM is single-threaded — no benefit from read/write splitting.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        None
    }
}
