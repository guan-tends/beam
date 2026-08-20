//! OPFS (Origin Private File System) storage adapter — persistent browser storage.
//!
//! Uses the [OPFS API](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system)
//! to store graph data as postcard-serialized files in the browser's
//! private file system. Data persists across page reloads and browser
//! restarts.
//!
//! # Architecture
//!
//! - **Storage model**: One file per node ID (soul) in the OPFS root directory
//! - **Serialization**: [`postcard`] (binary, consistent with native redb/fjall
//!   and the Node.js fs adapter)
//! - **Cache**: In-memory `HashMap` for synchronous reads of recently written data
//! - **Write strategy**: Write-through — cache updated synchronously, OPFS write
//!   is fire-and-forget via `wasm_bindgen_futures::spawn_local`
//!
//! # Browser Support
//!
//! OPFS is supported in Chrome 86+, Firefox 111+, and Safari 15.2+.
//! Requires a secure context (HTTPS or localhost).
//!
//! # Usage from JavaScript
//!
//! ```js
//! import init, { Beam } from "./beam.js";
//! await init();
//!
//! // Persistent storage — data survives page reload
//! const beam = Beam.new_with_opfs();
//! beam.connect("ws://relay.example.com");
//! beam.put("chat.001", "hello");
//!
//! // Reload the page — data is still there.
//! ```
//!
//! # Conflict Resolution
//!
//! Same as [`MemoryStorage`]: last-write-wins per child, using `updated_at`
//! timestamps. On `Get`, data is read from the cache (fast path) or from OPFS
//! (slow path). On `Put`, data is written to cache and OPFS (write-through).

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
use wasm_bindgen::{JsCast, UnwrapThrowExt};
use wasm_bindgen_futures::spawn_local;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions,
    FileSystemWritableFileStream, Navigator, WritableStream,
};

/// OPFS storage adapter for browser WASM.
///
/// Persists graph data to the browser's Origin Private File System using
/// postcard serialization. Each node (identified by its soul string) is
/// stored as a single file. Data survives page reloads and browser restarts.
///
/// Created with [`WasmOpfsStorage::new`] (default directory name `"beam"`)
/// or [`WasmOpfsStorage::with_name`] for a custom directory name.
///
/// # Serialization
///
/// Uses [`postcard`] for binary serialization, consistent with native
/// [`RedbStorage`](crate::adapters::RedbStorage) and the Node.js fs adapter.
///
/// # Async Pattern
///
/// All OPFS operations are asynchronous. The adapter uses the same
/// `spawn_local` + `JsFuture` pattern as [`WasmNodeFsStorage`] — the
/// `pre_start` method spawns an async task to acquire the OPFS root
/// directory handle, and read/write operations are fire-and-forget.
pub struct WasmOpfsStorage {
    /// In-memory write cache for fast reads of recently written data.
    cache: Arc<RwLock<FxHashMap<String, Children>>>,
    /// OPFS root directory handle (set asynchronously during pre_start).
    root_dir: Arc<RwLock<Option<FileSystemDirectoryHandle>>>,
    /// Whether the OPFS directory handle is ready for I/O.
    db_ready: Arc<AtomicBool>,
    /// Directory name within OPFS (default: "beam").
    dir_name: String,
}

impl Default for WasmOpfsStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmOpfsStorage {
    /// Creates a new OPFS storage adapter with the default directory name
    /// (`"beam"`).
    ///
    /// The OPFS directory is opened asynchronously during `pre_start`.
    /// Until it's ready, reads fall back to the in-memory cache and writes
    /// are buffered in the cache.
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(FxHashMap::default())),
            root_dir: Arc::new(RwLock::new(None)),
            db_ready: Arc::new(AtomicBool::new(false)),
            dir_name: "beam".to_string(),
        }
    }

    /// Creates a new adapter with a custom OPFS directory name.
    ///
    /// Use this when multiple BEAM instances need separate storage in the
    /// same origin, or for testing.
    pub fn with_name(name: &str) -> Self {
        Self {
            cache: Arc::new(RwLock::new(FxHashMap::default())),
            root_dir: Arc::new(RwLock::new(None)),
            db_ready: Arc::new(AtomicBool::new(false)),
            dir_name: name.to_string(),
        }
    }

    /// Serializes `Children` to postcard bytes for disk storage.
    fn serialize_children(children: &Children) -> Vec<u8> {
        postcard::to_allocvec(children).unwrap_or_default()
    }

    /// Deserializes `Children` from postcard bytes.
    fn deserialize_children(bytes: &[u8]) -> Children {
        if bytes.is_empty() {
            return BTreeMap::default();
        }
        match postcard::from_bytes::<Children>(bytes) {
            Ok(children) => children,
            Err(e) => {
                warn!("WasmOpfsStorage: deserialize error: {}", e);
                BTreeMap::default()
            }
        }
    }

    /// Sanitizes a node ID for use as a file name.
    /// Empty node IDs are replaced with `_root`.
    fn safe_name(node_id: &str) -> String {
        if node_id.is_empty() {
            "_root".to_string()
        } else {
            node_id.to_string()
        }
    }

    /// Handles a `Get` request by checking the cache first, then OPFS.
    ///
    /// Cache hit → immediate reply (fast path).
    /// Cache miss → async OPFS read, reply when data arrives.
    /// File not found → reply with empty children.
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        // Fast path: cache hit
        if let Some(children) = self.cache.read().get(&get.node_id).cloned() {
            self.reply_get(&get, children, ctx);
            return;
        }

        // Slow path: read from OPFS asynchronously
        let node_id = get.node_id.clone();
        let from = get.from.clone();
        let get_id = get.id.clone();
        let child_key = get.child_key.clone();
        let my_addr = ctx.addr.clone();
        let cache = self.cache.clone();
        let root_dir = self.root_dir.clone();

        spawn_local(async move {
            let dir = root_dir.read().clone();
            let Some(dir) = dir else {
                // OPFS not ready — reply with empty
                let mut reply_nodes = BTreeMap::default();
                reply_nodes.insert(node_id, BTreeMap::default());
                let put = Put::new(reply_nodes, Some(get_id), my_addr);
                let _ = from.send(Message::Put(put));
                return;
            };

            let file_name = WasmOpfsStorage::safe_name(&node_id);
            let promise = FileSystemDirectoryHandle::get_file_handle(&dir, &file_name);
            match wasm_bindgen_futures::JsFuture::from(promise).await {
                Ok(handle_js) => {
                    let file_handle: FileSystemFileHandle = handle_js.into();
                    let file_promise = FileSystemFileHandle::get_file(&file_handle);
                    match wasm_bindgen_futures::JsFuture::from(file_promise).await {
                        Ok(file_js) => {
                            let file: web_sys::File = file_js.into();
                            let buf_promise = file.array_buffer();
                            match wasm_bindgen_futures::JsFuture::from(buf_promise).await {
                                Ok(buf_js) => {
                                    let array_buffer = js_sys::ArrayBuffer::from(buf_js);
                                    let bytes = js_sys::Uint8Array::new(&array_buffer);
                                    let raw = bytes.to_vec();
                                    let children = WasmOpfsStorage::deserialize_children(&raw);
                                    cache.write().insert(node_id.clone(), children.clone());

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
                                Err(e) => {
                                    error!("WasmOpfsStorage: arrayBuffer error: {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("WasmOpfsStorage: getFile error: {:?}", e);
                        }
                    }
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
    /// through to OPFS (async, fire-and-forget).
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

        // Write through to OPFS (async, fire-and-forget)
        if self.db_ready.load(Ordering::SeqCst) {
            let root_dir = self.root_dir.read().clone();
            if let Some(dir) = root_dir {
                for (node_id, _update_data) in put.updated_nodes.iter() {
                    let file_name = WasmOpfsStorage::safe_name(node_id);
                    let children = self.cache.read().get(node_id).cloned().unwrap_or_default();
                    let bytes = WasmOpfsStorage::serialize_children(&children);
                    // bytes used directly — write_with_u8_array takes &[u8]
                    let dir = dir.clone();

                    spawn_local(async move {
                        // Get file handle (create if doesn't exist)
                        let opts = FileSystemGetFileOptions::new();
                        opts.set_create(true);
                        let promise = FileSystemDirectoryHandle::get_file_handle_with_options(
                            &dir, &file_name, &opts,
                        );
                        match wasm_bindgen_futures::JsFuture::from(promise).await {
                            Ok(handle_js) => {
                                let file_handle: FileSystemFileHandle = handle_js.into();
                                let writable_promise =
                                    FileSystemFileHandle::create_writable(&file_handle);
                                match wasm_bindgen_futures::JsFuture::from(writable_promise).await {
                                    Ok(stream_js) => {
                                        let stream: FileSystemWritableFileStream = stream_js.into();
                                        // Write postcard bytes
                                        let write_promise =
                                            stream.write_with_u8_array(&bytes).unwrap_throw();
                                        if let Err(e) =
                                            wasm_bindgen_futures::JsFuture::from(write_promise)
                                                .await
                                        {
                                            error!("WasmOpfsStorage: write error: {:?}", e);
                                        }
                                        // Close the stream (flushes to disk)
                                        let close_promise =
                                            stream.unchecked_into::<WritableStream>().close();
                                        if let Err(e) =
                                            wasm_bindgen_futures::JsFuture::from(close_promise)
                                                .await
                                        {
                                            error!("WasmOpfsStorage: close error: {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        error!("WasmOpfsStorage: createWritable error: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("WasmOpfsStorage: getFileHandle error: {:?}", e);
                            }
                        }
                    });
                }
            }
        }

        // Send ack
        self.send_put_ack(&put, ctx);
    }

    /// Handles a `BatchPut` by applying each put to cache, then writing
    /// all to OPFS, and sending a single ack.
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

        // Write through to OPFS
        if self.db_ready.load(Ordering::SeqCst) {
            let root_dir = self.root_dir.read().clone();
            if let Some(dir) = root_dir {
                for put in batch.puts.iter() {
                    for (node_id, _update_data) in put.updated_nodes.iter() {
                        let file_name = WasmOpfsStorage::safe_name(node_id);
                        let children = self.cache.read().get(node_id).cloned().unwrap_or_default();
                        let bytes = WasmOpfsStorage::serialize_children(&children);
                        // bytes used directly — write_with_u8_array takes &[u8]
                        let dir = dir.clone();

                        spawn_local(async move {
                            let opts = FileSystemGetFileOptions::new();
                            opts.set_create(true);
                            let promise = FileSystemDirectoryHandle::get_file_handle_with_options(
                                &dir, &file_name, &opts,
                            );
                            if let Ok(handle_js) =
                                wasm_bindgen_futures::JsFuture::from(promise).await
                            {
                                let file_handle: FileSystemFileHandle = handle_js.into();
                                if let Ok(stream_js) = wasm_bindgen_futures::JsFuture::from(
                                    FileSystemFileHandle::create_writable(&file_handle),
                                )
                                .await
                                {
                                    let stream: FileSystemWritableFileStream = stream_js.into();
                                    let write_promise =
                                        stream.write_with_u8_array(&bytes).unwrap_throw();
                                    let _ =
                                        wasm_bindgen_futures::JsFuture::from(write_promise).await;
                                    let _ = wasm_bindgen_futures::JsFuture::from(
                                        stream.unchecked_into::<WritableStream>().close(),
                                    )
                                    .await;
                                }
                            }
                        });
                    }
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
impl Actor for WasmOpfsStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        info!("WasmOpfsStorage adapter starting (dir: {})", self.dir_name);

        // Acquire the OPFS root directory handle asynchronously.
        // Uses spawn_local because JsFuture is !Send.
        let dir_name = self.dir_name.clone();
        let root_dir = self.root_dir.clone();
        let db_ready = self.db_ready.clone();

        spawn_local(async move {
            // navigator.storage.getDirectory() returns a Promise<FileSystemDirectoryHandle>
            let navigator: Navigator = web_sys::window().expect("no window").navigator();
            let storage = navigator.storage();
            let promise = storage.get_directory();
            match wasm_bindgen_futures::JsFuture::from(promise).await {
                Ok(handle_js) => {
                    let root: FileSystemDirectoryHandle = handle_js.into();

                    // Create a sub-directory for BEAM data
                    let opts = web_sys::FileSystemGetDirectoryOptions::new();
                    opts.set_create(true);
                    let sub_promise = FileSystemDirectoryHandle::get_directory_handle_with_options(
                        &root, &dir_name, &opts,
                    );
                    match wasm_bindgen_futures::JsFuture::from(sub_promise).await {
                        Ok(sub_js) => {
                            let sub: FileSystemDirectoryHandle = sub_js.into();
                            *root_dir.write() = Some(sub);
                            db_ready.store(true, Ordering::SeqCst);
                            info!("WasmOpfsStorage: directory ready: {}", dir_name);
                        }
                        Err(e) => {
                            error!("WasmOpfsStorage: create sub-directory error: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("WasmOpfsStorage: getDirectory error: {:?}", e);
                }
            }
        });
    }

    async fn handle(&mut self, message: Arc<Message>, ctx: &ActorContext) {
        match &*message {
            Message::Get(get) => self.handle_get(get.clone(), ctx),
            Message::Put(put) => self.handle_put(put.clone(), ctx),
            Message::Flush(flush) => {
                // OPFS writes are fire-and-forget. Ack immediately.
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

// Test-only accessors for unit tests in `wasm_tests.rs`.
/// Test/introspection accessors — used by Playwright browser tests to verify
/// construction and readiness. Not called from Rust tests (OPFS requires browser APIs).
#[cfg(test)]
#[allow(dead_code)]
impl WasmOpfsStorage {
    /// Returns the directory name (test accessor).
    pub(crate) fn dir_name_str(&self) -> &str {
        &self.dir_name
    }

    /// Returns whether the storage is ready for OPFS I/O (test accessor).
    pub(crate) fn is_ready(&self) -> bool {
        self.db_ready.load(Ordering::SeqCst)
    }
}
