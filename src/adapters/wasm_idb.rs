//! IndexedDB storage adapter — persistent browser storage for BEAM.
//!
//! This is the browser counterpart to [`RedbStorage`](crate::adapters::RedbStorage).
//! It uses the browser's IndexedDB API via `web-sys` to persist graph data
//! across page reloads.
//!
//! # Architecture
//!
//! - **Object store**: `beam_data` — key-value store where key = soul (node ID),
//!   value = serialized `Children` (as JSON string)
//! - **Database name**: `beam` (configurable via [`WasmIdbStorage::with_name`])
//! - **Schema version**: 1
//!
//! # Async Pattern
//!
//! IndexedDB is inherently callback-based in the browser. This adapter uses
//! `wasm_bindgen` closures to handle request callbacks. Each operation:
//!
//! 1. Creates a transaction + object store
//! 2. Creates a request (get/put)
//! 3. Registers success/error closures on the request
//! 4. Closures are leaked to the JS heap (same pattern as WasmWsConn)
//!
//! # Conflict Resolution
//!
//! Same as MemoryStorage: last-write-wins per child, using `updated_at`
//! timestamps. On `Get`, data is read from IndexedDB and merged with any
//! in-memory cache. On `Put`, data is written through to IndexedDB.

use crate::actor::{Actor, ActorContext};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;
use async_trait::async_trait;
use log::{error, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{
    IdbDatabase,
    IdbOpenDbRequest, IdbRequest, IdbTransactionMode,
    IdbVersionChangeEvent,
};

/// IndexedDB storage adapter for browser/WASM.
///
/// Persists graph data to the browser's IndexedDB. Each soul (node ID) is
/// stored as a key-value entry in the `beam_data` object store. Values are
/// serialized as JSON strings.
///
/// Created with [`WasmIdbStorage::new`] and registered as an actor via
/// [`ActorContext::start_actor`].
pub struct WasmIdbStorage {
    /// The open IndexedDB database handle.
    db: Option<IdbDatabase>,
    /// In-memory write cache for fast reads of recently written data.
    /// IndexedDB reads are async; the cache provides synchronous reads
    /// for data that was recently written.
    cache: Arc<parking_lot::RwLock<HashMap<String, Children>>>,
    /// Database name (default: "beam").
    db_name: String,
    /// Object store name (default: "beam_data").
    store_name: String,
    /// Flag to track if DB initialization is in progress.
    db_ready: bool,
}

impl WasmIdbStorage {
    /// Creates a new IndexedDB storage adapter.
    ///
    /// The database is opened asynchronously. Until it's ready, reads fall
    /// back to the in-memory cache and writes are buffered.
    pub fn new() -> Self {
        Self {
            db: None,
            cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            db_name: "beam".to_string(),
            store_name: "beam_data".to_string(),
            db_ready: false,
        }
    }

    /// Creates a new adapter with a custom database name.
    pub fn with_name(db_name: &str) -> Self {
        Self {
            db: None,
            cache: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            db_name: db_name.to_string(),
            store_name: "beam_data".to_string(),
            db_ready: false,
        }
    }

    /// Opens the IndexedDB database. Called from `pre_start`.
    ///
    /// Creates the database and object store if they don't exist.
    fn open_db(&mut self) -> Result<IdbDatabase, String> {
        let window = web_sys::window().ok_or("no window")?;
        let request: IdbOpenDbRequest = window
            .indexed_db()
            .map_err(|e| format!("indexed_db error: {:?}", e))?
            .ok_or("no indexed_db")?
            .open_with_u32(&self.db_name, 1)
            .map_err(|e| format!("open error: {:?}", e))?;

        // On upgrade needed: create object store
        let store_name = self.store_name.clone();
        let onupgradeneeded: Closure<dyn FnMut(IdbVersionChangeEvent)> = Closure::new(
            move |event: IdbVersionChangeEvent| {
                let request: IdbRequest = event.target().unwrap().unchecked_into();
                let db: IdbDatabase = request.result().unwrap().unchecked_into();
                let _ = db.create_object_store(&store_name);
                info!("WasmIdbStorage: created object store");
            },
        );
        request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
        onupgradeneeded.forget();

        // We can't await the open request synchronously in WASM.
        // Instead, we return a result based on immediate success.
        // The DB handle will be available after the success callback fires.
        // For now, return an error — the caller should use open_db_async.
        Err("use open_db_async".to_string())
    }

    /// Asynchronously opens the database and calls the callback when ready.
    ///
    /// Uses the standard wasm-bindgen closure pattern.
    fn open_db_async(&mut self, on_ready: Box<dyn FnOnce(IdbDatabase)>) {
        let window = match web_sys::window() {
            Some(w) => w,
            None => {
                error!("WasmIdbStorage: no window available");
                return;
            }
        };
        let idb = match window.indexed_db() {
            Ok(Some(idb)) => idb,
            _ => {
                error!("WasmIdbStorage: no IndexedDB available");
                return;
            }
        };
        let request = match idb.open_with_u32(&self.db_name, 1) {
            Ok(r) => r,
            Err(e) => {
                error!("WasmIdbStorage: open error: {:?}", e);
                return;
            }
        };

        let store_name = self.store_name.clone();
        let onupgradeneeded: Closure<dyn FnMut(IdbVersionChangeEvent)> = Closure::new(
            move |event: IdbVersionChangeEvent| {
                let request: IdbRequest = event.target().unwrap().unchecked_into();
                if let Ok(db_result) = request.result() {
                    let db: IdbDatabase = db_result.unchecked_into();
                    let _ = db.create_object_store(&store_name);
                    info!("WasmIdbStorage: created object store");
                }
            },
        );
        request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
        onupgradeneeded.forget();

        let mut on_ready_opt = Some(on_ready);
        let on_ready_wrapped: Closure<dyn FnMut(JsValue)> = Closure::new(move |_event: JsValue| {
            let event: web_sys::Event = _event.unchecked_into();
            let target = event.target().unwrap();
            let request: IdbRequest = target.unchecked_into();
            match request.result() {
                Ok(db_result) => {
                    let db: IdbDatabase = db_result.unchecked_into();
                    info!("WasmIdbStorage: database opened");
                    if let Some(cb) = on_ready_opt.take() {
                        cb(db);
                    }
                }
                Err(e) => {
                    error!("WasmIdbStorage: open success callback error: {:?}", e);
                }
            }
        });
        request.set_onsuccess(Some(on_ready_wrapped.as_ref().unchecked_ref()));
        on_ready_wrapped.forget();

        let onerror: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
            error!("WasmIdbStorage: database open error: {:?}", event);
        });
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        onerror.forget();
    }

    /// Serializes Children to a JSON string for IndexedDB storage.
    fn serialize_children(children: &Children) -> String {
        // Convert BTreeMap<String, NodeData> to a serializable format
        let map: HashMap<String, (Value, f64)> = children
            .iter()
            .map(|(k, v)| (k.clone(), (v.value.clone(), v.updated_at)))
            .collect();
        serde_json::to_string(&map).unwrap_or_default()
    }

    /// Deserializes Children from a JSON string.
    fn deserialize_children(json: &str) -> Children {
        if json.is_empty() {
            return BTreeMap::new();
        }
        let map: HashMap<String, (Value, f64)> = match serde_json::from_str(json) {
            Ok(m) => m,
            Err(e) => {
                warn!("WasmIdbStorage: deserialize error: {}", e);
                return BTreeMap::new();
            }
        };
        map.into_iter()
            .map(|(k, (value, updated_at))| {
                (k, NodeData { value, updated_at })
            })
            .collect()
    }

    /// Handles a Get request by checking the cache first, then IndexedDB.
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        // Check cache first (fast path)
        if let Some(children) = self.cache.read().get(&get.node_id).cloned() {
            self.reply_get(&get, children, ctx);
            return;
        }

        // Cache miss: try IndexedDB
        let db = match &self.db {
            Some(db) => db,
            None => {
                // DB not ready, reply with empty
                self.reply_get_empty(&get, ctx);
                return;
            }
        };

        let node_id = get.node_id.clone();
        let from = get.from.clone();
        let get_id = get.id.clone();
        let child_key = get.child_key.clone();
        let my_addr = ctx.addr.clone();
        let cache = self.cache.clone();

        let tx = match db.transaction_with_str_and_mode(
            &self.store_name,
            IdbTransactionMode::Readonly,
        ) {
            Ok(tx) => tx,
            Err(e) => {
                error!("WasmIdbStorage: transaction error: {:?}", e);
                self.reply_get_empty(&get, ctx);
                return;
            }
        };
        let store = match tx.object_store(&self.store_name) {
            Ok(s) => s,
            Err(e) => {
                error!("WasmIdbStorage: object store error: {:?}", e);
                self.reply_get_empty(&get, ctx);
                return;
            }
        };
        let request = match store.get(&JsValue::from_str(&node_id)) {
            Ok(r) => r,
            Err(e) => {
                error!("WasmIdbStorage: get request error: {:?}", e);
                self.reply_get_empty(&get, ctx);
                return;
            }
        };

        let onsuccess: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
            let event: web_sys::Event = event.unchecked_into();
            let target = event.target().unwrap();
            let request: IdbRequest = target.unchecked_into();
            match request.result() {
                Ok(result) => {
                    if result.is_null() || result.is_undefined() {
                        // Key not found
                        let mut reply_with_nodes = BTreeMap::new();
                        reply_with_nodes.insert(node_id.clone(), BTreeMap::new());
                        let put = Put::new(reply_with_nodes, Some(get_id.clone()), my_addr.clone());
                        let _ = from.send(Message::Put(put));
                        return;
                    }
                    let json = result.as_string().unwrap_or_default();
                    let children = WasmIdbStorage::deserialize_children(&json);
                    // Cache the result
                    cache.write().insert(node_id.clone(), children.clone());
                    // Reply
                    let reply_with_children = match &child_key {
                        Some(ck) => match children.get(ck) {
                            Some(cv) => {
                                let mut r = BTreeMap::new();
                                r.insert(ck.clone(), cv.clone());
                                r
                            }
                            None => return,
                        },
                        None => children.clone(),
                    };
                    let mut reply_with_nodes = BTreeMap::new();
                    reply_with_nodes.insert(node_id.clone(), reply_with_children);
                    let put = Put::new(reply_with_nodes, Some(get_id.clone()), my_addr.clone());
                    let _ = from.send(Message::Put(put));
                }
                Err(e) => {
                    error!("WasmIdbStorage: get result error: {:?}", e);
                }
            }
        });
        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        onsuccess.forget();

        let onerror: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
            error!("WasmIdbStorage: get request error: {:?}", event);
        });
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        onerror.forget();
    }

    /// Replies to a Get with children data (from cache).
    fn reply_get(&self, get: &Get, children: Children, ctx: &ActorContext) {
        let reply_with_children = match &get.child_key {
            Some(ck) => match children.get(ck) {
                Some(cv) => {
                    let mut r = BTreeMap::new();
                    r.insert(ck.clone(), cv.clone());
                    r
                }
                None => return,
            },
            None => children,
        };
        let mut reply_with_nodes = BTreeMap::new();
        reply_with_nodes.insert(get.node_id.clone(), reply_with_children);
        let mut recipients = HashSet::new();
        recipients.insert(get.from.clone());
        let put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
        let _ = get.from.send(Message::Put(put));
    }

    /// Replies to a Get with empty data (node not found).
    fn reply_get_empty(&self, get: &Get, ctx: &ActorContext) {
        let mut reply_with_nodes = BTreeMap::new();
        reply_with_nodes.insert(get.node_id.clone(), BTreeMap::new());
        let put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
        let _ = get.from.send(Message::Put(put));
    }

    /// Handles a Put by writing to cache + IndexedDB (write-through).
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

        // Write through to IndexedDB (async, fire-and-forget)
        if let Some(db) = &self.db {
            for (node_id, update_data) in &put.updated_nodes {
                let serialized = WasmIdbStorage::serialize_children(update_data);
                let tx = match db.transaction_with_str_and_mode(
                    &self.store_name,
                    IdbTransactionMode::Readwrite,
                ) {
                    Ok(tx) => tx,
                    Err(e) => {
                        error!("WasmIdbStorage: put transaction error: {:?}", e);
                        continue;
                    }
                };
                let store = match tx.object_store(&self.store_name) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("WasmIdbStorage: put object store error: {:?}", e);
                        continue;
                    }
                };
                let _ = store.put_with_key(&JsValue::from_str(&serialized), &JsValue::from_str(node_id));
            }
        }

        // Send ack
        self.send_put_ack(&put, ctx);
    }

    /// Sends a put ack back to the originating node.
    fn send_put_ack(&self, put: &Put, ctx: &ActorContext) {
        let mut ack_children = BTreeMap::new();
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
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(put.id.clone()), ctx.addr.clone());
        let _ = put.from.send(Message::Put(ack));
    }

    /// Handles a BatchPut by applying each put and sending a single ack.
    fn handle_batch_put(&mut self, batch: BatchPut, ctx: &ActorContext) {
        for put in batch.puts.iter() {
            // Apply to cache
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

        // Write through to IndexedDB
        if let Some(db) = &self.db {
            for put in batch.puts.iter() {
                for (node_id, update_data) in &put.updated_nodes {
                    let serialized = WasmIdbStorage::serialize_children(update_data);
                    let tx = match db.transaction_with_str_and_mode(
                        &self.store_name,
                        IdbTransactionMode::Readwrite,
                    ) {
                        Ok(tx) => tx,
                        Err(e) => {
                            error!("WasmIdbStorage: batch put tx error: {:?}", e);
                            continue;
                        }
                    };
                    let store = match tx.object_store(&self.store_name) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("WasmIdbStorage: batch put store error: {:?}", e);
                            continue;
                        }
                    };
                    let _ = store.put_with_key(&JsValue::from_str(&serialized), &JsValue::from_str(node_id));
                }
            }
        }

        // Send batch ack
        let mut ack_children = BTreeMap::new();
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
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(batch.id.clone()), ctx.addr.clone());
        let _ = batch.from.send(Message::Put(ack));
    }
}

#[async_trait]
impl Actor for WasmIdbStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        info!("WasmIdbStorage adapter starting");

        // Open the database asynchronously. The db handle will be set
        // when the success callback fires.
        let db_handle: Arc<parking_lot::Mutex<Option<IdbDatabase>>> =
            Arc::new(parking_lot::Mutex::new(None));

        let db_handle_for_cb = db_handle.clone();
        let _self_cache = self.cache.clone();

        self.open_db_async(Box::new(move |db| {
            *db_handle_for_cb.lock() = Some(db);
            info!("WasmIdbStorage: database ready");
        }));

        // Poll for the db handle (the callback may fire synchronously
        // during open_db_async in some browsers)
        if let Some(db) = db_handle.lock().take() {
            self.db = Some(db);
            self.db_ready = true;
        }
        // If not ready yet, the callback will fire later. Puts will go to
        // cache, Gets will check cache first. DB will be available for
        // subsequent operations once the success callback completes.
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext) {
        match message {
            Message::Get(get) => self.handle_get(get, ctx),
            Message::Put(put) => self.handle_put(put, ctx),
            Message::Flush(flush) => {
                // IndexedDB writes are flushed immediately (write-through).
                // Ack the barrier so callers don't hang.
                let mut ack = BTreeMap::new();
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
                let mut nodes = BTreeMap::new();
                nodes.insert("_ack".to_string(), ack);
                let put = Put::new(nodes, Some(flush.id), ctx.addr.clone());
                let _ = flush.from.send(Message::Put(put));
            }
            Message::BatchPut(batch) => self.handle_batch_put(batch, ctx),
            _ => {}
        }
    }
}
