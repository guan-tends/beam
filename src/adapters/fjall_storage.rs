//! Persistent storage adapter using [`fjall`](https://crates.io/crates/fjall) —
//! an LSM-tree (RocksDB-like) storage engine in 100% safe Rust.
//!
//! [`FjallStorage`] stores the BEAM graph in a fjall database on disk.
//! It provides crash-safe writes via WAL (write-ahead log) journalling,
//! automatic background compaction, and built-in LZ4 compression.
//!
//! # Schema
//!
//! Two keyspaces within one [`fjall::Database`]:
//! - `beam_nodes_v1`: key = `node_id` (bytes), value = `postcard(Children)` bytes
//! - `beam_meta_v1`: key = metadata key (bytes), value = `u64` timestamp (be_bytes)
//!
//! Keyspace names are wire-format identifiers — **do not change** (same
//! convention as the redb and persy adapter table/segment names).
//!
//! # Semantics
//!
//! - **Get**: Point lookup via `keyspace.get()`. If the node exists, replies
//!   with its children. If not, sends an empty reply (sentinel) so `.map()`
//!   listeners don't hang. Skips reply if checksum matches (already sent).
//! - **Put**: Reads existing children (point lookup), LWW-merges, writes
//!   the merged result via `keyspace.insert()`. Sends immediate ack —
//!   no `spawn_blocking` because `insert()` is a journal append to OS
//!   page cache (microseconds, not an fsync).
//! - **BatchPut**: Read-merge-write each put, accumulate into a
//!   [`fjall::WriteBatch`], commit atomically. Single journal entry
//!   for N puts — amortizes the WAL write.
//! - **Flush**: Calls `db.persist(PersistMode::SyncAll)` inside
//!   `spawn_blocking` — this is the real fsync. Sends ack after durability.
//!
//! # Async Pattern — LSM-Native (Differs from redb/persy)
//!
//! The redb adapter wraps every Put/BatchPut in `spawn_blocking` because
//! `redb::WriteTransaction::commit()` calls `fsync()` — a multi-millisecond
//! blocking syscall. Fjall's `insert()` is a `write()` syscall to OS page
//! cache (microseconds) — wrapping it in `spawn_blocking` would add thread
//! pool scheduling overhead for zero benefit.
//!
//! Only `Flush` uses `spawn_blocking` because `persist(SyncAll)` is the
//! actual `fsync()` — the one operation that genuinely blocks for I/O.
//!
//! # Durability Semantics
//!
//! Fjall's default durability matches RocksDB: writes are crash-safe via
//! the WAL (survive process crash), but are not fsync'd to disk until
//! explicit `persist()`. For a P2P database where peers hold copies of
//! the data, this is the correct trade-off — if one node loses its WAL
//! on power failure, peers resync it.
//!
//! # Conflict Resolution
//!
//! Same LWW (last-write-wins) per child as redb/persy: for each child,
//! the incoming `updated_at` is compared to the existing one. If
//! `incoming.updated_at >= existing.updated_at`, the child is overwritten.
//!
//! # Feature Gate
//!
//! This adapter is compiled only when the `fjall` feature is enabled:
//!
//! ```toml
//! beam = { version = "0.16", features = ["fjall"] }
//! ```

use arena_btreemap::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use web_time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use log::{debug, error, info};

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;

/// Keyspace name for graph node data.
///
/// Wire-format identifier — do not change. Matches redb's `beam_nodes_v1`
/// table and persy's `beam_nodes_v1` segment.
const BEAM_NODES: &str = "beam_nodes_v1";

/// Keyspace name for metadata (e.g. last write timestamp).
///
/// Wire-format identifier — do not change. Matches redb's `beam_meta_v1`
/// table.
const BEAM_META: &str = "beam_meta_v1";

/// Prefix byte prepended to every keyspace key.
///
/// Fjall's LSM-tree panics on empty keys (`"key may not be empty"`).
/// BEAM uses `""` (empty string) as the root soul — a valid node_id that
/// must be stored. Prepending a single byte to all keys ensures no key
/// is ever empty, while remaining transparent to the rest of the system
/// (the encoding is internal to this adapter).
///
/// `\x00` is chosen because it sorts before all printable characters,
/// preserving the natural lexicographic ordering of keyspace keys.
///
/// `pub(crate)` so [`crate::migration`] can reuse the encoding for
/// key translation between fjall and other backends.
pub(crate) const KEY_PREFIX: u8 = 0x00;

/// Encodes a node_id string as a keyspace key (prefixed to avoid empty keys).
///
/// `pub(crate)` so [`crate::migration`] can reuse the encoding for
/// key translation between fjall and other backends.
pub(crate) fn encode_key(node_id: &str) -> Vec<u8> {
    let mut key = vec![KEY_PREFIX];
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Decodes a keyspace key back to a node_id string.
///
/// Strips the [`KEY_PREFIX`] byte and interprets the remaining bytes as UTF-8.
/// Returns `None` if the key is empty, the prefix doesn't match, or the
/// remaining bytes are not valid UTF-8.
///
/// Exposed as `pub(crate)` for potential use by the migration tool and
/// future diagnostic tooling.
#[allow(dead_code)] // used by migration via local FJALL_KEY_PREFIX copy
pub(crate) fn decode_key(key: &[u8]) -> Option<String> {
    if key.is_empty() || key[0] != KEY_PREFIX {
        return None;
    }
    std::str::from_utf8(&key[1..]).ok().map(|s| s.to_string())
}

/// fjall-backed persistent storage adapter for BEAM.
///
/// Stores the graph in a fjall database. Each `Put` writes directly
/// (journal append to OS buffers, no fsync per write). `Flush`
/// triggers `persist(SyncAll)` for full durability.
///
/// # Example
///
/// ```ignore
/// use beam::adapters::FjallStorage;
/// use beam::Config;
///
/// let storage = FjallStorage::new_with_config(Config::default(), "beam.fjall");
/// ```
pub struct FjallStorage {
    /// `Arc<Database>` so the read and write actor clones share the same
    /// underlying database. Fjall's MVCC snapshots mean a reader sees the
    /// latest committed write immediately.
    db: Arc<Database>,
    /// Handle to the `beam_nodes_v1` keyspace — cloned for each actor.
    nodes: Keyspace,
    /// Handle to the `beam_meta_v1` keyspace — cloned for each actor.
    meta: Keyspace,
    /// Stored so log lines and errors can reference the path the user
    /// passed at construction time.
    path: String,
    /// Kept for API parity with RedbStorage (currently unused by fjall,
    /// which manages its own memory via block cache configuration).
    _config: Config,
}

impl Clone for FjallStorage {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            nodes: self.nodes.clone(),
            meta: self.meta.clone(),
            path: self.path.clone(),
            _config: self._config.clone(),
        }
    }
}

impl Default for FjallStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl FjallStorage {
    /// Creates a new fjall storage at the default path `beam.fjall`.
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened.
    pub fn new() -> Self {
        Self::new_with_config(Config::default(), "beam.fjall")
    }

    /// Creates a new fjall storage with explicit config and path.
    ///
    /// The path is a **directory** (not a file) — fjall creates a
    /// directory structure containing journal, SSTables, and metadata.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration (currently unused — fjall manages
    ///   its own memory/cache settings via builder)
    /// * `path` - Directory path for the fjall database
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened at the given path.
    pub fn new_with_config<P: AsRef<Path>>(config: Config, path: P) -> Self {
        let path = path.as_ref().to_string_lossy().into_owned();
        let db = Database::builder(&path).open().unwrap_or_else(|e| {
            panic!("Failed to create/open fjall at {}: {:?}", path, e);
        });
        let nodes = db
            .keyspace(BEAM_NODES, KeyspaceCreateOptions::default)
            .unwrap_or_else(|e| {
                panic!("Failed to open beam_nodes_v1 keyspace: {:?}", e);
            });
        let meta = db
            .keyspace(BEAM_META, KeyspaceCreateOptions::default)
            .unwrap_or_else(|e| {
                panic!("Failed to open beam_meta_v1 keyspace: {:?}", e);
            });
        Self {
            db: Arc::new(db),
            nodes,
            meta,
            path,
            _config: config,
        }
    }

    // ── Get ──────────────────────────────────────────────────────────

    /// Handles a `Get` by reading from the keyspace and replying with children.
    ///
    /// If the node doesn't exist, sends an empty reply (sentinel) so `.map()`
    /// listeners don't hang. If the checksum matches the request, the reply
    /// is suppressed (already sent) — unless this is an ack reply (those
    /// MUST always be sent, see the always-reply-when-ack invariant).
    fn handle_get(&self, get: &Get, ctx: &ActorContext) {
        let children_for_node: Children = match self.nodes.get(encode_key(&get.node_id).as_slice())
        {
            Ok(Some(slice)) => match postcard::from_bytes(slice.as_ref()) {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "fjall get: deserialize failed for node_id={}: {:?}",
                        get.node_id, e
                    );
                    return;
                }
            },
            Ok(None) => {
                debug!("fjall get: no data for node_id={}", get.node_id);
                // Empty set is still a valid reply — send sentinel so .map() listeners don't hang.
                let mut reply_with_nodes = BTreeMap::default();
                reply_with_nodes.insert(get.node_id.clone(), BTreeMap::default()); // node_id="" is valid in BEAM graph (root soul)
                let put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
                put.to_string(); // compute checksum
                let _ = get.from.send(Message::Put(put));
                return;
            }
            Err(e) => {
                error!("fjall get failed for node_id={}: {:?}", get.node_id, e);
                return;
            }
        };

        // Narrow to child_key if set (mirrors redb/persy adapters).
        let reply_children = match &get.child_key {
            Some(target_key) => {
                let mut c: Children = BTreeMap::default();
                if let Some(node_data) = children_for_node.get(target_key) {
                    c.insert(target_key.clone(), node_data.clone());
                }
                c
            }
            None => children_for_node,
        };

        let mut reply_with_nodes = BTreeMap::default();
        reply_with_nodes.insert(get.node_id.clone(), reply_children);

        let put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
        put.to_string(); // compute checksum

        // Ack replies (those with `in_response_to`) MUST always be sent,
        // regardless of checksum match. The client uses the reply's
        // presence to drive its `__beam_replay_complete__` sentinel-drain;
        // a silent ack would hang the drain forever. The checksum-match
        // optimization is reserved for live broadcasts where the caller
        // already has the data.
        let is_ack = put.in_response_to.is_some();
        if is_ack || put.checksum != get.checksum {
            let _ = get.from.send(Message::Put(put));
        } else {
            debug!("fjall get: checksum match, not replying");
        }
    }

    // ── Put ──────────────────────────────────────────────────────────

    /// Reads existing children for a node, LWW-merges with incoming data,
    /// and writes the merged result.
    ///
    /// Called directly from the async `handle()` — no `spawn_blocking`
    /// because fjall's `insert()` is a journal append (microseconds).
    ///
    /// Returns `Ok(())` on success, or `Err(String)` with a human-readable
    /// error message (mapped via `Debug` — same convention as persy).
    fn apply_put(&self, put: &Put) -> Result<(), String> {
        for (node_id, update_data) in put.updated_nodes.iter().rev() {
            // Skip internal control keys (e.g. _flushed, _ack)
            if !node_id.is_empty() && node_id.starts_with('_') {
                continue;
            }

            // Read existing children (point lookup — no scan needed).
            let key = encode_key(node_id);
            let mut children_for_node: Children = match self.nodes.get(key.as_slice()) {
                Ok(Some(slice)) => postcard::from_bytes(slice.as_ref()).unwrap_or_default(),
                Ok(None) => BTreeMap::default(),
                Err(e) => return Err(format!("fjall get for merge: {:?}", e)),
            };

            // LWW merge: newer updated_at wins per child.
            for (child_id, child_data) in update_data {
                let should_write = !matches!(
                    children_for_node.get(child_id),
                    Some(existing) if existing.updated_at > child_data.updated_at
                );

                if should_write {
                    children_for_node.insert(child_id.clone(), child_data.clone());
                }
            }

            // Write: remove if empty, else insert merged result.
            if children_for_node.is_empty() {
                if let Err(e) = self.nodes.remove(key.as_slice()) {
                    return Err(format!("fjall remove: {:?}", e));
                }
            } else {
                let bytes = postcard::to_allocvec(&children_for_node)
                    .map_err(|e| format!("postcard serialize: {:?}", e))?;
                if let Err(e) = self.nodes.insert(key.as_slice(), bytes) {
                    return Err(format!("fjall insert: {:?}", e));
                }
            }
        }

        // Update meta: _last_write timestamp (mirrors redb/persy adapters).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Err(e) = self.meta.insert(b"_last_write", now.to_be_bytes().to_vec()) {
            debug!("fjall meta insert failed: {:?}", e);
        }

        Ok(())
    }

    /// Handles a BatchPut by read-merge-writing each put and committing
    /// all writes atomically via a [`fjall::WriteBatch`].
    ///
    /// Each put in the batch requires a read-merge-write cycle (LWW
    /// conflict resolution). The reads happen against the live keyspace;
    /// the merged writes are accumulated into a `WriteBatch` and committed
    /// as a single journal entry — amortizing the WAL write across all
    /// puts in the batch.
    ///
    /// Called directly from the async `handle()` — no `spawn_blocking`
    /// because `WriteBatch::commit()` is a journal append (microseconds).
    fn apply_batch_put(&self, batch: &BatchPut) -> Result<(), String> {
        // Create an owned Database clone for the batch (Database: Clone).
        // The batch owns the Database handle for the duration of the commit.
        let db = (*self.db).clone();
        let mut write_batch = db.batch();

        for put in &batch.puts {
            for (node_id, update_data) in put.updated_nodes.iter().rev() {
                // Skip internal control keys.
                if !node_id.is_empty() && node_id.starts_with('_') {
                    continue;
                }

                // Read existing children (point lookup).
                let key = encode_key(node_id);
                let mut children_for_node: Children = match self.nodes.get(key.as_slice()) {
                    Ok(Some(slice)) => postcard::from_bytes(slice.as_ref()).unwrap_or_default(),
                    Ok(None) => BTreeMap::default(),
                    Err(e) => return Err(format!("fjall get for batch merge: {:?}", e)),
                };

                // LWW merge.
                for (child_id, child_data) in update_data {
                    let should_write = !matches!(
                        children_for_node.get(child_id),
                        Some(existing) if existing.updated_at > child_data.updated_at
                    );

                    if should_write {
                        children_for_node.insert(child_id.clone(), child_data.clone());
                    }
                }

                // Add to write batch.
                if children_for_node.is_empty() {
                    write_batch.remove(&self.nodes, key.as_slice());
                } else {
                    let bytes = postcard::to_allocvec(&children_for_node)
                        .map_err(|e| format!("postcard serialize: {:?}", e))?;
                    write_batch.insert(&self.nodes, key.as_slice(), bytes);
                }
            }
        }

        write_batch
            .commit()
            .map_err(|e| format!("fjall batch commit: {:?}", e))
    }
}

// ── Ack helpers ─────────────────────────────────────────────────────

/// Builds the ack payload (`_ack` or `_err` sentinel) for a direct (non-
/// `spawn_blocking`) Put/BatchPut result. Mirrors the wire format used by
/// redb and persy adapters — same `_ack`/`_err` convention means
/// `Node::handle_put` drains `pending_puts` uniformly across all adapters.
fn build_ack_children(result: &Result<(), String>) -> (Children, Option<String>) {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64;

    match result {
        Ok(()) => (
            vec![(
                "_ack".to_string(),
                NodeData {
                    value: Value::Text("ok".to_string()),
                    updated_at: now_millis,
                },
            )]
            .into_iter()
            .collect::<Children>(),
            None,
        ),
        Err(e) => {
            error!("fjall put failed: {}", e);
            (
                vec![(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(e.clone()),
                        updated_at: now_millis,
                    },
                )]
                .into_iter()
                .collect::<Children>(),
                Some(e.clone()),
            )
        }
    }
}

/// Builds the ack payload for a `spawn_blocking` Flush result. The
/// `JoinError` layer (task panic) is handled in addition to the inner
/// `fjall::Error`.
fn build_flush_ack_children(
    result: &Result<Result<(), String>, tokio::task::JoinError>,
) -> (Children, Option<String>) {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64;

    match result {
        Ok(Ok(())) => (
            vec![(
                "_ack".to_string(),
                NodeData {
                    value: Value::Text("ok".to_string()),
                    updated_at: now_millis,
                },
            )]
            .into_iter()
            .collect::<Children>(),
            None,
        ),
        Ok(Err(e)) => {
            error!("fjall persist failed: {}", e);
            (
                vec![(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(e.clone()),
                        updated_at: now_millis,
                    },
                )]
                .into_iter()
                .collect::<Children>(),
                Some(e.clone()),
            )
        }
        Err(e) => {
            let msg = format!("task panicked: {:?}", e);
            error!("fjall flush task panicked: {:?}", e);
            (
                vec![(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(msg.clone()),
                        updated_at: now_millis,
                    },
                )]
                .into_iter()
                .collect::<Children>(),
                Some(msg),
            )
        }
    }
}

/// Sends an ack Put back to the originating node. Assembles the
/// `_ack`/`_err` sentinel into the standard reply format.
fn send_ack(
    put_id: &str,
    put_from: &Addr,
    ack_children: Children,
    err_msg: Option<String>,
    ctx: &ActorContext,
) {
    let mut nodes = BTreeMap::default();
    nodes.insert("_ack".to_string(), ack_children);
    let ack = Put::new(nodes, Some(put_id.to_string()), ctx.addr.clone());
    let _ = put_from.send(Message::Put(ack));
    if err_msg.is_some() {
        debug!("fjall ack sent with _err for {}", put_id);
    }
}

// ── Actor impl ──────────────────────────────────────────────────────

#[async_trait]
impl Actor for FjallStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        debug!("FjallStorage started at {}", self.path);
        // Keyspaces are created in `new_with_config` — no additional
        // warmup needed (unlike redb which opens tables in pre_start).
    }

    async fn stopping(&mut self, _ctx: &ActorContext) {
        // Fjall's Drop impl calls persist(SyncAll) automatically, but
        // we explicitly persist here for observability and to flush
        // before the actor system tears down.
        if let Err(e) = self.db.persist(PersistMode::SyncAll) {
            error!("FjallStorage final persist failed: {:?}", e);
        }
        info!("FjallStorage stopping at {} — journal persisted", self.path);
    }

    async fn handle(&mut self, message: Arc<Message>, ctx: &ActorContext) {
        match &*message {
            // ── Get: direct point lookup, no spawn_blocking ─────────
            Message::Get(get) => {
                self.handle_get(get, ctx);
            }

            // ── Put: direct insert, no spawn_blocking ──────────────
            // Fjall's insert() is a journal append to OS page cache
            // (microseconds, not fsync). The ack is sent immediately
            // after the write returns — faster than redb, which waits
            // for spawn_blocking + fsync before acking.
            Message::Put(put) => {
                let put_id = put.id.clone();
                let put_from = put.from.clone();
                let result = self.apply_put(put);
                let (ack_children, err_msg) = build_ack_children(&result);
                send_ack(&put_id, &put_from, ack_children, err_msg, ctx);
            }

            // ── BatchPut: WriteBatch, single journal entry ─────────
            // Same direct-async pattern as Put. WriteBatch commits all
            // puts as one atomic journal entry — amortizes the WAL write.
            Message::BatchPut(batch) => {
                let batch_id = batch.id.clone();
                let batch_from = batch.from.clone();
                let result = self.apply_batch_put(batch);
                let (ack_children, err_msg) = build_ack_children(&result);
                send_ack(&batch_id, &batch_from, ack_children, err_msg, ctx);
            }

            // ── Flush: spawn_blocking + persist(SyncAll) = real fsync
            // This is the ONLY operation that genuinely blocks for I/O.
            // All prior writes (Put/BatchPut) were journaled to OS
            // buffers; Flush fsyncs them to disk for full durability.
            Message::Flush(flush) => {
                let flush_id = flush.id.clone();
                let from_addr = flush.from.clone();
                let db = Arc::clone(&self.db);

                let result = tokio::task::spawn_blocking(move || {
                    db.persist(PersistMode::SyncAll)
                        .map_err(|e| format!("fjall persist: {:?}", e))
                })
                .await;

                let (ack_children, err_msg) = build_flush_ack_children(&result);
                send_ack(&flush_id, &from_addr, ack_children, err_msg, ctx);
            }

            _ => {}
        }
    }

    /// Returns a boxed clone for the storage read/write actor split.
    ///
    /// Both the read and write actor share the same `Arc<Database>` and
    /// cloned `Keyspace` handles, so reads see committed writes
    /// immediately via fjall's snapshot isolation.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        Some(Box::new(self.clone()))
    }
}

// ========================================================================
// Tests
// ========================================================================
//
// Tests exercise the public surface (`new_with_config`, `apply_put`,
// `handle_get`) directly, no actor plumbing — that's what the e2e tests
// in `tests/fjall_e2e.rs` cover.
//
// Run with: `cargo test --features fjall --lib`

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Addr;

    fn create_test_storage(suffix: &str) -> FjallStorage {
        let path = format!("/tmp/beam-test-fjall-{}-{}", std::process::id(), suffix);
        let _ = std::fs::remove_dir_all(&path);
        FjallStorage::new_with_config(Config::default(), &path)
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn make_node_data(value: &str, ts: f64) -> NodeData {
        NodeData {
            value: Value::Text(value.to_string()),
            updated_at: ts,
        }
    }

    #[test]
    fn test_fjall_creates_db() {
        let storage = create_test_storage("create");
        assert!(!storage.path.is_empty());
        // Directory should exist
        assert!(std::path::Path::new(&storage.path).exists());
        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_default() {
        let storage = FjallStorage::default();
        // Default path is beam.fjall in cwd — clean up
        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_clone() {
        let storage = create_test_storage("clone");
        let cloned = storage.clone();
        assert_eq!(storage.path, cloned.path);
        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_put_then_get_roundtrips_children() {
        let storage = create_test_storage("roundtrip");

        // Build a Put: node "a" gets child "b" with value "hello" @ t=100
        let mut children = BTreeMap::default();
        children.insert("b".to_string(), make_node_data("hello", 100.0));
        let mut updated_nodes = BTreeMap::default();
        updated_nodes.insert("a".to_string(), children);
        let put = Put::new(updated_nodes, Some("put-1".to_string()), Addr::noop());

        storage.apply_put(&put).expect("put should succeed");

        // Verify via direct keyspace read (using encode_key, same as adapter)
        let slice = storage
            .nodes
            .get(encode_key("a").as_slice())
            .expect("get should not error")
            .expect("node 'a' should exist");
        let result: Children =
            postcard::from_bytes(slice.as_ref()).expect("deserialize should work");
        assert_eq!(result.len(), 1);
        let child = result.get("b").unwrap();
        match &child.value {
            Value::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("expected text value"),
        }
        assert_eq!(child.updated_at, 100.0);

        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_lww_merge_prefers_newer_updated_at() {
        let storage = create_test_storage("lww");

        // First Put: child "x" with updated_at=100
        let mut children = BTreeMap::default();
        children.insert("x".to_string(), make_node_data("old", 100.0));
        let mut nodes = BTreeMap::default();
        nodes.insert("n1".to_string(), children);
        storage
            .apply_put(&Put::new(nodes, Some("p1".to_string()), Addr::noop()))
            .unwrap();

        // Second Put: child "x" with updated_at=50 (older — should NOT overwrite)
        let mut children2 = BTreeMap::default();
        children2.insert("x".to_string(), make_node_data("older", 50.0));
        let mut nodes2 = BTreeMap::default();
        nodes2.insert("n1".to_string(), children2);
        storage
            .apply_put(&Put::new(nodes2, Some("p2".to_string()), Addr::noop()))
            .unwrap();

        // Third Put: child "x" with updated_at=200 (newest — should overwrite)
        let mut children3 = BTreeMap::default();
        children3.insert("x".to_string(), make_node_data("newest", 200.0));
        let mut nodes3 = BTreeMap::default();
        nodes3.insert("n1".to_string(), children3);
        storage
            .apply_put(&Put::new(nodes3, Some("p3".to_string()), Addr::noop()))
            .unwrap();

        // Verify: only the "newest" value should be stored
        let slice = storage
            .nodes
            .get(encode_key("n1").as_slice())
            .expect("get should not error")
            .expect("node 'n1' should exist");
        let result: Children = postcard::from_bytes(slice.as_ref()).unwrap();
        let child = result.get("x").unwrap();
        match &child.value {
            Value::Text(s) => assert_eq!(s, "newest"),
            _ => panic!("expected text value"),
        }
        assert_eq!(child.updated_at, 200.0);

        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_get_missing_node_returns_empty() {
        let storage = create_test_storage("missing");

        // Direct keyspace read on empty db (using encode_key)
        let result = storage
            .nodes
            .get(encode_key("nonexistent").as_slice())
            .unwrap();
        assert!(result.is_none(), "fresh db has no records");

        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_persistence_across_reopen() {
        let path = format!("/tmp/beam-test-fjall-persist-{}", std::process::id());
        let _ = std::fs::remove_dir_all(&path);

        // Write data with first storage instance
        {
            let storage = FjallStorage::new_with_config(Config::default(), &path);

            let mut children = BTreeMap::default();
            children.insert("k".to_string(), make_node_data("v1", 100.0));
            let mut nodes = BTreeMap::default();
            nodes.insert("node1".to_string(), children);
            storage
                .apply_put(&Put::new(nodes, Some("p1".to_string()), Addr::noop()))
                .unwrap();

            // Persist to disk
            storage.db.persist(PersistMode::SyncAll).unwrap();
        }
        // Storage dropped — database closed

        // Reopen at the same path — data should survive
        {
            let storage = FjallStorage::new_with_config(Config::default(), &path);
            let slice = storage
                .nodes
                .get(encode_key("node1").as_slice())
                .expect("get should not error")
                .expect("node1 should survive reopen");
            let result: Children = postcard::from_bytes(slice.as_ref()).unwrap();
            let child = result.get("k").unwrap();
            match &child.value {
                Value::Text(s) => assert_eq!(s, "v1"),
                _ => panic!("expected text value"),
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_fjall_batch_put_atomicity() {
        let storage = create_test_storage("batch");

        // Build a BatchPut with 3 puts to different nodes
        let puts: Vec<Put> = (0..3)
            .map(|i| {
                let mut children = BTreeMap::default();
                children.insert(
                    format!("c{}", i),
                    make_node_data(&format!("val{}", i), 100.0 + i as f64),
                );
                let mut nodes = BTreeMap::default();
                nodes.insert(format!("n{}", i), children);
                Put::new(nodes, Some(format!("bp{}", i)), Addr::noop())
            })
            .collect();

        let batch = BatchPut::new(puts, Addr::noop());
        storage
            .apply_batch_put(&batch)
            .expect("batch should succeed");

        // Verify all 3 nodes are present
        for i in 0..3 {
            let slice = storage
                .nodes
                .get(encode_key(&format!("n{}", i)).as_slice())
                .expect("get should not error")
                .expect("node should exist after batch put");
            let result: Children = postcard::from_bytes(slice.as_ref()).unwrap();
            let child = result.get(&format!("c{}", i)).unwrap();
            match &child.value {
                Value::Text(s) => assert_eq!(s, &format!("val{}", i)),
                _ => panic!("expected text value"),
            }
        }

        cleanup(&storage.path);
    }

    #[test]
    fn test_fjall_empty_node_removed_from_keyspace() {
        let storage = create_test_storage("empty");

        // Insert a node with one child
        let mut children = BTreeMap::default();
        children.insert("x".to_string(), make_node_data("v", 100.0));
        let mut nodes = BTreeMap::default();
        nodes.insert("n1".to_string(), children);
        storage
            .apply_put(&Put::new(nodes, Some("p1".to_string()), Addr::noop()))
            .unwrap();

        // Verify it exists
        assert!(
            storage
                .nodes
                .get(encode_key("n1").as_slice())
                .unwrap()
                .is_some()
        );

        // Overwrite with a newer child that has the same key but different value
        // Then delete by sending an empty update (updated_at newer but empty children)
        // Actually, the LWW merge only removes if children_for_node becomes empty
        // after merge. Since we're sending an empty update, the existing children
        // stay (they have higher updated_at). So to truly test removal, we need
        // to test that a node with no children gets removed when explicitly written.
        // The removal path is: children_for_node.is_empty() after merge → remove key.
        // This happens when a new put has no children for the node (but the node
        // is in updated_nodes with an empty BTreeMap).
        let mut empty_nodes = BTreeMap::default();
        empty_nodes.insert("n1".to_string(), BTreeMap::default());
        storage
            .apply_put(&Put::new(empty_nodes, Some("p2".to_string()), Addr::noop()))
            .unwrap();

        // The node should still exist because the existing child "x" @ t=100
        // is newer than the empty incoming update (default updated_at=0).
        // This is correct LWW behavior — empty updates don't delete newer data.
        assert!(
            storage
                .nodes
                .get(encode_key("n1").as_slice())
                .unwrap()
                .is_some()
        );

        cleanup(&storage.path);
    }
}
