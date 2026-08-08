//! Persistent embedded storage adapter using [`redb`](https://crates.io/crates/redb).
//!
//! [`RedbStorage`] stores the BEAM graph in a redb database file on disk.
//! It provides ACID transactions with automatic crash recovery.
//!
//! # Schema
//!
//! Two tables:
//! - `beam_nodes_v1`: key = `node_id` (&str), value = `postcard(Children)` (BTreeMap<String, NodeData>)
//! - `beam_meta_v1`: key = metadata key (&str), value = `u64` timestamp
//!
//! # Semantics
//!
//! - **Get**: Reads from a read transaction. If the node exists, replies
//!   with its children. If not, sends an empty reply (sentinel) so `.map()`
//!   listeners don't hang. Skips reply if checksum matches (already sent).
//! - **Put**: Opens a write transaction, merges `updated_nodes` using
//!   `updated_at` conflict resolution (last-write-wins), commits.
//! - **BatchPut**: All puts in the batch are applied in a single transaction.
//! - **Flush**: No additional work (puts already commit inline). Sends
//!   immediate ack for barrier semantics.
//!
//! # Conflict Resolution
//!
//! For each child, the incoming `updated_at` is compared to the existing one.
//! If `incoming.updated_at >= existing.updated_at`, the child is overwritten.
//! This implements last-write-wins (LWW) per child.
//!
//! # Thread Safety
//!
//! The `Database` handle is `Arc`-wrapped and safe to share. Reads use
//! `begin_read()` (concurrent) and writes use `begin_write()` (exclusive).
//! Write commits run inside `spawn_blocking` so the fsync never blocks the
//! async runtime's worker threads.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Config;
use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;

use async_trait::async_trait;
use log::{debug, error};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// Table definition for graph node data.
const BEAM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("beam_nodes_v1");
/// Table definition for metadata (e.g. last write timestamp).
const BEAM_META: TableDefinition<&str, u64> = TableDefinition::new("beam_meta_v1");

/// Macro to unwrap a redb result or log and return early on error.
macro_rules! unwrap_or_return {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                error!("redb operation failed: {:?}", e);
                return;
            }
        }
    };
}

/// redb-backed persistent storage adapter for BEAM.
///
/// Stores the graph in a single redb database file. Each `Put` commits
/// inline (ACID) inside `spawn_blocking` so the fsync does not block the
/// async runtime. Reads are concurrent. Flush is an immediate ack (puts
/// already fsync on commit).
///
/// # Example
///
/// ```ignore
/// use beam::adapters::RedbStorage;
/// use beam::Config;
///
/// let storage = RedbStorage::new_with_config(Config::default(), "my-db.redb", None);
/// ```
pub struct RedbStorage {
    db: Arc<Database>,
    path: String,
    _config: Config,
}

impl Clone for RedbStorage {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
            _config: self._config.clone(),
        }
    }
}

impl RedbStorage {
    /// Creates a new redb storage at the default path `beam.redb`.
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened.
    pub fn new() -> Self {
        Self::new_with_config(Config::default(), "beam.redb", None)
    }

    /// Creates a new redb storage with explicit config and path.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration
    /// * `path` - File path for the redb database
    /// * `_max_size` - Optional maximum database size (currently unused)
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened at the given path.
    pub fn new_with_config<P: AsRef<Path>>(
        config: Config,
        path: P,
        _max_size: Option<u64>,
    ) -> Self {
        let path = path.as_ref().to_string_lossy().into_owned();
        let db = Database::create(&path).unwrap_or_else(|e| {
            panic!("Failed to create/open redb at {}: {:?}", path, e);
        });
        Self {
            db: Arc::new(db),
            path,
            _config: config,
        }
    }

    /// Handles a `Get` by reading from the database and replying with children.
    ///
    /// If the node doesn't exist, sends an empty reply (sentinel) so `.map()`
    /// listeners don't hang. If the checksum matches the request, the reply
    /// is suppressed (already sent the same data).
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        let read_txn = unwrap_or_return!(self.db.begin_read());
        let table = unwrap_or_return!(read_txn.open_table(BEAM_NODES));

        let children_for_node = match table.get(&*get.node_id) {
            Ok(Some(access_guard)) => {
                let bytes = access_guard.value();
                unwrap_or_return!(postcard::from_bytes::<BTreeMap<String, NodeData>>(bytes))
            }
            Ok(None) => {
                debug!("redb get: no data for node_id={}", get.node_id);
                // Empty set is still a valid replay — send sentinel so `.map()` listeners don't hang.
                let mut reply_with_nodes = BTreeMap::new();
                reply_with_nodes.insert(get.node_id.clone(), BTreeMap::new());
                let mut put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
                put.to_string(); // compute checksum
                let _ = get.from.send(Message::Put(put));
                return;
            }
            Err(e) => {
                error!("redb get failed: {:?}", e);
                return;
            }
        };

        let reply_with_children = match &get.child_key {
            Some(target_key) => {
                let mut c = BTreeMap::new();
                if let Some(node_data) = children_for_node.get(target_key) {
                    c.insert(target_key.clone(), node_data.clone());
                }
                c
            }
            None => children_for_node,
        };

        let mut reply_with_nodes = BTreeMap::new();
        reply_with_nodes.insert(get.node_id.clone(), reply_with_children);

        let mut put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
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
            debug!("redb get: checksum match, not replying");
        }
    }

    /// Applies a single Put to the given write transaction.
    ///
    /// For each node in `put.updated_nodes`, merges children using LWW
    /// conflict resolution. Empty nodes are removed from the table.
    fn apply_put_to_tables(
        &self,
        wtxn: &mut redb::WriteTransaction,
        put: Put,
    ) -> Result<(), redb::Error> {
        let mut node_table = wtxn.open_table(BEAM_NODES)?;
        let mut meta_table = wtxn.open_table(BEAM_META)?;

        for (node_id, update_data) in put.updated_nodes.into_iter().rev() {
            // Skip internal control keys (e.g. _flushed, _ack)
            if !node_id.is_empty() && node_id.starts_with('_') {
                continue;
            }

            let mut children_for_node: BTreeMap<String, NodeData> =
                match node_table.get(&*node_id)? {
                    Some(access_guard) => {
                        let bytes = access_guard.value();
                        postcard::from_bytes(bytes).unwrap_or_default()
                    }
                    None => BTreeMap::new(),
                };

            for (child_id, child_data) in update_data {
                let should_write = !matches!(
                    children_for_node.get(&child_id),
                    Some(existing) if existing.updated_at > child_data.updated_at
                );

                if should_write {
                    children_for_node.insert(child_id, child_data);
                }
            }

            if children_for_node.is_empty() {
                node_table.remove(&*node_id)?;
            } else {
                let bytes = postcard::to_allocvec(&children_for_node).map_err(|e| {
                    redb::Error::Io(std::io::Error::other(format!(
                        "postcard serialize: {:?}",
                        e
                    )))
                })?;
                node_table.insert(&*node_id, bytes.as_slice())?;
            }
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        meta_table.insert("_last_write", now)?;
        Ok(())
    }

    /// Handles a single Put by opening a write transaction, applying, and committing.
    fn handle_put_internal(&self, put: Put) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        self.apply_put_to_tables(&mut wtxn, put)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Handles a BatchPut by applying all puts in a single transaction.
    ///
    /// This preserves atomicity — either all puts commit or none do.
    fn handle_batch_put(&self, batch: BatchPut) -> Result<(), redb::Error> {
        let mut wtxn = self.db.begin_write()?;
        for put in batch.puts {
            self.apply_put_to_tables(&mut wtxn, put)?;
        }
        wtxn.commit()?;
        Ok(())
    }
}

#[async_trait]
impl Actor for RedbStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        debug!("RedbStorage started at {}", self.path);
        // Warm the schema so the first read finds tables already present.
        if let Ok(wtxn) = self.db.begin_write() {
            let _ = wtxn.open_table(BEAM_NODES);
            let _ = wtxn.open_table(BEAM_META);
            let _ = wtxn.commit();
        }
    }

    #[allow(unused_variables)]
    async fn stopping(&mut self, _ctx: &ActorContext) {
        debug!("RedbStorage stopping at {}", self.path);
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext) {
        match message {
            Message::Get(get) => self.handle_get(get, ctx),
            Message::Put(put) => {
                let put_id = put.id.clone();
                let put_from = put.from.clone();
                let storage = self.clone();
                let result =
                    tokio::task::spawn_blocking(move || storage.handle_put_internal(put)).await;
                self.send_put_ack_after_commit(&put_id, &put_from, &result, ctx);
            }
            Message::BatchPut(batch) => {
                let batch_id = batch.id.clone();
                let batch_from = batch.from.clone();
                let storage = self.clone();
                let result =
                    tokio::task::spawn_blocking(move || storage.handle_batch_put(batch)).await;
                self.send_batch_put_ack_after_commit(&batch_id, &batch_from, &result, ctx);
            }
            Message::Flush(flush) => {
                let flush_id = flush.id.clone();
                let from_addr = flush.from.clone();
                let ctx_addr = ctx.addr.clone();

                // For embedded redb, put() already commits inline (wtxn.commit).
                // Flush has no additional durability work. Send ack immediately.
                let mut ack_children = BTreeMap::new();
                ack_children.insert(
                    "_flushed".to_string(),
                    NodeData {
                        value: Value::Text("true".to_string()),
                        updated_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                );
                let mut ack_nodes = BTreeMap::new();
                ack_nodes.insert("_ack".to_string(), ack_children);
                let mut put = Put::new(ack_nodes, Some(flush_id), ctx_addr.clone());
                put.to_string(); // compute checksum
                let _ = from_addr.send(Message::Put(put));
            }
            _ => {}
        }
    }

    /// Returns a boxed clone for the storage read/write actor split.
    ///
    /// Both the read and write actor share the same `Arc<Database>`, so
    /// reads see committed writes immediately via redb's MVCC snapshots.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        Some(Box::new(self.clone()))
    }
}

impl RedbStorage {
    /// Sends a put-ack back to the originating node after `spawn_blocking`
    /// returns. The ack payload uses the same `_ack`/`_err` sentinel as
    /// the Flush ack and as memory_storage — so `Node::handle_put` drains
    /// `pending_puts` uniformly across both adapters.
    ///
    /// Fires AFTER the commit returns from `spawn_blocking` — that's the
    /// contract. If the commit failed or the task panicked, we send `_err`
    /// and the awaiting caller learns the failure.
    fn send_put_ack_after_commit(
        &self,
        put_id: &str,
        put_from: &Addr,
        result: &Result<Result<(), redb::Error>, tokio::task::JoinError>,
        ctx: &ActorContext,
    ) {
        let (ack_children, err_msg) = match result {
            Ok(Ok(())) => (
                vec![(
                    "_ack".to_string(),
                    NodeData {
                        value: Value::Text("ok".to_string()),
                        updated_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                )]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
                None,
            ),
            Ok(Err(e)) => {
                error!("redb put commit failed: {:?}", e);
                (
                    vec![(
                        "_err".to_string(),
                        NodeData {
                            value: Value::Text(format!("{:?}", e)),
                            updated_at: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    Some(format!("redb put commit failed: {:?}", e)),
                )
            }
            Err(e) => {
                error!("redb put task panicked: {:?}", e);
                (
                    vec![(
                        "_err".to_string(),
                        NodeData {
                            value: Value::Text(format!("task panicked: {:?}", e)),
                            updated_at: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    Some(format!("redb put task panicked: {:?}", e)),
                )
            }
        };
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(put_id.to_string()), ctx.addr.clone());
        let _ = put_from.send(Message::Put(ack));
        if err_msg.is_some() {
            debug!("redb put ack sent with _err for {}", put_id);
        }
    }

    /// Sends a batch_put ack back to the originating node after commit.
    ///
    /// Mirrors `send_put_ack_after_commit` but for the batch case. Uses the
    /// same `_ack`/`_err` sentinel so the originating `Node::handle_put`
    /// drains `pending_puts` keyed by `batch.id`.
    fn send_batch_put_ack_after_commit(
        &self,
        batch_id: &str,
        batch_from: &Addr,
        result: &Result<Result<(), redb::Error>, tokio::task::JoinError>,
        ctx: &ActorContext,
    ) {
        let (ack_children, err_msg) = match result {
            Ok(Ok(())) => (
                vec![(
                    "_ack".to_string(),
                    NodeData {
                        value: Value::Text("ok".to_string()),
                        updated_at: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                )]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
                None,
            ),
            Ok(Err(e)) => {
                error!("redb batch_put commit failed: {:?}", e);
                (
                    vec![(
                        "_err".to_string(),
                        NodeData {
                            value: Value::Text(format!("{:?}", e)),
                            updated_at: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    Some(format!("redb batch_put commit failed: {:?}", e)),
                )
            }
            Err(e) => {
                error!("redb batch_put task panicked: {:?}", e);
                (
                    vec![(
                        "_err".to_string(),
                        NodeData {
                            value: Value::Text(format!("task panicked: {:?}", e)),
                            updated_at: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as f64,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    Some(format!("redb batch_put task panicked: {:?}", e)),
                )
            }
        };
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(batch_id.to_string()), ctx.addr.clone());
        let _ = batch_from.send(Message::Put(ack));
        if err_msg.is_some() {
            debug!("redb batch_put ack sent with _err for {}", batch_id);
        }
    }
}

impl Default for RedbStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage(suffix: &str) -> RedbStorage {
        let path = format!("/tmp/beam-test-{}-{}.redb", std::process::id(), suffix);
        RedbStorage::new_with_config(Config::default(), &path, None)
    }

    #[tokio::test]
    async fn test_redb_storage_creates_db() {
        let storage = create_test_storage("create");
        assert!(!storage.path.is_empty());
        let _ = std::fs::remove_file(&storage.path);
    }

    #[tokio::test]
    async fn test_redb_storage_default() {
        let storage = RedbStorage::default();
        let _ = std::fs::remove_file(&storage.path);
    }

    #[tokio::test]
    async fn test_redb_storage_clone() {
        let storage = create_test_storage("clone");
        let cloned = storage.clone();
        assert_eq!(storage.path, cloned.path);
        let _ = std::fs::remove_file(&storage.path);
    }

    /// Sentinel-drain protocol test: storage MUST always reply when
    /// `in_response_to` is set on the Get, regardless of checksum match.
    ///
    /// # Why this test exists
    ///
    /// BEAM's `Node::handle_put` only sends the `__beam_replay_complete__`
    /// sentinel after a Put with `in_response_to` is received. If storage
    /// stays silent when checksum matches, the client's `drain_until_sentinel`
    /// hangs forever. The client use case doesn't pre-set checksum (so this
    /// bug is latent), but ANY future caller who caches checksums would hit
    /// it.
    ///
    /// This test forces the bug by pre-computing the reply's checksum and
    /// putting it on the Get — exactly the pattern a caching client would
    /// use.
    #[tokio::test]
    async fn test_redb_get_always_replies_when_in_response_to_set() {
        use crate::actor::{Actor, ActorContext};
        use crate::message::Put;
        use std::collections::BTreeMap;
        use tokio::sync::mpsc::unbounded_channel;

        let mut storage = create_test_storage("ack-always");
        let ctx = ActorContext::new("test".to_string());

        // Pre-populate: store a child under node "n1" via the Actor entry point.
        let mut children = BTreeMap::new();
        children.insert(
            "k".to_string(),
            NodeData {
                value: Value::Text("v".to_string()),
                updated_at: 0.0,
            },
        );
        let mut nodes = BTreeMap::new();
        nodes.insert("n1".to_string(), children.clone());
        let seed_put = Put::new(nodes, None, ctx.addr.clone());
        Actor::handle(&mut storage, Message::Put(seed_put), &ctx).await;

        // Build a buffered `from` address so we can read the reply.
        let (tx, mut rx) = unbounded_channel::<Message>();
        let from_addr = crate::actor::Addr::new(tx);

        // Compute the checksum the storage will produce for the reply.
        let mut reply = Put::new(
            {
                let mut m = BTreeMap::new();
                m.insert("n1".to_string(), children.clone());
                m
            },
            Some("get-id-42".to_string()),
            ctx.addr.clone(),
        );
        reply.to_string(); // sets reply.checksum
        let matching_checksum = reply.checksum;

        // Construct a Get with checksum pre-set to MATCH the reply's
        // checksum. In the buggy code this triggers the no-reply branch.
        let get = Get {
            id: "get-id-42".to_string(),
            from: from_addr.clone(),
            recipients: None,
            node_id: "n1".to_string(),
            checksum: matching_checksum,
            child_key: None,
            json_str: None,
        };

        Actor::handle(&mut storage, Message::Get(get), &ctx).await;

        // Bug: with the old code, no reply arrives (timeout would be required).
        // Fix: redb_storage MUST always reply when in_response_to is Some.
        let received = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await;

        let _ = std::fs::remove_file(&storage.path);

        match received {
            Ok(Some(Message::Put(reply_put))) => {
                assert_eq!(
                    reply_put.in_response_to.as_deref(),
                    Some("get-id-42"),
                    "reply must carry in_response_to so client can drain sentinel"
                );
            }
            Ok(Some(other)) => panic!("expected Put reply, got {:?}", other),
            Ok(None) => panic!("sender closed before reply sent"),
            Err(_) => panic!(
                "BUG: redb_storage stayed silent despite matching in_response_to. \
                 This hangs drain_until_sentinel forever."
            ),
        }
    }
}
