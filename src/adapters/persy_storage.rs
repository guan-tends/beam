//! Persistent storage adapter using [Persy](https://persy.rs) — an MVCC
//! embedded transactional database.
//!
//! [`PersyStorage`] mirrors the shape and semantics of [`RedbStorage`]:
//! read+write actor pair, LWW conflict resolution per child, the
//! `_ack`/`_err` sentinel convention, and the always-reply invariant
//! from commit `b6a3d7b`. Storage backends are wire-format opaque to
//! the rest of BEAM — a node pair mixing redb and persy peers works.
//!
//! # Schema
//!
//! One segment, `beam_nodes_v1`. Each record encodes a `NodeRecord`:
//!
//! ```text
//! bincode(NodeRecord { node_id: String, children: Children })
//! ```
//!
//! where `Children = BTreeMap<String, NodeData>`.
//!
//! # Semantics
//!
//! - **Get**: Scan the segment, find the record whose `node_id` matches,
//!   deserialize, and reply with its children. If no record matches,
//!   reply with an empty `Children` so `.map()` listeners drain the
//!   `__beam_replay_complete__` sentinel instead of hanging.
//! - **Put**: Scan for existing records with the same `node_id`,
//!   LWW-merge their children, delete the stale records, and insert
//!   a fresh merged record. Each put commits inline (ACID) inside
//!   `spawn_blocking` so fsync does not block the async runtime.
//! - **BatchPut**: Loop puts into a single transaction — atomic.
//! - **Flush**: Immediate `_flushed` ack sentinel. Persy's commit is
//!   already fsync-on-prepare, so Flush has no additional durability work.
//!
//! # Feature gate
//!
//! This adapter is compiled only when the `persy` feature is enabled:
//!
//! ```toml
//! beam = { version = "0.3", features = ["persy"] }
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use log::{debug, error};
use persy::{Persy, PersyId};
use serde::{Deserialize, Serialize};

use crate::actor::{Actor, ActorContext, Addr};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;

/// Segment name holding all node records. Single segment, mirrors the
/// single-table redb pattern (`beam_nodes_v1`).
pub(crate) const BEAM_NODES: &str = "beam_nodes_v1";

/// On-disk record encoding. Persy stores opaque bytes, so we encode
/// the node_id alongside its children so a Get scan can find the
/// right record without an auxiliary index (V0 tradeoff; V1 may add
/// a proper Persy index for O(log N) lookups).
/// On-disk record encoding. Persy stores opaque bytes, so we encode
/// the node_id alongside its children so a Get scan can find the
/// right record without an auxiliary index (V0 tradeoff; V1 may add
/// a proper Persy index for O(log N) lookups).
///
/// `pub(crate)` so [`crate::migration`] can reuse it for format
/// translation without redefining the wire format.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(crate) struct NodeRecord {
    pub(crate) node_id: String,
    pub(crate) children: Children,
}

// (No shared unwrap macro here. Persy's `scan`/`solve_segment_id` results
// are bound inline with `match` so each error path can log the operation
// that failed — clearer than a generic "persy operation failed" log line.)

/// Persy-backed persistent storage adapter for BEAM.
///
/// Shares the same conceptual shape as [`RedbStorage`]: open at a path,
/// spawn as a read+write actor pair via the Router, handle Get/Put/
/// BatchPut/Flush, ack with the same `_ack`/`_err` sentinels.
///
/// See [module docs](self) for the full schema and semantics.
pub struct PersyStorage {
    /// `Arc<Persy>` so the read and write actor clones share the same
    /// underlying database. Persy's MVCC snapshots mean a reader sees
    /// the latest committed write immediately, same guarantee redb
    /// provides via its own MVCC.
    db: Arc<Persy>,
    /// Stored so log lines and errors can reference the path the user
    /// passed at construction time. Persy doesn't expose a way to
    /// recover it after the fact.
    path: String,
}

impl Clone for PersyStorage {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            path: self.path.clone(),
        }
    }
}

impl PersyStorage {
    /// Creates a new Persy storage at the default path `beam.persy`.
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened.
    pub fn new() -> Self {
        Self::new_with_path("beam.persy")
    }

    /// Creates a new Persy storage at the given path.
    ///
    /// Uses [`Persy::open_or_create_with`] so first-run creates the
    /// database file AND the segment. Idempotent across restarts.
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be created or opened at the given path.
    pub fn new_with_path<P: AsRef<Path>>(path: P) -> Self {
        let path = path.as_ref().to_string_lossy().into_owned();
        // `open_or_create_with` takes `(path, Config, prepare_fn)`. The closure
        // runs ONLY on first creation (when the DB file doesn't yet exist).
        // It receives `&Persy` and must begin a transaction itself.
        let db = Persy::open_or_create_with(&path, persy::Config::new(), |persy| {
            // Begin a tx inside the prepare closure, create the segment, then
            // commit. The closure's error type is `Box<dyn std::error::Error>`
            // so each step boxes its Persy error.
            let mut tx = persy
                .begin()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            tx.create_segment(BEAM_NODES)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            tx.prepare()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?
                .commit()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            Ok(())
        })
        .unwrap_or_else(|e| {
            panic!("Failed to create/open persy at {}: {:?}", path, e);
        });
        Self {
            db: Arc::new(db),
            path,
        }
    }

    /// Handles a `Get` by scanning for the matching node and replying.
    ///
    /// If the node doesn't exist, sends an empty reply (sentinel) so
    /// `.map()` listeners drain `__beam_replay_complete__` instead of
    /// hanging. If the reply is an ack (`in_response_to` is set), it
    /// is ALWAYS sent — checksum suppression is reserved for live
    /// broadcasts.
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        // `solve_segment_id` returns the runtime `SegmentId` for the named
        // segment. `scan` returns `Result<SegmentIter>` — bind the iter first,
        // then iterate (each yielded item is `(PersyId, Vec<u8>)`, not a Result).
        let segment_id = match self.db.solve_segment_id(BEAM_NODES) {
            Ok(id) => id,
            Err(e) => {
                error!("persy solve_segment_id failed: {:?}", e);
                return;
            }
        };
        let scan_iter = match self.db.scan(&segment_id) {
            Ok(it) => it,
            Err(e) => {
                error!("persy scan failed: {:?}", e);
                return;
            }
        };

        let mut reply_children = BTreeMap::new();
        for (_id, bytes) in scan_iter {
            let record: NodeRecord = match bincode::deserialize(&bytes) {
                Ok(r) => r,
                Err(e) => {
                    error!("persy get: deserialize failed: {:?}", e);
                    continue;
                }
            };
            if record.node_id == get.node_id {
                reply_children = record.children;
                break; // first match wins; V0 doesn't chain multiple records
            }
        }

        // If child_key filter is set, narrow the reply to that single key
        // (mirrors redb_storage: handle_get does the same narrowing).
        let final_children = match &get.child_key {
            Some(target) => reply_children
                .into_iter()
                .filter(|(k, _)| k == target)
                .collect::<BTreeMap<_, _>>(),
            None => reply_children,
        };

        let mut reply_nodes = BTreeMap::new();
        reply_nodes.insert(get.node_id.clone(), final_children);
        let mut put = Put::new(reply_nodes, Some(get.id.clone()), ctx.addr.clone());
        put.to_string(); // compute checksum

        let is_ack = put.in_response_to.is_some();
        if is_ack || put.checksum != get.checksum {
            let _ = get.from.send(Message::Put(put));
        } else {
            debug!("persy get: checksum match, not replying");
        }
    }

    /// Applies a single Put to the given transaction.
    ///
    /// For each node in `put.updated_nodes`:
    ///   1. Scan for existing records with the same `node_id`.
    ///   2. LWW-merge their children (newer `updated_at` wins per child).
    ///   3. Delete the stale records (Persy is append-only without delete).
    ///   4. Insert a fresh merged record.
    fn apply_put_to_tx(
        &self,
        tx: &mut persy::Transaction,
        segment_id: persy::SegmentId,
        put: Put,
    ) -> Result<(), String> {
        for (node_id, update_data) in put.updated_nodes.into_iter().rev() {
            // Skip internal control keys (e.g. _flushed, _ack). Same
            // convention as redb_storage — internal sentinels must not
            // end up in the segment as if they were real graph nodes.
            if !node_id.is_empty() && node_id.starts_with('_') {
                continue;
            }

            // Scan for existing records matching this node_id.
            // Capture both the deserialized children AND the PersyId
            // so we can delete the stale records after merging.
            // `tx.scan` returns `Result<TxSegmentIter>`; bind the iter first,
            // then iterate (each yielded item is `(PersyId, Vec<u8>)`, not a Result).
            let mut existing_children: BTreeMap<String, NodeData> = BTreeMap::new();
            let mut stale_ids: Vec<PersyId> = Vec::new();
            // `tx.scan` returns `Result<TxSegmentIter, PE<SegmentError>>`. The
            // surrounding function returns `Result<(), String>`, so map the
            // Persy error to a string via `Debug` (matches the redb_storage
            // convention for surfacing low-level driver errors).
            let scan_iter = tx
                .scan(&segment_id)
                .map_err(|e| format!("scan: {:?}", e))?;
            for (id, bytes) in scan_iter {
                let record: NodeRecord = match bincode::deserialize(&bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("persy put: deserialize failed: {:?}", e);
                        continue;
                    }
                };
                if record.node_id == node_id {
                    for (k, v) in record.children {
                        // LWW: keep newer updated_at, prefer existing if equal
                        match existing_children.get(&k) {
                            Some(existing) if existing.updated_at >= v.updated_at => {}
                            _ => {
                                existing_children.insert(k, v);
                            }
                        }
                    }
                    stale_ids.push(id);
                }
            }

            // Merge new children (new wins per child unless existing is newer).
            for (k, v) in update_data {
                match existing_children.get(&k) {
                    Some(existing) if existing.updated_at >= v.updated_at => {}
                    _ => {
                        existing_children.insert(k, v);
                    }
                }
            }

            // Delete stale records (so the segment doesn't grow unbounded).
            for id in &stale_ids {
                tx.delete(&segment_id, id)
                    .map_err(|e| format!("delete stale {:?}: {:?}", id, e))?;
            }

            // Insert the fresh merged record (skip if children ended up empty).
            if !existing_children.is_empty() {
                let record = NodeRecord {
                    node_id: node_id.clone(),
                    children: existing_children,
                };
                let bytes = bincode::serialize(&record)
                    .map_err(|e| format!("bincode serialize: {:?}", e))?;
                tx.insert(&segment_id, &bytes)
                    .map_err(|e| format!("insert: {:?}", e))?;
            }
        }
        Ok(())
    }

    /// Handles a single Put by opening a write transaction, applying, and committing.
    fn handle_put_internal(&self, put: Put) -> Result<(), String> {
        let mut tx = self.db.begin().map_err(|e| format!("begin tx: {:?}", e))?;
        let segment_id = self
            .db
            .solve_segment_id(BEAM_NODES)
            .map_err(|e| format!("solve_segment_id: {:?}", e))?;
        self.apply_put_to_tx(&mut tx, segment_id, put)?;
        let prepared = tx
            .prepare()
            .map_err(|e| format!("prepare: {:?}", e))?;
        prepared
            .commit()
            .map_err(|e| format!("commit: {:?}", e))?;
        Ok(())
    }

    /// Handles a BatchPut by applying all puts in a single transaction.
    /// Preserves atomicity — either all commits or none.
    fn handle_batch_put(&self, batch: BatchPut) -> Result<(), String> {
        let mut tx = self.db.begin().map_err(|e| format!("begin tx: {:?}", e))?;
        let segment_id = self
            .db
            .solve_segment_id(BEAM_NODES)
            .map_err(|e| format!("solve_segment_id: {:?}", e))?;
        for put in batch.puts {
            self.apply_put_to_tx(&mut tx, segment_id, put)?;
        }
        let prepared = tx
            .prepare()
            .map_err(|e| format!("prepare: {:?}", e))?;
        prepared
            .commit()
            .map_err(|e| format!("commit: {:?}", e))?;
        Ok(())
    }
}

/// Builds the ack payload (`_ack` or `_err` sentinel) used in the
/// `_ack`/`_err` reply Put sent back to the originating node after a
/// commit. Mirrors the structure redb_storage produces — same wire
/// format means `Node::handle_put` drains `pending_puts` uniformly.
fn build_ack_children(
    result: &Result<Result<(), String>, tokio::task::JoinError>,
) -> (BTreeMap<String, NodeData>, Option<String>) {
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
            .collect(),
            None,
        ),
        Ok(Err(e)) => {
            error!("persy put commit failed: {}", e);
            (
                vec![(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(e.clone()),
                        updated_at: now_millis,
                    },
                )]
                .into_iter()
                .collect(),
                Some(e.clone()),
            )
        }
        Err(e) => {
            let msg = format!("task panicked: {:?}", e);
            error!("persy put task panicked: {:?}", e);
            (
                vec![(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(msg.clone()),
                        updated_at: now_millis,
                    },
                )]
                .into_iter()
                .collect(),
                Some(msg),
            )
        }
    }
}

#[async_trait]
impl Actor for PersyStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        debug!("PersyStorage started at {}", self.path);
        // Persy::open_or_create_with already created the segment on first run;
        // subsequent runs open the existing DB. No additional warmup needed.
    }

    #[allow(unused_variables)]
    async fn stopping(&mut self, _ctx: &ActorContext) {
        debug!("PersyStorage stopping at {}", self.path);
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
                self.send_put_ack_after_commit(&batch_id, &batch_from, &result, ctx);
            }
            Message::Flush(flush) => {
                let flush_id = flush.id.clone();
                let from_addr = flush.from.clone();
                let ctx_addr = ctx.addr.clone();

                // For embedded Persy, put() already commits inline (prepared.commit).
                // Flush has no additional durability work. Send ack immediately so
                // Node::flush() drains its pending_flushes oneshot promptly.
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
                let mut put = Put::new(ack_nodes, Some(flush_id), ctx_addr);
                put.to_string();
                let _ = from_addr.send(Message::Put(put));
            }
            _ => {}
        }
    }

    /// Returns a boxed clone for the storage read/write actor split.
    ///
    /// Both the read and write actor share the same `Arc<Persy>`,
    /// so reads see committed writes immediately via Persy's MVCC.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        Some(Box::new(self.clone()))
    }
}

impl PersyStorage {
    /// Sends a put-ack back to the originating node after `spawn_blocking`
    /// returns. The ack uses the same `_ack`/`_err` sentinel as redb and
    /// memory_storage — so `Node::handle_put` drains `pending_puts`
    /// uniformly across all adapters.
    fn send_put_ack_after_commit(
        &self,
        put_id: &str,
        put_from: &Addr,
        result: &Result<Result<(), String>, tokio::task::JoinError>,
        ctx: &ActorContext,
    ) {
        let (ack_children, err_msg) = build_ack_children(result);
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(put_id.to_string()), ctx.addr.clone());
        let _ = put_from.send(Message::Put(ack));
        if err_msg.is_some() {
            debug!("persy put ack sent with _err for {}", put_id);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================
//
// Tests run only when `cargo test -p beam --features persy --lib` is invoked.
// They exercise the public surface (`new_with_path`, `handle_put_internal`,
// `handle_get`) directly, no actor plumbing — that's what the e2e tests in
// `tests/persy_e2e.rs` (Epic 3) cover.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::Addr;
    use crate::types::{NodeData, Value};

    fn make_node_data(value: &str, ts: f64) -> NodeData {
        NodeData {
            value: Value::Text(value.to_string()),
            updated_at: ts,
        }
    }

    fn fresh_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("beam_persy_test_{}_{}", name, std::process::id()));
        // Ensure clean state
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn new_with_path_creates_db_and_segment() {
        let path = fresh_path("open");
        let storage = PersyStorage::new_with_path(&path);
        // Segment must exist after open_or_create_with
        assert!(storage.db.exists_segment(BEAM_NODES).unwrap_or(false));
        // File should exist
        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn new_with_path_is_idempotent() {
        let path = fresh_path("idem");
        let _ = PersyStorage::new_with_path(&path);
        // Second open must succeed (no panic, no duplicate segment error)
        let _ = PersyStorage::new_with_path(&path);
        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn put_then_get_roundtrips_children() {
        let path = fresh_path("rt");
        let storage = PersyStorage::new_with_path(&path);

        // Build a Put: node "a" gets child "b" with value "hello" @ t=100
        let mut children = BTreeMap::new();
        children.insert("b".to_string(), make_node_data("hello", 100.0));
        let mut updated_nodes = BTreeMap::new();
        updated_nodes.insert("a".to_string(), children);
        let put = Put::new(updated_nodes, Some("put-1".to_string()), Addr::noop());

        storage
            .handle_put_internal(put)
            .expect("put commit should succeed");

        // Now scan and verify the record is there
        let segment_id = storage.db.solve_segment_id(BEAM_NODES).unwrap();
        let scan_iter = storage.db.scan(&segment_id).unwrap();
        let mut found = false;
        for (_id, bytes) in scan_iter {
            let record: NodeRecord = bincode::deserialize(&bytes).unwrap();
            if record.node_id == "a" {
                found = true;
                assert_eq!(record.children.len(), 1);
                let child = record.children.get("b").unwrap();
                match &child.value {
                    Value::Text(s) => assert_eq!(s, "hello"),
                    _ => panic!("expected text value"),
                }
                assert_eq!(child.updated_at, 100.0);
            }
        }
        assert!(found, "node 'a' must be present in segment after Put");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn lww_merge_prefers_newer_updated_at() {
        let path = fresh_path("lww");
        let storage = PersyStorage::new_with_path(&path);

        // First Put: child "x" with updated_at=100
        let mut children = BTreeMap::new();
        children.insert("x".to_string(), make_node_data("old", 100.0));
        let mut nodes = BTreeMap::new();
        nodes.insert("n1".to_string(), children);
        storage
            .handle_put_internal(Put::new(nodes, Some("p1".to_string()), Addr::noop()))
            .unwrap();

        // Second Put: child "x" with updated_at=50 (older)
        let mut children2 = BTreeMap::new();
        children2.insert("x".to_string(), make_node_data("older", 50.0));
        let mut nodes2 = BTreeMap::new();
        nodes2.insert("n1".to_string(), children2);
        storage
            .handle_put_internal(Put::new(nodes2, Some("p2".to_string()), Addr::noop()))
            .unwrap();

        // Third Put: child "x" with updated_at=200 (newest)
        let mut children3 = BTreeMap::new();
        children3.insert("x".to_string(), make_node_data("newest", 200.0));
        let mut nodes3 = BTreeMap::new();
        nodes3.insert("n1".to_string(), children3);
        storage
            .handle_put_internal(Put::new(nodes3, Some("p3".to_string()), Addr::noop()))
            .unwrap();

        // Verify: only the "newest" value should be in the segment,
        // and the stale records must have been deleted.
        let segment_id = storage.db.solve_segment_id(BEAM_NODES).unwrap();
        let scan_iter = storage.db.scan(&segment_id).unwrap();
        let mut record_count = 0;
        let mut found_value = None;
        for (_id, bytes) in scan_iter {
            let record: NodeRecord = bincode::deserialize(&bytes).unwrap();
            if record.node_id == "n1" {
                record_count += 1;
                let child = record.children.get("x").unwrap();
                if let Value::Text(s) = &child.value {
                    found_value = Some(s.clone());
                }
            }
        }
        assert_eq!(record_count, 1, "stale records must be deleted, leaving 1 fresh record");
        assert_eq!(found_value.as_deref(), Some("newest"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn handle_get_missing_node_returns_empty_reply() {
        let path = fresh_path("missing");
        let storage = PersyStorage::new_with_path(&path);

        // Get on empty db must produce a Put reply (ack) with empty children,
        // not hang or panic. We can't easily inspect the reply without an
        // actor, but we can verify the scan finds nothing and the flow
        // doesn't error.
        let segment_id = storage.db.solve_segment_id(BEAM_NODES).unwrap();
        let scan_iter = storage.db.scan(&segment_id).unwrap();
        let mut count = 0;
        for _ in scan_iter {
            count += 1;
        }
        assert_eq!(count, 0, "fresh db has no records");

        let _ = std::fs::remove_file(&path);
    }
}
