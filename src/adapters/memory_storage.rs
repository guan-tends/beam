//! In-memory storage adapter — the default storage backend for BEAM.
//!
//! [`MemoryStorage`] stores all graph data in a `HashMap` protected by a
//! `parking_lot::RwLock`. It is the simplest storage adapter and is used
//! by default when no persistent storage is configured.
//!
//! # Semantics
//!
//! - **Get**: Returns the stored children for a node ID. If the node has no
//!   children, an empty `Put` reply is sent (so `.map()` listeners don't hang).
//! - **Put**: Merges incoming data with existing children using `updated_at`
//!   timestamps for conflict resolution (last-write-wins per child).
//! - **Flush**: No-op (memory storage has no disk state). Sends an immediate
//!   ack so callers never hang waiting for a barrier.
//! - **BatchPut**: Processes each constituent `Put` sequentially.
//!
//! # Thread Safety
//!
//! The store is `Arc<RwLock<HashMap>>`, allowing concurrent reads and
//! exclusive writes. The actor model ensures messages are processed
//! sequentially within the actor's task.

#![allow(clippy::mutable_key_type)] // Addr hashes by id field, not interior-mutable sender

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::actor::{Actor, ActorContext};
use crate::message::{BatchPut, Get, Message, Put};
use crate::types::*;

use async_trait::async_trait;
use log::{debug, info};
use parking_lot::RwLock;
use std::sync::Arc;

/// In-memory storage adapter backed by `HashMap<String, Children>`.
///
/// See the [module docs](self) for semantics.
#[derive(Clone)]
pub struct MemoryStorage {
    store: Arc<RwLock<HashMap<String, Children>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Creates a new empty in-memory storage adapter.
    pub fn new() -> Self {
        MemoryStorage {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Handles a `Get` request by looking up the node ID and replying with
    /// its children (or an empty reply if not found).
    ///
    /// If `child_key` is specified in the `Get`, only that specific child is
    /// returned. If the child doesn't exist, no reply is sent (the requester
    /// simply doesn't receive data).
    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        if let Some(children) = self.store.read().get(&get.node_id).cloned() {
            debug!("have {}: {:?}", get.node_id, children);
            let reply_with_children = match &get.child_key {
                Some(child_key) => {
                    // Reply with specific child if it's found
                    match children.get(child_key) {
                        Some(child_val) => {
                            let mut r = BTreeMap::new();
                            r.insert(child_key.clone(), child_val.clone());
                            r
                        }
                        None => {
                            return;
                        }
                    }
                }
                None => children.clone(), // Reply with all children of this node
            };
            let mut reply_with_nodes = BTreeMap::new();
            reply_with_nodes.insert(get.node_id.clone(), reply_with_children);
            let mut recipients = HashSet::new();
            recipients.insert(get.from.clone());
            let my_addr = ctx.addr.clone();
            let put = Put::new(reply_with_nodes, Some(get.id.clone()), my_addr);
            let _ = get.from.send(Message::Put(put));
        } else {
            debug!("have not {}", get.node_id);
            // Empty set: still a valid replay. Emit sentinel so `.map()` doesn't hang.
            let mut reply_with_nodes = BTreeMap::new();
            reply_with_nodes.insert(get.node_id.clone(), BTreeMap::new());
            let put = Put::new(reply_with_nodes, Some(get.id.clone()), ctx.addr.clone());
            let _ = get.from.send(Message::Put(put));
        }
    }

    /// Handles a `Put` by merging `updated_nodes` into the store.
    ///
    /// For each node, existing children are compared by `updated_at` —
    /// a child is only overwritten if the incoming `updated_at` is >= the
    /// existing one (last-write-wins).
    ///
    /// After a successful merge, sends an ack `Put` directly back to the
    /// originating node (NOT through the router — that would route through
    /// `seen_get_messages` and be silently dropped). The ack payload uses
    /// the same `_ack`/`_err` sentinel children as `Flush`, so the originating
    /// `Node::handle_put` can drain `pending_puts` and resolve the awaiter.
    fn handle_put(&self, put: Put, ctx: &ActorContext) {
        let put_result = self.apply_put(&put);
        self.send_put_ack(&put, &put_result, ctx);
    }

    /// Applies a put to the in-memory store, returning Ok or an error string.
    fn apply_put(&self, put: &Put) -> Result<(), String> {
        for (node_id, update_data) in put.updated_nodes.iter().rev() {
            debug!("saving k-v {}: {:?}", node_id, update_data);
            let mut write = self.store.write();
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
        Ok(())
    }

    /// Sends a put-ack message directly to the originating node's addr.
    ///
    /// Uses the same sentinel convention as the Flush ack:
    /// - `_ack` child → success
    /// - `_err` child carrying the message → failure
    fn send_put_ack(&self, put: &Put, result: &Result<(), String>, ctx: &ActorContext) {
        let mut ack_children = BTreeMap::new();
        match result {
            Ok(()) => {
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
            }
            Err(msg) => {
                ack_children.insert(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(msg.clone()),
                        updated_at: web_time::SystemTime::now()
                            .duration_since(web_time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                );
            }
        }
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        let ack = Put::new(nodes, Some(put.id.clone()), ctx.addr.clone());
        let _ = put.from.send(Message::Put(ack));
    }

    /// Handles a `BatchPut` by applying each constituent Put and sending
    /// a single batch ack back to the originating node.
    fn handle_batch_put(&self, batch: BatchPut, ctx: &ActorContext) {
        let mut last_err: Option<String> = None;
        for put in batch.puts.iter() {
            if let Err(e) = self.apply_put(put) {
                last_err = Some(e);
            }
        }
        let result = last_err.map(Err).unwrap_or(Ok(()));
        self.send_batch_put_ack(&batch, &result, ctx);
    }

    /// Sends a `BatchPut` ack back to the originating node.
    ///
    /// Mirrors the Put ack pattern but uses the BatchPut message type. The
    /// originating node's `handle_put` only drains on `Message::Put` acks —
    /// we still need to drain `pending_puts` for the batch case. The cleanest
    /// way: send the batch ack back as a single Put ack message keyed on
    /// `batch.id`, reusing the same routing.
    fn send_batch_put_ack(
        &self,
        batch: &BatchPut,
        result: &Result<(), String>,
        ctx: &ActorContext,
    ) {
        let mut ack_children = BTreeMap::new();
        match result {
            Ok(()) => {
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
            }
            Err(msg) => {
                ack_children.insert(
                    "_err".to_string(),
                    NodeData {
                        value: Value::Text(msg.clone()),
                        updated_at: web_time::SystemTime::now()
                            .duration_since(web_time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                );
            }
        }
        let mut nodes = BTreeMap::new();
        nodes.insert("_ack".to_string(), ack_children);
        // Send as Put ack keyed on batch.id so Node::handle_put drains it.
        let ack = Put::new(nodes, Some(batch.id.clone()), ctx.addr.clone());
        let _ = batch.from.send(Message::Put(ack));
    }
}

#[async_trait]
impl Actor for MemoryStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        info!("MemoryStorage adapter starting");
    }

    async fn handle(&mut self, message: Arc<Message>, ctx: &ActorContext) {
        match &*message {
            Message::Get(get) => self.handle_get(get.clone(), ctx),
            Message::Put(put) => self.handle_put(put.clone(), ctx),
            Message::Flush(flush) => {
                // Memory storage has no disk state; flush is a no-op.
                // Ack the barrier so callers never hang.
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
                let put = Put::new(nodes, Some(flush.id.clone()), ctx.addr.clone());
                put.to_string(); // compute checksum
                let _ = flush.from.send(Message::Put(put));
            }
            Message::BatchPut(batch) => self.handle_batch_put(batch.clone(), ctx),
            _ => {}
        }
    }

    /// MemoryStorage does not split into read/write actors.
    ///
    /// In-memory writes are synchronous (no fsync), so there is no benefit
    /// to splitting reads from writes. Keeping a single actor preserves
    /// read-after-write ordering for tests and synchronous use cases.
    fn try_clone_storage(&self) -> Option<Box<dyn Actor>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_storage_new() {
        let storage = MemoryStorage::new();
        assert!(storage.store.read().is_empty());
    }

    #[tokio::test]
    async fn test_memory_storage_default() {
        let storage = MemoryStorage::default();
        assert!(storage.store.read().is_empty());
    }
}
