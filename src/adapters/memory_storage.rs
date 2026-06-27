//! In-memory storage adapter — the default storage backend for Rod.
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
use crate::message::{Get, Message, Put};
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
    fn handle_put(&self, put: Put, _ctx: &ActorContext) {
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
    }
}

#[async_trait]
impl Actor for MemoryStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        info!("MemoryStorage adapter starting");
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext) {
        match message {
            Message::Get(get) => self.handle_get(get, ctx),
            Message::Put(put) => self.handle_put(put, ctx),
            Message::Flush(flush) => {
                // Memory storage has no disk state; flush is a no-op.
                // Ack the barrier so callers never hang.
                let mut ack = BTreeMap::new();
                ack.insert(
                    "_flushed".to_string(),
                    NodeData {
                        value: Value::Text("true".to_string()),
                        updated_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as f64,
                    },
                );
                let mut nodes = BTreeMap::new();
                nodes.insert("_ack".to_string(), ack);
                let mut put = Put::new(nodes, Some(flush.id), ctx.addr.clone());
                put.to_string(); // compute checksum
                let _ = flush.from.send(Message::Put(put));
            }
            Message::BatchPut(batch) => {
                for put in batch.puts {
                    self.handle_put(put, ctx);
                }
            }
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
