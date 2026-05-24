use std::collections::{BTreeMap, HashMap, HashSet};

use crate::actor::{Actor, ActorContext};
use crate::message::{Get, Message, Put};
use crate::types::*;

use async_trait::async_trait;
use log::{debug, info};
use std::sync::Arc;
use parking_lot::RwLock;

pub struct MemoryStorage {
    store: Arc<RwLock<HashMap<String, Children>>>, // could use an LRU cache or other existing option
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        if let Some(children) = self.store.read().get(&get.node_id).cloned() {
            debug!("have {}: {:?}", get.node_id, children);
            let reply_with_children = match &get.child_key {
                Some(child_key) => {
                    // reply with specific child if it's found
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
                None => children.clone(), // reply with all children of this node
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

    fn handle_put(&self, put: Put, _ctx: &ActorContext) {
        for (node_id, update_data) in put.updated_nodes.iter().rev() {
            // return in reverse
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
        /*
        if self.config.stats {
            self.update_stats();
        }
         */
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext) {
        match message {
            Message::Get(get) => self.handle_get(get, ctx),
            Message::Put(put) => self.handle_put(put, ctx),
            Message::Flush(flush) => {
                // Memory storage has no disk state; flush is a no-op.
                // Ack the barrier so callers never hang.
                let mut ack = BTreeMap::new();
                ack.insert("_flushed".to_string(), NodeData {
                    value: Value::Text("true".to_string()),
                    updated_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as f64,
                });
                let mut nodes = BTreeMap::new();
                nodes.insert("_ack".to_string(), ack);
                let mut put = Put::new(nodes, Some(flush.id), ctx.addr.clone());
                put.to_string();
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
}
