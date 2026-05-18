use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actor::{Actor, ActorContext};
use crate::message::{Flush, Get, Message, Put};
use crate::types::*;
use crate::Config;

use async_trait::async_trait;
use log::{debug, error};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::task;

const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");
const ROD_META: TableDefinition<&str, u64> = TableDefinition::new("rod_meta_v1");

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

/// redb-backed persistent storage adapter for Rod.
///
/// Schema: single table `rod_nodes_v1` where
///   key   = node_id (&str)
///   value = bincode(BTreeMap<String, NodeData>)
///
/// Read operations are sync within the actor task.
/// Put commits inline for ACID ordering (fsync on local redb is fast).
/// Flush writes a meta marker and acks for barrier semantics.
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
    /// Open (or create) a redb database at the given path.
    pub fn new() -> Self {
        Self::new_with_config(Config::default(), "rod.redb", None)
    }

    /// Open (or create) with explicit config and optional size cap.
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

    fn handle_get(&self, get: Get, ctx: &ActorContext) {
        let read_txn = unwrap_or_return!(self.db.begin_read());
        let table = unwrap_or_return!(read_txn.open_table(ROD_NODES));

        let children_for_node = match table.get(&*get.node_id) {
            Ok(Some(access_guard)) => {
                let bytes = access_guard.value();
                unwrap_or_return!(
                    bincode::deserialize::<BTreeMap<String, NodeData>>(bytes)
                )
            }
            Ok(None) => {
                debug!("redb get: no data for node_id={}", get.node_id);
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
        put.to_string();

        if put.checksum != get.checksum {
            let _ = get.from.send(Message::Put(put));
        } else {
            debug!("redb get: checksum match, not replying");
        }
    }

    fn handle_put_internal(&self, put: Put) -> Result<(), redb::Error> {
        let wtxn = self.db.begin_write()?;
        {
            let mut node_table = wtxn.open_table(ROD_NODES)?;
            let mut meta_table = wtxn.open_table(ROD_META)?;

            for (node_id, update_data) in put.updated_nodes.into_iter().rev() {
                if !node_id.is_empty() && node_id.starts_with('_') {
                    continue;
                }

                let mut children_for_node: BTreeMap<String, NodeData> =
                    match node_table.get(&*node_id)? {
                        Some(access_guard) => {
                            let bytes = access_guard.value();
                            bincode::deserialize(bytes).unwrap_or_default()
                        }
                        None => BTreeMap::new(),
                    };

                for (child_id, child_data) in update_data {
                    let should_write = match children_for_node.get(&child_id) {
                        Some(existing) if existing.updated_at > child_data.updated_at => false,
                        _ => true,
                    };

                    if should_write {
                        children_for_node.insert(child_id, child_data);
                    }
                }

                if children_for_node.is_empty() {
                    node_table.remove(&*node_id)?;
                } else {
                    let bytes = bincode::serialize(&children_for_node)
                        .map_err(|e| redb::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("bincode serialize: {:?}", e),
                        )))?;
                    node_table.insert(&*node_id, bytes.as_slice())?;
                }
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            meta_table.insert("_last_write", now)?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn handle_flush_internal(&self, _flush: Flush) -> Result<(), redb::Error> {
        let wtxn = self.db.begin_write()?;
        {
            let mut meta_table = wtxn.open_table(ROD_META)?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            meta_table.insert("_last_flush", now)?;
        }
        wtxn.commit()?;
        Ok(())
    }
}

#[async_trait]
impl Actor for RedbStorage {
    async fn pre_start(&mut self, _ctx: &ActorContext) {
        debug!("RedbStorage started at {}", self.path);
    }

    #[allow(unused_variables)]
    async fn stopping(&mut self, _ctx: &ActorContext) {
        debug!("RedbStorage stopping at {}", self.path);
    }

    async fn handle(&mut self, message: Message, ctx: &ActorContext) {
        match message {
            Message::Get(get) => self.handle_get(get, ctx),
            Message::Put(put) => {
                // Inline commit: redb is local embedded storage; fsync is fast
                if let Err(e) = self.handle_put_internal(put) {
                    error!("redb put commit failed: {:?}", e);
                }
            }
            Message::Flush(flush) => {
                let self_clone = self.clone();
                let flush_id = flush.id.clone();
                let from_addr = flush.from.clone();
                let ctx_addr = ctx.addr.clone();
                let _ = task::spawn_blocking(move || {
                    if let Err(e) = self_clone.handle_flush_internal(flush) {
                        error!("redb flush commit failed: {:?}", e);
                        return;
                    }

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
                    put.to_string();
                    let _ = from_addr.send(Message::Put(put));
                })
                .await;
            }
            _ => {}
        }
    }
}
