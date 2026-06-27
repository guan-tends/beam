//! Dump redb storage — inspect on-disk contents of a Rod redb database.
//!
//! Usage:
//! ```bash
//! cargo run --example dump_redb -- /path/to/rod.redb
//! ```
//! Prints all node IDs, their children, update timestamps, and value types.

use redb::{Database, ReadableTable, TableDefinition};
use redb::{ReadableDatabase, ReadableTableMetadata};

const ROD_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("rod_nodes_v1");

fn main() {
    let path = std::env::args().nth(1).expect("Usage: dump_redb <path>");
    let db = Database::create(&path).unwrap();
    let rtx = db.begin_read().unwrap();
    let table = rtx.open_table(ROD_NODES).unwrap();

    for entry in table.iter().unwrap() {
        let (k, v) = entry.unwrap();
        let node_id: String = k.value().to_string();
        let bytes = v.value();
        let children: std::collections::BTreeMap<String, rod::types::NodeData> =
            match bincode::deserialize(bytes) {
                Ok(c) => c,
                Err(e) => {
                    println!("{}: BINCODE_ERR={}", node_id, e);
                    continue;
                }
            };
        println!("NODE_ID: '{}'  children={}", node_id, children.len());
        for (child_key, node_data) in &children {
            println!(
                "  child={} updated_at={} value_type={}",
                child_key,
                node_data.updated_at,
                node_data.value.to_string()
            );
        }
    }
    println!("=== TOTAL NODES: {:?} ===", table.len().unwrap());
}
