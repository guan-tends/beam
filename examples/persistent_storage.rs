//! # Persistent Storage with RedbStorage
//!
//! Demonstrates writing data to a BEAM node backed by `RedbStorage`,
//! flushing it to disk, then opening a **new** node on the same database
//! file and reading the data back — proving durability across restarts.
//!
//! ## What You'll Learn
//!
//! - How to create a node with persistent storage (`RedbStorage`)
//! - How `flush_storage` acts as a write barrier
//! - How to reopen a database file and read persisted data
//!
//! ## Run
//!
//! ```sh
//! cargo run --example persistent_storage
//! ```
//!
//! ## Expected Output
//!
//! ```text
//! Phase 1: Writing data to persistent storage...
//! Wrote: name = "BEAM"
//! Flushing storage (write barrier)...
//! Flush acknowledged — data is on disk.
//!
//! Phase 2: Reopening database in a new node...
//! Read back: name = "BEAM"
//! Persistence confirmed — data survived node restart.
//! ```

use std::time::Duration;

use beam::adapters::RedbStorage;
use beam::{Config, Node, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a temp file so the example is self-contained and repeatable.
    let db_path = std::env::temp_dir().join(format!("beam-example-{}.redb", std::process::id()));
    let _ = std::fs::remove_file(&db_path);
    let path_str = db_path.to_string_lossy().to_string();

    // ── Phase 1: Write + Flush ─────────────────────────────────────────
    println!("Phase 1: Writing data to persistent storage...");

    {
        let config = Config::default();
        let mut db = Node::new_with_config(
            config.clone(),
            vec![Box::new(RedbStorage::new_with_config(
                config,
                &path_str,
                None,
            ))],
            vec![],
        );

        // Put a value and let the storage adapter process it.
        db.get("name").put(Value::Text("BEAM".into())).await?;
        println!("Wrote: name = \"BEAM\"");

        // flush_storage blocks until the write is committed to disk.
        // This is the write barrier — data is durable when this returns Ok.
        println!("Flushing storage (write barrier)...");
        db.flush_storage(Some(Duration::from_secs(5))).await?;
        println!("Flush acknowledged — data is on disk.");

        // Stop the node so the database file handle is released.
        db.stop();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // ── Phase 2: Reopen + Read ─────────────────────────────────────────
    println!("\nPhase 2: Reopening database in a new node...");

    {
        let config = Config::default();
        let mut db = Node::new_with_config(
            config.clone(),
            vec![Box::new(RedbStorage::new_with_config(
                config,
                &path_str,
                None,
            ))],
            vec![],
        );

        // once() reads the current value without subscribing.
        let value = db
            .get("name")
            .once(Some(Duration::from_secs(3)))
            .await;

        match value {
            Some(Value::Text(s)) => {
                println!("Read back: name = {:?}", s);
                assert_eq!(s, "BEAM", "persisted value should match");
                println!("Persistence confirmed — data survived node restart.");
            }
            other => panic!("Expected Value::Text(\"BEAM\"), got {:?}", other),
        }

        db.stop();
    }

    // Clean up the temp database file.
    let _ = std::fs::remove_file(&db_path);

    // Exit explicitly — background actor tasks keep the tokio runtime alive.
    std::process::exit(0);
}
