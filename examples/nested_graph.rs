//! Nested graph — traversing children, batch writes, and `map()`.
//!
//! Demonstrates BEAM's graph data model: nodes have named children, and
//! `map()` lets you subscribe to *all* children of a node at once. This is
//! the building block for collections, indexes, and any parent→children
//! relationship.
//!
//! # Concepts
//!
//! - **`get("a").get("b")`** — traverse two levels deep. The path is
//!   `["a", "b"]`, and the leaf node holds the value.
//! - **`batch_put(ops)`** — atomically write multiple values in one call.
//!   Each op is a `(path, value)` pair.
//! - **`map()`** — subscribe to every child key under a node. Existing
//!   children are replayed first, then a `__beam_replay_complete__` sentinel
//!   signals that the replay is done. After that, new children arrive in
//!   real time.
//!
//! # Run
//!
//! ```bash
//! cargo run --example nested_graph
//! ```
//!
//! # Expected Output
//!
//! ```text
//! Children under 'users':
//!   alice = Alice
//!   bob = Bob
//!   carol = Carol
//! ```

use beam::{Node, Value};
use tokio::time::{Duration, timeout};

/// Sentinel emitted by `map()` to signal that the replay of existing
/// children is complete. After this sentinel, new children arrive in
/// real time.
const REPLAY_SENTINEL: &str = "__beam_replay_complete__";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Node::new();

    // Write three users under the "users" node using a single batch call.
    // Each entry is (path, value) — the path is a list of keys from root.
    db.batch_put(vec![
        (
            vec!["users".into(), "alice".into()],
            Value::Text("Alice".into()),
        ),
        (
            vec!["users".into(), "bob".into()],
            Value::Text("Bob".into()),
        ),
        (
            vec!["users".into(), "carol".into()],
            Value::Text("Carol".into()),
        ),
    ])
    .await?;

    // `map()` subscribes to all children of the "users" node. The router
    // replays existing children first, then sends the sentinel.
    let mut sub = db.get("users").map();

    // Collect all replayed children (until sentinel or timeout).
    let mut children = Vec::new();
    let _ = timeout(Duration::from_secs(5), async {
        loop {
            match sub.recv().await {
                Ok((key, value)) => {
                    if key == REPLAY_SENTINEL {
                        break;
                    }
                    children.push((key, value));
                }
                Err(_) => break,
            }
        }
    })
    .await;

    // Sort for deterministic output regardless of replay order.
    children.sort_by(|a, b| a.0.cmp(&b.0));

    println!("Children under 'users':");
    for (key, value) in &children {
        println!("  {} = {}", key, value.to_string());
    }

    assert_eq!(children.len(), 3);
    assert_eq!(children[0].0, "alice");
    assert_eq!(children[1].0, "bob");
    assert_eq!(children[2].0, "carol");

    db.stop();
    Ok(())
}
