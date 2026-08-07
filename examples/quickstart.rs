//! Quickstart — the smallest BEAM program.
//!
//! Creates an in-memory node, puts a value, and receives it via a
//! subscription. No network, no persistence — just the core pub/sub loop.
//!
//! # Run
//!
//! ```bash
//! cargo run --example quickstart
//! ```
//!
//! # Expected Output
//!
//! ```text
//! Received: Hello, BEAM!
//! ```

use beam::{Node, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A `Node` is the fundamental unit of BEAM's graph database.
    // `new()` creates an ephemeral in-memory node with no peers.
    let mut db = Node::new();

    // `get(key)` traverses (or creates) a child node in the graph.
    // The returned `Node` is a handle to that child — writes go to it,
    // and subscriptions on it fire when the value changes.
    let mut greeting = db.get("greeting");

    // Subscribe *before* putting so we don't miss the event.
    // `on()` returns a `broadcast::Receiver<Value>`.
    let mut sub = greeting.on();

    // `put(value)` writes a value to this node and propagates it
    // through the graph. The subscription we just registered will
    // fire when the value lands.
    greeting.put(Value::Text("Hello, BEAM!".into())).await?;

    // Receive the value we just put.
    let received = sub.recv().await?;
    assert_eq!(received, Value::Text("Hello, BEAM!".into()));
    println!("Received: {}", received.to_string());

    db.stop();
    Ok(())
}
