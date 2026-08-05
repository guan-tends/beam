//! Minimal example — create a Node, put a value, subscribe, and receive.
//!
//! No network, no storage — just the core pub/sub loop in memory.
//!
//! ```bash
//! cargo run --example minimal
//! ```

use beam::Node;

#[tokio::main]
async fn main() {
    eprintln!("A: Node::new()");
    let mut db = Node::new();
    eprintln!("B: get child 'greeting'");
    let mut node = db.get("greeting");
    eprintln!("C: id = {}", node.id());
    eprintln!("D: put value");
    let _ = node.put("Hello World!".into());
    eprintln!("E: subscribe");
    let mut sub = node.on();
    eprintln!("F: recv");
    match sub.recv().await {
        Ok(beam::Value::Text(s)) => eprintln!("G: RECEIVED = {}", s),
        Ok(other) => eprintln!("G: other = {:?}", other),
        Err(e) => eprintln!("G: error = {}", e),
    }
    eprintln!("H: stop");
    db.stop();
    eprintln!("I: DONE!");
}
