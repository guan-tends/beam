// Minimal reproducer for Node::put + Node::map interaction.
// Tests single-key (broken), multi-key (mnemos pattern), and batch_put (reference).

use beam::{Node, Value};
use std::time::Duration;

async fn drain_map(rx: &mut tokio::sync::broadcast::Receiver<(String, Value)>) -> Vec<(String, Value)> {
    let mut children = Vec::new();
    let timeout = tokio::time::sleep(Duration::from_secs(2));
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            Ok((k, v)) = rx.recv() => {
                if k == "__rod_replay_complete__" {
                    break;
                }
                children.push((k, v));
            }
            _ = &mut timeout => break,
        }
    }
    children
}

#[tokio::main]
async fn main() {
    println!("=== Reproducer: Node::put + Node::map ===\n");

    // TEST 1: single-key path
    {
        println!("--- TEST 1: single-key path (single put then map) ---");
        let mut root = Node::new();
        let mut listing = root.get("listing");
        listing.put(Value::Text("single_value".into())).await.unwrap();

        let mut rx = root.get("listing").map();
        let children = drain_map(&mut rx).await;
        println!("TEST 1 RESULT: {} children (expected 1)\n", children.len());
        for (k, v) in &children { println!("  child: {} = {:?}", k, v); }
    }

    // TEST 2: multi-key path (mnemos audit pattern)
    {
        println!("--- TEST 2: multi-key path (mnemos audit pattern) ---");
        let mut root = Node::new();
        // mnemos does: traverse(&root, ["audit", "chain1"]).put(value)
        let mut leaf = root.get("audit").get("chain1");
        leaf.put(Value::Text("audit_value".into())).await.unwrap();

        let mut rx = root.get("audit").map();
        let children = drain_map(&mut rx).await;
        println!("TEST 2 RESULT: {} children (expected 1)", children.len());
        for (k, v) in &children { println!("  child: {} = {:?}", k, v); }
    }

    // TEST 3: batch_put (working reference)
    {
        println!("\n--- TEST 3: batch_put reference ---");
        let mut root = Node::new();
        root.batch_put(vec![
            (vec!["audit".to_string(), "chain_a".to_string()], Value::Text("a".into())),
            (vec!["audit".to_string(), "chain_b".to_string()], Value::Text("b".into())),
        ]).await.unwrap();

        let mut rx = root.get("audit").map();
        let children = drain_map(&mut rx).await;
        println!("TEST 3 RESULT: {} children (expected 2)", children.len());
        for (k, v) in &children { println!("  child: {} = {:?}", k, v); }
    }

    println!("\n=== Reproducer complete ===");
}
