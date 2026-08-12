//! Local-only put benchmark — no WebSocket, no relay.
//! Tests if the local put → ack path scales to 10k.
//!
//! Native-only: uses `multi_thread` tokio runtime (unavailable on WASM)
//! and `std::time::Instant` (not available on WASM).

#![cfg(not(target_arch = "wasm32"))]

use beam::{Config, Node, Value};
use std::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark"]
async fn local_put_10k() {
    let mut node = Node::new_with_config(
        Config::default(),
        vec![Box::new(beam::adapters::MemoryStorage::new())],
        vec![],
    );
    // No adapters = no WsConn, no relay. Pure local put → ack.

    let start = Instant::now();
    for i in 0..10_000 {
        let key = format!("bench/{}", i);
        let _ = node.get(&key).put(Value::Text(format!("msg_{}", i))).await;
        if i == 99 || i == 499 || i == 999 || i % 5000 == 4999 {
            eprintln!(
                "[LOCAL] {} puts ({:.1}s)",
                i + 1,
                start.elapsed().as_secs_f64()
            );
        }
    }
    let elapsed = start.elapsed();
    eprintln!(
        "[LOCAL] 10k puts done in {:.3}s ({:.0} puts/sec)",
        elapsed.as_secs_f64(),
        10000.0 / elapsed.as_secs_f64()
    );
}
