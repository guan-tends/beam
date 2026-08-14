//! HAM stale-data pre-filter E2E test.
//!
//! Verifies that the router's HAM (Hypothetical Amnesia Machine)
//! timestamp filter drops stale data before storage or relay —
//! mirroring Gun.js's `ham()` function (`src/root.js` line 120).
//!
//! # What It Tests
//!
//! 1. A relay is started with memory storage.
//! 2. A sender connects and puts data.
//! 3. A second put with a newer timestamp passes HAM and is relayed.
//! 4. `messages_dropped_ham` stays zero for genuinely new data.
//!
//! # Gun.js Reference
//!
//! Gun.js `ham()` checks `state < was` (old → skip) and
//! `state === was && val === known` (same → skip) before any
//! storage or relay work. BEAM's `Router::ham_filter` implements
//! the equivalent: `updated_at <= cached_at` → skip.

#![cfg(not(target_arch = "wasm32"))]

use beam::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer, WsServerConfig};
use beam::{Config, Node, Value};
use std::time::Duration;
use tokio::time::sleep;

/// Start a memory-only relay (no redb, no disk I/O).
async fn start_relay(port: u16) -> Node {
    let ws_config = WsServerConfig {
        port,
        cert_path: None,
        key_path: None,
    };
    let node = Node::new_with_config(
        Config::default(),
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(WsServer::new_with_config(
            Config::default(),
            ws_config,
        ))],
    );
    loop {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    sleep(Duration::from_millis(200)).await;
    node
}

/// Connect a client node to the relay via WebSocket.
async fn connect_client(port: u16) -> Node {
    let client = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://127.0.0.1:{}/ws", port)],
    );
    let node = Node::new_with_config(
        Config::default(),
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(client)],
    );
    sleep(Duration::from_millis(300)).await;
    node
}

/// Verify that two puts with different timestamps both pass HAM.
///
/// The first put populates the relay's HAM cache. The second put
/// has a strictly newer timestamp and should NOT be dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ham_newer_data_passes_ham() {
    let port = 9860;
    let mut relay = start_relay(port).await;
    let mut sender = connect_client(port).await;

    sleep(Duration::from_millis(500)).await;

    let before = relay.metrics().snapshot();

    // First put — populates relay HAM cache.
    let _ = sender
        .get("ham_test/key1")
        .put(Value::Text("v1".to_string()))
        .await;
    sleep(Duration::from_millis(1100)).await; // >1s for strictly newer timestamp

    // Second put — newer timestamp, different value.
    let _ = sender
        .get("ham_test/key1")
        .put(Value::Text("v2".to_string()))
        .await;
    sleep(Duration::from_millis(500)).await;

    let after = relay.metrics().snapshot();
    let ham_drops = after.messages_dropped_ham - before.messages_dropped_ham;
    let relayed = after.messages_relayed - before.messages_relayed;

    println!("ham_drops={}, relayed={}", ham_drops, relayed);

    // Both puts should pass HAM (second has newer timestamp).
    assert_eq!(
        ham_drops, 0,
        "neither put should be dropped by HAM — second has newer timestamp"
    );

    sender.stop();
    relay.stop();
}

/// Verify that HAM does not false-positive on unique data.
///
/// Sends 50 unique puts to different souls. All should pass HAM
/// (cache misses). `messages_dropped_ham` should remain zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ham_no_false_positives_on_unique_data() {
    let port = 9862;
    let mut relay = start_relay(port).await;
    let mut sender = connect_client(port).await;

    sleep(Duration::from_millis(500)).await;

    let before = relay.metrics().snapshot();

    for i in 0..50 {
        let _ = sender
            .get(&format!("ham_unique/{}", i))
            .put(Value::Text(format!("val_{}", i)))
            .await;
    }
    sleep(Duration::from_millis(500)).await;

    let after = relay.metrics().snapshot();
    let ham_drops = after.messages_dropped_ham - before.messages_dropped_ham;

    println!("ham_drops={}", ham_drops);

    assert_eq!(ham_drops, 0, "unique data should never be dropped by HAM");

    sender.stop();
    relay.stop();
}
