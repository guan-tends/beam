//! Relay throughput integration benchmark — measures real WebSocket
//! relay throughput with memory-only storage.
//!
//! This test answers: "How many messages per second can a BEAM relay
//! route between WebSocket clients?"
//!
//! # How to Run
//!
//! ```bash
//! cargo test --test relay_throughput_bench -- --ignored --nocapture
//! ```
//!
//! The `--ignored` flag is required because these are benchmarks, not
//! CI tests. `--nocapture` prints the throughput report to stdout.

use beam::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer, WsServerConfig};
use beam::{Config, Node, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::time::Duration;

/// Start a memory-only relay (no redb, no disk I/O).
///
/// Returns the Node so the caller can read metrics after the benchmark.
async fn start_relay(port: u16) -> Node {
    let config = Config::default();
    let ws_config = WsServerConfig {
        port,
        cert_path: None,
        key_path: None,
    };

    let node = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(WsServer::new_with_config(
            Config::default(),
            ws_config,
        ))],
    );

    // Wait for the WebSocket port to be bound
    loop {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Give the web server child task time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
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
    // Let the WebSocket handshake complete
    tokio::time::sleep(Duration::from_millis(300)).await;
    node
}

/// Run a single throughput measurement and print a report.
///
/// 1. Start a memory-only relay
/// 2. Connect a subscriber that counts received messages
/// 3. Connect N sender clients, each sending M Put messages
/// 4. Wait for messages to be received (or timeout)
/// 5. Report throughput and metrics snapshot
async fn run_bench(senders: usize, messages: usize, port: u16) {
    let mut relay = start_relay(port).await;

    // Connect subscriber first so it's ready before messages flow
    let mut subscriber_node = connect_client(port).await;
    let mut sub = subscriber_node.get("bench").on();

    // Connect sender clients
    let mut sender_nodes = Vec::new();
    for _ in 0..senders {
        let node = connect_client(port).await;
        sender_nodes.push(node);
    }

    // Let all handshakes and Hi exchanges settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send messages from all senders
    let total_messages = senders * messages;
    let start = Instant::now();

    for (sender_idx, sender_node) in sender_nodes.iter_mut().enumerate() {
        for i in 0..messages {
            let key = format!("bench/{}_{}", sender_idx, i);
            let _ = sender_node
                .get(&key)
                .put(Value::Text(format!("msg_{}", i)))
                .await;
        }
    }

    // Collect received messages with a generous timeout
    let received = Arc::new(AtomicU64::new(0));
    let received_clone = received.clone();
    let deadline = Instant::now() + Duration::from_secs(60);

    let collector = tokio::spawn(async move {
        loop {
            match tokio::time::timeout(Duration::from_millis(100), sub.recv()).await {
                Ok(Ok(_)) => {
                    received_clone.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) => break, // channel closed
                Err(_) => {
                    // No message for 100ms — if we're past the deadline, stop
                    if Instant::now() >= deadline {
                        break;
                    }
                    // If no message arrived for 100ms and we've received
                    // something, the relay is likely drained
                    if received_clone.load(Ordering::Relaxed) > 0 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if received_clone.load(Ordering::Relaxed)
                            == received_clone.load(Ordering::Relaxed)
                        {
                            // No new messages in 500ms — relay drained
                            break;
                        }
                    }
                }
            }
        }
    });

    collector.await.unwrap();

    let elapsed = start.elapsed();
    let received_count = received.load(Ordering::Relaxed);
    let throughput = received_count as f64 / elapsed.as_secs_f64();

    // Print report
    println!("\n============================================================");
    println!("  RELAY THROUGHPUT BENCHMARK");
    println!("============================================================");
    println!("  Senders:          {}", senders);
    println!("  Messages/sender:  {}", messages);
    println!("  Total sent:       {}", total_messages);
    println!("  Total received:   {}", received_count);
    println!("  Elapsed:          {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput:       {:.0} msgs/sec", throughput);
    println!();

    // Print metrics snapshot from the relay
    let snap = relay.metrics().snapshot();
    println!("  --- Hot-Path Metrics Snapshot ---");
    println!("  ws_messages_received: {}", snap.ws_messages_received);
    println!("  messages_parsed:      {}", snap.messages_parsed);
    println!("  messages_dropped_dup: {}", snap.messages_dropped_dup);
    println!("  messages_relayed:     {}", snap.messages_relayed);
    println!("  subscriber_fanout:    {}", snap.subscriber_fanout_total);
    println!("  serialization_calls:  {}", snap.serialization_calls);
    println!("  ws_messages_sent:     {}", snap.ws_messages_sent);
    println!("  dropped_sends:        {}", snap.dropped_sends);
    println!("============================================================\n");

    // Cleanup
    for mut node in sender_nodes {
        node.stop();
    }
    subscriber_node.stop();
    relay.stop();
}

/// Single sender, 10k messages — baseline throughput.
#[tokio::test]
#[ignore = "benchmark — run with --ignored --nocapture"]
async fn relay_throughput_1_sender_10k() {
    run_bench(1, 10_000, 9950).await;
}

/// Single sender, 50k messages — sustained throughput.
#[tokio::test]
#[ignore = "benchmark — run with --ignored --nocapture"]
async fn relay_throughput_1_sender_50k() {
    run_bench(1, 50_000, 9952).await;
}

/// 10 senders × 5k messages each — concurrent throughput.
#[tokio::test]
#[ignore = "benchmark — run with --ignored --nocapture"]
async fn relay_throughput_10_senders_5k_each() {
    run_bench(10, 5_000, 9954).await;
}
