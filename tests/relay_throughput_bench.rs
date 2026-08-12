#![cfg(not(target_arch = "wasm32"))]
//! Relay throughput integration benchmark — measures real WebSocket
//! relay throughput with memory-only storage.
//!
//! This test answers: "How many messages per second can a BEAM relay
//! route between WebSocket clients?"
//!
//! # How to Run
//!
//! ```bash
//! cargo test --release --test relay_throughput_bench -- --ignored --nocapture
//! ```
//!
//! The `--release` flag is essential — debug mode is 10-100x slower.
//! `--ignored` is required because these are benchmarks, not CI tests.
//!
//! # Methodology
//!
//! Throughput is measured from the relay's perspective using the hot-path
//! metrics counters (T1-T2). The relay's `messages_relayed` and
//! `ws_messages_sent` counters are the ground truth for relay throughput —
//! they count messages that the router actually dispatched to peers.
//!
//! We send N puts from a single sender, wait for the relay's counters to
//! stabilize, then report throughput as messages_relayed / elapsed.

use beam::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer, WsServerConfig};
use beam::{Config, Node, Value};
use std::time::{Duration, Instant};
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
    // Wait for the WebSocket port to be bound
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

/// Run a single throughput measurement and print a report.
///
/// 1. Start a memory-only relay
/// 2. Connect a subscriber (so relay has someone to fan-out to)
/// 3. Connect N sender clients
/// 4. Send M puts per sender, timing the send phase
/// 5. Wait for relay counters to stabilize
/// 6. Report throughput from relay metrics (ground truth)
async fn run_bench(senders: usize, messages: usize, port: u16) {
    let mut relay = start_relay(port).await;

    // Connect subscriber so the relay has a fan-out target
    let mut subscriber_node = connect_client(port).await;

    // Connect sender clients
    let mut sender_nodes = Vec::new();
    for _ in 0..senders {
        let node = connect_client(port).await;
        sender_nodes.push(node);
    }

    // Let all handshakes and Hi exchanges settle
    sleep(Duration::from_millis(500)).await;

    // Snapshot metrics before sending
    let before = relay.metrics().snapshot();

    // Send messages
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
    let send_elapsed = start.elapsed();

    // Wait for relay counters to stabilize (no new messages for 500ms)
    let stabilize_start = Instant::now();
    loop {
        let snap = relay.metrics().snapshot();
        sleep(Duration::from_millis(500)).await;
        let snap2 = relay.metrics().snapshot();
        if snap.messages_relayed == snap2.messages_relayed {
            break;
        }
        if stabilize_start.elapsed() > Duration::from_secs(30) {
            break;
        }
    }

    let total_elapsed = start.elapsed();
    let after = relay.metrics().snapshot();

    // Calculate throughput from relay metrics (ground truth)
    let relayed = after.messages_relayed - before.messages_relayed;
    let ws_sent = after.ws_messages_sent - before.ws_messages_sent;
    let ws_recv = after.ws_messages_received - before.ws_messages_received;
    let parsed = after.messages_parsed - before.messages_parsed;
    let dropped_dup = after.messages_dropped_dup - before.messages_dropped_dup;
    let fanout = after.subscriber_fanout_total - before.subscriber_fanout_total;
    let serialized = after.serialization_calls - before.serialization_calls;

    let throughput = if total_elapsed.as_secs_f64() > 0.0 {
        relayed as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let send_rate = if send_elapsed.as_secs_f64() > 0.0 {
        (senders * messages) as f64 / send_elapsed.as_secs_f64()
    } else {
        0.0
    };

    // Print report
    println!("\n============================================================");
    println!("  RELAY THROUGHPUT BENCHMARK");
    println!("============================================================");
    println!("  Senders:          {}", senders);
    println!("  Messages/sender:  {}", messages);
    println!("  Total sent:       {}", senders * messages);
    println!(
        "  Send phase:        {:.3}s ({:.0} puts/sec)",
        send_elapsed.as_secs_f64(),
        send_rate
    );
    println!("  Total elapsed:    {:.3}s", total_elapsed.as_secs_f64());
    println!();
    println!("  --- Relay Hot-Path Counters ---");
    println!("  ws_messages_received: {}", ws_recv);
    println!("  messages_parsed:      {}", parsed);
    println!("  messages_dropped_dup: {}", dropped_dup);
    println!("  messages_relayed:     {}", relayed);
    println!("  subscriber_fanout:    {}", fanout);
    println!("  serialization_calls:  {}", serialized);
    println!("  ws_messages_sent:     {}", ws_sent);
    println!(
        "  dropped_sends:        {}",
        after.dropped_sends - before.dropped_sends
    );
    println!();
    println!("  Throughput:       {:.0} msgs/sec (relayed)", throughput);
    println!(
        "  Fanout ratio:     {:.1}",
        if relayed > 0 {
            fanout as f64 / relayed as f64
        } else {
            0.0
        }
    );
    println!(
        "  Dedup rate:       {:.1}%",
        if parsed > 0 {
            100.0 * dropped_dup as f64 / parsed as f64
        } else {
            0.0
        }
    );
    println!("============================================================\n");

    // Cleanup
    for mut node in sender_nodes {
        node.stop();
    }
    subscriber_node.stop();
    relay.stop();
}

/// Single sender, 10k messages — baseline throughput.
///
/// Uses `current_thread` runtime — the actor system's `yield_now()` in
/// `handle_batch` prevents cooperative scheduling starvation. S4 used
/// `current_thread` and achieved 3,221 puts/sec; S5's switch to
/// `multi_thread(4)` added 3.2% scheduler overhead + Condvar parking
/// with no throughput benefit for the sequential `put().await` pattern.
#[tokio::test]
#[ignore = "benchmark — run with --release --ignored --nocapture"]
async fn relay_throughput_1_sender_10k() {
    run_bench(1, 10_000, 9970).await;
}

/// Single sender, 50k messages — sustained throughput.
#[tokio::test]
#[ignore = "benchmark — run with --release --ignored --nocapture"]
async fn relay_throughput_1_sender_50k() {
    run_bench(1, 50_000, 9972).await;
}

/// 10 senders × 5k messages each — concurrent throughput.
#[tokio::test]
#[ignore = "benchmark — run with --release --ignored --nocapture"]
async fn relay_throughput_10_senders_5k_each() {
    run_bench(10, 5_000, 9974).await;
}
