//! Echo-back regression test — verifies that messages received from a relay
//! are NOT echoed back to the relay.
//!
//! This test prevents the regression where a subscriber's `server_peers`
//! loop forwards a put back to the relay it received it from, causing
//! message amplification (4x-18x) and throughput collapse.
//!
//! # What It Tests
//!
//! 1. A relay is started
//! 2. A sender and subscriber connect to the relay
//! 3. The sender sends N puts
//! 4. The relay's `ws_messages_received` is checked: it should be ~N (the
//!    original puts), NOT 2N or 3N (which would indicate echo-back)
//! 5. The relay's `messages_relayed` should be ~N (each put relayed once)
//!
//! # Gun.js Reference
//!
//! Gun.js `mesh.say` prevents echo-back via two checks:
//!   1. `if(peer === meta.via){ return false }` — don't send back to sender
//!   2. `if(meta.yo && meta.yo[peer.id]){ return false }` — hops check
//!
//! BEAM's `handle_put_relay` implements the equivalent:
//!   1. `from_remote_peer` check — skip server_peers if message came from
//!      a known peer (equivalent to `meta.via`)
//!   2. Hops check in subscribers and known_peers sections

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
    // Wait for the WebSocket port to be bound.
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

/// Verify that the relay does NOT receive echo-back messages from subscribers.
///
/// Sends 100 puts from a sender. The relay should receive ~100 messages
/// (the original puts). If echo-back is present, the relay will receive
/// 200-300+ messages because subscribers echo received puts back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_echo_back_single_sender() {
    let port = 9800;
    let mut relay = start_relay(port).await;
    let mut subscriber = connect_client(port).await;
    let mut sender = connect_client(port).await;

    // Let all Hi handshakes settle.
    sleep(Duration::from_millis(500)).await;

    let before = relay.metrics().snapshot();

    // Send 100 puts.
    for i in 0..100 {
        let _ = sender
            .get(&format!("echo_test/{}", i))
            .put(Value::Text(format!("val_{}", i)))
            .await;
    }

    // Wait for relay counters to stabilize.
    sleep(Duration::from_millis(500)).await;
    let after = relay.metrics().snapshot();

    let ws_recv = after.ws_messages_received - before.ws_messages_received;
    let relayed = after.messages_relayed - before.messages_relayed;
    let dropped_dup = after.messages_dropped_dup - before.messages_dropped_dup;

    println!(
        "ws_recv={}, relayed={}, dropped_dup={}",
        ws_recv, relayed, dropped_dup
    );

    // The relay should receive ~100 messages (the original puts).
    // With echo-back, it would receive 200-400+.
    // Allow some slack for Hi messages and handshake noise.
    assert!(
        ws_recv <= 150,
        "relay received {} WS messages for 100 puts — echo-back detected! \
         (expected ~100, got {})",
        ws_recv,
        ws_recv
    );

    // The relay should relay ~100 messages (one per put).
    assert!(
        relayed >= 90 && relayed <= 110,
        "relay relayed {} messages for 100 puts — expected ~100",
        relayed
    );

    sender.stop();
    subscriber.stop();
    relay.stop();
}

/// Verify that with 10 senders, message amplification stays low.
///
/// Without echo-back prevention, 10 senders × 50 puts = 500 puts generates
/// 8800+ WS messages on the relay (17.6x amplification). With echo-back
/// prevention, amplification should be minimal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark — run with --release --ignored --nocapture"]
async fn no_echo_back_10_senders() {
    let port = 9802;
    let mut relay = start_relay(port).await;
    let mut subscriber = connect_client(port).await;

    let mut senders = Vec::new();
    for _ in 0..10 {
        senders.push(connect_client(port).await);
    }

    sleep(Duration::from_millis(500)).await;

    let before = relay.metrics().snapshot();

    for (idx, sender) in senders.iter_mut().enumerate() {
        for i in 0..50 {
            let _ = sender
                .get(&format!("multi/{}/{}", idx, i))
                .put(Value::Text(format!("val_{}_{}", idx, i)))
                .await;
        }
    }

    // Wait for stabilization.
    sleep(Duration::from_secs(2)).await;
    let after = relay.metrics().snapshot();

    let ws_recv = after.ws_messages_received - before.ws_messages_received;
    let relayed = after.messages_relayed - before.messages_relayed;
    let total_sent = 10 * 50;

    println!(
        "total_sent={}, ws_recv={}, relayed={}, amplification={:.1}x",
        total_sent,
        ws_recv,
        relayed,
        ws_recv as f64 / total_sent as f64
    );

    // Amplification should be low. Without echo-back: 17x+.
    // With echo-back prevention: should be close to 1x.
    let amplification = ws_recv as f64 / total_sent as f64;
    assert!(
        amplification < 3.0,
        "message amplification is {:.1}x — echo-back still present \
         (expected <3x, got {:.1}x with {} recv for {} sent)",
        amplification,
        amplification,
        ws_recv,
        total_sent
    );

    for mut s in senders {
        s.stop();
    }
    subscriber.stop();
    relay.stop();
}

/// Verify that a subscriber receives messages relayed from the sender,
/// confirming that echo-back prevention doesn't break legitimate forwarding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_forwarding_still_works() {
    let port = 9804;
    let mut relay = start_relay(port).await;
    let mut subscriber = connect_client(port).await;
    let mut sender = connect_client(port).await;

    sleep(Duration::from_millis(500)).await;

    // Subscriber subscribes to the topic.
    let _ = subscriber.get("fwd_test").once(None).await;
    sleep(Duration::from_millis(200)).await;

    let before = relay.metrics().snapshot();

    // Sender puts to the same topic.
    for i in 0..10 {
        let _ = sender
            .get("fwd_test")
            .get(&format!("{}", i))
            .put(Value::Text(format!("msg_{}", i)))
            .await;
    }

    sleep(Duration::from_millis(500)).await;
    let after = relay.metrics().snapshot();

    let relayed = after.messages_relayed - before.messages_relayed;

    println!("relayed={} for 10 puts", relayed);

    // The relay should have relayed the puts.
    assert!(
        relayed >= 8,
        "relay only relayed {} messages for 10 puts — forwarding broken!",
        relayed
    );

    sender.stop();
    subscriber.stop();
    relay.stop();
}
