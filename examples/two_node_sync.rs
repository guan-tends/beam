//! Two-node sync — connect two BEAM nodes over WebSocket and replicate data.
//!
//! This is the "aha" moment for BEAM: two independent nodes, connected only
//! by a WebSocket link, automatically synchronize graph data. A `put` on one
//! node arrives at the other in real time.
//!
//! # Concepts
//!
//! - **`WsServer`** — a BEAM node that listens for incoming WebSocket
//!   connections. Runs inside the node's actor system.
//! - **`OutgoingWebsocketManager`** — a network adapter that connects to a
//!   remote `WsServer` and relays graph operations over the wire.
//! - **`Node::new_with_config(config, storage, network)`** — full constructor
//!   that lets you specify storage and network adapters explicitly.
//!
//! # Architecture
//!
//! ```text
//!   Node A (server)                    Node B (client)
//!   ┌───────────────┐                  ┌───────────────┐
//!   │ MemoryStorage │                  │ MemoryStorage │
//!   │ WsServer:9494 │ ◄── WebSocket ── │ WsClient      │
//!   └───────────────┘                  └───────────────┘
//!         │                                   │
//!    put("hello") ────────── wire ──────► on() receives
//! ```
//!
//! # Run
//!
//! ```bash
//! cargo run --example two_node_sync
//! ```
//!
//! # Expected Output
//!
//! ```text
//! Server listening on port 9494
//! Client connected to server
//! Put 'Hello from Node A!' on server
//! Node B received: Hello from Node A!
//! Sync complete!
//! ```

use beam::adapters::{OutgoingWebsocketManager, WsServer, WsServerConfig};
use beam::{Config, Node, Value};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};

/// Poll a TCP port until it accepts connections or the timeout elapses.
///
/// This avoids blind `sleep()` races against the actor's `pre_start`
/// phase — the `WsServer` binds its listener asynchronously inside the
/// actor system, so the port isn't ready the instant `new_with_config`
/// returns.
async fn wait_for_port(port: u16, timeout_ms: u64) {
    let deadline = Duration::from_millis(timeout_ms);
    let start = tokio::time::Instant::now();
    while start.elapsed() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("port {port} did not become ready within {timeout_ms}ms");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = 9494u16;
    let url = format!("ws://localhost:{port}/ws");

    // --- Node A: server ---
    // Hosts a WsServer that listens for incoming connections. Uses
    // in-memory storage (no persistence needed for this demo).
    let config_a = Config::default();
    let ws_server = WsServer::new_with_config(
        config_a.clone(),
        WsServerConfig { port, ..WsServerConfig::default() },
    );
    let mut node_a = Node::new_with_config(
        config_a,
        vec![],
        vec![Box::new(ws_server.clone())],
    );

    // --- Node B: client ---
    // Connects to Node A's WsServer. The OutgoingWebsocketManager handles
    // reconnection automatically with exponential backoff.
    let ws_client = OutgoingWebsocketManager::new(Config::default(), vec![url]);
    let mut node_b = Node::new_with_config(
        Config::default(),
        vec![],
        vec![Box::new(ws_client)],
    );

    // Wait for the server's TCP listener to be ready, then wait for
    // the client to complete the WebSocket handshake.
    wait_for_port(port, 5000).await;
    println!("Server listening on port {port}");

    // Poll until the WebSocket handshake completes (peer registered).
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        if ws_server.peer_count() >= 1 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        ws_server.peer_count() >= 1,
        "client did not connect within 5s"
    );
    println!("Client connected to server");

    // Subscribe on Node B *before* Node A puts, so we catch the broadcast.
    let mut sub_b = node_b.get("message").on();

    // Put on Node A — the value propagates over the WebSocket to Node B.
    node_a
        .get("message")
        .put(Value::Text("Hello from Node A!".into()))
        .await?;
    println!("Put 'Hello from Node A!' on server");

    // Receive on Node B.
    let received = timeout(Duration::from_secs(10), sub_b.recv())
        .await
        .expect("timeout: Node B did not receive the message")?;
    assert_eq!(received, Value::Text("Hello from Node A!".into()));
    println!("Node B received: {}", received.to_string());

    println!("Sync complete!");

    node_a.stop();
    node_b.stop();
    Ok(())
}
