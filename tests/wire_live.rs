//! Layer 3: Live integration tests for BEAM ↔ Gun.js wire compatibility.
//!
//! These tests spawn a real Gun.js relay server as a subprocess, connect a
//! BEAM node to it via WebSocket, and verify bidirectional data sync.
//!
//! All tests are `#[ignore]` by default — they require Node.js and Gun.js
//! to be installed. Run with:  cargo test --test wire_live -- --ignored
//!
//! # Prerequisites
//!
//! ```sh
//! cd tests/wire-live && npm install
//! ```
//!
//! # Architecture
//!
//! ```text
//!  ┌──────────┐   WebSocket   ┌───────────┐
//!  │ BEAM     │ ────────────► │ Gun.js    │
//!  │ Node     │ ◄──────────── │ Relay     │
//!  └──────────┘   wire msgs   └───────────┘
//!       │                           │
//!       ▼                           ▼
//!  Node::get().put()          HTTP API (port+1)
//!  Node::get().on()           GET /get?soul=X&key=Y
//!                             POST /put {soul,key,value}
//! ```
//!
//! The Gun.js relay exposes an HTTP API on a separate port for the test
//! harness to verify data from Gun.js's side.

#![cfg(test)]

mod common;

use beam::adapters::*;
use beam::{Config, Node, Value};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

// ---------------------------------------------------------------------------
// Relay lifecycle helpers
// ---------------------------------------------------------------------------

/// Gun.js relay process handle. Drop to kill.
struct GunRelay {
    child: Child,
    ws_port: u16,
    api_port: u16,
}

impl GunRelay {
    /// Spawn a Gun.js relay on the given WebSocket port.
    /// The HTTP API listens on `ws_port + 1`.
    fn spawn(ws_port: u16) -> Self {
        let api_port = ws_port + 1;
        let child = Command::new("node")
            .arg("gun_relay.js")
            .arg(ws_port.to_string())
            .arg(api_port.to_string())
            .current_dir("tests/wire-live")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn gun_relay.js — is Node.js installed?");

        Self {
            child,
            ws_port,
            api_port,
        }
    }

    /// Wait for the HTTP API to become ready.
    async fn wait_for_ready(&self, timeout_ms: u64) {
        let start = std::time::Instant::now();
        let limit = Duration::from_millis(timeout_ms);
        while start.elapsed() < limit {
            if TcpStream::connect(format!("127.0.0.1:{}", self.api_port))
                .await
                .is_ok()
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("Gun relay API did not become ready within {}ms", timeout_ms);
    }

    /// HTTP GET to the relay's API.
    async fn api_get(&self, path: &str) -> String {
        let url = format!("http://127.0.0.1:{}{}", self.api_port, path);
        reqwest_text(&url).await
    }

    /// HTTP POST to the relay's API.
    async fn api_post(&self, path: &str, body: &str) -> String {
        let url = format!("http://127.0.0.1:{}{}", self.api_port, path);
        reqwest_post(&url, body).await
    }
}

impl Drop for GunRelay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers (using tokio-tungstenite's HTTP client or raw TCP)
// ---------------------------------------------------------------------------

/// Minimal HTTP GET using raw TCP — no extra dependencies.
async fn reqwest_text(url: &str) -> String {
    // Parse URL manually (host:port/path)
    let url = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = url.split_once('/').unwrap_or((url, "/"));
    let (host, port_str) = host_port.rsplit_once(':').unwrap_or((host_port, "80"));
    let port: u16 = port_str.parse().unwrap_or(80);

    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .expect("failed to connect to API");

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        path, host
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf).to_string();

    // Extract body (after \r\n\r\n)
    if let Some(idx) = response.find("\r\n\r\n") {
        response[idx + 4..].to_string()
    } else {
        response
    }
}

/// Minimal HTTP POST using raw TCP — no extra dependencies.
async fn reqwest_post(url: &str, body: &str) -> String {
    let url = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = url.split_once('/').unwrap_or((url, "/"));
    let (host, port_str) = host_port.rsplit_once(':').unwrap_or((host_port, "80"));
    let port: u16 = port_str.parse().unwrap_or(80);

    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .expect("failed to connect to API");

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        host,
        body.len(),
        body
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf).to_string();

    if let Some(idx) = response.find("\r\n\r\n") {
        response[idx + 4..].to_string()
    } else {
        response
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test 1: BEAM puts data → Gun.js receives and stores it.
/// BEAM writes a value, then we query Gun.js's HTTP API to verify
/// the relay received and stored the data.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn beam_put_gun_receives() {
    let relay = GunRelay::spawn(9871);
    relay.wait_for_ready(10000).await;

    // Create BEAM node connected to the Gun.js relay via WebSocket
    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(
        config.clone(),
        vec![format!("ws://127.0.0.1:{}/gun", relay.ws_port)],
    );
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    // Wait for WebSocket connection
    wait_for_connected(&ws_client, 1, 10000).await;

    // BEAM puts data
    beam.get("beamtest/put1")
        .get("name")
        .put("Alice".into())
        .await
        .unwrap();

    // Give the relay time to receive and store the data
    sleep(Duration::from_secs(2)).await;

    // Verify Gun.js received the data
    let response = relay.api_get("/get?soul=beamtest/put1&key=name").await;
    assert!(
        response.contains("Alice"),
        "Gun.js did not receive BEAM's put. Response: {}",
        response
    );

    beam.stop();
}

/// Test 2: Gun.js puts data → BEAM receives and stores it.
/// We use the relay's HTTP API to put data into Gun.js, then
/// verify BEAM receives it via the WebSocket subscription.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn gun_put_beam_receives() {
    let relay = GunRelay::spawn(9873);
    relay.wait_for_ready(10000).await;

    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(
        config.clone(),
        vec![format!("ws://127.0.0.1:{}/gun", relay.ws_port)],
    );
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    // Wait for WebSocket connection
    wait_for_connected(&ws_client, 1, 10000).await;

    // Subscribe to a soul before Gun.js puts data
    let mut sub = beam.get("beamtest/put2").get("name").on();

    // Give the subscription a moment to propagate to the relay
    sleep(Duration::from_secs(1)).await;

    // Gun.js puts data via HTTP API
    relay
        .api_post(
            "/put",
            r#"{"soul":"beamtest/put2","key":"name","value":"Bob"}"#,
        )
        .await;

    // Wait for BEAM to receive the data via WebSocket
    let result = timeout(Duration::from_secs(15), sub.recv())
        .await
        .expect("timeout waiting for Gun.js put to propagate to BEAM")
        .expect("subscription channel closed");

    match result {
        Value::Text(s) => assert_eq!(s, "Bob"),
        other => panic!("expected Value::Text, got {:?}", other),
    }

    beam.stop();
}

/// Test 3: Bidirectional convergence — both BEAM and Gun.js write
/// different data, then verify both sides have all the data.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn bidirectional_convergence() {
    let relay = GunRelay::spawn(9875);
    relay.wait_for_ready(10000).await;

    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(
        config.clone(),
        vec![format!("ws://127.0.0.1:{}/gun", relay.ws_port)],
    );
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    wait_for_connected(&ws_client, 1, 10000).await;

    // BEAM writes one value
    beam.get("beamtest/conv")
        .get("from_beam")
        .put("beam_data".into())
        .await
        .unwrap();

    // Gun.js writes a different value
    relay
        .api_post(
            "/put",
            r#"{"soul":"beamtest/conv","key":"from_gun","value":"gun_data"}"#,
        )
        .await;

    // Wait for convergence
    sleep(Duration::from_secs(3)).await;

    // Verify Gun.js has BEAM's data
    let beam_val = relay.api_get("/get?soul=beamtest/conv&key=from_beam").await;
    assert!(
        beam_val.contains("beam_data"),
        "Gun.js missing BEAM's data. Response: {}",
        beam_val
    );

    // Verify BEAM has Gun.js's data — use subscription since once() is a direct read
    let mut gun_sub = beam.get("beamtest/conv").get("from_gun").on();
    let result = timeout(Duration::from_secs(10), gun_sub.recv())
        .await
        .expect("timeout waiting for Gun.js data to reach BEAM")
        .expect("subscription channel closed");
    match result {
        Value::Text(s) => assert_eq!(s, "gun_data"),
        other => panic!("expected Value::Text, got {:?}", other),
    }

    beam.stop();
}

/// Test 4: Reconnection — disconnect BEAM, reconnect, verify sync resumes.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn reconnection_sync() {
    let relay = GunRelay::spawn(9877);
    relay.wait_for_ready(10000).await;

    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(
        config.clone(),
        vec![format!("ws://127.0.0.1:{}/gun", relay.ws_port)],
    );
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    wait_for_connected(&ws_client, 1, 10000).await;

    // Put initial data
    beam.get("beamtest/recon")
        .get("phase1")
        .put("first".into())
        .await
        .unwrap();
    sleep(Duration::from_secs(2)).await;

    // Verify initial sync
    let v1 = relay.api_get("/get?soul=beamtest/recon&key=phase1").await;
    assert!(v1.contains("first"), "initial sync failed: {}", v1);

    // Stop BEAM (simulating disconnect)
    beam.stop();
    sleep(Duration::from_secs(2)).await;

    // Gun.js puts data while BEAM is disconnected
    relay
        .api_post(
            "/put",
            r#"{"soul":"beamtest/recon","key":"phase2","value":"second"}"#,
        )
        .await;
    sleep(Duration::from_secs(1)).await;

    // Reconnect BEAM
    let ws_client2 = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://127.0.0.1:{}/gun", relay.ws_port)],
    );
    let mut beam2 = Node::new_with_config(
        Config::default(),
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client2.clone())],
    );

    wait_for_connected(&ws_client2, 1, 10000).await;

    // Subscribe and wait for the data Gun.js wrote while we were disconnected
    let mut sub = beam2.get("beamtest/recon").get("phase2").on();
    let result = timeout(Duration::from_secs(15), sub.recv())
        .await
        .expect("timeout waiting for reconnection sync")
        .expect("channel closed");
    match result {
        Value::Text(s) => assert_eq!(s, "second"),
        other => panic!("expected Value::Text, got {:?}", other),
    }

    beam2.stop();
}

// ---------------------------------------------------------------------------
// Internal helper — adapted from tests/common/mod.rs
// ---------------------------------------------------------------------------

async fn wait_for_connected(client: &OutgoingWebsocketManager, expected: usize, timeout_ms: u64) {
    let start = std::time::Instant::now();
    let limit = Duration::from_millis(timeout_ms);
    while start.elapsed() < limit {
        if client.connected_count().await >= expected {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "OutgoingWebsocketManager not connected within {}ms (expected {} connections)",
        timeout_ms, expected
    );
}

// ===========================================================================
// Part 2: Gun.js client → BEAM relay → BEAM native (BEAM as server)
//
// These tests verify the reverse direction: BEAM acts as the WebSocket
// server, and a Gun.js client connects to it. This is the scenario the
// browser test was exercising when it "semi-worked."
//
// Architecture:
//
//   ┌───────────┐   WebSocket   ┌───────────┐   in-process   ┌──────────┐
//   │ Gun.js    │ ────────────► │ BEAM      │ ─────────────► │ BEAM     │
//   │ Client    │ ◄──────────── │ Relay     │ ◄───────────── │ Native   │
//   │ (Node.js) │   wire msgs   │ (WsServer)│    actor msgs  │ (Node)   │
//   └───────────┘               └───────────┘                └──────────┘
//        │                                                        │
//        └── HTTP API ─────────────────────────────────────────────┘
//            (Rust test harness verifies both sides)
// ===========================================================================

use beam::adapters::{WsServer, WsServerConfig};

// ---------------------------------------------------------------------------
// GunClient — manages a gun_client.js subprocess with an HTTP API
// ---------------------------------------------------------------------------

/// Gun.js client process handle. Drop to kill.
///
/// Mirrors [`GunRelay`] but spawns `gun_client.js` instead of `gun_relay.js`.
/// The Gun.js client connects to an external relay URL (the BEAM relay)
/// and exposes the same HTTP API shape for test verification.
struct GunClient {
    child: Child,
    api_port: u16,
}

impl GunClient {
    /// Spawn a Gun.js client that connects to `relay_url` and exposes an
    /// HTTP API on `api_port`.
    fn spawn(relay_url: &str, api_port: u16) -> Self {
        let child = Command::new("node")
            .arg("gun_client.js")
            .arg(relay_url)
            .arg(api_port.to_string())
            .current_dir("tests/wire-live")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn gun_client.js — is Node.js installed?");

        Self { child, api_port }
    }

    /// Wait for the HTTP API to become ready.
    async fn wait_for_ready(&self, timeout_ms: u64) {
        let start = std::time::Instant::now();
        let limit = Duration::from_millis(timeout_ms);
        while start.elapsed() < limit {
            if TcpStream::connect(format!("127.0.0.1:{}", self.api_port))
                .await
                .is_ok()
            {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "Gun client API did not become ready within {}ms",
            timeout_ms
        );
    }

    /// HTTP GET to the client's API.
    async fn api_get(&self, path: &str) -> String {
        let url = format!("http://127.0.0.1:{}{}", self.api_port, path);
        reqwest_text(&url).await
    }

    /// HTTP POST to the client's API.
    async fn api_post(&self, path: &str, body: &str) -> String {
        let url = format!("http://127.0.0.1:{}{}", self.api_port, path);
        reqwest_post(&url, body).await
    }
}

impl Drop for GunClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// BeamRelay — spawns a BEAM WsServer in-process on an ephemeral port
// ---------------------------------------------------------------------------

/// BEAM relay running in-process via the `WsServer` adapter.
///
/// Unlike [`GunRelay`] (which spawns a Node.js subprocess), the BEAM relay
/// runs directly in the test process as a [`Node`] with a `WsServer` network
/// adapter. The relay node stores and forwards data between connected peers
/// (Gun.js client and BEAM native).
struct BeamRelay {
    ws_server: WsServer,
    node: Node,
    port: u16,
}

impl BeamRelay {
    /// Start a BEAM relay on the given port with memory storage and
    /// public space enabled (required for unsigned test data).
    fn new(port: u16) -> Self {
        let mut config = Config::default();
        config.allow_public_space = true;
        let ws_server = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port,
                cert_path: None,
                key_path: None,
            },
        );
        // The relay node owns the WsServer adapter — the Router starts it
        // as an actor, which binds the TCP listener and accepts connections.
        let node = Node::new_with_config(
            config,
            vec![Box::new(MemoryStorage::new())],
            vec![Box::new(ws_server.clone())],
        );
        Self {
            ws_server,
            node,
            port,
        }
    }

    /// The WebSocket URL for Gun.js / BEAM clients to connect to.
    fn url(&self) -> String {
        format!("ws://127.0.0.1:{}/gun", self.port)
    }

    /// Wait for the TCP port to accept connections.
    async fn wait_for_ready(&self, timeout_ms: u64) {
        common::wait_for_port(self.port, timeout_ms).await;
    }

    /// Wait for `expected` peers to complete WebSocket handshakes.
    async fn wait_for_peers(&self, expected: usize, timeout_ms: u64) {
        common::wait_for_peer_count(&self.ws_server, expected, timeout_ms).await;
    }

    /// Stop the relay node (graceful shutdown).
    fn stop(&mut self) {
        self.node.stop();
    }
}

// ---------------------------------------------------------------------------
// Tests: Gun.js client → BEAM relay → BEAM native
// ---------------------------------------------------------------------------

/// Test 5: Gun.js client puts data → BEAM relay forwards → BEAM native receives.
///
/// A Gun.js client connects to a BEAM relay. A BEAM native node also
/// connects to the same relay. Gun.js puts data via the HTTP API, and
/// we verify BEAM native receives it via a subscription.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn gun_client_put_beam_receives() {
    let relay_port = 9901;
    let api_port = 9903; // BEAM web UI auto-binds on 9902 (relay_port+1)

    // Start BEAM relay in-process
    let mut beam_relay = BeamRelay::new(relay_port);
    beam_relay.wait_for_ready(5000).await;

    // Start Gun.js client pointing at the BEAM relay
    let gun_client = GunClient::spawn(&beam_relay.url(), api_port);
    gun_client.wait_for_ready(10000).await;

    // Wait for Gun.js client to complete the WebSocket handshake with BEAM
    beam_relay.wait_for_peers(1, 10000).await;

    // Create a BEAM native node also connected to the relay
    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(config.clone(), vec![beam_relay.url()]);
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    wait_for_connected(&ws_client, 1, 10000).await;
    // Now 2 peers on the relay: Gun.js + BEAM native
    beam_relay.wait_for_peers(2, 10000).await;

    // Subscribe to the test path before Gun.js puts
    let mut sub = beam.get("guntest/put1").get("name").on();
    sleep(Duration::from_secs(1)).await;

    // Gun.js puts data via HTTP API
    gun_client
        .api_post(
            "/put",
            r#"{"soul":"guntest/put1","key":"name","value":"Charlie"}"#,
        )
        .await;

    // Wait for BEAM native to receive the data via WebSocket
    let result = timeout(Duration::from_secs(15), sub.recv())
        .await
        .expect("timeout waiting for Gun.js client put to reach BEAM native")
        .expect("subscription channel closed");

    match result {
        Value::Text(s) => assert_eq!(s, "Charlie"),
        other => panic!("expected Value::Text, got {:?}", other),
    }

    beam_relay.stop();
    beam.stop();
}

/// Test 6: BEAM native puts data → BEAM relay forwards → Gun.js client receives.
///
/// BEAM native puts a value. We verify the Gun.js client received and
/// stored it by querying the Gun.js client's HTTP API.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn beam_put_gun_client_receives() {
    let relay_port = 9905;
    let api_port = 9907; // BEAM web UI auto-binds on 9906 (relay_port+1)

    let mut beam_relay = BeamRelay::new(relay_port);
    beam_relay.wait_for_ready(5000).await;

    let gun_client = GunClient::spawn(&beam_relay.url(), api_port);
    gun_client.wait_for_ready(10000).await;

    beam_relay.wait_for_peers(1, 10000).await;

    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(config.clone(), vec![beam_relay.url()]);
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    wait_for_connected(&ws_client, 1, 10000).await;
    beam_relay.wait_for_peers(2, 10000).await;

    // BEAM native puts data
    beam.get("beamtest/gunrecv")
        .get("city")
        .put("Miami".into())
        .await
        .unwrap();

    // Give Gun.js time to receive and store the data
    sleep(Duration::from_secs(3)).await;

    // Verify Gun.js client received the data
    let response = gun_client
        .api_get("/get?soul=beamtest/gunrecv&key=city")
        .await;
    assert!(
        response.contains("Miami"),
        "Gun.js client did not receive BEAM's put. Response: {}",
        response
    );

    beam_relay.stop();
    beam.stop();
}

/// Test 7: Bidirectional convergence — Gun.js and BEAM both put different
/// data on the same soul, then we verify both sides have all the data.
#[tokio::test]
#[ignore = "requires Node.js + Gun.js installed in tests/wire-live/"]
async fn gun_beam_bidirectional_convergence() {
    let relay_port = 9909;
    let api_port = 9911; // BEAM web UI auto-binds on 9910 (relay_port+1)

    let mut beam_relay = BeamRelay::new(relay_port);
    beam_relay.wait_for_ready(5000).await;

    let gun_client = GunClient::spawn(&beam_relay.url(), api_port);
    gun_client.wait_for_ready(10000).await;

    beam_relay.wait_for_peers(1, 10000).await;

    let config = Config::default();
    let ws_client = OutgoingWebsocketManager::new(config.clone(), vec![beam_relay.url()]);
    let mut beam = Node::new_with_config(
        config,
        vec![Box::new(MemoryStorage::new())],
        vec![Box::new(ws_client.clone())],
    );

    wait_for_connected(&ws_client, 1, 10000).await;
    beam_relay.wait_for_peers(2, 10000).await;

    // BEAM native writes one value
    beam.get("convergetest")
        .get("from_beam")
        .put("beam_writes".into())
        .await
        .unwrap();

    // Gun.js client writes a different value
    gun_client
        .api_post(
            "/put",
            r#"{"soul":"convergetest","key":"from_gun","value":"gun_writes"}"#,
        )
        .await;

    // Wait for convergence
    sleep(Duration::from_secs(3)).await;

    // Verify Gun.js has BEAM's data
    let beam_val = gun_client
        .api_get("/get?soul=convergetest&key=from_beam")
        .await;
    assert!(
        beam_val.contains("beam_writes"),
        "Gun.js client missing BEAM's data. Response: {}",
        beam_val
    );

    // Verify BEAM has Gun.js's data
    let mut gun_sub = beam.get("convergetest").get("from_gun").on();
    let result = timeout(Duration::from_secs(10), gun_sub.recv())
        .await
        .expect("timeout waiting for Gun.js client data to reach BEAM")
        .expect("subscription channel closed");
    match result {
        Value::Text(s) => assert_eq!(s, "gun_writes"),
        other => panic!("expected Value::Text, got {:?}", other),
    }

    beam_relay.stop();
    beam.stop();
}
