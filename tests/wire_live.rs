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
