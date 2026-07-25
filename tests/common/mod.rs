//! Shared async readiness helpers for beam e2e tests.
//!
//! # The Flakiness Pattern
//!
//! Rod tests traditionally used `tokio::time::sleep(N)` as a "ready"
//! signal. This is wrong on two axes:
//!
//! 1. **Time is not readiness.** A 1500ms sleep might be too short on
//!    cold process starts (first 1-2 runs of a session) when the
//!    tokio runtime is still warming up the actor scheduler. It
//!    might also be far longer than needed on warm runs, wasting
//!    test time.
//!
//! 2. **The substrate exposes real readiness signals.** Every
//!    network adapter in `src/adapters/` has observable state we
//!    can poll on:
//!    - `WsServer::peer_count()` — completed WebSocket handshakes
//!    - `OutgoingWebsocketManager::connected_count()` — successful
//!      `connect_async` results
//!    - TCP port bound (verifiable via `TcpStream::connect`)
//!
//! # The Pattern
//!
//! Every `sleep(N)` in a test should be replaced with a `wait_for_X(...)`
//! helper that polls on the actual invariant. Each helper accepts a
//! timeout — when the timeout elapses, the helper panics with a clear
//! message so the failure mode is diagnosable.
//!
//! # Why panic instead of returning `Result`
//!
//! Tests should fail loudly at the point of the readiness violation.
//! Returning `Result` would force every caller to `.await?` or
//! `.expect(...)` it. A panic with context is more diagnostic and
//! keeps test bodies readable.
//!
//! # Substrate Truths (verified 2026-07-25)
//!
//! - `WsServer::peer_count()` — non-blocking read of the `clients`
//!   `RwLock`. Returns 0 if the lock is held (rare, retry handles it).
//! - `OutgoingWebsocketManager::connected_count()` — reads
//!   `self.clients.len()`. Only increments after `connect_async`
//!   succeeds AND the `WsConn` actor has been spawned.
//! - TCP port bound — `TcpStream::connect` succeeds when the kernel
//!   accept queue has room. Does NOT guarantee the user-space
//!   handshake completed — pair with `wait_for_peer_count` for that.

use beam::adapters::{OutgoingWebsocketManager, WsServer};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::sleep;

/// Default poll interval. 50ms is fast enough to keep total test time
/// close to the time the invariant actually takes to settle, while not
/// wasting CPU on tight loops.
pub const POLL_INTERVAL_MS: u64 = 50;

/// Poll a TCP port until it accepts connections or the timeout elapses.
///
/// Eliminates blind-sleep races against the actor's `pre_start`.
/// Verifies only that the kernel listen socket has been bound — does
/// NOT verify the WebSocket handshake. For that, pair with
/// [`wait_for_peer_count`] or [`wait_for_connected_count`].
///
/// # Panics
/// If the port is not accepting connections within `timeout_ms` ms.
pub async fn wait_for_port(port: u16, timeout_ms: u64) {
    let start = Instant::now();
    let limit = Duration::from_millis(timeout_ms);
    while start.elapsed() < limit {
        if TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!(
        "port {} did not become ready within {}ms (kernel listen socket never bound)",
        port, timeout_ms
    );
}

/// Poll the [`WsServer`] until `expected_peers` have completed the
/// WebSocket handshake and registered as connected clients.
///
/// # Why this is correct
///
/// `WsServer::peer_count()` increments when a
/// `beam::adapters::ws_conn::WsConn` actor finishes the WS upgrade
/// and registers its address. That happens AFTER the TCP listener
/// accepts AND the WS handshake completes — the same condition the
/// broadcast needs to succeed.
///
/// # Panics
/// If the expected peer count is not reached within `timeout_ms` ms.
pub async fn wait_for_peer_count(
    ws_server: &WsServer,
    expected_peers: usize,
    timeout_ms: u64,
) {
    let start = Instant::now();
    let limit = Duration::from_millis(timeout_ms);
    while start.elapsed() < limit {
        if ws_server.peer_count() >= expected_peers {
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!(
        "WsServer mesh not ready: expected {} peers within {}ms, observed {}. \
         Likely cause: OutgoingWebsocketManager never completed connect_async.",
        expected_peers,
        timeout_ms,
        ws_server.peer_count()
    );
}

/// Poll the [`OutgoingWebsocketManager`] until `expected_urls` remote
/// URLs have an active WebSocket connection.
///
/// Mirrors [`wait_for_peer_count`] from the client side. The two
/// together form the readiness invariant for any test that crosses
/// a WebSocket mesh boundary: server side sees N peers AND client
/// side has N connected clients.
///
/// # Panics
/// If the expected connected count is not reached within `timeout_ms` ms.
pub async fn wait_for_connected_count(
    client_manager: &OutgoingWebsocketManager,
    expected_connections: usize,
    timeout_ms: u64,
) {
    let start = Instant::now();
    let limit = Duration::from_millis(timeout_ms);
    while start.elapsed() < limit {
        if client_manager.connected_count().await >= expected_connections {
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!(
        "OutgoingWebsocketManager mesh not ready: expected {} connections within {}ms, \
         observed {}. Likely cause: WsServer never bound or connect_async failed.",
        expected_connections,
        timeout_ms,
        client_manager.connected_count().await
    );
}