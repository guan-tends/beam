//!
//! Cross-backend mesh convergence — the killer test for the Persy
//! storage adapter.
//!
//! # What this proves
//!
//! Two nodes with `RedbStorage` and one node with `PersyStorage`
//! connect over a WebSocket mesh. When peer1 (redb) puts a value, the
//! put propagates through the wire protocol to peer2 (redb) and peer3
//! (persy). All three nodes see the value.
//!
//! This is the killer test because it proves:
//!
//! 1. The wire format is **opaque to storage backend** — the `Put`
//!    message carries `(soul, value, updated_at)` regardless of which
//!    adapter persists it locally.
//! 2. Gun.js semantics survive a mixed-backend mesh.
//! 3. `PersyStorage` is a drop-in replacement for `RedbStorage` at
//!    the protocol level (admin can swap backends without coordinating
//!    with peers).
//!
//! # Topology
//!
//! ```text
//!   peer1 (RedbStorage) ─── ws://peer1 ─── peer2 (RedbStorage)
//!         │                                       │
//!         └──────────── ws://peer3 ──────────────┘
//!                          peer3 (PersyStorage)
//! ```
//!
//! peer1 is the hub: peer2 and peer3 both connect to its `WsServer`.
//! A single put on peer1 should arrive at both peer2 (redb) and peer3
//! (persy).
//!
//! # Feature gate
//!
//! ```bash
//! cargo test -p rod --features persy --test cross_backend_mesh_e2e -- --test-threads=1
//! ```

#![cfg(feature = "persy")]

use rod::adapters::{OutgoingWebsocketManager, RedbStorage, WsServer, WsServerConfig};
use rod::{Config, Node, Value};
use std::env;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// Poll a TCP port until it accepts connections or the timeout elapses.
/// Eliminates blind-sleep races against the actor's `pre_start`.
async fn wait_for_port(port: u16, timeout_ms: u64) {
    let start = Instant::now();
    let limit = Duration::from_millis(timeout_ms);
    while start.elapsed() < limit {
        if TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("port {} did not become ready within {}ms", port, timeout_ms);
}

/// Poll the WsServer until `expected_peers` have completed the
/// WebSocket handshake and registered as connected clients.
///
/// Replaces the previous blind `sleep(1500ms)` pattern. The blind
/// sleep was racy: on cold process starts (first 1-2 runs of a test
/// session), 1500ms was insufficient for the tokio runtime to
/// schedule the OutgoingWebsocketManager's connection + WS upgrade
/// + WsServer's accept + handshake-actor spawn chain. Active
/// readiness waiting eliminates the cold-vs-warm variance.
///
/// # Why this is correct
///
/// `WsServer::peer_count()` increments when a [`WsConn`] actor
/// finishes the WS upgrade and calls `clients.write().await.insert(addr)`.
/// That happens AFTER the TCP listener accepts AND the WS handshake
/// completes — the same condition the broadcast needs to succeed.
async fn wait_for_peer_count(
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
        sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "mesh not ready: expected {} peers within {}ms, saw {}",
        expected_peers,
        timeout_ms,
        ws_server.peer_count()
    );
}

/// Build a unique redb path for test isolation. Each test gets its own
/// file under `/tmp/rod-redb-{name}-{pid}-{nanos}.redb`.
fn unique_redb_path(test_name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir()
        .join(format!(
            "rod-redb-{}-{}-{}.redb",
            test_name,
            std::process::id(),
            nanos
        ))
        .to_str()
        .expect("temp path must be utf-8")
        .to_string()
}

/// Build a unique Persy path.
fn unique_persy_path(test_name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir()
        .join(format!(
            "rod-persy-{}-{}-{}.persy",
            test_name,
            std::process::id(),
            nanos
        ))
        .to_str()
        .expect("temp path must be utf-8")
        .to_string()
}

/// 2 redb + 1 persy on a 3-node mesh. peer1 puts, peer2 and peer3 receive.
///
/// Port allocation: peer1's `WsServer` listens on **4956** (free port).
/// Both peer2 (redb) and peer3 (persy) connect to peer1.
#[tokio::test]
async fn e2e_cross_backend_three_node_mesh_convergence() {
    use rod::adapters::PersyStorage;

    let port = 4956;
    let key = "from_peer1";
    let value = "convergence_value";

    let redb_path_a = unique_redb_path("mesh_a");
    let redb_path_b = unique_redb_path("mesh_b");
    let persy_path = unique_persy_path("mesh_persy");

    // --- peer1: redb, hosts WsServer ---
    let config1 = Config::default();
    let storage1 = RedbStorage::new_with_config(Config::default(), &redb_path_a, None);
    let ws_server1 = WsServer::new_with_config(
        config1.clone(),
        WsServerConfig { port, ..WsServerConfig::default() },
    );
    let mut peer1 = Node::new_with_config(
        config1.clone(),
        vec![Box::new(storage1) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_server1.clone())],
    );

    // --- peer2: redb, connects to peer1 ---
    let ws_client2 = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://localhost:{}/ws", port)],
    );
    let storage2 = RedbStorage::new_with_config(Config::default(), &redb_path_b, None);
    let mut peer2 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage2) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_client2)],
    );

    // --- peer3: persy, connects to peer1 ---
    let ws_client3 = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://localhost:{}/ws", port)],
    );
    let storage3 = PersyStorage::new_with_path(&persy_path);
    let mut peer3 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage3) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_client3)],
    );

    wait_for_port(port, 5000).await;
    // Wait for BOTH peer2 (redb) and peer3 (persy) to complete their
    // WebSocket handshake with peer1. Replaces blind `sleep(1500)` —
    // see `wait_for_peer_count` doc for why.
    wait_for_peer_count(&ws_server1, 2, 5000).await;

    // Subscribe before the put, so we don't miss the broadcast.
    let mut sub2 = peer2.get(key).on();
    let mut sub3 = peer3.get(key).on();

    // peer1 (redb) puts.
    peer1.get(key).put(value.into()).await.expect("peer1 put");

    // peer2 (redb) receives.
    let recv2 = timeout(Duration::from_secs(15), sub2.recv())
        .await
        .expect("timeout: peer2 (redb) did not receive peer1's put")
        .expect("peer2 sub channel closed");
    assert_eq!(recv2, Value::Text(value.to_string()));

    // peer3 (persy) receives — the killer assertion.
    let recv3 = timeout(Duration::from_secs(15), sub3.recv())
        .await
        .expect("timeout: peer3 (persy) did not receive peer1's put")
        .expect("peer3 sub channel closed");
    assert_eq!(recv3, Value::Text(value.to_string()));

    peer1.stop();
    peer2.stop();
    peer3.stop();

    let _ = std::fs::remove_file(&redb_path_a);
    let _ = std::fs::remove_file(&redb_path_b);
    let _ = std::fs::remove_file(&persy_path);
}

/// Reverse: persy puts, redb nodes receive. Proves the wire is symmetric.
#[tokio::test]
async fn e2e_cross_backend_persy_initiator_convergence() {
    use rod::adapters::PersyStorage;

    let port = 4958;
    let key = "from_peer3";
    let value = "persy_sender_value";

    let redb_path_a = unique_redb_path("rev_a");
    let redb_path_b = unique_redb_path("rev_b");
    let persy_path = unique_persy_path("rev_persy");

    // peer1 = redb, host
    let ws_server1 = WsServer::new_with_config(
        Config::default(),
        WsServerConfig { port, ..WsServerConfig::default() },
    );
    let storage1 = RedbStorage::new_with_config(Config::default(), &redb_path_a, None);
    let mut peer1 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage1) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_server1.clone())],
    );

    // peer2 = redb, client
    let ws_client2 = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://localhost:{}/ws", port)],
    );
    let storage2 = RedbStorage::new_with_config(Config::default(), &redb_path_b, None);
    let mut peer2 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage2) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_client2)],
    );

    // peer3 = persy, client
    let ws_client3 = OutgoingWebsocketManager::new(
        Config::default(),
        vec![format!("ws://localhost:{}/ws", port)],
    );
    let storage3 = PersyStorage::new_with_path(&persy_path);
    let mut peer3 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage3) as Box<dyn rod::actor::Actor>],
        vec![Box::new(ws_client3)],
    );

    wait_for_port(port, 5000).await;
    wait_for_peer_count(&ws_server1, 2, 5000).await;

    let mut sub1 = peer1.get(key).on();
    let mut sub2 = peer2.get(key).on();

    // peer3 (persy) puts.
    peer3.get(key).put(value.into()).await.expect("peer3 put");

    let recv1 = timeout(Duration::from_secs(15), sub1.recv())
        .await
        .expect("timeout: peer1 (redb) did not receive peer3's put")
        .expect("peer1 sub channel closed");
    assert_eq!(recv1, Value::Text(value.to_string()));

    let recv2 = timeout(Duration::from_secs(15), sub2.recv())
        .await
        .expect("timeout: peer2 (redb) did not receive peer3's put")
        .expect("peer2 sub channel closed");
    assert_eq!(recv2, Value::Text(value.to_string()));

    peer1.stop();
    peer2.stop();
    peer3.stop();

    let _ = std::fs::remove_file(&redb_path_a);
    let _ = std::fs::remove_file(&redb_path_b);
    let _ = std::fs::remove_file(&persy_path);
}