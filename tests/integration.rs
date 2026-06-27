//! Integration tests for Rod — the Rust Gun Protocol implementation.
//!
//! These tests exercise the full stack: Node + adapters + network protocols.
//! They spin up real WebSocket servers on localhost and verify mesh sync,
//! storage persistence, and peer-to-peer data propagation.
//!
//! # Test Categories
//!
//! - **Smoke**: `it_doesnt_error` — minimal Node creation, no panics
//! - **Pub/Sub**: `first_get_then_put`, `first_put_then_get` — subscription ordering
//! - **Storage**: `once_returns_value_or_none` — read-after-write consistency
//! - **WebSocket mesh**: `connect_and_sync_over_websocket`, `websocket_sync_over_relay_peer`,
//!   `websocket_sync_over_2_relay_peers` — multi-hop sync over WS relay
//! - **Persistence**: `redb_storage_persists`, `redb_storage_flush_returns_ok` — disk storage
//!
//! # Known Flaky Test
//!
//! `websocket_sync_over_2_relay_peers` may time out on slow CI. It uses a 30s
//! timeout per subscription receive. If it fails, re-run before investigating.

#[cfg(test)]
mod tests {
    use log::info;
    use rod::adapters::*;
    use rod::{Config, Node, Value};
    use std::sync::Once;
    use std::time::Instant;
    use tokio::net::TcpStream;
    use tokio::time::{Duration, sleep, timeout};

    /// Poll a TCP port until it accepts connections or timeout elapses.
    /// Replaces blind sleep races with deterministic readiness.
    async fn wait_for_port(port: u16, timeout_ms: u64) {
        let start = Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        while start.elapsed() < timeout {
            match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
                Ok(_) => return,
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }
        panic!("Port {} did not become ready within {}ms", port, timeout_ms);
    }

    static INIT: Once = Once::new();

    fn enable_logger() {
        INIT.call_once(|| {
            env_logger::init();
        });
    }

    // TODO proper test
    // TODO test .map()
    // TODO benchmark
    #[tokio::test]
    async fn it_doesnt_error() {
        let mut db = Node::new();
        let _ = db.get("Meneldor"); // Pick Tolkien names from https://www.behindthename.com/namesakes/list/tolkien/alpha
    }

    #[tokio::test]
    async fn first_get_then_put() {
        let mut db = Node::new();
        let mut node = db.get("Anborn");
        let mut sub = node.on();
        node.put("Ancalagon".into());
        if let Value::Text(str) = sub.recv().await.unwrap() {
            assert_eq!(&str, "Ancalagon");
        }
    }

    #[tokio::test]
    async fn first_put_then_get() {
        let mut db = Node::new_with_config(
            Config::default(),
            vec![Box::new(MemoryStorage::new())],
            vec![],
        );
        let mut node = db.get("Finglas1").get("Finglas2"); // apparently shorter path db.get("Finglas") wouldn't work
        node.put("Fingolfin".into());
        let mut sub = node.on();
        if let Value::Text(str) = sub.recv().await.unwrap() {
            assert_eq!(&str, "Fingolfin");
        }
    }

    #[tokio::test]
    async fn once_returns_value_or_none() {
        let mut db = Node::new_with_config(
            Config::default(),
            vec![Box::new(MemoryStorage::new())],
            vec![],
        );
        let mut node = db.get("Finglas1").get("Finglas2");
        node.put("Fingolfin".into());
        let Some(Value::Text(str)) = node.once(None).await else {
            panic!("once didn't find val");
        };
        assert_eq!(&str, "Fingolfin");
        assert!(
            db.get("Fin")
                .get("golf")
                .get("fin")
                .once(None)
                .await
                .is_none()
        );
        db.get("Fin").get("golf").get("fin").put(Value::Null);
        assert!(
            !db.get("Fin")
                .get("golf")
                .get("fin")
                .once(None)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    #[allow(unreachable_code)]
    async fn connect_and_sync_over_websocket() {
        let config = Config::default();
        let mut peer1 = Node::new_with_config(
            config.clone(),
            vec![],
            vec![Box::new(WsServer::new(config.clone()))],
        );
        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4944/ws".to_string()],
        );
        let mut peer2 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);
        wait_for_port(4944, 5000).await;
        sleep(Duration::from_millis(1000)).await;
        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());

        // Timeout: WebSocket handshake may race with actor pre_start.
        // If mesh propagation fails, fail fast with clear message rather than hanging.
        let recv_val = timeout(Duration::from_secs(30), sub1.recv())
            .await
            .expect("timeout waiting for Beregond — mesh propagation from peer2 failed")
            .expect("sub1 channel closed");
        match recv_val {
            Value::Text(str) => {
                assert_eq!(&str, "Beregond");
            }
            _ => panic!("Expected Value::Text, got {:?}", recv_val),
        }
        let recv_val = timeout(Duration::from_secs(30), sub2.recv())
            .await
            .expect("timeout waiting for Amandil — mesh propagation from peer1 failed")
            .expect("sub2 channel closed");
        match recv_val {
            Value::Text(str) => {
                assert_eq!(&str, "Amandil");
            }
            _ => panic!("Expected Value::Text, got {:?}", recv_val),
        }
        peer1.stop();
        peer2.stop();
    }

    /*
    #[tokio::test]
    async fn connect_and_sync_longer_path_over_websocket() {
        let config = Config::default();

        let ws_server = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port: 4946,
                ..WsServerConfig::default()
            },
        );
        let mut peer1 = Node::new_with_config(
            config.clone(),
            vec![],
            vec![Box::new(ws_server)],
        );
        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4946/ws".to_string()],
        );
        let mut peer2 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);
        sleep(Duration::from_millis(2000)).await;
        let mut sub1 = peer1.get("beta").get("charlie").get("name").on();
        let mut sub2 = peer2.get("alpha").get("beta").get("name").on();
        peer1
            .get("alpha")
            .get("beta")
            .get("name")
            .put("Amandil".into());
        peer2
            .get("beta")
            .get("charlie")
            .get("name")
            .put("Beregond".into());
        match sub1.recv().await.unwrap() {
            Value::Text(str) => {
                assert_eq!(&str, "Beregond");
            }
            _ => panic!("Expected Value::Text"),
        }
        match sub2.recv().await.unwrap() {
            Value::Text(str) => {
                assert_eq!(&str, "Amandil");
            }
            _ => panic!("Expected Value::Text"),
        }
        peer1.stop();
        peer2.stop();
    }
    */

    #[tokio::test]
    async fn websocket_sync_over_relay_peer() {
        let config = Config::default();

        let ws_server = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port: 4948,
                ..WsServerConfig::default()
            },
        );
        let mut relay = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_server)]);

        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4948/ws".to_string()],
        );
        let mut peer1 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4948/ws".to_string()],
        );
        let mut peer2 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

        wait_for_port(4948, 5000).await;
        sleep(Duration::from_millis(1000)).await;
        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());
        let val = timeout(Duration::from_secs(30), sub1.recv())
            .await
            .expect("timeout waiting for sub1 — mesh propagation from peer2 failed")
            .expect("sub1 channel closed");
        match val {
            Value::Text(str) => {
                assert_eq!(&str, "Beregond");
            }
            _ => panic!("Expected Value::Text, got {:?}", val),
        }
        let val = timeout(Duration::from_secs(30), sub2.recv())
            .await
            .expect("timeout waiting for sub2 — mesh propagation from peer1 failed")
            .expect("sub2 channel closed");
        match val {
            Value::Text(str) => {
                assert_eq!(&str, "Amandil");
            }
            _ => panic!("Expected Value::Text, got {:?}", val),
        }
        peer1.stop();
        peer2.stop();
        relay.stop();
    }

    #[tokio::test]
    async fn websocket_sync_over_2_relay_peers() {
        let config = Config::default();

        let ws_server1 = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port: 4950,
                ..WsServerConfig::default()
            },
        );
        let ws_server2 = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port: 4952,
                ..WsServerConfig::default()
            },
        );
        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4950/ws".to_string()],
        );
        let mut relay1 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_server1)]);
        let mut relay2 = Node::new_with_config(
            config.clone(),
            vec![],
            vec![Box::new(ws_server2), Box::new(ws_client)],
        );

        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4950/ws".to_string()],
        );
        let mut peer1 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4952/ws".to_string()],
        );
        let mut peer2 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

        wait_for_port(4950, 5000).await;
        wait_for_port(4952, 5000).await;
        // Small delay for client connections to establish
        sleep(Duration::from_millis(1500)).await;

        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        sleep(Duration::from_millis(100)).await;
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());
        let val = timeout(Duration::from_secs(30), sub1.recv())
            .await
            .expect("timeout waiting for sub1 — mesh propagation from peer2 via relay failed")
            .expect("sub1 channel closed");
        match val {
            Value::Text(str) => {
                assert_eq!(&str, "Beregond");
            }
            _ => panic!("Expected Value::Text, got {:?}", val),
        }
        let val = timeout(Duration::from_secs(30), sub2.recv())
            .await
            .expect("timeout waiting for sub2 — mesh propagation from peer1 via relay failed")
            .expect("sub2 channel closed");
        match val {
            Value::Text(str) => {
                assert_eq!(&str, "Amandil");
            }
            _ => panic!("Expected Value::Text, got {:?}", val),
        }

        assert!(peer2.get("gamma").get("name").once(None).await.is_none());
        assert!(peer1.get("gamma").get("name").once(None).await.is_none());
        peer1.get("gamma").get("name").put("once".into());
        let Some(Value::Text(str)) = peer2.get("gamma").get("name").once(None).await else {
            panic!("once: Expected Value::Text");
        };
        assert_eq!(&str, "once");

        peer1.stop();
        peer2.stop();
        relay1.stop();
        relay2.stop();
    }

    /*
    #[tokio::test]
    async fn ws_server_stats() {
        let config = Config::default();

        let ws_server1 = WsServer::new_with_config(
            config.clone(),
            WsServerConfig {
                port: 4954,
                ..WsServerConfig::default()
            },
        );
        let mut peer1 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_server1)]);

        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4954/ws".to_string()],
        );
        let mut peer2 = Node::new_with_config(config.clone(), vec![], vec![Box::new(ws_client)]);

        sleep(Duration::from_millis(2000)).await;

        let peer1_id = peer1.peer_id();
        assert!(!peer1_id.is_empty());

        let mut sub = peer2
            .get("node_stats")
            .get(&peer1_id)
            .get("ws_server_connections")
            .on();
        sleep(Duration::from_millis(1000)).await;
        let res = sub.recv().await;
        info!("res {:?}", res);
        peer1.stop();
        peer2.stop();
    }
    */
    /*
    #[tokio::test]
    async fn sync_over_multicast() {
        let config = Config::default();
        let mut peer1 = Node::new_with_config(
            config.clone(),
            vec![Box::new(MemoryStorage::new())],
            vec![Box::new(Multicast::new(config.clone()))],
        );
        let mut peer2 = Node::new_with_config(
            config.clone(),
            vec![Box::new(MemoryStorage::new())],
            vec![Box::new(Multicast::new(config.clone()))],
        );
        sleep(Duration::from_millis(1000)).await;
        peer1.get("gamma").put("Gorlim".into());
        peer2.get("sigma").put("Smaug".into());
        let mut sub1 = peer1.get("sigma").on();
        let mut sub2 = peer2.get("gamma").on();
        match sub1.recv().await.unwrap() {
            Value::Text(str) => {
                assert_eq!(&str, "Smaug");
            }
            _ => panic!("Expected Value::Text"),
        };
        match sub2.recv().await.unwrap() {
            Value::Text(str) => {
                assert_eq!(&str, "Gorlim");
            }
            _ => panic!("Expected Value::Text"),
        };
        peer1.stop();
        peer2.stop();
    }*/

    /*
    #[test] // use #[bench] when it's stable
    fn write_benchmark() { // to see the result with optimized binary, run: cargo test --release -- --nocapture
        setup();
        let start = Instant::now();
        let mut db = Node::new();
        let n = 1000;
        for i in 0..n {
            db.get(&format!("a{:?}", i)).get("Pelendur").put(format!("{:?}b", i).into());
        }
        let duration = start.elapsed();
        let per_second = (n as f64) / (duration.as_nanos() as f64) * 1000000000.0;
        println!("Wrote {} entries in {:?} ({} / second)", n, duration, per_second);
        // compare with db.js: var i = 100000, j = i, s = +new Date; while(--i){ db.get('a'+i).get('lol').put(i+'yo') } console.log(j / ((+new Date - s) / 1000), 'ops/sec');
    }
     */

    #[tokio::test]
    async fn redb_storage_persists() {
        let _ = env_logger::try_init();
        use rod::adapters::RedbStorage;
        use std::time::Duration;
        use tokio::time::sleep;

        let temp_path = std::env::temp_dir().join("rod-redb-test.ron");
        let _ = std::fs::remove_file(&temp_path);

        let config = Config::default();

        // Phase 1: write
        {
            let mut db = Node::new_with_config(
                config.clone(),
                vec![Box::new(RedbStorage::new_with_config(
                    config.clone(),
                    temp_path.to_string_lossy().as_ref(),
                    None,
                ))],
                vec![],
            );

            db.get("Feanor").put("Noldor".into());
            sleep(Duration::from_millis(500)).await;
            db.stop();
            sleep(Duration::from_millis(1000)).await;
        }

        // Phase 2: read — only redb, no memory
        {
            let mut db2 = Node::new_with_config(
                config.clone(),
                vec![Box::new(RedbStorage::new_with_config(
                    config.clone(),
                    temp_path.to_string_lossy().as_ref(),
                    None,
                ))],
                vec![],
            );

            // map() on root replays existing children from storage
            // (data stored at root node "" with "Feanor" as child key)
            let mut sub = db2.map();

            let result = tokio::time::timeout(Duration::from_secs(3), sub.recv()).await;
            let (key, value) = result
                .expect("timeout waiting for map replay")
                .expect("broadcast recv error");

            assert_eq!(key, "Feanor"); // child key from root
            if let Value::Text(s) = value {
                assert_eq!(&s, "Noldor");
            } else {
                panic!("Expected Value::Text, got {:?}", value);
            }

            db2.stop();
        }

        let _ = std::fs::remove_file(&temp_path);
    }

    #[tokio::test]
    async fn redb_storage_flush_returns_ok() {
        use rod::adapters::RedbStorage;
        use std::time::Duration;
        use tokio::time::sleep;

        let temp_path = std::env::temp_dir().join("rod-redb-flush.ron");
        let _ = std::fs::remove_file(&temp_path);

        let config = Config::default();
        let mut db = Node::new_with_config(
            config.clone(),
            vec![Box::new(RedbStorage::new_with_config(
                config.clone(),
                temp_path.to_string_lossy().as_ref(),
                None,
            ))],
            vec![],
        );

        db.get("FlushTest").put("pre_flush".into());
        sleep(Duration::from_millis(200)).await;

        // flush_storage awaits the ack from RedbStorage
        let result = db.flush_storage(Some(Duration::from_secs(3))).await;
        assert!(result.is_ok(), "flush_storage returned {:?}", result);

        db.stop();
        let _ = std::fs::remove_file(&temp_path);
    }

    /// Proves that `flush_storage` acts as a write barrier: data written
    /// before the flush is committed to disk before the flush ack returns.
    ///
    /// Writes a key, flushes, then opens a new `Node` on the same database
    /// file and reads the key back. If the flush barrier is broken, the
    /// read returns `None` (data not yet committed).
    #[tokio::test]
    async fn flush_acts_as_write_barrier() {
        use rod::adapters::RedbStorage;
        use std::time::Duration;

        let temp_path = std::env::temp_dir()
            .join(format!("rod-flush-barrier-{}.redb", std::process::id()));
        let _ = std::fs::remove_file(&temp_path);

        let config = Config::default();

        // Write phase: put data, then flush.
        {
            let mut db = Node::new_with_config(
                config.clone(),
                vec![Box::new(RedbStorage::new_with_config(
                    config.clone(),
                    temp_path.to_string_lossy().as_ref(),
                    None,
                ))],
                vec![],
            );

            db.get("BarrierKey").put("barrier_value".into());

            // Flush must block until the Put is committed.
            let result = db.flush_storage(Some(Duration::from_secs(5))).await;
            assert!(result.is_ok(), "flush_storage returned {:?}", result);

            db.stop();
            // Give the actor tasks time to fully release the database handle.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Read phase: new Node on the same database file.
        {
            let mut db = Node::new_with_config(
                config,
                vec![Box::new(RedbStorage::new_with_config(
                    Config::default(),
                    temp_path.to_string_lossy().as_ref(),
                    None,
                ))],
                vec![],
            );

            let val = db
                .get("BarrierKey")
                .once(Some(Duration::from_secs(3)))
                .await;
            assert_eq!(
                val,
                Some(Value::Text("barrier_value".to_string())),
                "data should be persisted before flush ack returned"
            );

            db.stop();
        }

        let _ = std::fs::remove_file(&temp_path);
    }
}
