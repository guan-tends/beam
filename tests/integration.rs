#[cfg(test)]
mod tests {
    use log::info;
    use rod::adapters::*;
    use rod::{Config, Node, Value};
    use std::sync::Once;
    use tokio::time::{sleep, Duration};

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
        assert!(db.get("Fin").get("golf").get("fin").once(None).await.is_none());
        db.get("Fin").get("golf").get("fin").put(Value::Null);
        assert!(!db.get("Fin").get("golf").get("fin").once(None).await.is_none());
    }

    #[tokio::test]
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
        sleep(Duration::from_millis(2000)).await;
        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());
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
        let mut relay = Node::new_with_config(
            config.clone(),
            vec![],
            vec![Box::new(ws_server)],
        );

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

        sleep(Duration::from_millis(2000)).await;

        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());
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

        sleep(Duration::from_millis(2000)).await;

        let mut sub1 = peer1.get("beta").get("name").on();
        let mut sub2 = peer2.get("alpha").get("name").on();
        sleep(Duration::from_millis(100)).await;
        peer1.get("alpha").get("name").put("Amandil".into());
        peer2.get("beta").get("name").put("Beregond".into());
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
            let (key, value) = result.expect("timeout waiting for map replay")
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
}
