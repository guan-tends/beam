//! Phase D3.5: Two Rod Node WebRTC DataChannel sync test.
//!
//! Creates two Node instances connected via WebSocket mesh for signaling,
//! then bootstraps a direct WebRTC DataChannel between them.
//! Verifies that a put() on Node B reaches a subscriber on Node A.

#[cfg(feature = "webrtc")]
mod tests {
    use rod::{Config, Node, Value};
    use rod::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer};
    use std::time::Instant;
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout, Duration};

    async fn wait_for_port(port: u16, timeout_ms: u64) {
        let start = Instant::now();
        let deadline = std::time::Duration::from_millis(timeout_ms);
        while start.elapsed() < deadline {
            match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
                Ok(_) => return,
                Err(_) => sleep(Duration::from_millis(50)).await,
            }
        }
        panic!("Port {} did not become ready within {}ms", port, timeout_ms);
    }

    #[tokio::test]
    async fn webrtc_sync_over_mesh() {
        let config = Config::default();

        // Node A: WsServer (for signaling mesh) + will initiate WebRTC as offerer
        let mut peer_a = Node::new_with_config(
            config.clone(),
            vec![Box::new(MemoryStorage::new())],
            vec![Box::new(WsServer::new(config.clone()))],
        );

        // Node B: WsClient (connects to A for signaling) + will answer WebRTC
        let ws_client = OutgoingWebsocketManager::new(
            config.clone(),
            vec!["ws://localhost:4944/ws".to_string()],
        );
        let mut peer_b = Node::new_with_config(
            config.clone(),
            vec![Box::new(MemoryStorage::new())],
            vec![Box::new(ws_client)],
        );

        // Wait for websocket mesh to establish
        wait_for_port(4944, 5000).await;
        sleep(Duration::from_millis(1500)).await;

        // Subscribe on peer_a BEFORE WebRTC setup and put
        let mut sub = peer_a.get("sync").get("test").on();

        // Bootstrap WebRTC: both sides create WebRtcPeer adapters.
        // The offer signal flows over the websocket mesh to peer_b.
        peer_a.connect_webrtc_peer("peer-a", "peer-b", rod::adapters::WebRtcRole::Offerer);
        peer_b.connect_webrtc_peer("peer-b", "peer-a", rod::adapters::WebRtcRole::Answerer);

        // Allow ICE handshake + DTLS + SCTP + DataChannel to complete.
        // In a real network this is ~1-3s; loopback is faster.
        sleep(Duration::from_millis(5000)).await;

        // Write a value on peer_b; it should propagate to peer_a via DataChannel.
        peer_b.get("sync").get("test").put("Hello from WebRTC".into());

        // Assert peer_a receives the synced value.
        let val = timeout(Duration::from_secs(30), sub.recv())
            .await
            .expect("timeout waiting for WebRTC sync — mesh/DataChannel may not have established")
            .expect("subscription channel closed");

        match val {
            Value::Text(str) => assert_eq!(&str, "Hello from WebRTC"),
            other => panic!("Expected Value::Text, got {:?}", other),
        }

        peer_a.stop();
        peer_b.stop();
    }
}
