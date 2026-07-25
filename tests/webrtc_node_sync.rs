//! Phase D3.5: Two Rod Node WebRTC DataChannel sync test.
//!
//! Creates two Node instances connected via WebSocket mesh for signaling,
//! then bootstraps a direct WebRTC DataChannel between them.
//! Verifies that a put() on Node B reaches a subscriber on Node A.

#[cfg(feature = "webrtc")]
mod tests {
    use beam::adapters::{MemoryStorage, OutgoingWebsocketManager, WsServer};
    use beam::{Config, Node, Value};
    use std::time::Instant;
    use tokio::net::TcpStream;
    use tokio::time::{Duration, sleep, timeout};

    /// Poll until a TCP port accepts connections, or panic after `timeout_ms`.
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
        // Initialize str0m crypto provider (required for DTLS certificate generation)
        str0m::crypto::from_feature_flags().install_process_default();

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

        // Wait for the WebSocket signaling mesh to be ready (port accepts
        // connections + OWM has time to complete the handshake).
        wait_for_port(4944, 5000).await;

        // Subscribe on peer_a BEFORE WebRTC setup and put.
        let mut sub = peer_a.get("sync").get("test").on();

        // Bootstrap WebRTC: create answerer FIRST so it is ready when the offer
        // arrives. The offer signal flows over the websocket mesh to peer_b.
        // Actor `pre_start` is synchronous — no sleep needed between the two
        // connect calls; the answerer's task is spawned before the offerer
        // sends.
        peer_b.connect_webrtc_peer("peer-b", "peer-a", beam::adapters::WebRtcRole::Answerer);
        peer_a.connect_webrtc_peer("peer-a", "peer-b", beam::adapters::WebRtcRole::Offerer);

        // Retry put + recv until the WebRTC DataChannel is ready and the
        // value arrives. This replaces a blind `sleep(5000)` with active
        // polling on the actual invariant: does the data channel deliver
        // the message?
        //
        // Each iteration awaits peer_b's `put` (which blocks until peer_b's
        // local MemoryStorage acks — proving the Put reached the Router).
        // The Router then relays the Put to peer_a via the WebRTC
        // DataChannel. If the DataChannel isn't ready yet, the relay is
        // silently dropped; the 200ms retry interval gives the ICE/DTLS/SCTP
        // state machines time to complete.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut received: Option<Value> = None;

        while Instant::now() < deadline {
            // Await the put on peer_b. The Put reaches peer_b's local
            // MemoryStorage (which acks), and the Router relays it to
            // peer_a via WebRTC. If the DataChannel isn't open yet, the
            // relay is dropped — we retry on the next iteration.
            let _ = peer_b
                .get("sync")
                .get("test")
                .put("Hello from WebRTC".into())
                .await;

            // Check if peer_a received the value (short timeout per
            // attempt — the put already waited for peer_b's local ack,
            // so data channel delivery is the only remaining variable).
            match timeout(Duration::from_millis(500), sub.recv()).await {
                Ok(Ok(val)) => {
                    received = Some(val);
                    break;
                }
                _ => {
                    // Not received yet — DataChannel may still be
                    // handshaking. Loop back and retry.
                    sleep(Duration::from_millis(200)).await;
                }
            }
        }

        let val = received.expect(
            "timeout waiting for WebRTC sync — mesh/DataChannel may not have established",
        );

        match val {
            Value::Text(str) => assert_eq!(&str, "Hello from WebRTC"),
            other => panic!("Expected Value::Text, got {:?}", other),
        }

        peer_a.stop();
        peer_b.stop();
    }
}
