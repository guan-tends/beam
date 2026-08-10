#![cfg(not(target_arch = "wasm32"))]
//! End-to-end tests for BEAM graceful shutdown.
//!
//! These tests verify the shutdown sequence:
//!
//! - **Data integrity**: writes before shutdown are persisted to disk
//! - **Node::shutdown**: the full flush → signal → drain → stop sequence
//! - **Storage flush**: flush_storage completes before shutdown returns
//! - **Idempotent stop**: calling stop() after shutdown() is safe
//!
//! # Strategy
//!
//! We use redb storage (the default persistent backend) and verify that
//! data written before shutdown survives a restart. The tests exercise
//! the `Node::shutdown()` method directly rather than relying on signal
//! delivery (which is OS-dependent and hard to test deterministically).

mod common;

#[cfg(test)]
mod tests {
    use beam::adapters::RedbStorage;
    use beam::{Config, Node, Value};
    use std::time::Duration;
    use tokio::time::timeout;

    /// Generate a random u64 for unique temp file names.
    fn rand_u64() -> u64 {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// A node with memory storage should shut down gracefully without error.
    ///
    /// This is the simplest smoke test: create a node, write nothing,
    /// call shutdown, verify Ok.
    #[tokio::test]
    async fn shutdown_memory_storage_returns_ok() {
        let mut node = Node::new();
        let result = node.shutdown(Duration::from_secs(5)).await;
        assert!(result.is_ok(), "graceful shutdown should return Ok");
    }

    /// A node with redb storage should flush pending writes during shutdown.
    ///
    /// Strategy: write data, shutdown, create a new node on the same redb
    /// file, verify the data is present.
    #[tokio::test]
    async fn shutdown_persists_data_to_redb() {
        let db_path = format!(
            "/tmp/beam-shutdown-persist-{}-{}.redb",
            std::process::id(),
            rand_u64()
        );
        // Clean up from any previous run
        let _ = std::fs::remove_file(&db_path);

        // Phase 1: Write data and shut down gracefully.
        {
            let config = Config::default();
            let storage = RedbStorage::new_with_config(config.clone(), &db_path, None);
            let mut node = Node::new_with_config(config, vec![Box::new(storage)], Vec::new());

            // Write a value
            let mut sub_node = node.get("test-key");
            sub_node.put("test-value".into()).await.unwrap();

            // Give the write time to propagate through the actor system
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Graceful shutdown — should flush storage
            let mut node = node;
            let result = node.shutdown(Duration::from_secs(10)).await;
            assert!(result.is_ok(), "shutdown should succeed: {:?}", result);
            // Explicitly drop the node to release the redb file lock.
            drop(node);
        }

        // Phase 2: Reopen the same redb file and verify data persisted.
        // Give the OS time to release the file lock
        tokio::time::sleep(Duration::from_millis(500)).await;
        {
            let config = Config::default();
            let storage = RedbStorage::new_with_config(config.clone(), &db_path, None);
            let mut node = Node::new_with_config(config, vec![Box::new(storage)], Vec::new());

            // Read the value back via once() — which reads from storage
            let mut sub_node = node.get("test-key");
            let val = sub_node.once(Some(Duration::from_secs(5))).await;
            assert!(
                val.is_some(),
                "value should be present in redb after graceful shutdown"
            );
            if let Some(Value::Text(v)) = val {
                assert_eq!(v, "test-value", "persisted value should match");
            }
        }

        // Cleanup
        let _ = std::fs::remove_file(&db_path);
    }

    /// Node::shutdown with a very short timeout should still complete
    /// (possibly with an error if flush takes longer, but not hang).
    #[tokio::test]
    async fn shutdown_with_short_timeout_does_not_hang() {
        let mut node = Node::new();
        // 1ms timeout — flush should complete quickly for empty memory storage
        let result = timeout(
            Duration::from_secs(10),
            node.shutdown(Duration::from_millis(1)),
        )
        .await;

        // Should complete within 10s regardless of the internal timeout
        assert!(
            result.is_ok(),
            "shutdown should not hang even with 1ms timeout"
        );
    }

    /// Calling Node::stop() after shutdown() should be safe (idempotent-ish).
    ///
    /// shutdown() already calls stop() at the end. Calling stop() again
    /// should not panic — it just re-aborts already-aborted handles.
    #[tokio::test]
    async fn stop_after_shutdown_is_safe() {
        let mut node = Node::new();
        let _ = node.shutdown(Duration::from_secs(5)).await;
        // This should not panic
        node.stop();
    }

    /// Multiple nodes in the same process should shut down independently.
    ///
    /// Each Node creates its own watch channel, so signaling one should
    /// not affect the other. This verifies the isolation tested at the
    /// ActorContext level holds at the Node level too.
    #[tokio::test]
    async fn shutdown_isolation_between_nodes() {
        let mut node_a = Node::new();
        let mut node_b = Node::new();

        // Shut down A — B should still be operational
        let result_a = node_a.shutdown(Duration::from_secs(5)).await;
        assert!(result_a.is_ok());

        // B should still work
        let mut sub = node_b.get("isolation-test");
        sub.put("still-here".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now shut down B
        let result_b = node_b.shutdown(Duration::from_secs(5)).await;
        assert!(result_b.is_ok());
    }
}
