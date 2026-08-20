//! End-to-end tests for the [`FjallStorage`] adapter.
//!
//! These tests go through the full Node → Router → FjallStorage → ack reply
//! drain — exercising the actor plumbing on top of the LSM-tree backend.
//! The unit tests in `src/adapters/fjall_storage.rs::tests` cover the
//! storage adapter directly (no actor system).
//!
//! # Fjall-specific notes
//!
//! Unlike persy (which holds an exclusive flock), fjall uses a directory-based
//! layout with no mandatory file locking. Reopening after drop is safe — the
//! background actor may still be tearing down, but fjall's Drop impl handles
//! concurrent access gracefully. However, we still avoid immediate reopen
//! in tests to prevent flakiness from actor teardown races.
//!
//! # Feature gate
//!
//! All tests require the `fjall` feature:
//!
//! ```bash
//! cargo test -p beam --features fjall --test fjall_e2e
//! ```

#![cfg(feature = "fjall")]

use beam::actor::Actor;
use beam::types::Value;
use std::time::Duration;

/// Build a unique fjall directory path. Each test gets its own directory
/// under `/tmp/beam-fjall-{name}-{pid}-{nanos}/`.
fn unique_fjall_path(test_name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "beam-fjall-{}-{}-{}",
            test_name,
            std::process::id(),
            nanos
        ))
        .to_str()
        .expect("temp path must be utf-8")
        .to_string()
}

/// Build a `Node` wired to a fresh `FjallStorage` at `path`.
fn node_with_fjall(path: &str) -> beam::Node {
    use beam::Config;
    use beam::adapters::FjallStorage;
    let storage = FjallStorage::new_with_config(Config::default(), path);
    beam::Node::new_with_config(
        Config::default(),
        vec![Box::new(storage) as Box<dyn Actor>],
        vec![],
    )
}

/// Put-then-get on a single node. Proves the full round-trip:
/// Put → actor → keyspace.insert() → ack → get → keyspace.get() → reply.
///
/// This is the fjall equivalent of `e2e_redb_put_await_durability` and
/// `e2e_persy_put_get_roundtrip`. The key difference: the ack fires
/// AFTER `insert()` (journal append, microseconds) — not after fsync.
/// The data is crash-safe via WAL but not fsync'd until Flush.
#[tokio::test]
async fn e2e_fjall_put_get_roundtrip() {
    let path = unique_fjall_path("roundtrip");
    let mut node = node_with_fjall(&path);

    node.get("k")
        .put("v".into())
        .await
        .expect("fjall put should ack after insert");

    let got = node.get("k").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got, Some(Value::Text("v".to_string())));

    let _ = std::fs::remove_dir_all(&path);
}

/// 25 sequential puts on the same node all ack, and a final get on each
/// key returns the right value. Mirrors `e2e_persy_sequential_puts_serialize_correctly`.
///
/// Proves the actor + ack-drain stack works against the LSM-tree backend
/// — no fsync per put, so this should be faster than the persy equivalent.
#[tokio::test]
async fn e2e_fjall_sequential_puts_serialize_correctly() {
    let path = unique_fjall_path("sequential");
    let mut node = node_with_fjall(&path);

    for i in 0..25 {
        let key = format!("conc_{}", i);
        let val = format!("val_{}", i);
        node.get(&key)
            .put(val.clone().into())
            .await
            .expect("put should ack");
        let got = node.get(&key).once(Some(Duration::from_secs(2))).await;
        assert_eq!(
            got,
            Some(Value::Text(val.clone())),
            "put {} → get observed {:?}, expected {:?}",
            i,
            got,
            val
        );
    }

    let _ = std::fs::remove_dir_all(&path);
}

/// Three children under one parent — exercises that the LSM-tree point
/// lookup correctly deserializes and returns all three children.
#[tokio::test]
async fn e2e_fjall_nested_children_roundtrip() {
    let path = unique_fjall_path("nested");
    let mut node = node_with_fjall(&path);

    for (child, val) in [
        ("child_a", "a_value"),
        ("child_b", "b_value"),
        ("child_c", "c_value"),
    ] {
        node.get("parent")
            .get(child)
            .put(val.into())
            .await
            .expect("child put");
    }

    for (child, expected) in [
        ("child_a", "a_value"),
        ("child_b", "b_value"),
        ("child_c", "c_value"),
    ] {
        let got = node
            .get("parent")
            .get(child)
            .once(Some(Duration::from_secs(2)))
            .await;
        assert_eq!(got, Some(Value::Text(expected.to_string())));
    }

    let _ = std::fs::remove_dir_all(&path);
}

/// LWW: two sequential puts on the SAME key — newer `updated_at` wins.
/// After both puts ack, get returns the newer value.
#[tokio::test]
async fn e2e_fjall_lww_prefers_newer_value() {
    let path = unique_fjall_path("lww");
    let mut node = node_with_fjall(&path);

    node.get("lww_key").put("older".into()).await.unwrap();
    // Tiny gap so the second put's timestamp is strictly newer.
    tokio::time::sleep(Duration::from_millis(10)).await;
    node.get("lww_key").put("newer".into()).await.unwrap();

    let got = node.get("lww_key").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got, Some(Value::Text("newer".to_string())));

    let _ = std::fs::remove_dir_all(&path);
}

/// Flush after puts, then verify data is still retrievable.
/// Proves the Flush → persist(SyncAll) → ack path works end-to-end.
#[tokio::test]
async fn e2e_fjall_flush_persists_data() {
    let path = unique_fjall_path("flush");
    let mut node = node_with_fjall(&path);

    // Put some data
    node.get("f1").put("v1".into()).await.unwrap();
    node.get("f2").put("v2".into()).await.unwrap();

    // Flush — this triggers spawn_blocking + persist(SyncAll)
    node.flush_storage(Some(Duration::from_secs(5)))
        .await
        .expect("flush should ack after persist");

    // Data should still be retrievable
    let got1 = node.get("f1").once(Some(Duration::from_secs(2))).await;
    let got2 = node.get("f2").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got1, Some(Value::Text("v1".to_string())));
    assert_eq!(got2, Some(Value::Text("v2".to_string())));

    let _ = std::fs::remove_dir_all(&path);
}

/// Two independent fjall databases in the same test. Proves that
/// FjallStorage's actor path correctly commits each put before acking.
#[tokio::test]
async fn e2e_fjall_isolated_persistence_two_dirs() {
    let path_a = unique_fjall_path("iso_a");
    let path_b = unique_fjall_path("iso_b");

    {
        let mut node = node_with_fjall(&path_a);
        node.get("alpha").put("1".into()).await.unwrap();
        let got = node.get("alpha").once(Some(Duration::from_secs(2))).await;
        assert_eq!(got, Some(Value::Text("1".to_string())));
    }
    {
        let mut node = node_with_fjall(&path_b);
        node.get("beta").put("2".into()).await.unwrap();
        let got = node.get("beta").once(Some(Duration::from_secs(2))).await;
        assert_eq!(got, Some(Value::Text("2".to_string())));
    }

    let _ = std::fs::remove_dir_all(&path_a);
    let _ = std::fs::remove_dir_all(&path_b);
}
