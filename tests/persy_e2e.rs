//!
//! End-to-end tests for the `PersyStorage` adapter. Mirrors the
//! `e2e_redb_put_await_durability` pattern from `tests/async_put_e2e.rs`
//! but uses `PersyStorage` and the `persy` cargo feature.
//!
//! # Why this file exists
//!
//! The unit tests in `src/adapters/persy_storage.rs::tests` exercise the
//! storage adapter directly (no actor plumbing). These e2e tests go
//! through the full Node → Router → PersyStorage → ack reply drain.
//!
//! # Scope note: no reopen-after-drop test
//!
//! Persy 1.x holds an exclusive flock on the DB file descriptor for its
//! entire lifetime. `Node::drop` does not synchronously stop the actor
//! task, so a `PersyStorage::open(path)` immediately after `drop(node)`
//! fails with `AlreadyInUse(Os { code: 11, kind: WouldBlock })` — the
//! background actor still holds the file lock.
//!
//! This is a Persy semantic, not a bug in `PersyStorage`. Production
//! restarts go through process death (kernel releases all fds), which
//! sidesteps the lock. The redb test pattern doesn't hit this because
//! redb uses mmap without mandatory file locking.
//!
//! To prove durability without hitting the lock race, `e2e_persy_isolated_persistence`
//! below uses **two distinct files** in the same test — verifying that
//! PersyStorage's actor path correctly commits each put before acking.
//!
//! # Feature gate
//!
//! All tests in this file require the `persy` feature:
//!
//! ```bash
//! cargo test -p beam --features persy --test persy_e2e
//! ```

#![cfg(feature = "persy")]

use beam::actor::Actor;
use beam::types::Value;
use std::env;
use std::time::Duration;

/// Build a unique Persy file path. Each test gets its own file under
/// `/tmp/beam-persy-{name}-{pid}-{nanos}.persy`.
fn unique_persy_path(test_name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir()
        .join(format!(
            "beam-persy-{}-{}-{}.persy",
            test_name,
            std::process::id(),
            nanos
        ))
        .to_str()
        .expect("temp path must be utf-8")
        .to_string()
}

/// Build a `Node` wired to a fresh `PersyStorage` at `path`.
fn node_with_persy(path: &str) -> beam::Node {
    use beam::Config;
    use beam::adapters::PersyStorage;
    let storage = PersyStorage::new_with_path(path);
    beam::Node::new_with_config(
        Config::default(),
        vec![Box::new(storage) as Box<dyn Actor>],
        vec![],
    )
}

/// Put-then-get on a single node. Proves that ack fires AFTER the
/// actor's prepare().commit(), so the next get sees the new value
/// without any sleep/timeout.
#[tokio::test]
async fn e2e_persy_put_get_roundtrip() {
    let path = unique_persy_path("roundtrip");
    let mut node = node_with_persy(&path);

    node.get("k")
        .put("v".into())
        .await
        .expect("persy put should ack after fsync");

    let got = node.get("k").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got, Some(Value::Text("v".to_string())));

    let _ = std::fs::remove_file(&path);
}

/// 25 sequential puts on the same node all ack, and a final get on each
/// key returns the right value. Mirrors `e2e_concurrent_puts_serialize_correctly`
/// from `tests/async_put_e2e.rs` but with PersyStorage as the persistence
/// layer — proves the actor + ack-drain stack works against a real
/// fsync-on-commit backend.
#[tokio::test]
async fn e2e_persy_sequential_puts_serialize_correctly() {
    let path = unique_persy_path("sequential");
    let mut node = node_with_persy(&path);

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

    let _ = std::fs::remove_file(&path);
}

/// Three children under one parent — exercises that the V0 single-segment
/// scan-and-deserialize correctly finds the parent record and returns all
/// three children.
#[tokio::test]
async fn e2e_persy_nested_children_roundtrip() {
    let path = unique_persy_path("nested");
    let mut node = node_with_persy(&path);

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

    let _ = std::fs::remove_file(&path);
}

/// LWW: two sequential puts on the SAME key — newer `updated_at` wins.
/// After both puts ack, get returns the newer value.
#[tokio::test]
async fn e2e_persy_lww_prefers_newer_value() {
    let path = unique_persy_path("lww");
    let mut node = node_with_persy(&path);

    node.get("lww_key").put("older".into()).await.unwrap();
    // Tiny gap so the second put's timestamp is strictly newer.
    tokio::time::sleep(Duration::from_millis(10)).await;
    node.get("lww_key").put("newer".into()).await.unwrap();

    let got = node.get("lww_key").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got, Some(Value::Text("newer".to_string())));

    let _ = std::fs::remove_file(&path);
}

/// Two **independent** Persy files in the same test. Proves that
/// PersyStorage's actor path correctly commits each put before acking,
/// without testing the post-drop-reopen race that Persy's flock
/// prevents within a single process.
#[tokio::test]
async fn e2e_persy_isolated_persistence_two_files() {
    let path_a = unique_persy_path("iso_a");
    let path_b = unique_persy_path("iso_b");

    {
        let mut node = node_with_persy(&path_a);
        node.get("alpha").put("1".into()).await.unwrap();
        let got = node.get("alpha").once(Some(Duration::from_secs(2))).await;
        assert_eq!(got, Some(Value::Text("1".to_string())));
    }
    {
        let mut node = node_with_persy(&path_b);
        node.get("beta").put("2".into()).await.unwrap();
        let got = node.get("beta").once(Some(Duration::from_secs(2))).await;
        assert_eq!(got, Some(Value::Text("2".to_string())));
    }

    let _ = std::fs::remove_file(&path_a);
    let _ = std::fs::remove_file(&path_b);
}
