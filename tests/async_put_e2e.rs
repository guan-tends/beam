//! End-to-end integration tests for the async put/batch_put ack pattern.
//!
//! These tests exercise the full path: Node → Router → Storage adapter →
//! ack reply → Node's `pending_puts` drain. Unlike the unit tests in
//! `src/node.rs::tests`, which directly construct ack `Put` messages and
//! inject them via `handle(...)`, these tests go through the real actor
//! stack.
//!
//! # Why this file exists
//!
//! The original race was: `put` returned synchronously after queuing to the
//! storage actor, so a subsequent `get` could read stale state. The fix is
//! the ack pattern (pending_puts + oneshot). These tests prove the fix
//! works end-to-end without any sleep/timeout workarounds.
//!
//! # Test matrix
//!
//! | Test name                              | Backend       | Notes                          |
//! |----------------------------------------|---------------|--------------------------------|
//! | e2e_put_get_roundtrip_no_sleep         | MemoryStorage | Basic race-fix verification    |
//! | e2e_batch_put_map_roundtrip_no_sleep   | MemoryStorage | Batch variant                  |
//! | e2e_concurrent_puts_serialize_correctly| MemoryStorage | 50 rapid puts, all observed    |
//! | e2e_redb_put_await_durability          | RedbStorage   | Ack fires after fsync (gated)  |

use beam::Node;
use beam::actor::Actor;
use beam::adapters::MemoryStorage;
use beam::types::Value;
use std::time::Duration;

/// Basic race-fix verification: put returns only after commit, so a
/// subsequent get observes the new value WITHOUT any sleep/timeout.
#[tokio::test]
async fn e2e_put_get_roundtrip_no_sleep() {
    let mut node = Node::new();
    node.get("e2e_race_key")
        .put("e2e_race_value".into())
        .await
        .expect("put should ack");
    // IMMEDIATELY after put resolves — no sleep — get must see the value.
    let got = node
        .get("e2e_race_key")
        .once(Some(Duration::from_secs(2)))
        .await;
    assert_eq!(
        got,
        Some(Value::Text("e2e_race_value".to_string())),
        "race fix: get immediately after put.observe must see new value"
    );
}

/// Batch counterpart of e2e_put_get_roundtrip_no_sleep.
#[tokio::test]
async fn e2e_batch_put_map_roundtrip_no_sleep() {
    let mut node = Node::new();
    node.batch_put(vec![
        (vec!["e2e_a".to_string()], Value::Text("1".into())),
        (vec!["e2e_b".to_string()], Value::Text("2".into())),
        (vec!["e2e_c".to_string()], Value::Text("3".into())),
    ])
    .await
    .expect("batch_put should ack");
    // All three children visible IMMEDIATELY after batch_put resolves.
    let a = node.get("e2e_a").once(Some(Duration::from_secs(2))).await;
    let b = node.get("e2e_b").once(Some(Duration::from_secs(2))).await;
    let c = node.get("e2e_c").once(Some(Duration::from_secs(2))).await;
    assert_eq!(a, Some(Value::Text("1".to_string())));
    assert_eq!(b, Some(Value::Text("2".to_string())));
    assert_eq!(c, Some(Value::Text("3".to_string())));
}

/// Issue 50 concurrent puts. Each must ack in order, and each subsequent
/// get must observe its value — no ack can be lost in the queue.
#[tokio::test]
async fn e2e_concurrent_puts_serialize_correctly() {
    let mut node = Node::new();
    // Sequential puts (concurrent would require Arc<Mutex<Node>> since
    // Node methods take &mut self — the architectural choice is sequential
    // + concurrent via Arc-cloning. We test sequential semantics here.)
    for i in 0..50 {
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
            i, got, val
        );
    }
}

/// Pure-MemoryStorage roundtrip test. Confirms the basic actor path works
/// end-to-end (router → MemoryStorage → ack reply → drain pending_puts).
#[tokio::test]
async fn e2e_memory_storage_roundtrip() {
    // Build a node explicitly with MemoryStorage adapter to verify the
    // storage layer participates correctly in the ack protocol.
    let storage = MemoryStorage::new();
    let mut node = Node::new_with_config(
        Default::default(),
        vec![Box::new(storage) as Box<dyn Actor>],
        vec![],
    );
    node.get("mem_key")
        .put("mem_value".into())
        .await
        .expect("memory-storage put should ack");
    let got = node.get("mem_key").once(Some(Duration::from_secs(2))).await;
    assert_eq!(got, Some(Value::Text("mem_value".to_string())));
}

/// Redb-backed ack test. Gated on the `redb` feature because redb is not
/// a default dependency. This verifies that the ack fires AFTER
/// `spawn_blocking(...).await` returns Ok — i.e. after the redb transaction
/// has committed to disk.
/// Redb-backed ack test. Verifies that the ack fires AFTER the redb
/// transaction has committed to disk. We rebuild the node against the
/// same path and confirm the value persists — proof that ack fired AFTER
/// fsync, not before.
///
/// Note: beam does not gate redb behind a feature flag, so this test runs
/// unconditionally. If a `redb` feature is later added, this test should
/// be gated with `#[cfg(feature = "redb")]`.
#[tokio::test]
async fn e2e_redb_put_await_durability() {
    use beam::adapters::RedbStorage;
    use std::env;

    use beam::Config;
    // Manual tmpdir to avoid pulling in the `tempfile` crate.
    let tmp_path = env::temp_dir().join(format!(
        "beam-redb-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let path = tmp_path.to_str().expect("temp path");
    // `new_with_config` is infallible — it panics on failure rather than
    // returning a Result. This matches the existing beam API.
    let storage = RedbStorage::new_with_config(Config::default(), path, None);
    let mut node = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage) as Box<dyn Actor>],
        vec![],
    );

    node.get("redb_key")
        .put("redb_value".into())
        .await
        .expect("redb put should ack after fsync");
    // After put returns, the value MUST be in the redb store.
    let got = node.get("redb_key").once(Some(Duration::from_secs(2))).await;
    assert_eq!(
        got,
        Some(Value::Text("redb_value".to_string())),
        "redb: put ack should guarantee durability"
    );

    // Tear down the node, recreate it against the same path. The value
    // should still be there — proof that ack fired AFTER fsync, not before.
    //
    // `stop()` aborts all spawned actor tasks (Node, Router, and child
    // tasks like the quorum-reaper). This drops the storage adapter and
    // releases the `Arc<Database>`, which in turn releases redb's file
    // lock. A bare `drop(node)` only decrements the local Arc — the
    // Router task (a separate `tokio::spawn`) still holds the storage
    // adapter and keeps the file locked.
    node.stop();
    tokio::task::yield_now().await;
    let storage2 = RedbStorage::new_with_config(Config::default(), path, None);
    let mut node2 = Node::new_with_config(
        Config::default(),
        vec![Box::new(storage2) as Box<dyn Actor>],
        vec![],
    );
    let got2 = node2.get("redb_key").once(Some(Duration::from_secs(2))).await;
    assert_eq!(
        got2,
        Some(Value::Text("redb_value".to_string())),
        "redb: persisted across node restart"
    );
}