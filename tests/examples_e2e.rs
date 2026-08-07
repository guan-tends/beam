//! # E2E Tests for Examples
//!
//! Integration tests that verify the core behavior patterns demonstrated
//! in `examples/`. Each test mirrors an example's logic programmatically —
//! not just "does it compile" but "does the behavior actually work."
//!
//! ## Coverage
//!
//! | Test | Mirrors Example | Pattern |
//! |------|----------------|---------|
//! | `quickstart_put_subscribe` | quickstart.rs | put + on() subscription |
//! | `nested_graph_map` | nested_graph.rs | batch_put + map() traversal |
//! | `user_auth_lifecycle` | user_auth.rs | SEA User create/auth/leave/recall |
//! | `encrypt_decrypt_roundtrip` | encrypted_data.rs | SEA encrypt/decrypt + sign/verify |
//! | `persistent_storage_restart` | persistent_storage.rs | RedbStorage write/flush/reopen |
//!
//! ## Not Covered Here
//!
//! `two_node_sync.rs` — WebSocket peer sync is already covered by
//! [`tests/cross_backend_mesh_e2e.rs`] (full mesh: 3 nodes, mixed storage
//! backends, bidirectional propagation). Re-testing it here would duplicate
//! coverage and introduce port-collision flakiness for no added value.
//!
//! [`tests/cross_backend_mesh_e2e.rs`]: ../cross_backend_mesh_e2e.rs

mod common;

use std::time::Duration;

use tokio::time::timeout;

use beam::adapters::RedbStorage;
use beam::sea;
use beam::sea::session::InMemorySessionStorage;
use beam::{Config, Node, Value};

// ── quickstart.rs pattern ─────────────────────────────────────────────

#[tokio::test]
async fn quickstart_put_subscribe() {
    let mut db = Node::new();
    let mut sub = db.get("greeting").on();

    db.get("greeting").put(Value::Text("hello".into())).await.unwrap();

    let value = timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("timeout waiting for on() callback")
        .expect("broadcast recv error");

    assert_eq!(value, Value::Text("hello".into()));
    db.stop();
}

// ── nested_graph.rs pattern ───────────────────────────────────────────

#[tokio::test]
async fn nested_graph_map() {
    let mut db = Node::new();

    db.batch_put(vec![
        (vec!["users".into(), "alice".into()], Value::Text("admin".into())),
        (vec!["users".into(), "bob".into()], Value::Text("user".into())),
    ])
    .await
    .unwrap();

    let mut sub = db.get("users").map();

    let mut received = std::collections::HashMap::new();
    for _ in 0..2 {
        let (key, value) = timeout(Duration::from_secs(3), sub.recv())
            .await
            .expect("timeout waiting for map() replay")
            .expect("broadcast recv error");
        received.insert(key, value);
    }

    assert_eq!(received.get("alice"), Some(&Value::Text("admin".into())));
    assert_eq!(received.get("bob"), Some(&Value::Text("user".into())));
    db.stop();
}

// ── user_auth.rs pattern ──────────────────────────────────────────────

#[tokio::test]
async fn user_auth_lifecycle() {
    let mut db = Node::new();
    let storage = InMemorySessionStorage::new();

    let alice = db
        .user()
        .create("testuser", "testpass")
        .await
        .expect("user creation failed");
    assert!(alice.is_authenticated());
    assert_eq!(alice.alias().as_deref(), Some("testuser"));

    let authed = db
        .user()
        .auth("testuser", "testpass")
        .await
        .expect("auth failed");
    assert!(authed.is_authenticated());
    assert_eq!(authed.pub_key(), alice.pub_key());

    alice.save_to(&storage).await.expect("save_to failed");

    let recalled = sea::User::recall("testuser", &storage)
        .await
        .expect("recall failed");
    assert!(recalled.is_authenticated());
    assert_eq!(recalled.pub_key(), alice.pub_key());

    let alice_clone = alice.clone();
    alice.leave();
    assert!(!alice_clone.is_authenticated());

    db.stop();
}

// ── encrypted_data.rs pattern ─────────────────────────────────────────

#[tokio::test]
async fn encrypt_decrypt_roundtrip() {
    let alice = sea::generate_pair().await.expect("generate_pair failed");
    let bob = sea::generate_pair().await.expect("generate_pair failed");

    // Sign + verify
    let payload = serde_json::json!({ "message": "Hello from Alice!" });
    let signed = sea::sign(&payload, &alice).await.expect("sign failed");
    let verified = sea::verify(&signed, &alice.pub_key).await.expect("verify failed");
    assert_eq!(verified, payload);

    // Verify with wrong key — must fail
    let wrong = sea::verify(&signed, &bob.pub_key).await;
    assert!(wrong.is_err());

    // Encrypt + decrypt (ECDH + AES-256-GCM)
    let secret_data = serde_json::json!({ "message": "secret" });
    let encrypted = sea::encrypt(&secret_data, &alice, bob.epub_key.as_deref())
        .await
        .expect("encrypt failed");
    let decrypted = sea::decrypt(&encrypted, &bob, alice.epub_key.as_deref())
        .await
        .expect("decrypt failed");
    assert_eq!(decrypted, secret_data);

    // Symmetric encrypt + decrypt
    let sym_key = [0u8; 32];
    let sym_enc = sea::encrypt_symmetric(&payload, &sym_key).await.expect("sym encrypt failed");
    let sym_dec = sea::decrypt_symmetric(&sym_enc, &sym_key).await.expect("sym decrypt failed");
    assert_eq!(sym_dec, payload);
}

// ── persistent_storage.rs pattern ─────────────────────────────────────

#[tokio::test]
async fn persistent_storage_restart() {
    let temp_path =
        std::env::temp_dir().join(format!("beam-e2e-persist-{}.redb", std::process::id()));
    let _ = std::fs::remove_file(&temp_path);
    let path_str = temp_path.to_string_lossy().to_string();

    let config = Config::default();

    // Write phase
    {
        let mut db = Node::new_with_config(
            config.clone(),
            vec![Box::new(RedbStorage::new_with_config(
                config.clone(),
                &path_str,
                None,
            ))],
            vec![],
        );

        db.get("persisted_key")
            .put(Value::Text("persisted_value".into()))
            .await
            .unwrap();

        db.flush_storage(Some(Duration::from_secs(5)))
            .await
            .expect("flush failed");

        db.stop();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Read phase — new node, same database file
    {
        let mut db = Node::new_with_config(
            config,
            vec![Box::new(RedbStorage::new_with_config(
                Config::default(),
                &path_str,
                None,
            ))],
            vec![],
        );

        let value = db
            .get("persisted_key")
            .once(Some(Duration::from_secs(3)))
            .await;

        assert_eq!(
            value,
            Some(Value::Text("persisted_value".into())),
            "data should survive node restart via RedbStorage"
        );

        db.stop();
    }

    let _ = std::fs::remove_file(&temp_path);
}
