//! User authentication — the SEA `User` lifecycle.
//!
//! Demonstrates creating, authenticating, persisting, and recalling a user
//! with BEAM's Security, Encryption, and Authorization (SEA) layer. This is
//! the foundation for any application that needs authenticated graph data.
//!
//! # Concepts
//!
//! - **`User::create(alias, pass, &mut db)`** — generates a key pair,
//!   encrypts it with a password-derived key, and stores it in the graph
//!   at `~@<alias>`. The returned `User` is authenticated and ready to use.
//! - **`User::auth(alias, pass, &mut db)`** — re-authenticates from the
//!   stored encrypted credentials. Returns a new authenticated `User`.
//! - **`SessionStorage`** — persists the key pair so the user can be
//!   recalled without re-entering their password. `InMemorySessionStorage`
//!   is used here for demonstration; `EncryptedFileSessionStorage` is the
//!   production option.
//! - **`User::recall(alias, &storage)`** — restores a session from storage.
//! - **`User::leave()`** — zeroizes the key pair in memory and marks the
//!   session unauthenticated. All clones are invalidated simultaneously.
//!
//! # Run
//!
//! ```bash
//! cargo run --example user_auth
//! ```
//!
//! # Expected Output
//!
//! ```text
//! Created user: alice
//! Authenticated: alice
//! Saved session, recalled: alice
//! After leave, authenticated: false
//! ```

use beam::sea::session::InMemorySessionStorage;
#[allow(unused_imports)]
use beam::sea::{SessionStorage, User};
use beam::Node;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Node::new();

    // --- Create ---
    // Generates an ECDSA P-256 key pair, encrypts it with a password-derived
    // key (PBKDF2), and stores the encrypted credentials in the graph.
    let alice = User::create("alice", "password123", &mut db).await?;
    assert!(alice.is_authenticated());
    assert_eq!(alice.alias().as_deref(), Some("alice"));
    println!("Created user: {}", alice.alias().unwrap());

    // --- Auth ---
    // Re-derives the key from the stored encrypted credentials. The returned
    // user has the same public key as the original.
    let alice2 = User::auth("alice", "password123", &mut db).await?;
    assert!(alice2.is_authenticated());
    assert_eq!(alice2.pub_key(), alice.pub_key());
    println!("Authenticated: {}", alice2.alias().unwrap());

    // --- Session persistence ---
    // Save the key pair to session storage so the user can be recalled
    // without re-entering their password.
    let storage = InMemorySessionStorage::new();
    alice.save_to(&storage).await?;

    let recalled = User::recall("alice", &storage).await?;
    assert!(recalled.is_authenticated());
    assert_eq!(recalled.pub_key(), alice.pub_key());
    println!("Saved session, recalled: {}", recalled.alias().unwrap());

    // --- Leave (logout) ---
    // Zeroizes the key pair in memory. All clones sharing the session are
    // invalidated — none can read the private key afterward.
    let alice_clone = alice.clone();
    alice.leave();
    assert!(!alice_clone.is_authenticated());
    assert!(alice_clone.pair().priv_key.is_empty());
    println!("After leave, authenticated: {}", alice_clone.is_authenticated());

    db.stop();
    Ok(())
}
