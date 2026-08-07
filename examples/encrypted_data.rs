//! Encrypted data — sign, verify, encrypt, and decrypt with SEA.
//!
//! Demonstrates BEAM's cryptographic primitives for secure communication
//! between peers. These are the building blocks for end-to-end encrypted
//! messaging, signed data provenance, and secure key exchange.
//!
//! # Concepts
//!
//! - **`generate_pair()`** — creates an ECDSA P-256 key pair with signing
//!   and ECDH encryption keys.
//! - **`sign(data, &pair)`** — signs JSON data with the private key.
//!   Produces a `{ "m": message, "s": signature }` envelope.
//! - **`verify(signed, &pub_key)`** — verifies a signature and returns the
//!   original data if valid, or an error if the key doesn't match.
//! - **`encrypt(data, &pair, Some(their_epub))`** — encrypts JSON data
//!   using ECDH-derived shared secret + AES-256-GCM. Only the holder of
//!   the matching private key can decrypt.
//! - **`decrypt(encrypted, &pair, Some(their_epub))`** — decrypts data
//!   that was encrypted for your key pair by someone else.
//!
//! # Run
//!
//! ```bash
//! cargo run --example encrypted_data
//! ```
//!
//! # Expected Output
//!
//! ```text
//! Alice's key pair generated
//! Bob's key pair generated
//! Signed and verified: {\"message\":\"Hello from Alice!\"}
//! Encrypted with ECDH + AES-256-GCM
//! Decrypted: {"message":"Hello from Alice!"}
//! ```

use beam::sea::{decrypt, encrypt, generate_pair, sign};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Key generation ---
    // Each party generates their own key pair. The public keys (pub, epub)
    // are shared freely; the private keys (priv, epriv) stay secret.
    let alice = generate_pair().await?;
    let bob = generate_pair().await?;
    println!("Alice's key pair generated");
    println!("Bob's key pair generated");

    // --- Sign and verify ---
    // Alice signs a message with her private key. Anyone with Alice's
    // public key can verify the signature and read the message.
    let payload = json!({ "message": "Hello from Alice!" });

    let signed = sign(&payload, &alice).await?;
    let verified = beam::sea::verify(&signed, &alice.pub_key).await?;
    assert_eq!(verified, payload);
    println!(
        "Signed and verified: {}",
        serde_json::to_string(&verified)?
    );

    // Verify with the wrong key — must fail.
    let wrong_key_result = beam::sea::verify(&signed, &bob.pub_key).await;
    assert!(wrong_key_result.is_err());

    // --- Encrypt and decrypt ---
    // Alice encrypts data for Bob using Bob's encryption public key (epub).
    // ECDH derives a shared secret from Alice's epriv + Bob's epub, then
    // AES-256-GCM encrypts the payload with that secret.
    let secret_data = json!({ "message": "Hello from Alice!" });

    let encrypted =
        encrypt(&secret_data, &alice, bob.epub_key.as_deref()).await?;
    println!("Encrypted with ECDH + AES-256-GCM");

    // Bob decrypts using his key pair and Alice's encryption public key.
    // ECDH derives the same shared secret from Bob's epriv + Alice's epub.
    let decrypted =
        decrypt(&encrypted, &bob, alice.epub_key.as_deref()).await?;
    assert_eq!(decrypted, secret_data);
    println!("Decrypted: {}", serde_json::to_string(&decrypted)?);

    Ok(())
}
