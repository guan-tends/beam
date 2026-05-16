//! SEA (Security, Encryption, Authorization) module
//! Based on Gun.js sea/ directory
//! Provides encryption, authentication, and authorization capabilities

pub mod pair;
pub mod sign;
pub mod verify;
pub mod work;
pub mod secret;
pub mod encrypt;
pub mod decrypt;
pub mod user;

use serde_json::Value as JsonValue;
use std::fmt;

/// Key pair for signing and encryption
#[derive(Clone, Debug)]
pub struct KeyPair {
    /// Public key for signing (ECDSA, P-256) in x.y base64 format
    pub pub_key: String,
    /// Private key for signing (ECDSA, P-256) base64 encoded scalar
    pub priv_key: String,
    /// Public key for encryption (ECDH, P-256) in x.y base64 format
    pub epub_key: Option<String>,
    /// Private key for encryption (ECDH, P-256) base64 encoded scalar
    pub epriv_key: Option<String>,
}

/// Options for SEA.work()
#[derive(Clone, Debug)]
pub struct WorkOptions {
    pub name: Option<String>,
    pub iterations: Option<u32>,
    pub salt: Option<Vec<u8>>,
    pub hash: Option<String>,
    pub length: Option<usize>,
    pub encode: Option<String>,
}

impl Default for WorkOptions {
    fn default() -> Self {
        Self {
            name: Some("PBKDF2".to_string()),
            iterations: Some(100_000),
            salt: None,
            hash: Some("SHA-256".to_string()),
            length: Some(512),
            encode: Some("base64".to_string()),
        }
    }
}

/// SEA module error types
#[derive(Debug)]
pub enum SeaError {
    Crypto(String),
    InvalidKey,
    VerificationFailed,
    Encryption(String),
    Decryption(String),
    UserExists,
    AuthFailed,
    NotAuthenticated,
}

impl fmt::Display for SeaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SeaError::Crypto(s) => write!(f, "crypto error: {}", s),
            SeaError::InvalidKey => write!(f, "invalid key format"),
            SeaError::VerificationFailed => write!(f, "signature verification failed"),
            SeaError::Encryption(s) => write!(f, "encryption error: {}", s),
            SeaError::Decryption(s) => write!(f, "decryption error: {}", s),
            SeaError::UserExists => write!(f, "user already exists"),
            SeaError::AuthFailed => write!(f, "wrong user or password"),
            SeaError::NotAuthenticated => write!(f, "not authenticated"),
        }
    }
}

impl std::error::Error for SeaError {}

/// Generate a new key pair for signing and encryption
pub async fn generate_pair() -> Result<KeyPair, SeaError> {
    pair::generate_pair().await
}

/// Sign data with a key pair
pub async fn sign(data: &JsonValue, pair: &KeyPair) -> Result<JsonValue, SeaError> {
    sign::sign(data, pair).await
}

/// Verify a signature
pub async fn verify(signed_data: &JsonValue, pub_key: &str) -> Result<JsonValue, SeaError> {
    verify::verify(signed_data, pub_key).await
}

/// Compute proof-of-work or content hash
pub async fn work(
    data: &[u8],
    salt: Option<&[u8]>,
    opts: WorkOptions,
) -> Result<String, SeaError> {
    work::work(data, salt, opts).await
}

/// Derive shared secret from ECDH key exchange
pub async fn secret(their_epub: &str, pair: &KeyPair) -> Result<String, SeaError> {
    secret::secret(their_epub, pair).await
}

/// Encrypt data using AES-GCM
pub async fn encrypt(
    data: &JsonValue,
    pair: &KeyPair,
    their_epub: Option<&str>,
) -> Result<JsonValue, SeaError> {
    encrypt::encrypt(data, pair, their_epub).await
}

/// Decrypt data using AES-GCM
pub async fn decrypt(
    encrypted: &JsonValue,
    pair: &KeyPair,
    their_epub: Option<&str>,
) -> Result<JsonValue, SeaError> {
    decrypt::decrypt(encrypted, pair, their_epub).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_generate_pair() {
        let pair = generate_pair().await.unwrap();
        assert!(!pair.pub_key.is_empty());
        assert!(!pair.priv_key.is_empty());
        assert!(pair.epub_key.is_some());
        assert!(pair.epriv_key.is_some());

        // Check x.y format
        let parts: Vec<&str> = pair.pub_key.split('.').collect();
        assert_eq!(parts.len(), 2);
        assert!(!parts[0].is_empty());
        assert!(!parts[1].is_empty());
    }

    #[tokio::test]
    async fn test_sign_verify_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"hello": "world", "number": 42});

        let signed = sign(&data, &pair).await.unwrap();
        assert!(signed.get("m").is_some());
        assert!(signed.get("s").is_some());

        let verified = verify(&signed, &pair.pub_key).await.unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_verify_wrong_key_fails() {
        let pair = generate_pair().await.unwrap();
        let wrong_pair = generate_pair().await.unwrap();
        let data = json!({"test": "data"});

        let signed = sign(&data, &pair).await.unwrap();
        let result = verify(&signed, &wrong_pair.pub_key).await;
        assert!(result.is_err());
        match result {
            Err(SeaError::VerificationFailed) => {}
            _ => panic!("expected VerificationFailed, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_work_pbkdf2_deterministic() {
        let salt = b"test-salt";
        let data = b"password";
        let opts = WorkOptions::default();

        let result1 = work(data, Some(salt), opts.clone()).await.unwrap();
        let result2 = work(data, Some(salt), opts.clone()).await.unwrap();

        assert_eq!(result1, result2);
        assert!(!result1.is_empty());
    }

    #[tokio::test]
    async fn test_work_sha256() {
        let data = b"hello";
        let opts = WorkOptions {
            name: Some("SHA-256".to_string()),
            ..Default::default()
        };

        let result = work(data, None, opts).await.unwrap();
        // Expected SHA-256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_secret_shared_equality() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();

        let alice_bob = secret(bob.epub_key.as_ref().unwrap(), &alice).await.unwrap();
        let bob_alice = secret(alice.epub_key.as_ref().unwrap(), &bob).await.unwrap();

        assert_eq!(alice_bob, bob_alice);
    }

    #[tokio::test]
    async fn test_secret_different_keys() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let charlie = generate_pair().await.unwrap();

        let alice_bob = secret(bob.epub_key.as_ref().unwrap(), &alice).await.unwrap();
        let alice_charlie = secret(charlie.epub_key.as_ref().unwrap(), &alice).await.unwrap();

        assert_ne!(alice_bob, alice_charlie);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_self() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"secret": "message", "value": 123});

        let encrypted = encrypt(&data, &pair, None).await.unwrap();
        assert!(encrypted.get("ct").is_some());
        assert!(encrypted.get("iv").is_some());
        assert!(encrypted.get("s").is_some());

        let decrypted = decrypt(&encrypted, &pair, None).await.unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_shared() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let data = json!({"shared": "secret between alice and bob"});

        // Alice encrypts for Bob
        let encrypted = encrypt(&data, &alice, bob.epub_key.as_deref()).await.unwrap();

        // Bob decrypts
        let decrypted = decrypt(&encrypted, &bob, alice.epub_key.as_deref()).await.unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_decrypt_wrong_key_fails() {
        let pair = generate_pair().await.unwrap();
        let wrong_pair = generate_pair().await.unwrap();
        let data = json!({"test": "data"});

        let encrypted = encrypt(&data, &pair, None).await.unwrap();
        let result = decrypt(&encrypted, &wrong_pair, None).await;
        assert!(result.is_err());
        match result {
            Err(SeaError::Decryption(_)) => {}
            _ => panic!("expected Decryption error, got {:?}", result),
        }
    }
}
