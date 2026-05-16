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
