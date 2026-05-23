//! SEA (Security, Encryption, Authorization) module
//! Based on Gun.js sea/ directory
//! Provides encryption, authentication, and authorization capabilities

pub mod pair;
pub mod session;
pub mod sign;
pub mod verify;
pub mod work;
pub mod secret;
pub mod encrypt;
pub mod decrypt;
pub mod certify;
pub mod user;

use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::{Arc, RwLock};
use async_trait::async_trait;
use zeroize::Zeroize;

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

impl Zeroize for KeyPair {
    fn zeroize(&mut self) {
        self.priv_key.zeroize();
        self.pub_key.zeroize();
        if let Some(ref mut e) = self.epub_key {
            e.zeroize();
        }
        self.epub_key = None;
        if let Some(ref mut e) = self.epriv_key {
            e.zeroize();
        }
        self.epriv_key = None;
    }
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
    SessionStorage(String),
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
            SeaError::SessionStorage(s) => write!(f, "session storage error: {}", s),
        }
    }
}

impl std::error::Error for SeaError {}

/// Session state behind Arc<RwLock> for shared invalidation across clones
#[derive(Clone, Debug)]
pub struct SessionState {
    pub pair: KeyPair,
    pub alias: Option<String>,
    pub is_authenticated: bool,
}

impl Zeroize for SessionState {
    fn zeroize(&mut self) {
        self.pair.zeroize();
        if let Some(ref mut a) = self.alias {
            a.zeroize();
        }
        self.alias = None;
        self.is_authenticated = false;
    }
}

impl Drop for SessionState {
    fn drop(&mut self) {
        self.pair.zeroize();
    }
}

/// Authenticated user with shared session state
/// Clones share the same underlying session — leave() invalidates all holders.
#[derive(Clone)]
pub struct User {
    pub(crate) inner: Arc<RwLock<SessionState>>,
}

impl fmt::Debug for User {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().map_err(|_| fmt::Error)?;
        f.debug_struct("User")
            .field("alias", &inner.alias)
            .field("is_authenticated", &inner.is_authenticated)
            .field("pub_key", &inner.pair.pub_key)
            .finish_non_exhaustive()
    }
}

impl User {
    pub fn from_state(state: SessionState) -> Self {
        Self {
            inner: Arc::new(RwLock::new(state)),
        }
    }

    pub fn pub_key(&self) -> String {
        self.inner.read().unwrap().pair.pub_key.clone()
    }

    pub fn pair(&self) -> KeyPair {
        self.inner.read().unwrap().pair.clone()
    }

    pub fn alias(&self) -> Option<String> {
        self.inner.read().unwrap().alias.clone()
    }

    pub fn is_authenticated(&self) -> bool {
        self.inner.read().unwrap().is_authenticated
    }

    /// Clear key pair from memory and mark unauthenticated (all clones invalidated)
    pub fn leave(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.pair.zeroize();
            inner.alias.zeroize();
            inner.alias = None;
            inner.is_authenticated = false;
        }
    }
}

/// Session storage trait for recall() persistence — async by default
#[async_trait]
pub trait SessionStorage: Send + Sync {
    async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError>;
    async fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError>;
    async fn clear(&self, alias: &str) -> Result<(), SeaError>;
}

/// Generate a new key pair for signing and encryption
pub async fn generate_pair() -> Result<KeyPair, SeaError> {
    pair::generate_pair().await
}

/// Sign data with a key pair
pub async fn sign(data: &JsonValue, pair: &KeyPair) -> Result<JsonValue, SeaError> {
    sign::sign(data, pair).await
}

/// Verify a signature synchronously (for use from message.rs)
pub fn verify_sync(signed_data: &JsonValue, pub_key: &str) -> Result<JsonValue, SeaError> {
    verify::verify_sync(signed_data, pub_key)
}

/// Verify a signature (async wrapper for backwards compat)
pub async fn verify(signed_data: &JsonValue, pub_key: &str) -> Result<JsonValue, SeaError> {
    Ok(verify_sync(signed_data, pub_key)?)
}

/// Verify a signature asynchronously (non-blocking wrapper via spawn_blocking)
/// Preferred for new code that must not block the async executor.
pub async fn verify_async(signed_data: &JsonValue, pub_key: &str) -> Result<JsonValue, SeaError> {
    let data = signed_data.clone();
    let key = pub_key.to_string();
    tokio::task::spawn_blocking(move || verify_sync(&data, &key))
        .await
        .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

/// Re-export synchronous secret derivation for use inside spawn_blocking closures
pub use secret::secret_sync;

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

// ─── Capability certificate re-exports ───

/// Build and sign a capability certificate authorizing certificants.
pub async fn certify(
    certificants: &[String],
    policies: Option<&JsonValue>,
    authority: &KeyPair,
) -> Result<JsonValue, SeaError> {
    certify::certify(authority, certificants, policies).await
}

/// Verify a signed certificate against authority pubkey (sync).
pub fn verify_certificate(
    signed_cert: &JsonValue,
    authority_pubkey: &str,
) -> Result<JsonValue, SeaError> {
    certify::verify_certificate(signed_cert, authority_pubkey)
}

/// Check if a pubkey appears in certificate's certificants list.
pub fn is_pubkey_certified(payload: &JsonValue, pubkey: &str) -> bool {
    certify::is_certified(payload, pubkey)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::sea::session::InMemorySessionStorage;
    use crate::sea::{KeyPair, SessionStorage};

    #[tokio::test]
    async fn test_generate_pair() {
        let pair = generate_pair().await.unwrap();
        assert!(!pair.pub_key.is_empty());
        assert!(!pair.priv_key.is_empty());
        assert!(pair.epub_key.is_some());
        assert!(pair.epriv_key.is_some());
        let parts: Vec<&str> = pair.pub_key.split('.').collect();
        assert_eq!(parts.len(), 2);
    }

    #[tokio::test]
    async fn test_sign_verify_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"hello": "world"});
        let signed = sign(&data, &pair).await.unwrap();
        let verified = verify(&signed, &pair.pub_key).await.unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_verify_wrong_key_fails() {
        let pair = generate_pair().await.unwrap();
        let wrong = generate_pair().await.unwrap();
        let data = json!({"test": "data"});
        let signed = sign(&data, &pair).await.unwrap();
        assert!(verify(&signed, &wrong.pub_key).await.is_err());
    }

    #[tokio::test]
    async fn test_work_pbkdf2_deterministic() {
        let salt = b"test";
        let data = b"pass";
        let opts = WorkOptions::default();
        let r1 = work(data, Some(salt), opts.clone()).await.unwrap();
        let r2 = work(data, Some(salt), opts.clone()).await.unwrap();
        assert_eq!(r1, r2);
    }

    #[tokio::test]
    async fn test_work_sha256() {
        let result = work(b"hello", None, WorkOptions { name: Some("SHA-256".to_string()), ..Default::default() }).await.unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_secret_shared_equality() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let ab = secret(bob.epub_key.as_ref().unwrap(), &alice).await.unwrap();
        let ba = secret(alice.epub_key.as_ref().unwrap(), &bob).await.unwrap();
        assert_eq!(ab, ba);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"secret": "msg"});
        let enc = encrypt(&data, &pair, None).await.unwrap();
        let dec = decrypt(&enc, &pair, None).await.unwrap();
        assert_eq!(dec, data);
    }

    #[tokio::test]
    async fn test_user_create_and_auth() {
        let mut node = crate::Node::new();
        let user = User::create("testuser", "testpass", &mut node).await.unwrap();
        assert!(user.is_authenticated());
        assert_eq!(user.alias(), Some("testuser".to_string()));
        let auth = User::auth("testuser", "testpass", &mut node).await.unwrap();
        assert_eq!(auth.pub_key(), user.pub_key());
    }

    #[tokio::test]
    async fn test_user_create_duplicate() {
        let mut node = crate::Node::new();
        let _ = User::create("dupuser", "duppass", &mut node).await.unwrap();
        assert!(User::create("dupuser", "duppass", &mut node).await.is_ok());
    }

    #[tokio::test]
    async fn test_user_leave_zeroizes() {
        let mut node = crate::Node::new();
        let user = User::create("leaveuser", "leavepass", &mut node).await.unwrap();
        assert!(!user.pair().priv_key.is_empty());
        user.leave();
        assert!(!user.is_authenticated());
        assert!(user.pair().priv_key.is_empty());
        assert!(user.pair().pub_key.is_empty());
        assert!(user.pair().epriv_key.is_none());
        assert!(user.pair().epub_key.is_none());
    }

    #[tokio::test]
    async fn test_user_builder_create() {
        let mut node = crate::Node::new();
        let user = node.user().create("b", "p").await.unwrap();
        assert_eq!(user.alias(), Some("b".to_string()));
        assert!(user.is_authenticated());
    }

    #[tokio::test]
    async fn test_user_builder_auth() {
        let mut node = crate::Node::new();
        let user = node.user().create("a", "p").await.unwrap();
        let auth = node.user().auth("a", "p").await.unwrap();
        assert_eq!(auth.pub_key(), user.pub_key());
    }

    // === Session Tests (extracted InMemorySessionStorage) ===

    #[tokio::test]
    async fn test_session_memory_save_load_recall() {
        let mut node = crate::Node::new();
        let storage = InMemorySessionStorage::new();
        let user = User::create("sessuser", "sesspass", &mut node).await.unwrap();
        user.save_to(&storage).await.unwrap();
        let recalled = User::recall("sessuser", &storage).await.unwrap();
        assert!(recalled.is_authenticated());
        assert_eq!(recalled.pub_key(), user.pub_key());
    }

    #[tokio::test]
    async fn test_session_recall_missing() {
        let storage = InMemorySessionStorage::new();
        assert!(matches!(User::recall("nosuch", &storage).await, Err(SeaError::AuthFailed)));
    }

    #[tokio::test]
    async fn test_session_leave_invalidates_clones() {
        let mut node = crate::Node::new();
        let user = User::create("cloneuser", "clonepass", &mut node).await.unwrap();
        let clone = user.clone();
        user.leave();
        assert!(!user.is_authenticated());
        assert!(!clone.is_authenticated());
        assert!(user.pair().priv_key.is_empty());
        assert!(clone.pair().priv_key.is_empty());
    }

    #[tokio::test]
    async fn test_session_caller_side_remember() {
        let mut node = crate::Node::new();
        let storage = InMemorySessionStorage::new();
        let _ = User::create("remember_user", "remember_pass", &mut node).await.unwrap();
        let user = node.user().auth("remember_user", "remember_pass").await.unwrap();
        user.save_to(&storage).await.unwrap();
        assert!(User::recall("remember_user", &storage).await.is_ok());
    }

    #[tokio::test]
    async fn test_verify_async_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"hello": "world"});
        let signed = sign(&data, &pair).await.unwrap();
        let verified = verify_async(&signed, &pair.pub_key).await.unwrap();
        assert_eq!(verified, data);
    }

    // ─── SEA.certify tests ───

    #[tokio::test]
    async fn test_certify_and_verify() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();

        let certificants = vec![alice.pub_key.clone(), bob.pub_key.clone()];
        let policies = Some(json!({"e": 9999999999999.0_f64, "r": ".*", "w": "skills/"}));
        let signed = certify(&certificants, policies.as_ref(), &authority).await.unwrap();

        // Verify with correct authority
        let payload = verify_certificate(&signed, &authority.pub_key).unwrap();
        assert!(is_pubkey_certified(&payload, &alice.pub_key));
        assert!(is_pubkey_certified(&payload, &bob.pub_key));
        assert!(!is_pubkey_certified(&payload, "someRandomKey"));
        assert_eq!(payload["r"].as_str(), Some(".*"));
        assert_eq!(payload["w"].as_str(), Some("skills/"));
    }

    #[tokio::test]
    async fn test_certify_expired_fails() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();

        // Expiry in the past (1970)
        let policies = Some(json!({"e": 1000.0_f64}));
        let signed = certify(&[alice.pub_key.clone()], policies.as_ref(), &authority).await.unwrap();

        assert!(verify_certificate(&signed, &authority.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_certify_wrong_authority_fails() {
        let authority = generate_pair().await.unwrap();
        let wrong = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();

        let signed = certify(&[alice.pub_key], None, &authority).await.unwrap();
        assert!(verify_certificate(&signed, &wrong.pub_key).is_err());
    }
}
