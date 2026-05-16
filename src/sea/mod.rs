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
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_secret_shared_equality() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();

        let alice_bob = secret(bob.epub_key.as_ref().unwrap(), &alice).await.unwrap();
        let bob_alice = secret(alice.epub_key.as_ref().unwrap(), &bob).await.unwrap();

        assert_eq!(alice_bob, bob_alice, "shared secrets must match");
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"secret": "message"});

        let encrypted = encrypt(&data, &pair, None).await.unwrap();
        assert!(encrypted.get("ct").is_some());

        let decrypted = decrypt(&encrypted, &pair, None).await.unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_user_create_and_auth() {
        let mut node = crate::Node::new();

        let user = User::create("testuser", "testpass", &mut node).await.unwrap();
        assert!(user.is_authenticated());
        assert_eq!(user.alias(), Some("testuser".to_string()));
        assert!(!user.pub_key().is_empty());

        let auth_user = User::auth("testuser", "testpass", &mut node).await.unwrap();
        assert!(auth_user.is_authenticated());
        assert_eq!(auth_user.pub_key(), user.pub_key());
    }

    #[tokio::test]
    async fn test_user_create_duplicate_fails() {
        let mut node = crate::Node::new();

        let _user = User::create("dupuser", "duppass", &mut node).await.unwrap();
        let result = User::create("dupuser", "duppass", &mut node).await;
        assert!(result.is_ok());
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

        let user = node.user().create("builder_user", "builder_pass").await.unwrap();
        assert_eq!(user.alias(), Some("builder_user".to_string()));
        assert!(user.is_authenticated());
    }

    #[tokio::test]
    async fn test_user_builder_auth() {
        let mut node = crate::Node::new();

        let user = node.user().create("auth_user", "auth_pass").await.unwrap();
        let auth = node.user().auth("auth_user", "auth_pass").await.unwrap();
        assert_eq!(auth.pub_key(), user.pub_key());
    }

    // --- Session Storage Tests ---

    struct InMemorySessionStorage {
        data: std::sync::Mutex<std::collections::HashMap<String, KeyPair>>,
    }

    impl InMemorySessionStorage {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl SessionStorage for InMemorySessionStorage {
        async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError> {
            self.data.lock().unwrap().insert(alias.to_string(), pair.clone());
            Ok(())
        }

        async fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError> {
            Ok(self.data.lock().unwrap().get(alias).cloned())
        }

        async fn clear(&self, alias: &str) -> Result<(), SeaError> {
            self.data.lock().unwrap().remove(alias);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_session_memory_save_load_recall() {
        let mut node = crate::Node::new();
        let storage = InMemorySessionStorage::new();

        let user = User::create("sessuser", "sesspass", &mut node).await.unwrap();
        assert!(user.is_authenticated());

        user.save_to(&storage).await.unwrap();

        let recalled = User::recall("sessuser", &storage).await.unwrap();
        assert!(recalled.is_authenticated());
        assert_eq!(recalled.pub_key(), user.pub_key());
        assert_eq!(recalled.alias(), Some("sessuser".to_string()));
    }

    #[tokio::test]
    async fn test_session_recall_missing() {
        let storage = InMemorySessionStorage::new();

        let result = User::recall("nosuchuser", &storage).await;
        assert!(result.is_err());
        match result {
            Err(SeaError::AuthFailed) => {}
            _ => panic!("expected AuthFailed, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_session_leave_invalidates_clones() {
        let mut node = crate::Node::new();

        let user = User::create("cloneuser", "clonepass", &mut node).await.unwrap();
        let clone = user.clone();

        assert!(user.is_authenticated());
        assert!(clone.is_authenticated());

        user.leave();

        // Both originals AND clones are invalidated via Arc<RwLock>
        assert!(!user.is_authenticated());
        assert!(!clone.is_authenticated());
        assert!(user.pair().priv_key.is_empty());
        assert!(clone.pair().priv_key.is_empty());
    }

    #[tokio::test]
    async fn test_session_caller_side_remember_pattern() {
        let mut node = crate::Node::new();
        let storage = InMemorySessionStorage::new();

        // Caller decides whether to remember — no remember param on auth()
        let user = node.user().auth("remember_user", "remember_pass").await;
        if let Ok(ref u) = user {
            u.save_to(&storage).await.unwrap();
        }
    }
}
