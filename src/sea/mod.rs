//! SEA (Security, Encryption, Authorization) module
//! Based on Gun.js sea/ directory
//! Provides encryption, authentication, and authorization capabilities

pub mod certify;
pub mod decrypt;
pub mod encrypt;
pub mod pair;
pub mod secret;
pub mod session;
pub mod sign;
pub mod user;
pub mod verify;
pub mod work;

use crate::types::Value as BeamValue;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::{Arc, RwLock};
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

/// Session state behind `Arc<RwLock>` for shared invalidation across clones
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

/// Identity metadata for an authenticated user.
/// Mirrors Gun.js `user.is` semantics.
#[derive(Clone, Debug)]
pub struct Identity {
    pub alias: String,
    pub pub_key: String,
    pub epub_key: Option<String>,
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

    /// Return the user's identity if authenticated.
    /// Mirrors Gun.js `user.is` — returns alias, pub, epub or None.
    pub fn is(&self) -> Option<Identity> {
        let inner = self.inner.read().ok()?;
        if !inner.is_authenticated {
            return None;
        }
        Some(Identity {
            alias: inner.alias.clone()?,
            pub_key: inner.pair.pub_key.clone(),
            epub_key: inner.pair.epub_key.clone(),
        })
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
    verify_sync(signed_data, pub_key)
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
pub use user::{accept_grant, verify_trust};

/// Compute proof-of-work or content hash
pub async fn work(data: &[u8], salt: Option<&[u8]>, opts: WorkOptions) -> Result<String, SeaError> {
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

// ─── Symmetric cipher re-exports (no ECDH/PBKDF2) ───

/// Encrypt data using a raw 32-byte AES-256 key.
pub async fn encrypt_symmetric(data: &JsonValue, key: &[u8]) -> Result<JsonValue, SeaError> {
    encrypt::encrypt_symmetric(data, key).await
}

/// Decrypt data using a raw 32-byte AES-256 key.
pub async fn decrypt_symmetric(encrypted: &JsonValue, key: &[u8]) -> Result<JsonValue, SeaError> {
    decrypt::decrypt_symmetric(encrypted, key).await
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

/// Sign JSON data and wrap as a BEAM Value::Text for user-space puts.
/// The returned value is a JSON-serialized {"m": message, "s": signature} string.
/// Call this before db.put(value) when writing authenticated user data.
pub async fn sign_value(data: &JsonValue, pair: &KeyPair) -> Result<BeamValue, SeaError> {
    let signed = sign(data, pair).await?;
    let text = serde_json::to_string(&signed)
        .map_err(|e| SeaError::Crypto(format!("serialize signed: {}", e)))?;
    Ok(BeamValue::Text(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::session::InMemorySessionStorage;
    use base64::prelude::*;

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
        let result = work(
            b"hello",
            None,
            WorkOptions {
                name: Some("SHA-256".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_secret_shared_equality() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let ab = secret(bob.epub_key.as_ref().unwrap(), &alice)
            .await
            .unwrap();
        let ba = secret(alice.epub_key.as_ref().unwrap(), &bob)
            .await
            .unwrap();
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
        let user = User::create("testuser", "testpass", &mut node)
            .await
            .unwrap();
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
        let user = User::create("leaveuser", "leavepass", &mut node)
            .await
            .unwrap();
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
        let user = User::create("sessuser", "sesspass", &mut node)
            .await
            .unwrap();
        user.save_to(&storage).await.unwrap();
        let recalled = User::recall("sessuser", &storage).await.unwrap();
        assert!(recalled.is_authenticated());
        assert_eq!(recalled.pub_key(), user.pub_key());
    }

    #[tokio::test]
    async fn test_session_recall_missing() {
        let storage = InMemorySessionStorage::new();
        assert!(matches!(
            User::recall("nosuch", &storage).await,
            Err(SeaError::AuthFailed)
        ));
    }

    #[tokio::test]
    async fn test_session_leave_invalidates_clones() {
        let mut node = crate::Node::new();
        let user = User::create("cloneuser", "clonepass", &mut node)
            .await
            .unwrap();
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
        let _ = User::create("remember_user", "remember_pass", &mut node)
            .await
            .unwrap();
        let user = node
            .user()
            .auth("remember_user", "remember_pass")
            .await
            .unwrap();
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
        let signed = certify(&certificants, policies.as_ref(), &authority)
            .await
            .unwrap();

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
        let signed = certify(
            std::slice::from_ref(&alice.pub_key),
            policies.as_ref(),
            &authority,
        )
        .await
        .unwrap();

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

    #[tokio::test]
    async fn test_trust_grant_accept_roundtrip() {
        let mut node = crate::Node::new();

        // Alice and Bob each create accounts
        let alice_user = User::create("alice_int", "secretA", &mut node)
            .await
            .unwrap();
        let bob_user = User::create("bob_int", "secretB", &mut node).await.unwrap();

        let alice_pair = alice_user.pair();
        let bob_pair = bob_user.pair();

        // Alice trusts Bob to write at path "test/data"
        alice_user
            .trust(&bob_pair.pub_key, Some("test/data"), &mut node)
            .await
            .unwrap();

        // Alice grants Bob access to secret at "test/data"
        alice_user
            .grant(
                &bob_pair.pub_key,
                bob_pair.epub_key.as_ref().unwrap(),
                "test/data",
                &mut node,
            )
            .await
            .unwrap();

        // Verify trust from Bob's perspective
        let trusted = verify_trust(
            &alice_pair.pub_key,
            &bob_pair.pub_key,
            Some("test/data"),
            &mut node,
        )
        .await
        .unwrap();
        assert!(trusted, "Bob should be trusted by Alice for test/data");

        // Bob accepts the grant and recovers the secret
        let secret = accept_grant(
            "test/data",
            &alice_pair.pub_key,
            alice_pair.epub_key.as_ref().unwrap(),
            &bob_pair,
            &mut node,
        )
        .await
        .unwrap();

        // Secret should be a non-empty base64 string
        assert!(!secret.is_empty(), "secret should be recovered");
    }

    #[tokio::test]
    async fn test_two_copy_grant_owner_can_recover() {
        let mut node = crate::Node::new();

        let alice_user = User::create("alice2", "passA", &mut node).await.unwrap();
        let bob_user = User::create("bob2", "passB", &mut node).await.unwrap();

        let alice_pair = alice_user.pair();
        let bob_pair = bob_user.pair();

        // Alice grants Bob
        alice_user
            .grant(
                &bob_pair.pub_key,
                bob_pair.epub_key.as_ref().unwrap(),
                "docs/shared",
                &mut node,
            )
            .await
            .unwrap();

        // Alice (as owner) reads her own backup copy at ~{pub}/grant/{path}/{my_pub}
        let mut owner_grant = node
            .get(&format!("~{}", alice_pair.pub_key))
            .get("grant")
            .get("docs__shared")
            .get(&alice_pair.pub_key);

        let owner_text = owner_grant.once(None).await.and_then(|v| match v {
            BeamValue::Text(t) => Some(t),
            _ => None,
        });

        assert!(owner_text.is_some(), "owner backup copy should exist");

        // Verify it's a signed payload {m,s}
        let parsed: JsonValue = serde_json::from_str(&owner_text.unwrap()).unwrap();
        assert!(
            parsed.get("m").is_some(),
            "backup should be signed payload with m"
        );
        assert!(
            parsed.get("s").is_some(),
            "backup should be signed payload with s"
        );
    }

    #[tokio::test]
    async fn test_user_secret_roundtrip() {
        let mut node = crate::Node::new();
        let user = User::create("secretAlice", "hunter42", &mut node)
            .await
            .unwrap();
        let pair = user.pair();

        let payload = json!({"token": "abracadabra", "exp": 1234567890});
        user.secret(&payload, "wallet/key", &mut node)
            .await
            .unwrap();

        let path_key = "wallet__key";
        let mut secret_node = node
            .get(&format!("~{}", pair.pub_key))
            .get("secret")
            .get(path_key);

        let stored = secret_node
            .once(None)
            .await
            .and_then(|v| match v {
                BeamValue::Text(t) => Some(t),
                _ => None,
            })
            .expect("secret should be stored");

        let outer: JsonValue = serde_json::from_str(&stored).unwrap();
        let msg = outer["m"].as_str().expect("m should be string");
        let enc: JsonValue = serde_json::from_str(msg).unwrap();

        let epub = pair.epub_key.as_ref().unwrap();
        let dh = secret(epub, &pair).await.unwrap();
        let dh_bytes = BASE64_URL_SAFE_NO_PAD.decode(&dh).unwrap();

        let decrypted = decrypt_symmetric(&enc, &dh_bytes).await.unwrap();
        assert_eq!(decrypted, payload);
    }

    #[tokio::test]
    async fn test_secret_grant_accept_full_roundtrip() {
        let mut node = crate::Node::new();

        // 1. Alice creates user and stores a self-encrypted secret
        let alice = User::create("roundtripAlice", "alicePass", &mut node)
            .await
            .unwrap();
        let alice_pair = alice.pair();
        let secret_data = json!({"api_key": "sk-live-4242", "tier": "pro"});
        alice
            .secret(&secret_data, "api/credentials", &mut node)
            .await
            .unwrap();

        // 2. Bob creates user
        let bob = User::create("roundtripBob", "bobPass", &mut node)
            .await
            .unwrap();
        let bob_pair = bob.pair();

        // 3. Alice trusts Bob for the same path
        alice
            .trust(&bob_pair.pub_key, Some("api/credentials"), &mut node)
            .await
            .unwrap();

        // 4. Alice grants Bob a random shared secret (grant generates 16 bytes internally)
        let bob_epub = bob_pair.epub_key.as_ref().expect("bob has epub");
        alice
            .grant(&bob_pair.pub_key, bob_epub, "api/credentials", &mut node)
            .await
            .unwrap();

        // 5. Bob accepts the grant and recovers a shared secret
        let alice_epub = alice_pair.epub_key.as_ref().expect("alice has epub");
        let recovered = accept_grant(
            "api/credentials",
            &alice_pair.pub_key,
            alice_epub,
            &bob_pair,
            &mut node,
        )
        .await
        .unwrap();
        assert!(
            !recovered.is_empty(),
            "Bob should recover a non-empty secret"
        );
        assert_eq!(
            recovered.len(),
            22,
            "grant generates 16 bytes => 22 chars base64 no-pad"
        );

        // 6. Alice's self-encrypted data is still intact and independent
        let mut alice_secret = node
            .get(&format!("~{}", alice_pair.pub_key))
            .get("secret")
            .get("api__credentials");

        let self_enc = alice_secret.once(None).await.and_then(|v| match v {
            BeamValue::Text(t) => Some(t),
            _ => None,
        });
        assert!(
            self_enc.is_some(),
            "Alice's self-encrypted secret should still exist"
        );

        // Verify Bob's recovered secret !== Alice's self-encrypted data (different things)
        let outer: JsonValue = serde_json::from_str(&self_enc.unwrap()).unwrap();
        let msg = outer["m"].as_str().expect("m should be string");
        let enc: JsonValue = serde_json::from_str(msg).unwrap();

        let epub = alice_pair.epub_key.as_ref().unwrap();
        let dh = secret(epub, &alice_pair).await.unwrap();
        let dh_bytes = BASE64_URL_SAFE_NO_PAD.decode(&dh).unwrap();

        let decrypted = decrypt_symmetric(&enc, &dh_bytes).await.unwrap();
        assert_eq!(
            decrypted, secret_data,
            "Alice's self-encrypted copy should match original"
        );
    }
}
