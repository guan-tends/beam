#![allow(deprecated)]
//! User authentication system
//! Provides create/auth/leave/recall using Rod's graph persistence

use super::{generate_pair, work, KeyPair, SeaError, WorkOptions};
use crate::{Node, Value as RodValue};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use rand::RngCore;
use serde_json::{json, Value as JsonValue};

/// Authenticated user with key pair and alias
#[derive(Clone, Debug)]
pub struct User {
    pub pair: KeyPair,
    pub alias: Option<String>,
    pub is_authenticated: bool,
}

/// Session storage trait for recall() persistence
pub trait SessionStorage: Send + Sync {
    fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError>;
    fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError>;
    fn clear(&self, alias: &str) -> Result<(), SeaError>;
}

impl User {
    /// Create a new user with alias and password
    /// Stores encrypted key pair in Rod's graph at `~@alias`
    pub async fn create(alias: &str, pass: &str, db: &mut Node) -> Result<Self, SeaError> {
        let pair = generate_pair().await?;

        // Derive proof from alias + password
        let proof_input = format!("{}{}", alias, pass);
        let proof = work(proof_input.as_bytes(), Some(alias.as_bytes()), WorkOptions::default()).await?;

        // Build key pair data to encrypt
        let auth_data = json!({
            "pub": pair.pub_key,
            "priv": pair.priv_key,
            "epub": pair.epub_key,
            "epriv": pair.epriv_key,
        });

        // Encrypt auth data with proof
        let encrypted_auth = encrypt_pass(&auth_data, &proof).await?;

        // Store in Rod at ~@alias as JSON text
        let alias_payload = json!({
            "pub": pair.pub_key,
            "epub": pair.epub_key,
            "auth": encrypted_auth,
        });

        let mut alias_node = db.get("~@").get(alias);
        alias_node.put(RodValue::Text(alias_payload.to_string()));

        Ok(User {
            pair,
            alias: Some(alias.to_string()),
            is_authenticated: true,
        })
    }

    /// Authenticate existing user from Rod's graph
    pub async fn auth(alias: &str, pass: &str, db: &mut Node) -> Result<Self, SeaError> {
        let mut alias_node = db.get("~@").get(alias);
        let value = alias_node
            .once(None)
            .await
            .ok_or(SeaError::AuthFailed)?;

        let text = match value {
            RodValue::Text(t) => t,
            _ => return Err(SeaError::AuthFailed),
        };

        let alias_data: JsonValue =
            serde_json::from_str(&text).map_err(|_| SeaError::AuthFailed)?;

        let encrypted_auth = alias_data
            .get("auth")
            .ok_or(SeaError::AuthFailed)?;

        let proof_input = format!("{}{}", alias, pass);
        let proof = work(proof_input.as_bytes(), Some(alias.as_bytes()), WorkOptions::default()).await?;

        let decrypted = decrypt_pass(encrypted_auth, &proof).await?;

        let pair = KeyPair {
            pub_key: decrypted
                .get("pub")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            priv_key: decrypted
                .get("priv")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            epub_key: decrypted
                .get("epub")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            epriv_key: decrypted
                .get("epriv")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        Ok(User {
            pair,
            alias: Some(alias.to_string()),
            is_authenticated: true,
        })
    }

    /// Create user directly from existing key pair
    pub fn from_pair(pair: KeyPair) -> Self {
        User {
            pair,
            alias: None,
            is_authenticated: true,
        }
    }

    /// Clear key pair from memory and mark unauthenticated
    pub fn leave(&mut self) {
        self.pair = KeyPair {
            pub_key: String::new(),
            priv_key: String::new(),
            epub_key: None,
            epriv_key: None,
        };
        self.is_authenticated = false;
    }

    /// Recall user from session storage (not yet implemented)
    pub async fn recall(
        _alias: &str,
        _storage: &dyn SessionStorage,
    ) -> Result<Self, SeaError> {
        Err(SeaError::AuthFailed)
    }
}

// --- Passphrase-based AES-GCM helpers (no KeyPair needed) ---

async fn encrypt_pass(data: &JsonValue, passphrase: &str) -> Result<JsonValue, SeaError> {
    let msg = serde_json::to_string(data)
        .map_err(|e| SeaError::Encryption(format!("serialization: {}", e)))?;

    let mut salt = [0u8; 9];
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut salt);
    rand::thread_rng().fill_bytes(&mut nonce);

    let aes_key = derive_key(passphrase, &salt)?;

    let cipher = Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| SeaError::Encryption(format!("cipher: {}", e)))?;

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), msg.as_bytes())
        .map_err(|e| SeaError::Encryption(format!("encrypt: {}", e)))?;

    Ok(json!({
        "ct": base64::encode_config(&ciphertext, base64::STANDARD_NO_PAD),
        "iv": base64::encode_config(&nonce, base64::STANDARD_NO_PAD),
        "s": base64::encode_config(&salt, base64::STANDARD_NO_PAD),
    }))
}

async fn decrypt_pass(encrypted: &JsonValue, passphrase: &str) -> Result<JsonValue, SeaError> {
    let ct = encrypted
        .get("ct")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SeaError::Decryption("missing ct".to_string()))?;
    let iv = encrypted
        .get("iv")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SeaError::Decryption("missing iv".to_string()))?;
    let s = encrypted
        .get("s")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SeaError::Decryption("missing s".to_string()))?;

    let ciphertext = base64::decode_config(ct, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("bad ct".to_string()))?;
    let nonce = base64::decode_config(iv, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("bad iv".to_string()))?;
    let salt = base64::decode_config(s, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("bad s".to_string()))?;

    let aes_key = derive_key(passphrase, &salt)?;

    let cipher = Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| SeaError::Decryption(format!("cipher: {}", e)))?;

    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|_| SeaError::Decryption("bad passphrase or tampered".to_string()))?;

    let text = String::from_utf8(plaintext)
        .map_err(|_| SeaError::Decryption("bad utf8".to_string()))?;

    serde_json::from_str(&text)
        .map_err(|e| SeaError::Decryption(format!("bad json: {}", e)))
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
    let mut key = vec![0u8; 32];
    pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), salt, 100_000, &mut key);
    Ok(key)
}

/// Builder for creating or authenticating users from a Node
pub struct UserBuilder<'a> {
    node: &'a mut Node,
}

impl Node {
    /// Begin user creation or authentication on this node
    pub fn user(&mut self) -> UserBuilder {
        UserBuilder { node: self }
    }
}

impl<'a> UserBuilder<'a> {
    /// Create a new user with alias and password
    pub async fn create(self, alias: &str, pass: &str) -> Result<User, SeaError> {
        User::create(alias, pass, self.node).await
    }

    /// Authenticate an existing user with alias and password
    pub async fn auth(self, alias: &str, pass: &str) -> Result<User, SeaError> {
        User::auth(alias, pass, self.node).await
    }
}
