#![allow(deprecated)]
//! User authentication and session management
//! Provides create/auth/leave/recall using Rod's graph persistence

use super::{generate_pair, work, KeyPair, SeaError, SessionState, SessionStorage, User, WorkOptions};
use crate::{Node, Value as RodValue};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;
use rand::RngCore;
use serde_json::{json, Value as JsonValue};

impl User {
    /// Create a new user with alias and password.
    /// Stores encrypted key pair in Rod's graph at `~@alias`.
    pub async fn create(alias: &str, pass: &str, db: &mut Node) -> Result<Self, SeaError> {
        let pair = generate_pair().await?;

        // Derive proof from alias + password
        let proof_input = format!("{}{}", alias, pass);
        // Generate random 9-byte salt per Gun.js convention
        let mut salt_bytes = [0u8; 9];
        rand::thread_rng().fill_bytes(&mut salt_bytes);

        let proof = work(proof_input.as_bytes(), Some(&salt_bytes), WorkOptions::default()).await?;
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
            "salt": base64::encode_config(&salt_bytes, base64::STANDARD_NO_PAD),
        });
        let mut alias_node = db.get("~@").get(alias);
        alias_node.put(RodValue::Text(alias_payload.to_string()));

        let state = SessionState {
            pair,
            alias: Some(alias.to_string()),
            is_authenticated: true,
        };
        Ok(User::from_state(state))
    }

    /// Authenticate existing user from Rod's graph.
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

        let salt_decoded = if let Some(salt_b64) = alias_data.get("salt").and_then(|v| v.as_str()) {
            base64::decode_config(salt_b64, base64::STANDARD_NO_PAD)
                .unwrap_or_else(|_| alias.as_bytes().to_vec())
        } else {
            alias.as_bytes().to_vec()
        };

        let proof_input = format!("{}{}", alias, pass);
        let proof = work(proof_input.as_bytes(), Some(&salt_decoded), WorkOptions::default()).await?;
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

        let state = SessionState {
            pair,
            alias: Some(alias.to_string()),
            is_authenticated: true,
        };
        Ok(User::from_state(state))
    }

    /// Create user directly from existing key pair.
    pub fn from_pair(pair: KeyPair, alias: Option<&str>) -> Self {
        let state = SessionState {
            pair,
            alias: alias.map(|s| s.to_string()),
            is_authenticated: true,
        };
        User::from_state(state)
    }

    /// Recall user from session storage.
    pub async fn recall(alias: &str, storage: &dyn SessionStorage) -> Result<Self, SeaError> {
        let pair = storage
            .load(alias)
            .await
            .map_err(|e| SeaError::SessionStorage(format!("{}", e)))?;

        if let Some(pair) = pair {
            let state = SessionState {
                pair,
                alias: Some(alias.to_string()),
                is_authenticated: true,
            };
            Ok(User::from_state(state))
        } else {
            Err(SeaError::AuthFailed)
        }
    }

    /// Save current session to storage.
    pub async fn save_to(&self, storage: &dyn SessionStorage) -> Result<(), SeaError> {
        let inner = self.inner.read().map_err(|_| SeaError::SessionStorage("lock poisoned".to_string()))?;
        if !inner.is_authenticated {
            return Err(SeaError::NotAuthenticated);
        }
        let alias = inner.alias.as_ref().ok_or(SeaError::NotAuthenticated)?;
        storage.save(alias, &inner.pair).await
    }
}

// --- Passphrase-based AES-GCM helpers (no KeyPair needed) ---

async fn encrypt_pass(data: &JsonValue, passphrase: &str) -> Result<JsonValue, SeaError> {
    let data = data.clone();
    let passphrase = passphrase.to_string();

    tokio::task::spawn_blocking(move || {
        let msg = serde_json::to_string(&data)
            .map_err(|e| SeaError::Encryption(format!("serialization: {}", e)))?;

        let mut salt = [0u8; 9];
        let mut nonce = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut salt);
        rand::thread_rng().fill_bytes(&mut nonce);

        let aes_key = derive_key_sync(&passphrase, &salt)?;

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
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

async fn decrypt_pass(encrypted: &JsonValue, passphrase: &str) -> Result<JsonValue, SeaError> {
    let encrypted = encrypted.clone();
    let passphrase = passphrase.to_string();

    tokio::task::spawn_blocking(move || {
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

        let aes_key = derive_key_sync(&passphrase, &salt)?;

        let cipher = Aes256Gcm::new_from_slice(&aes_key)
            .map_err(|e| SeaError::Decryption(format!("cipher: {}", e)))?;

        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| SeaError::Decryption("bad passphrase or tampered".to_string()))?;

        let text = String::from_utf8(plaintext)
            .map_err(|_| SeaError::Decryption("bad utf8".to_string()))?;

        serde_json::from_str(&text)
            .map_err(|e| SeaError::Decryption(format!("bad json: {}", e)))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

fn derive_key_sync(passphrase: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
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
    pub fn user(&mut self) -> UserBuilder<'_> {
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
