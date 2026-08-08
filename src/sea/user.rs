#![allow(clippy::await_holding_lock)] // TODO: Refactor in SEA review pass — extract data from MutexGuard before await
#![allow(deprecated)]
//! User authentication and session management
//! Provides create/auth/leave/recall using BEAM's graph persistence

use super::{
    KeyPair, SeaError, SessionState, SessionStorage, User, WorkOptions, certify, decrypt,
    decrypt_symmetric, encrypt, encrypt_symmetric, generate_pair, is_pubkey_certified, secret,
    sign_value, verify_certificate, work,
};
use crate::{Node, Value as BeamValue};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use rand::RngCore;
use serde_json::{Value as JsonValue, json};

impl User {
    /// Create a new user with alias and password.
    /// Stores encrypted key pair in BEAM's graph at `~@alias`.
    pub async fn create(alias: &str, pass: &str, db: &mut Node) -> Result<Self, SeaError> {
        let pair = generate_pair().await?;

        // Derive proof from alias + password
        let proof_input = format!("{}{}", alias, pass);
        // Generate random 9-byte salt per Gun.js convention
        let mut salt_bytes = [0u8; 9];
        rand::rng().fill_bytes(&mut salt_bytes);

        let proof = work(
            proof_input.as_bytes(),
            Some(&salt_bytes),
            WorkOptions::default(),
        )
        .await?;
        // Build key pair data to encrypt
        let auth_data = json!({
            "pub": pair.pub_key,
            "priv": pair.priv_key,
            "epub": pair.epub_key,
            "epriv": pair.epriv_key,
        });

        // Encrypt auth data with proof
        let encrypted_auth = encrypt_pass(&auth_data, &proof).await?;

        // Store in BEAM at ~@alias as JSON text
        let alias_payload = json!({
            "pub": pair.pub_key,
            "epub": pair.epub_key,
            "auth": encrypted_auth,
            "salt": BASE64_URL_SAFE_NO_PAD.encode(salt_bytes),
        });
        let mut alias_node = db.get("~@").get(alias);
        let _ = alias_node
            .put(BeamValue::Text(alias_payload.to_string()))
            .await;

        let state = SessionState {
            pair,
            alias: Some(alias.to_string()),
            is_authenticated: true,
        };
        Ok(User::from_state(state))
    }

    /// Authenticate existing user from BEAM's graph.
    pub async fn auth(alias: &str, pass: &str, db: &mut Node) -> Result<Self, SeaError> {
        let mut alias_node = db.get("~@").get(alias);
        let value = alias_node.once(None).await.ok_or(SeaError::AuthFailed)?;

        let text = match value {
            BeamValue::Text(t) => t,
            _ => return Err(SeaError::AuthFailed),
        };

        let alias_data: JsonValue =
            serde_json::from_str(&text).map_err(|_| SeaError::AuthFailed)?;

        let encrypted_auth = alias_data.get("auth").ok_or(SeaError::AuthFailed)?;

        let salt_decoded = if let Some(salt_b64) = alias_data.get("salt").and_then(|v| v.as_str()) {
            BASE64_URL_SAFE_NO_PAD
                .decode(salt_b64)
                .unwrap_or_else(|_| alias.as_bytes().to_vec())
        } else {
            alias.as_bytes().to_vec()
        };

        let proof_input = format!("{}{}", alias, pass);
        let proof = work(
            proof_input.as_bytes(),
            Some(&salt_decoded),
            WorkOptions::default(),
        )
        .await?;
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
    ///
    /// Note: clones the [`KeyPair`] and alias out of the session lock
    /// before awaiting `storage.save(...)` to keep the future `Send`.
    /// Holding the `std::sync::RwLock` read guard across an `.await`
    /// would make the returned future non-`Send` (the std guard is not
    /// `Send`), which breaks callers that need to await auth flows
    /// while holding a tokio lock (e.g. the MCP `identity_auth` tool).
    pub async fn save_to(&self, storage: &dyn SessionStorage) -> Result<(), SeaError> {
        let (alias, pair) = {
            let inner = self
                .inner
                .read()
                .map_err(|_| SeaError::SessionStorage("lock poisoned".to_string()))?;
            if !inner.is_authenticated {
                return Err(SeaError::NotAuthenticated);
            }
            let alias = inner.alias.clone().ok_or(SeaError::NotAuthenticated)?;
            let pair = inner.pair.clone();
            (alias, pair)
        }; // guard dropped here, before any await
        storage.save(&alias, &pair).await
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
        rand::rng().fill_bytes(&mut salt);
        rand::rng().fill_bytes(&mut nonce);

        let aes_key = derive_key_sync(&passphrase, &salt)?;

        let cipher = Aes256Gcm::new_from_slice(&aes_key)
            .map_err(|e| SeaError::Encryption(format!("cipher: {}", e)))?;

        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), msg.as_bytes())
            .map_err(|e| SeaError::Encryption(format!("encrypt: {}", e)))?;

        Ok(json!({
            "ct": BASE64_URL_SAFE_NO_PAD.encode(&ciphertext),
            "iv": BASE64_URL_SAFE_NO_PAD.encode(nonce),
            "s": BASE64_URL_SAFE_NO_PAD.encode(salt),
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

        let ciphertext = BASE64_URL_SAFE_NO_PAD
            .decode(ct)
            .map_err(|_| SeaError::Decryption("bad ct".to_string()))?;
        let nonce = BASE64_URL_SAFE_NO_PAD
            .decode(iv)
            .map_err(|_| SeaError::Decryption("bad iv".to_string()))?;
        let salt = BASE64_URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| SeaError::Decryption("bad s".to_string()))?;

        let aes_key = derive_key_sync(&passphrase, &salt)?;

        let cipher = Aes256Gcm::new_from_slice(&aes_key)
            .map_err(|e| SeaError::Decryption(format!("cipher: {}", e)))?;

        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| SeaError::Decryption("bad passphrase or tampered".to_string()))?;

        let text = String::from_utf8(plaintext)
            .map_err(|_| SeaError::Decryption("bad utf8".to_string()))?;

        serde_json::from_str(&text).map_err(|e| SeaError::Decryption(format!("bad json: {}", e)))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

// Passphrase-based key derivation uses the shared `derive_aes_key_sync` from
// `encrypt.rs` — SHA-256(key_string + salt_utf8), matching Gun.js aeskey.js.
use super::encrypt::derive_aes_key_sync as derive_key_sync;
use base64::prelude::*;

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

// ─── Social primitives: trust, grant, verify ───

/// Encode a path string for safe use as a graph key.
/// Replaces `/` with `__` to avoid segment collision in BEAM's key-path graph.
fn encode_path(path: &str) -> String {
    path.replace('/', "__")
}

impl User {
    /// Delegate write trust to a recipient for an optional path.
    /// Stores a capability certificate at `~{pub}/trust/{path}`.
    pub async fn trust(
        &self,
        recipient_pubkey: &str,
        path: Option<&str>,
        db: &mut Node,
    ) -> Result<(), SeaError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| SeaError::SessionStorage("lock poisoned".to_string()))?;
        if !inner.is_authenticated {
            return Err(SeaError::NotAuthenticated);
        }

        let certificants = vec![recipient_pubkey.to_string()];
        let policies = path.map(|p| json!({"w": p}));
        let signed = certify(&certificants, policies.as_ref(), &inner.pair).await?;

        let path_key = path
            .map(encode_path)
            .unwrap_or_else(|| "global".to_string());
        let mut trust_node = db
            .get(&format!("~{}", inner.pair.pub_key))
            .get("trust")
            .get(&path_key);
        let _ = trust_node.put(BeamValue::Text(signed.to_string())).await;

        Ok(())
    }

    /// Grant a recipient access to decrypt data at a path.
    /// Stores signed ECDH-encrypted copies at `~{pub}/grant/{path}/{recipient}` and `~{pub}/grant/{path}/{my_pub}`.
    pub async fn grant(
        &self,
        recipient_pubkey: &str,
        recipient_epub: &str,
        data_path: &str,
        db: &mut Node,
    ) -> Result<(), SeaError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| SeaError::SessionStorage("lock poisoned".to_string()))?;
        if !inner.is_authenticated {
            return Err(SeaError::NotAuthenticated);
        }
        let pair = &inner.pair;
        let path_key = encode_path(data_path);

        // 1. Retrieve or create a 16-byte random secret for this data path
        let sec = {
            let mut secret_node = db
                .get(&format!("~{}", pair.pub_key))
                .get("secrets")
                .get(&path_key);
            match secret_node.once(None).await {
                Some(BeamValue::Text(enc_text)) => {
                    let outer: JsonValue = serde_json::from_str(&enc_text)
                        .map_err(|e| SeaError::Decryption(format!("bad secret json: {}", e)))?;
                    let enc = if outer.get("m").is_some() && outer.get("s").is_some() {
                        let msg = outer["m"]
                            .as_str()
                            .ok_or_else(|| SeaError::Decryption("m not string".into()))?;
                        serde_json::from_str(msg)
                            .map_err(|e| SeaError::Decryption(format!("bad m: {}", e)))?
                    } else {
                        outer
                    };
                    decrypt(&enc, pair, None)
                        .await?
                        .as_str()
                        .ok_or_else(|| SeaError::Decryption("secret not string".into()))?
                        .to_string()
                }
                _ => {
                    let mut bytes = [0u8; 16];
                    rand::rng().fill_bytes(&mut bytes);
                    let new_sec = BASE64_URL_SAFE_NO_PAD.encode(bytes);
                    let enc = encrypt(&json!(new_sec), pair, None).await?;
                    let signed = sign_value(&enc, pair).await?;
                    let _ = secret_node.put(signed).await;
                    new_sec
                }
            }
        };

        // 2. ECDH shared secret with recipient
        let dh = secret(recipient_epub, pair).await?;
        let dh_bytes = BASE64_URL_SAFE_NO_PAD
            .decode(&dh)
            .map_err(|_| SeaError::Crypto("bad dh".into()))?;

        // 3. Encrypt data secret with shared secret
        let enc_for_recipient = encrypt_symmetric(&json!(sec), &dh_bytes).await?;
        let signed_recipient = sign_value(&enc_for_recipient, pair).await?;

        // 4a. Store recipient copy
        let mut grant_node = db
            .get(&format!("~{}", pair.pub_key))
            .get("grant")
            .get(&path_key)
            .get(recipient_pubkey);
        let _ = grant_node.put(signed_recipient).await;

        // 4b. Store owner backup (self-encrypted)
        let enc_for_owner = encrypt(&json!(sec), pair, None).await?;
        let signed_owner = sign_value(&enc_for_owner, pair).await?;
        let mut owner_grant_node = db
            .get(&format!("~{}", pair.pub_key))
            .get("grant")
            .get(&path_key)
            .get(&pair.pub_key);
        let _ = owner_grant_node.put(signed_owner).await;

        Ok(())
    }

    /// Encrypt and store data under a self-derived symmetric key.
    /// Only this user can decrypt it — the key comes from ECDH(self.pub, self.priv).
    /// Stored at `~{pub}/secret/{path_key}` as a signed payload.
    pub async fn secret(
        &self,
        data: &JsonValue,
        path: &str,
        db: &mut Node,
    ) -> Result<(), SeaError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| SeaError::SessionStorage("lock poisoned".to_string()))?;
        if !inner.is_authenticated {
            return Err(SeaError::NotAuthenticated);
        }
        let pair = &inner.pair;
        let path_key = encode_path(path);
        let user_root = format!("~{}", pair.pub_key);

        // Derive self-symmetric key: ECDH with own ephemeral public key
        let epub = pair
            .epub_key
            .as_ref()
            .ok_or_else(|| SeaError::Crypto("no epub".into()))?;
        let dh = secret(epub, pair).await?;
        let dh_bytes = BASE64_URL_SAFE_NO_PAD
            .decode(&dh)
            .map_err(|_| SeaError::Crypto("bad dh".into()))?;

        // Encrypt and sign
        let enc = encrypt_symmetric(data, &dh_bytes).await?;
        let signed = sign_value(&enc, pair).await?;

        let mut secret_node = db.get(&user_root).get("secret").get(&path_key);
        let _ = secret_node.put(signed).await;

        Ok(())
    }
}

/// Verify a trust certificate from the graph.
/// Returns true if `writer_pubkey` is certified by `authority_pubkey` for the given path.
pub async fn verify_trust(
    authority_pubkey: &str,
    writer_pubkey: &str,
    path: Option<&str>,
    db: &mut Node,
) -> Result<bool, SeaError> {
    let path_key = path
        .map(encode_path)
        .unwrap_or_else(|| "global".to_string());
    let mut trust_node = db
        .get(&format!("~{}", authority_pubkey))
        .get("trust")
        .get(&path_key);

    let cert_text = match trust_node.once(None).await {
        Some(BeamValue::Text(t)) => t,
        _ => return Ok(false),
    };

    let cert: JsonValue =
        serde_json::from_str(&cert_text).map_err(|_| SeaError::VerificationFailed)?;

    let payload =
        verify_certificate(&cert, authority_pubkey).map_err(|_| SeaError::VerificationFailed)?;

    Ok(is_pubkey_certified(&payload, writer_pubkey))
}

/// Accept a grant and return the shared decryption secret for a data path.
pub async fn accept_grant(
    data_path: &str,
    owner_pubkey: &str,
    owner_epub: &str,
    pair: &KeyPair,
    db: &mut Node,
) -> Result<String, SeaError> {
    let path_key = encode_path(data_path);
    let mut grant_node = db
        .get(&format!("~{}", owner_pubkey))
        .get("grant")
        .get(&path_key)
        .get(&pair.pub_key);

    let enc_text = grant_node
        .once(None)
        .await
        .and_then(|v| match v {
            BeamValue::Text(t) => Some(t),
            _ => None,
        })
        .ok_or_else(|| SeaError::Decryption("no grant found".into()))?;

    let outer: JsonValue = serde_json::from_str(&enc_text)
        .map_err(|e| SeaError::Decryption(format!("bad grant json: {}", e)))?;
    let enc_json = if outer.get("m").is_some() && outer.get("s").is_some() {
        let msg = outer["m"]
            .as_str()
            .ok_or_else(|| SeaError::Decryption("m not string".into()))?;
        serde_json::from_str(msg).map_err(|e| SeaError::Decryption(format!("bad m: {}", e)))?
    } else {
        outer
    };

    let dh = secret(owner_epub, pair).await?;
    let dh_bytes = BASE64_URL_SAFE_NO_PAD
        .decode(&dh)
        .map_err(|_| SeaError::Decryption("bad dh".into()))?;

    let sec_json = decrypt_symmetric(&enc_json, &dh_bytes).await?;
    let sec = sec_json
        .as_str()
        .ok_or_else(|| SeaError::Decryption("secret not string".into()))?;
    Ok(sec.to_string())
}
