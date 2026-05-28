#![allow(deprecated)]
//! AES-GCM decryption
//! Reverse of encrypt.rs

use super::{KeyPair, SeaError};
use sha2::{Digest, Sha256};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use serde_json::Value;

/// Decrypt data using AES-256-GCM
///
/// Parses {ct: ciphertext, iv: nonce, s: salt} format.
/// Shared decrypt: their_epub = Some
/// Self decrypt: their_epub = None
pub async fn decrypt(
    encrypted: &Value,
    pair: &KeyPair,
    their_epub: Option<&str>,
) -> Result<Value, SeaError> {
    // Extract fields
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

    // Decode from base64
    let ciphertext = base64::decode_config(ct, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("invalid ct base64".to_string()))?;

    let nonce_bytes = base64::decode_config(iv, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("invalid iv base64".to_string()))?;

    let salt_bytes = base64::decode_config(s, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("invalid s base64".to_string()))?;

    // Clone data for spawn_blocking closure
    let pair = pair.clone();
    let their_epub = their_epub.map(|s| s.to_string());
    let salt_owned = salt_bytes;
    let nonce_owned = nonce_bytes;

    // Run PBKDF2 + AES-GCM in spawn_blocking
    let plaintext = tokio::task::spawn_blocking(move || {
        // Derive AES key
        let aes_key = if let Some(ref their_pub) = their_epub {
            // Shared decryption: ECDH → PBKDF2
            let shared_secret = super::secret::secret_sync(their_pub, &pair)?;
            derive_aes_key_sync(&shared_secret, &salt_owned)?
        } else {
            // Self decryption: epriv directly
            let epriv = pair
                .epriv_key
                .as_ref()
                .ok_or_else(|| SeaError::Decryption("missing epriv key".to_string()))?;
            derive_aes_key_sync(epriv, &salt_owned)?
        };

        // Create AES-GCM cipher
        let cipher = Aes256Gcm::new_from_slice(&aes_key)
            .map_err(|e| SeaError::Decryption(format!("failed to create cipher: {}", e)))?;

        // Create nonce from IV
        let nonce = Nonce::from_slice(&nonce_owned);

        // Decrypt
        let plaintext = cipher
            .decrypt(nonce, ciphertext.as_ref())
            .map_err(|_| SeaError::Decryption("decryption failed — tampered or wrong key".to_string()))?;

        Ok::<Vec<u8>, SeaError>(plaintext)
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;

    let plaintext = plaintext?;

    // Parse JSON
    let plaintext_str = String::from_utf8(plaintext)
        .map_err(|_| SeaError::Decryption("invalid UTF-8 in plaintext".to_string()))?;

    serde_json::from_str(&plaintext_str)
        .map_err(|e| SeaError::Decryption(format!("invalid JSON in plaintext: {}", e)))
}

/// Derive AES-256 key from secret material + salt via SHA-256 (synchronous)
/// Matches Gun.js aeskey.js: SHA-256(key_string + salt_bytes.toString('utf8'))
fn derive_aes_key_sync(secret_b64: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
    let salt_str = String::from_utf8_lossy(salt);
    let combo = format!("{}{}", secret_b64, salt_str);
    let hash = Sha256::digest(combo.as_bytes());
    Ok(hash.to_vec())
}

/// Decrypt data using a raw symmetric key (AES-256-GCM, no ECDH/PBKDF2)
///
/// # Requirements
/// * `key` must be exactly 32 bytes (AES-256 key size)
/// * `encrypted` must be in `{ct, iv}` format (no `s` field, as no PBKDF2 was used)
///
/// Use this when the key material is already derived via ECDH or another KDF.
pub async fn decrypt_symmetric(encrypted: &Value, key: &[u8]) -> Result<Value, SeaError> {
    if key.len() != 32 {
        return Err(SeaError::Decryption(format!(
            "decrypt_symmetric: key must be 32 bytes, got {}",
            key.len()
        )));
    }

    let ct = encrypted
        .get("ct")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SeaError::Decryption("missing ct".to_string()))?;

    let iv = encrypted
        .get("iv")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SeaError::Decryption("missing iv".to_string()))?;

    let ciphertext = base64::decode_config(ct, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("invalid ct base64".to_string()))?;

    let nonce_bytes = base64::decode_config(iv, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::Decryption("invalid iv base64".to_string()))?;

    let key_owned = key.to_vec();
    let nonce_owned = nonce_bytes;

    let plaintext = tokio::task::spawn_blocking(move || {
        let cipher = Aes256Gcm::new_from_slice(&key_owned)
            .map_err(|e| SeaError::Decryption(format!("failed to create cipher: {}", e)))?;

        let nonce = Nonce::from_slice(&nonce_owned);

        let plaintext = cipher
            .decrypt(nonce, ciphertext.as_ref())
            .map_err(|_| SeaError::Decryption("symmetric decryption failed — tampered or wrong key".to_string()))?;

        Ok::<Vec<u8>, SeaError>(plaintext)
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;

    let plaintext = plaintext?;

    let plaintext_str = String::from_utf8(plaintext)
        .map_err(|_| SeaError::Decryption("invalid UTF-8 in plaintext".to_string()))?;

    serde_json::from_str(&plaintext_str)
        .map_err(|e| SeaError::Decryption(format!("invalid JSON in plaintext: {}", e)))
}
