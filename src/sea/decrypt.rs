#![allow(deprecated)]
//! AES-GCM decryption
//! Reverse of encrypt.rs

use super::{KeyPair, SeaError};
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

    // Derive AES key
    let aes_key = if let Some(their_pub) = their_epub {
        // Shared decryption: ECDH → PBKDF2
        let shared_secret = super::secret::secret(their_pub, pair).await?;
        derive_aes_key(&shared_secret, &salt_bytes).await?
    } else {
        // Self decryption: epriv directly
        let epriv = pair
            .epriv_key
            .as_ref()
            .ok_or_else(|| SeaError::Decryption("missing epriv key".to_string()))?;
        derive_aes_key(epriv, &salt_bytes).await?
    };

    // Create AES-GCM cipher
    let cipher = Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| SeaError::Decryption(format!("failed to create cipher: {}", e)))?;

    // Create nonce from IV
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Decrypt
    let plaintext = cipher
        .decrypt(nonce, ciphertext.as_ref())
        .map_err(|_| SeaError::Decryption("decryption failed — tampered or wrong key".to_string()))?;

    // Parse JSON
    let plaintext_str = String::from_utf8(plaintext)
        .map_err(|_| SeaError::Decryption("invalid UTF-8 in plaintext".to_string()))?;

    serde_json::from_str(&plaintext_str)
        .map_err(|e| SeaError::Decryption(format!("invalid JSON in plaintext: {}", e)))
}

/// Derive AES-256 key from secret material + salt via PBKDF2
async fn derive_aes_key(secret_b64: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
    let secret_bytes = base64::decode_config(secret_b64, base64::STANDARD_NO_PAD)
        .unwrap_or_else(|_| secret_b64.as_bytes().to_vec());

    let mut key = vec![0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(&secret_bytes, salt, 100_000, &mut key);

    Ok(key)
}
