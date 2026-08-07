//! AES-GCM encryption
#![allow(deprecated)]
//! Based on Gun.js sea/encrypt.js
//! Uses ECDH-derived key + SHA-256 + AES-256-GCM
//! Matches Gun.js aeskey.js semantics: SHA-256(key_string + salt_bytes)

use super::{KeyPair, SeaError};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};
use base64::prelude::*;

/// Encrypt data using AES-256-GCM
///
/// # Shared encryption (their_epub = Some)
/// ECDH shared secret → SHA-256(key_string + salt) → AES-256-GCM key
///
/// # Self encryption (their_epub = None)
/// epriv directly → SHA-256(key_string + salt) → AES-256-GCM key
///
/// Returns {ct: ciphertext, iv: nonce, s: salt} (all base64)
pub async fn encrypt(
    data: &Value,
    pair: &KeyPair,
    their_epub: Option<&str>,
) -> Result<Value, SeaError> {
    // Serialize data to string (IO-bound, safe in async)
    let msg = serde_json::to_string(data)
        .map_err(|e| SeaError::Encryption(format!("serialization error: {}", e)))?;

    // Generate random salt (9 bytes matching Gun.js) and nonce (12 bytes for AES-GCM)
    let mut salt_bytes = [0u8; 9];
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut salt_bytes);
    rand::thread_rng().fill_bytes(&mut nonce_bytes);

    // Clone data needed inside blocking closure
    let pair = pair.clone();
    let their_epub = their_epub.map(|s| s.to_string());
    let salt_owned = salt_bytes.to_vec();
    let nonce_owned = nonce_bytes.to_vec();

    // Run SHA-256 KDF + AES-GCM in spawn_blocking to avoid blocking the async executor
    let result = tokio::task::spawn_blocking(move || {
        // Derive AES key
        let aes_key = if let Some(ref their_pub) = their_epub {
            // Shared encryption: ECDH → SHA-256
            let shared_secret = super::secret::secret_sync(their_pub, &pair)?;
            derive_aes_key_sync(&shared_secret, &salt_owned)?
        } else {
            // Self encryption: epriv directly as SHA-256 input
            let epriv = pair
                .epriv_key
                .as_ref()
                .ok_or_else(|| SeaError::Encryption("missing epriv key".to_string()))?;
            derive_aes_key_sync(epriv, &salt_owned)?
        };

        // Create AES-GCM cipher
        let cipher = Aes256Gcm::new_from_slice(&aes_key)
            .map_err(|e| SeaError::Encryption(format!("failed to create cipher: {}", e)))?;

        // Create nonce from IV
        let nonce = Nonce::from_slice(&nonce_owned);

        // Encrypt
        let ciphertext = cipher
            .encrypt(nonce, msg.as_bytes())
            .map_err(|e| SeaError::Encryption(format!("encryption failed: {}", e)))?;

        // Encode everything as base64
        let ct_b64 = BASE64_URL_SAFE_NO_PAD.encode(&ciphertext);
        let iv_b64 = BASE64_URL_SAFE_NO_PAD.encode(&nonce_owned);
        let s_b64 = BASE64_URL_SAFE_NO_PAD.encode(&salt_owned);

        Ok::<(String, String, String), SeaError>((ct_b64, iv_b64, s_b64))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;

    let (ct_b64, iv_b64, s_b64) = result?;

    // Return in Gun.js format
    Ok(serde_json::json!({
        "ct": ct_b64,
        "iv": iv_b64,
        "s": s_b64
    }))
}

/// Derive AES-256 key from secret material + salt via SHA-256 (synchronous).
///
/// Matches Gun.js `aeskey.js`: `SHA-256(key_string + salt_bytes.toString('utf8'))`.
///
/// This is the shared KDF used by [`encrypt`], [`crate::sea::decrypt::decrypt`],
/// and [`crate::sea::user`] passphrase-based encryption.
pub(crate) fn derive_aes_key_sync(secret_b64: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
    let salt_str = String::from_utf8_lossy(salt);
    let combo = format!("{}{}", secret_b64, salt_str);
    let hash = Sha256::digest(combo.as_bytes());
    Ok(hash.to_vec())
}

/// Encrypt data using a raw symmetric key (AES-256-GCM, no ECDH/PBKDF2)
///
/// # Requirements
/// * `key` must be exactly 32 bytes (AES-256 key size)
///
/// # Returns
/// `{ct: ciphertext_b64, iv: nonce_b64}` — salt (`s`) is omitted because PBKDF2 is not used.
///
/// Use this when the key material is already derived via ECDH or another KDF.
pub async fn encrypt_symmetric(data: &Value, key: &[u8]) -> Result<Value, SeaError> {
    if key.len() != 32 {
        return Err(SeaError::Encryption(format!(
            "encrypt_symmetric: key must be 32 bytes, got {}",
            key.len()
        )));
    }

    let msg = serde_json::to_string(data)
        .map_err(|e| SeaError::Encryption(format!("serialization error: {}", e)))?;

    // Generate random nonce (12 bytes for AES-GCM)
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);

    let key_owned = key.to_vec();
    let nonce_owned = nonce_bytes.to_vec();

    let result = tokio::task::spawn_blocking(move || {
        let cipher = Aes256Gcm::new_from_slice(&key_owned)
            .map_err(|e| SeaError::Encryption(format!("failed to create cipher: {}", e)))?;

        let nonce = Nonce::from_slice(&nonce_owned);

        let ciphertext = cipher
            .encrypt(nonce, msg.as_bytes())
            .map_err(|e| SeaError::Encryption(format!("symmetric encryption failed: {}", e)))?;

        let ct_b64 = BASE64_URL_SAFE_NO_PAD.encode(&ciphertext);
        let iv_b64 = BASE64_URL_SAFE_NO_PAD.encode(&nonce_owned);

        Ok::<(String, String), SeaError>((ct_b64, iv_b64))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;

    let (ct_b64, iv_b64) = result?;

    Ok(serde_json::json!({
        "ct": ct_b64,
        "iv": iv_b64
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::generate_pair;
    use serde_json::json;

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"message": "hello"});
        let encrypted = encrypt(&data, &pair, None).await.unwrap();
        let decrypted = super::super::decrypt::decrypt(&encrypted, &pair, None)
            .await
            .unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_encrypt_with_their_epub() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let data = json!("secret data");
        let encrypted = encrypt(&data, &alice, bob.epub_key.as_deref())
            .await
            .unwrap();
        let decrypted = super::super::decrypt::decrypt(&encrypted, &bob, alice.epub_key.as_deref())
            .await
            .unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_encrypt_symmetric_roundtrip() {
        let key = [0x42u8; 32];
        let data = json!({"key": "value"});
        let encrypted = encrypt_symmetric(&data, &key).await.unwrap();
        let decrypted = super::super::decrypt::decrypt_symmetric(&encrypted, &key)
            .await
            .unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn test_encrypt_symmetric_wrong_key_fails() {
        let key = [0x42u8; 32];
        let wrong_key = [0x99u8; 32];
        let data = json!("secret");
        let encrypted = encrypt_symmetric(&data, &key).await.unwrap();
        assert!(
            super::super::decrypt::decrypt_symmetric(&encrypted, &wrong_key)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_encrypt_symmetric_bad_key_length() {
        let key = [0u8; 16]; // too short
        let data = json!("test");
        assert!(encrypt_symmetric(&data, &key).await.is_err());
    }

    #[test]
    fn test_derive_aes_key_sync_deterministic() {
        let salt = b"salt123";
        let key1 = derive_aes_key_sync("secret", salt).unwrap();
        let key2 = derive_aes_key_sync("secret", salt).unwrap();
        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 32); // SHA-256 = 32 bytes
    }

    #[test]
    fn test_derive_aes_key_sync_different_inputs() {
        let salt = b"salt";
        let key1 = derive_aes_key_sync("secret1", salt).unwrap();
        let key2 = derive_aes_key_sync("secret2", salt).unwrap();
        assert_ne!(key1, key2);
    }
}
