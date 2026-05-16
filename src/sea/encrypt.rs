//! AES-GCM encryption
#![allow(deprecated)]
//! Based on Gun.js sea/encrypt.js
//! Uses ECDH-derived key + PBKDF2 + AES-256-GCM

use super::{KeyPair, SeaError};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use rand::RngCore;
use serde_json::Value;

/// Encrypt data using AES-256-GCM
///
/// # Shared encryption (their_epub = Some)
/// ECDH shared secret → PBKDF2 → AES-256-GCM key
///
/// # Self encryption (their_epub = None)
/// epriv directly → PBKDF2 → AES-256-GCM key
///
/// Returns {ct: ciphertext, iv: nonce, s: salt} (all base64)
pub async fn encrypt(
    data: &Value,
    pair: &KeyPair,
    their_epub: Option<&str>,
) -> Result<Value, SeaError> {
    // Serialize data to string
    let msg = serde_json::to_string(data)
        .map_err(|e| SeaError::Encryption(format!("serialization error: {}", e)))?;

    // Generate random salt (9 bytes matching Gun.js) and nonce (12 bytes for AES-GCM)
    let mut salt_bytes = [0u8; 9];
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut salt_bytes);
    rand::thread_rng().fill_bytes(&mut nonce_bytes);

    // Derive AES key
    let aes_key = if let Some(their_pub) = their_epub {
        // Shared encryption: ECDH → PBKDF2
        let _our_epriv = pair
            .epriv_key
            .as_ref()
            .ok_or_else(|| SeaError::Encryption("missing epriv key".to_string()))?;

        // Get shared secret via ECDH
        let shared_secret = super::secret::secret(their_pub, pair).await?;

        derive_aes_key(&shared_secret, &salt_bytes).await?
    } else {
        // Self encryption: epriv directly as PBKDF2 input
        let epriv = pair
            .epriv_key
            .as_ref()
            .ok_or_else(|| SeaError::Encryption("missing epriv key".to_string()))?;

        derive_aes_key(epriv, &salt_bytes).await?
    };

    // Create AES-GCM cipher
    let cipher = Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| SeaError::Encryption(format!("failed to create cipher: {}", e)))?;

    // Create nonce from IV
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Encrypt
    let ciphertext = cipher
        .encrypt(nonce, msg.as_bytes())
        .map_err(|e| SeaError::Encryption(format!("encryption failed: {}", e)))?;

    // Encode everything as base64
    let ct_b64 = base64::encode_config(&ciphertext, base64::STANDARD_NO_PAD);
    let iv_b64 = base64::encode_config(&nonce_bytes, base64::STANDARD_NO_PAD);
    let s_b64 = base64::encode_config(&salt_bytes, base64::STANDARD_NO_PAD);

    // Return in Gun.js format
    Ok(serde_json::json!({
        "ct": ct_b64,
        "iv": iv_b64,
        "s": s_b64
    }))
}

/// Derive AES-256 key from secret material + salt via PBKDF2
async fn derive_aes_key(secret_b64: &str, salt: &[u8]) -> Result<Vec<u8>, SeaError> {
    let secret_bytes = base64::decode_config(secret_b64, base64::STANDARD_NO_PAD)
        .unwrap_or_else(|_| secret_b64.as_bytes().to_vec());

    let mut key = vec![0u8; 32]; // AES-256 key size
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(&secret_bytes, salt, 100_000, &mut key);

    Ok(key)
}
