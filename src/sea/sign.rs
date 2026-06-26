//! Digital signatures
//! Based on Gun.js sea/sign.js
//! ECDSA P-256 signing

use super::{KeyPair, SeaError};
use p256::ecdsa::{Signature, SigningKey, signature::Signer};
use serde_json::Value;
use std::convert::TryInto;

/// Sign data with a key pair
/// Returns signed data in format: {m: message, s: signature}
///
/// The message is JSON-serialized and signed using ECDSA P-256 with SHA-256.
/// Signing runs in tokio::task::spawn_blocking so the async executor is not blocked.
pub async fn sign(data: &Value, pair: &KeyPair) -> Result<Value, SeaError> {
    let data = data.clone();
    let priv_key = pair.priv_key.clone();

    tokio::task::spawn_blocking(move || {
        // Serialize data to JSON string
        let message = serde_json::to_string(&data)
            .map_err(|e| SeaError::Crypto(format!("serialization error: {}", e)))?;

        // Decode private key from base64
        let priv_bytes = base64::decode_config(&priv_key, base64::STANDARD_NO_PAD)
            .map_err(|_| SeaError::InvalidKey)?;

        if priv_bytes.len() != 32 {
            return Err(SeaError::InvalidKey);
        }

        let priv_array: [u8; 32] = priv_bytes.try_into().map_err(|_| SeaError::InvalidKey)?;

        // Create signing key from private key bytes
        let signing_key = SigningKey::from_bytes(&priv_array.into())
            .map_err(|e| SeaError::Crypto(format!("invalid private key: {}", e)))?;

        // Sign the message (ECDSA P-256 with internal SHA-256 hashing)
        let signature: Signature = signing_key.sign(message.as_bytes());
        let sig_bytes = signature.to_bytes();

        // Encode signature as base64 (r||s, 64 bytes)
        let sig_b64 = base64::encode_config(&sig_bytes[..], base64::STANDARD_NO_PAD);

        // Return in Gun.js format: {m: message, s: signature}
        Ok(serde_json::json!({
            "m": message,
            "s": sig_b64
        }))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}
