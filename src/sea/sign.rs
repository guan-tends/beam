//! Digital signatures — Gun.js `sea/sign.js` equivalent.
//!
//! Signs JSON data using ECDSA P-256 with SHA-256, returning the Gun.js
//! wire format: `{"m": message, "s": signature}` where:
//!
//! - `m` is the JSON-serialized message
//! - `s` is the base64-encoded 64-byte signature (r || s)
//!
//! # Blocking
//!
//! Signing is CPU-intensive and runs via [`tokio::task::spawn_blocking`]
//! so the async executor is not blocked.
//!
//! # Example
//!
//! ```no_run
//! use beam::sea::{generate_pair, sign};
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let pair = generate_pair().await.unwrap();
//! let data = serde_json::json!({"hello": "world"});
//! let signed = sign(&data, &pair).await.unwrap();
//! assert!(signed.get("m").is_some());
//! assert!(signed.get("s").is_some());
//! # });
//! ```

use super::{KeyPair, SeaError};
use p256::ecdsa::{Signature, SigningKey, signature::Signer};
use serde_json::Value;
use std::convert::TryInto;
use base64::prelude::*;

/// Sign JSON data with a key pair's private key.
///
/// The data is JSON-serialized, then signed using ECDSA P-256 with
/// SHA-256. The signature is returned in Gun.js format:
/// `{"m": message, "s": base64_signature}`.
///
/// # Arguments
///
/// * `data` — JSON value to sign (will be cloned and serialized)
/// * `pair` — Key pair containing the private key
///
/// # Errors
///
/// - [`SeaError::InvalidKey`] — private key is not 32 bytes or malformed
/// - [`SeaError::Crypto`] — serialization or signing error
pub async fn sign(data: &Value, pair: &KeyPair) -> Result<Value, SeaError> {
    let data = data.clone();
    let priv_key = pair.priv_key.clone();

    tokio::task::spawn_blocking(move || {
        // Serialize data to JSON string
        let message = serde_json::to_string(&data)
            .map_err(|e| SeaError::Crypto(format!("serialization error: {}", e)))?;

        // Decode private key from base64
        let priv_bytes = BASE64_URL_SAFE_NO_PAD.decode(&priv_key)
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
        let sig_b64 = BASE64_URL_SAFE_NO_PAD.encode(&sig_bytes[..]);

        // Return in Gun.js format: {m: message, s: signature}
        Ok(serde_json::json!({
            "m": message,
            "s": sig_b64
        }))
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::{generate_pair, verify_sync};
    use serde_json::json;

    #[tokio::test]
    async fn test_sign_basic() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"hello": "world"});
        let signed = sign(&data, &pair).await.unwrap();

        assert!(
            signed.get("m").is_some(),
            "signed output should have 'm' field"
        );
        assert!(
            signed.get("s").is_some(),
            "signed output should have 's' field"
        );
    }

    #[tokio::test]
    async fn test_sign_verify_roundtrip() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"message": "test", "count": 42});
        let signed = sign(&data, &pair).await.unwrap();

        // Verify against the pub key
        let verified = verify_sync(&signed, &pair.pub_key).unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_sign_empty_object() {
        let pair = generate_pair().await.unwrap();
        let data = json!({});
        let signed = sign(&data, &pair).await.unwrap();
        let verified = verify_sync(&signed, &pair.pub_key).unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_sign_wrong_key_fails_verification() {
        let pair_a = generate_pair().await.unwrap();
        let pair_b = generate_pair().await.unwrap();
        let data = json!({"secret": "data"});
        let signed = sign(&data, &pair_a).await.unwrap();

        // Verify with wrong pub key should fail
        assert!(verify_sync(&signed, &pair_b.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_sign_invalid_priv_key() {
        let pair = generate_pair().await.unwrap();
        let mut bad_pair = pair.clone();
        bad_pair.priv_key = "invalid_base64".to_string();
        let data = json!({"test": 1});
        assert!(sign(&data, &bad_pair).await.is_err());
    }

    #[tokio::test]
    async fn test_sign_wrong_length_priv_key() {
        let pair = generate_pair().await.unwrap();
        let mut bad_pair = pair.clone();
        // 16 bytes instead of 32
        bad_pair.priv_key = BASE64_URL_SAFE_NO_PAD.encode([0u8; 16]);
        let data = json!({"test": 1});
        assert!(sign(&data, &bad_pair).await.is_err());
    }
}
