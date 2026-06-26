//! Signature verification — Gun.js `sea/verify.js` equivalent.
//!
//! Verifies ECDSA P-256 signatures produced by [`crate::sea::sign::sign`].
//! The signed data is in Gun.js format: `{"m": message, "s": signature}`.
//!
//! # Functions
//!
//! - [`verify_sync`] — synchronous verification (use from non-async contexts)
//! - [`verify_async`] — async wrapper via `spawn_blocking` (preferred in async code)
//!
//! # Example
//!
//! ```no_run
//! use rod::sea::{generate_pair, sign, verify_sync};
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let pair = generate_pair().await.unwrap();
//! let data = serde_json::json!({"hello": "world"});
//! let signed = sign(&data, &pair).await.unwrap();
//! let verified = verify_sync(&signed, &pair.pub_key).unwrap();
//! assert_eq!(verified, data);
//! # });
//! ```

use super::SeaError;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier};
use serde_json::Value;
use std::convert::TryInto;

/// Verify a signature synchronously.
///
/// Takes signed data in Gun.js format `{"m": message, "s": signature}` and
/// a public key in `"x.y"` base64 format. If the signature is valid, returns
/// the parsed message as a [`serde_json::Value`].
///
/// # Arguments
///
/// * `signed_data` — JSON object with `m` (message string) and `s` (signature string)
/// * `pub_key` — Public key in `"x.y"` base64 format
///
/// # Errors
///
/// - [`SeaError::VerificationFailed`] — signature is missing, malformed, or invalid
/// - [`SeaError::InvalidKey`] — public key is malformed
/// - [`SeaError::Crypto`] — message is not valid JSON
///
/// # Security
///
/// This function performs constant-time signature verification via the `p256`
/// crate. The verification is not susceptible to timing attacks.
pub fn verify_sync(signed_data: &Value, pub_key: &str) -> Result<Value, SeaError> {
    let message = signed_data
        .get("m")
        .and_then(|v| v.as_str())
        .ok_or(SeaError::VerificationFailed)?;

    let signature = signed_data
        .get("s")
        .and_then(|v| v.as_str())
        .ok_or(SeaError::VerificationFailed)?;

    // Parse public key (format: x.y)
    let parts: Vec<&str> = pub_key.split('.').collect();
    if parts.len() != 2 {
        return Err(SeaError::InvalidKey);
    }

    let x = base64::decode_config(parts[0], base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::InvalidKey)?;
    let y = base64::decode_config(parts[1], base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::InvalidKey)?;

    // Reconstruct uncompressed public key (0x04 || x || y)
    let mut pub_bytes: Vec<u8> = Vec::with_capacity(65);
    pub_bytes.push(0x04);
    pub_bytes.extend_from_slice(&x);
    pub_bytes.extend_from_slice(&y);

    let verifying_key = VerifyingKey::from_sec1_bytes(&pub_bytes)
        .map_err(|e| SeaError::Crypto(format!("invalid public key: {}", e)))?;

    // Decode signature (r||s, 64 bytes)
    let sig_bytes = base64::decode_config(signature, base64::STANDARD_NO_PAD)
        .map_err(|_| SeaError::VerificationFailed)?;

    if sig_bytes.len() != 64 {
        return Err(SeaError::VerificationFailed);
    }

    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| SeaError::VerificationFailed)?;

    let signature = Signature::from_slice(&sig_array).map_err(|_| SeaError::VerificationFailed)?;

    // Verify (uses ECDSA with internal SHA-256 hashing)
    verifying_key
        .verify(message.as_bytes(), &signature)
        .map_err(|_| SeaError::VerificationFailed)?;

    // Parse and return the message
    serde_json::from_str(message).map_err(|e| SeaError::Crypto(format!("invalid JSON: {}", e)))
}

/// Verify a signature asynchronously via `spawn_blocking`.
///
/// This is the preferred function for async contexts — it offloads the
/// CPU-intensive verification to a blocking thread pool.
///
/// See [`verify_sync`] for argument and error documentation.
pub async fn verify_async(signed_data: &Value, pub_key: &str) -> Result<Value, SeaError> {
    let data = signed_data.clone();
    let key = pub_key.to_string();
    tokio::task::spawn_blocking(move || verify_sync(&data, &key))
        .await
        .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::{generate_pair, sign};
    use serde_json::json;

    #[tokio::test]
    async fn test_verify_sync_valid() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"hello": "world"});
        let signed = sign(&data, &pair).await.unwrap();
        let verified = verify_sync(&signed, &pair.pub_key).unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_verify_sync_wrong_key() {
        let pair_a = generate_pair().await.unwrap();
        let pair_b = generate_pair().await.unwrap();
        let data = json!({"secret": "data"});
        let signed = sign(&data, &pair_a).await.unwrap();
        assert!(verify_sync(&signed, &pair_b.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_verify_async_valid() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"async": true});
        let signed = sign(&data, &pair).await.unwrap();
        let verified = verify_async(&signed, &pair.pub_key).await.unwrap();
        assert_eq!(verified, data);
    }

    #[tokio::test]
    async fn test_verify_tampered_message() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"original": true});
        let mut signed = sign(&data, &pair).await.unwrap();
        // Tamper with the message — replace m with a different payload
        let tampered = serde_json::to_string(&json!({"tampered": true})).unwrap();
        signed["m"] = json!(tampered);
        assert!(verify_sync(&signed, &pair.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_verify_missing_m_field() {
        let pair = generate_pair().await.unwrap();
        let bad = json!({"s": "some_sig"});
        assert!(verify_sync(&bad, &pair.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_verify_missing_s_field() {
        let pair = generate_pair().await.unwrap();
        let bad = json!({"m": "some_msg"});
        assert!(verify_sync(&bad, &pair.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_verify_malformed_pub_key() {
        let pair = generate_pair().await.unwrap();
        let data = json!({"test": 1});
        let signed = sign(&data, &pair).await.unwrap();
        // No dot in pub key
        assert!(verify_sync(&signed, "invalidkey").is_err());
    }

    #[tokio::test]
    async fn test_verify_short_signature() {
        let pair = generate_pair().await.unwrap();
        let signed = json!({
            "m": "{}",
            "s": "short"
        });
        assert!(verify_sync(&signed, &pair.pub_key).is_err());
    }
}
