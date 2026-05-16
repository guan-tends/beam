//! Signature verification
//! Based on Gun.js sea/verify.js
//! ECDSA P-256 verification

use super::SeaError;
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use serde_json::Value;
use std::convert::TryInto;

/// Verify a signature
/// Returns the verified message data if valid
///
/// Takes signed data in format {m: message, s: signature} and a public key (x.y)
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

    let signature = Signature::from_slice(&sig_array)
        .map_err(|_| SeaError::VerificationFailed)?;

    // Verify (uses ECDSA with internal SHA-256 hashing)
    verifying_key
        .verify(message.as_bytes(), &signature)
        .map_err(|_| SeaError::VerificationFailed)?;

    // Parse and return the message
    serde_json::from_str(message)
        .map_err(|e| SeaError::Crypto(format!("invalid JSON: {}", e)))
}
