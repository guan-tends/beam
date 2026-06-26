//! Proof of Work / Content Hashing
//! Based on Gun.js sea/work.js
//! PBKDF2 key derivation and SHA-256 hashing

use super::{SeaError, WorkOptions};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Compute proof-of-work or content hash
///
/// # PBKDF2 Mode (default)
/// - Algorithm: PBKDF2-HMAC-SHA256
/// - Iterations: 100,000
/// - Salt: random 9 bytes if not provided
/// - Output: base64-encoded derived key
///
/// # SHA-256 Mode
/// - Triggered when opt.name starts with "sha" (case-insensitive)
/// - Direct SHA-256 hash of input data
/// - Output: base64-encoded hash
pub async fn work(data: &[u8], salt: Option<&[u8]>, opts: WorkOptions) -> Result<String, SeaError> {
    let opts = Arc::new(opts);
    let data = data.to_vec();

    // Check if SHA-256 mode
    let name_lower = opts
        .name
        .as_ref()
        .map(|n| n.to_lowercase())
        .unwrap_or_else(|| "pbkdf2".to_string());

    if name_lower.starts_with("sha") {
        // SHA-256 hashing mode
        return tokio::task::spawn_blocking(move || {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let hash = hasher.finalize();
            let encoded = base64::encode_config(&hash[..], base64::STANDARD_NO_PAD);
            Ok(encoded)
        })
        .await
        .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;
    }

    // PBKDF2 key derivation mode (default)
    let salt = if let Some(s) = salt {
        s.to_vec()
    } else if let Some(ref opt_salt) = opts.salt {
        opt_salt.clone()
    } else {
        // Generate random 9-byte salt (matching Gun.js)
        let mut salt_bytes = vec![0u8; 9];
        rand::thread_rng().fill_bytes(&mut salt_bytes);
        salt_bytes
    };

    let iterations = opts.iterations.unwrap_or(100_000);
    let length_bits = opts.length.unwrap_or(512);
    let length_bytes = length_bits / 8;

    // Perform PBKDF2 in blocking task (CPU-intensive)
    let result = tokio::task::spawn_blocking(move || {
        let mut output = vec![0u8; length_bytes];
        pbkdf2_hmac::<Sha256>(&data, &salt, iterations, &mut output);
        output
    })
    .await
    .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?;

    // Encode result as base64
    let encoded = base64::encode_config(&result, base64::STANDARD_NO_PAD);
    Ok(encoded)
}
