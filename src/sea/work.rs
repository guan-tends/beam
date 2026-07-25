//! Proof of Work and content hashing — Gun.js `sea/work.js` equivalent.
//!
//! Provides two modes of cryptographic hashing:
//!
//! - **PBKDF2 mode** (default): Key derivation using PBKDF2-HMAC-SHA256
//!   with 100,000 iterations and a random 9-byte salt (matching Gun.js).
//!   Used for password hashing and key derivation.
//!
//! - **SHA-256 mode**: Direct SHA-256 hash of input data. Triggered when
//!   `WorkOptions::name` starts with `"sha"` (case-insensitive).
//!
//! # Blocking
//!
//! PBKDF2 is CPU-intensive and runs via [`tokio::task::spawn_blocking`].
//!
//! # Example
//!
//! ```no_run
//! use beam::sea::{work, WorkOptions};
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let hash = work(b"password", None, WorkOptions::default()).await.unwrap();
//! assert!(!hash.is_empty());
//! # });
//! ```

use super::{SeaError, WorkOptions};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Compute proof-of-work or content hash.
///
/// # PBKDF2 Mode (default)
///
/// - Algorithm: PBKDF2-HMAC-SHA256
/// - Iterations: 100,000 (configurable via `WorkOptions::iterations`)
/// - Salt: random 9 bytes if not provided (matching Gun.js)
/// - Output: base64-encoded derived key
///
/// # SHA-256 Mode
///
/// - Triggered when `WorkOptions::name` starts with `"sha"` (case-insensitive)
/// - Direct SHA-256 hash of input data
/// - Output: base64-encoded hash
///
/// # Arguments
///
/// * `data` — Input bytes to hash
/// * `salt` — Optional salt (PBKDF2 mode only). If `None`, uses `WorkOptions::salt`
///   or generates a random 9-byte salt.
/// * `opts` — Configuration (see [`WorkOptions`])
///
/// # Errors
///
/// Returns [`SeaError::Crypto`] on task join failure or internal error.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_work_pbkdf2_default() {
        let result = work(b"password", None, WorkOptions::default())
            .await
            .unwrap();
        assert!(!result.is_empty());
        // 512 bits = 64 bytes = 86 chars base64 no-pad
        assert_eq!(result.len(), 86);
    }

    #[tokio::test]
    async fn test_work_sha256_mode() {
        let opts = WorkOptions {
            name: Some("SHA-256".to_string()),
            ..Default::default()
        };
        let result = work(b"data", None, opts).await.unwrap();
        // SHA-256 = 32 bytes = 43 chars base64 no-pad
        assert_eq!(result.len(), 43);
    }

    #[tokio::test]
    async fn test_work_sha256_deterministic() {
        let opts = WorkOptions {
            name: Some("sha".to_string()),
            ..Default::default()
        };
        let a = work(b"same input", None, opts.clone()).await.unwrap();
        let b = work(b"same input", None, opts).await.unwrap();
        assert_eq!(a, b, "SHA-256 should be deterministic for same input");
    }

    #[tokio::test]
    async fn test_work_pbkdf2_different_salt_different_output() {
        let opts = WorkOptions::default();
        let a = work(b"password", Some(b"salt_a"), opts.clone())
            .await
            .unwrap();
        let b = work(b"password", Some(b"salt_b"), opts).await.unwrap();
        assert_ne!(a, b, "different salts should produce different outputs");
    }

    #[tokio::test]
    async fn test_work_pbkdf2_same_salt_same_output() {
        let opts = WorkOptions::default();
        let a = work(b"password", Some(b"same_salt"), opts.clone())
            .await
            .unwrap();
        let b = work(b"password", Some(b"same_salt"), opts).await.unwrap();
        assert_eq!(a, b, "same salt should produce same output");
    }

    #[tokio::test]
    async fn test_work_custom_iterations() {
        let opts = WorkOptions {
            iterations: Some(100),
            ..Default::default()
        };
        let result = work(b"password", Some(b"salt"), opts).await.unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_work_empty_data() {
        let result = work(b"", Some(b"salt"), WorkOptions::default())
            .await
            .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_work_sha256_known_value() {
        // SHA-256 of empty string = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let opts = WorkOptions {
            name: Some("sha".to_string()),
            ..Default::default()
        };
        let result = work(b"", None, opts).await.unwrap();
        // base64 no-pad of the known SHA-256 of empty string
        let expected = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU";
        assert_eq!(result, expected);
    }
}
