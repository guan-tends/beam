//! Key pair generation — Gun.js `sea/pair.js` equivalent.
//!
//! Generates ECDSA (P-256) keys for digital signatures and ECDH (P-256)
//! keys for key exchange / encryption. The output format matches Gun.js:
//!
//! - `pub_key`: `"x.y"` where x, y are base64-encoded P-256 public key coordinates
//! - `priv_key`: base64-encoded 32-byte private scalar
//! - `epub_key` / `epriv_key`: same format for ECDH encryption keys
//!
//! # Security
//!
//! Uses [`p256`] with the [`Generate`] trait and the system's ambient CSPRNG
//! (via the `getrandom` feature). Key generation happens in a blocking context
//! (no `await`), which is fine since the OS RNG is fast and does not require async.
//!
//! # Example
//!
//! ```ignore
//! let pair = beam::sea::generate_pair().await.unwrap();
//! assert!(!pair.pub_key.is_empty());
//! assert!(!pair.priv_key.is_empty());
//! assert!(pair.epub_key.is_some());
//! assert!(pair.epriv_key.is_some());

use super::{KeyPair, SeaError};
use base64::prelude::*;
use p256::ecdsa::{SigningKey, VerifyingKey};
use p256::elliptic_curve::Generate;
use p256::elliptic_curve::sec1::ToSec1Point;
use p256::{PublicKey as EcdhPublicKey, SecretKey};

/// Generate a new ECDSA + ECDH key pair.
///
/// Creates both signing keys (ECDSA P-256) and encryption keys (ECDH P-256),
/// matching the Gun.js format: `pub = "x.y"`, `priv = base64-encoded scalar`.
///
/// # Errors
///
/// Returns [`SeaError::Crypto`] if key generation or encoding fails. In
/// practice this should never happen with a functioning OS CSPRNG.
pub async fn generate_pair() -> Result<KeyPair, SeaError> {
    // ECDSA signing key pair
    let signing_key = SigningKey::generate();
    let verifying_key = VerifyingKey::from(&signing_key);

    // Export signing private key (32 bytes)
    let priv_bytes = signing_key.to_bytes();
    let priv_key = BASE64_URL_SAFE_NO_PAD.encode(priv_bytes);

    // Get uncompressed public key point
    let pub_point = verifying_key.to_sec1_point(false);
    let pub_bytes = pub_point.as_bytes();

    // Extract x, y from uncompressed format (0x04 || x || y)
    if pub_bytes.len() != 65 || pub_bytes[0] != 0x04 {
        return Err(SeaError::Crypto("invalid public key format".to_string()));
    }

    let x = &pub_bytes[1..33];
    let y = &pub_bytes[33..65];

    // Convert to base64 (matching Gun.js format: x.y)
    let pub_key = format!(
        "{}.{}",
        BASE64_URL_SAFE_NO_PAD.encode(x),
        BASE64_URL_SAFE_NO_PAD.encode(y)
    );

    // ECDH encryption key pair
    let ecdh_secret = SecretKey::generate();
    let ecdh_public = EcdhPublicKey::from_secret_scalar(&ecdh_secret.to_nonzero_scalar());

    // Export ECDH keys in same format
    let epub_point = ecdh_public.to_sec1_point(false);
    let epub_bytes = epub_point.as_bytes();

    if epub_bytes.len() != 65 || epub_bytes[0] != 0x04 {
        return Err(SeaError::Crypto(
            "invalid ECDH public key format".to_string(),
        ));
    }

    let ex = &epub_bytes[1..33];
    let ey = &epub_bytes[33..65];

    let epub_key = Some(format!(
        "{}.{}",
        BASE64_URL_SAFE_NO_PAD.encode(ex),
        BASE64_URL_SAFE_NO_PAD.encode(ey)
    ));

    // Export ECDH private key (32 bytes for P-256)
    let epriv_bytes = ecdh_secret.to_bytes();
    let epriv_key = Some(BASE64_URL_SAFE_NO_PAD.encode(epriv_bytes));

    Ok(KeyPair {
        pub_key,
        priv_key,
        epub_key,
        epriv_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generate_pair_basic() {
        let pair = generate_pair().await.unwrap();
        assert!(!pair.pub_key.is_empty());
        assert!(!pair.priv_key.is_empty());
        assert!(pair.epub_key.is_some());
        assert!(pair.epriv_key.is_some());
    }

    #[tokio::test]
    async fn test_pub_key_format() {
        let pair = generate_pair().await.unwrap();
        // pub_key should be "x.y" format
        let parts: Vec<&str> = pair.pub_key.split('.').collect();
        assert_eq!(parts.len(), 2, "pub_key should have exactly one dot");
        // Each part should be valid base64 (no padding)
        for part in &parts {
            assert!(!part.is_empty(), "pub_key components should not be empty");
            assert!(!part.contains('='), "pub_key should use base64 no-pad");
            assert!(
                !part.contains('+') && !part.contains('/'),
                "pub_key should use URL-safe base64"
            );
        }
    }

    #[tokio::test]
    async fn test_priv_key_is_32_bytes() {
        let pair = generate_pair().await.unwrap();
        let priv_bytes = BASE64_URL_SAFE_NO_PAD.decode(&pair.priv_key).unwrap();
        assert_eq!(priv_bytes.len(), 32, "P-256 private key is 32 bytes");
    }

    #[tokio::test]
    async fn test_epub_key_format() {
        let pair = generate_pair().await.unwrap();
        let epub = pair.epub_key.unwrap();
        let parts: Vec<&str> = epub.split('.').collect();
        assert_eq!(parts.len(), 2, "epub_key should have exactly one dot");
        for part in &parts {
            assert!(!part.is_empty());
            assert!(!part.contains('='));
        }
    }

    #[tokio::test]
    async fn test_epriv_key_is_32_bytes() {
        let pair = generate_pair().await.unwrap();
        let epriv = pair.epriv_key.unwrap();
        let epriv_bytes = BASE64_URL_SAFE_NO_PAD.decode(&epriv).unwrap();
        assert_eq!(epriv_bytes.len(), 32, "P-256 ECDH private key is 32 bytes");
    }

    #[tokio::test]
    async fn test_unique_pairs() {
        let a = generate_pair().await.unwrap();
        let b = generate_pair().await.unwrap();
        assert_ne!(a.pub_key, b.pub_key, "each key pair must be unique");
        assert_ne!(a.priv_key, b.priv_key);
    }
}
