//! ECDH shared secret derivation
//! Based on Gun.js sea/secret.js
//! Derives a shared secret from ECDH key exchange

use super::{KeyPair, SeaError};
use base64::prelude::*;

/// Derive shared secret from ECDH key exchange
///
/// Takes a public key (their_epub, x.y base64) and our key pair.
/// Returns the derived secret key (base64 encoded x-coordinate).
///
/// The shared secret is the x-coordinate of the ECDH shared point.
/// Alice.secret(Bob.epub) == Bob.secret(Alice.epub)
pub async fn secret(their_epub: &str, pair: &KeyPair) -> Result<String, SeaError> {
    // ECDH is fast but the caller expects async; run in blocking task for consistency
    let their = their_epub.to_string();
    let pair = pair.clone();
    tokio::task::spawn_blocking(move || secret_sync(&their, &pair))
        .await
        .map_err(|e| SeaError::Crypto(format!("task join error: {}", e)))?
}

/// Derive shared secret from ECDH key exchange (synchronous version)
///
/// Takes a public key (their_epub, x.y base64) and our key pair.
/// Returns the derived secret key (base64 encoded x-coordinate).
pub fn secret_sync(their_epub: &str, pair: &KeyPair) -> Result<String, SeaError> {
    // Parse their public key
    let their_pub = parse_epub(their_epub)?;

    // Get our encryption private key
    let our_epriv = pair
        .epriv_key
        .as_ref()
        .ok_or_else(|| SeaError::Crypto("missing epriv key".to_string()))?;

    let our_priv_bytes = BASE64_URL_SAFE_NO_PAD.decode(our_epriv)
        .map_err(|_| SeaError::InvalidKey)?;

    if our_priv_bytes.len() != 32 {
        return Err(SeaError::InvalidKey);
    }

    let mut priv_array = [0u8; 32];
    priv_array.copy_from_slice(&our_priv_bytes);

    let our_secret =
        p256::SecretKey::from_bytes(&priv_array.into()).map_err(|_| SeaError::InvalidKey)?;

    // Derive shared secret using ECDH
    let shared_secret =
        p256::ecdh::diffie_hellman(our_secret.to_nonzero_scalar(), their_pub.as_affine());

    // Extract the x-coordinate of the shared point as the secret
    // This is what Gun.js does — uses the x coordinate
    let shared_bytes = shared_secret.raw_secret_bytes();

    // Return as base64 (matching Gun.js format)
    Ok(BASE64_URL_SAFE_NO_PAD.encode(&shared_bytes[..]))
}

/// Parse an epub key (format: x.y base64) into a p256 PublicKey
fn parse_epub(epub: &str) -> Result<p256::PublicKey, SeaError> {
    let parts: Vec<&str> = epub.split('.').collect();
    if parts.len() != 2 {
        return Err(SeaError::InvalidKey);
    }

    let x = BASE64_URL_SAFE_NO_PAD.decode(parts[0])
        .map_err(|_| SeaError::InvalidKey)?;
    let y = BASE64_URL_SAFE_NO_PAD.decode(parts[1])
        .map_err(|_| SeaError::InvalidKey)?;

    // Reconstruct uncompressed public key (0x04 || x || y)
    let mut pub_bytes = Vec::with_capacity(65);
    pub_bytes.push(0x04u8);
    pub_bytes.extend_from_slice(&x);
    pub_bytes.extend_from_slice(&y);

    // Import as public key
    p256::PublicKey::from_sec1_bytes(&pub_bytes).map_err(|_| SeaError::InvalidKey)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::generate_pair;

    #[tokio::test]
    async fn test_secret_symmetric() {
        // Alice.secret(Bob.epub) == Bob.secret(Alice.epub)
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();

        let alice_secret = secret(bob.epub_key.as_ref().unwrap(), &alice)
            .await
            .unwrap();

        let bob_secret = secret(alice.epub_key.as_ref().unwrap(), &bob)
            .await
            .unwrap();

        assert_eq!(alice_secret, bob_secret, "ECDH shared secrets must match");
    }

    #[tokio::test]
    async fn test_secret_sync_matches_async() {
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();

        let async_result = secret(bob.epub_key.as_ref().unwrap(), &alice)
            .await
            .unwrap();
        let sync_result = secret_sync(bob.epub_key.as_ref().unwrap(), &alice).unwrap();
        assert_eq!(async_result, sync_result);
    }

    #[tokio::test]
    async fn test_secret_invalid_epub_format() {
        let pair = generate_pair().await.unwrap();
        assert!(secret_sync("invalid_key", &pair).is_err());
    }

    #[tokio::test]
    async fn test_secret_missing_epriv() {
        let mut pair = generate_pair().await.unwrap();
        pair.epriv_key = None;
        let bob = generate_pair().await.unwrap();
        assert!(secret_sync(bob.epub_key.as_ref().unwrap(), &pair).is_err());
    }
}
