//! SEA.certify — capability certificates for delegated authorization
//!
//! Based on Gun.js SEA.certify and SEA.verify.certify semantics.
//! An authority signs a JSON payload authorizing certificants to
//! perform actions under certain policies.
//!
//! Certificate format (pre-signing):
//! ```json
//! { "c": ["pubkey1", "pubkey2"],   // certificants
//!   "e": 1716460800000,            // expiry (optional, ms since epoch)
//!   "r": ".*",                     // read policy (optional)
//!   "w": ".*",                     // write policy (optional)
//!   "rb": "",                      // read block (optional)
//!   "wb": "" }                     // write block (optional)
//! ```
//!
//! Signed format: {m: payload_json, s: signature_b64}

use super::{KeyPair, SeaError};
use serde_json::Value as JsonValue;

/// Build and sign a capability certificate.
///
/// # Arguments
/// * `authority` — The keypair of the authority granting rights
/// * `certificants` — List of pubkeys being authorized (the "c" field)
/// * `policies` — Optional JsonValue with recognized keys: e, r, w, rb, wb
///
/// Returns a signed certificate in Gun.js format: `{m: ..., s: ...}`
pub async fn certify(
    authority: &KeyPair,
    certificants: &[String],
    policies: Option<&JsonValue>,
) -> Result<JsonValue, SeaError> {
    let mut cert = serde_json::json!({
        "c": certificants,
    });

    if let Some(pol) = policies {
        if let Some(obj) = pol.as_object() {
            for key in ["e", "r", "w", "rb", "wb"] {
                if let Some(v) = obj.get(key) {
                    cert[key] = v.clone();
                }
            }
        }
    }

    super::sign::sign(&cert, authority).await
}

/// Verify a signed certificate against an authority's public key.
///
/// Checks signature validity and expiry (if present).
/// Returns the verified certificate payload.
pub fn verify_certificate(
    signed_cert: &JsonValue,
    authority_pubkey: &str,
) -> Result<JsonValue, SeaError> {
    let payload = super::verify::verify_sync(signed_cert, authority_pubkey)?;

    if let Some(expiry) = payload.get("e").and_then(|e| e.as_f64()) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64;
        if expiry < now {
            return Err(SeaError::VerificationFailed);
        }
    }

    Ok(payload)
}

/// Check if a given pubkey appears in the certificants list.
pub fn is_certified(payload: &JsonValue, pubkey: &str) -> bool {
    payload
        .get("c")
        .and_then(|c| c.as_array())
        .map(|arr| arr.iter().any(|v| v.as_str() == Some(pubkey)))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sea::generate_pair;
    use serde_json::json;

    #[tokio::test]
    async fn test_certify_basic() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let cert = certify(&authority, &[alice.pub_key.clone()], None).await.unwrap();
        let payload = verify_certificate(&cert, &authority.pub_key).unwrap();
        assert!(is_certified(&payload, &alice.pub_key));
    }

    #[tokio::test]
    async fn test_certify_with_policies() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let policies = json!({"w": "skills/", "r": ".*"});
        let cert = certify(
            &authority,
            &[alice.pub_key.clone()],
            Some(&policies),
        )
        .await
        .unwrap();
        let payload = verify_certificate(&cert, &authority.pub_key).unwrap();
        assert_eq!(payload["w"].as_str(), Some("skills/"));
        assert_eq!(payload["r"].as_str(), Some(".*"));
    }

    #[tokio::test]
    async fn test_certify_expired() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let policies = json!({"e": 1000.0_f64}); // 1970
        let cert = certify(&authority, &[alice.pub_key], Some(&policies)).await.unwrap();
        assert!(verify_certificate(&cert, &authority.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_certify_wrong_authority() {
        let authority = generate_pair().await.unwrap();
        let imposter = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let cert = certify(&authority, &[alice.pub_key], None).await.unwrap();
        assert!(verify_certificate(&cert, &imposter.pub_key).is_err());
    }

    #[tokio::test]
    async fn test_certify_multiple_certificants() {
        let authority = generate_pair().await.unwrap();
        let alice = generate_pair().await.unwrap();
        let bob = generate_pair().await.unwrap();
        let cert = certify(
            &authority,
            &[alice.pub_key.clone(), bob.pub_key.clone()],
            None,
        )
        .await
        .unwrap();
        let payload = verify_certificate(&cert, &authority.pub_key).unwrap();
        assert!(is_certified(&payload, &alice.pub_key));
        assert!(is_certified(&payload, &bob.pub_key));
        assert!(!is_certified(&payload, "unknown"));
    }

    #[test]
    fn test_is_certified_missing_c_field() {
        let payload = json!({"other": "data"});
        assert!(!is_certified(&payload, "anykey"));
    }

    #[test]
    fn test_is_certified_empty_array() {
        let payload = json!({"c": []});
        assert!(!is_certified(&payload, "anykey"));
    }
}
