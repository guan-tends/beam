//! ECDH shared secret derivation
use super::{KeyPair, SeaError};

pub async fn secret(_their_epub: &str, _pair: &KeyPair) -> Result<String, SeaError> {
    Ok(String::new())
}
