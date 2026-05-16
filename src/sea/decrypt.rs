//! AES-GCM decryption
use super::{KeyPair, SeaError};
use serde_json::Value;

pub async fn decrypt(_encrypted: &Value, _pair: &KeyPair, _their_epub: Option<&str>) -> Result<Value, SeaError> {
    Ok(Value::Null)
}
