//! AES-GCM encryption
use super::{KeyPair, SeaError};
use serde_json::Value;

pub async fn encrypt(_data: &Value, _pair: &KeyPair, _their_epub: Option<&str>) -> Result<Value, SeaError> {
    Ok(Value::Null)
}
