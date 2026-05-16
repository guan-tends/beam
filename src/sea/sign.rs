//! Digital signatures
use super::{KeyPair, SeaError};
use serde_json::Value;

pub async fn sign(_data: &Value, _pair: &KeyPair) -> Result<Value, SeaError> {
    Ok(Value::Null)
}
