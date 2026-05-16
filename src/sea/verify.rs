//! Signature verification
use super::SeaError;
use serde_json::Value;

pub async fn verify(_signed_data: &Value, _pub_key: &str) -> Result<Value, SeaError> {
    Ok(Value::Null)
}
