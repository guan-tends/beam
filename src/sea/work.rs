//! Proof of Work / Hashing
use super::SeaError;
use super::WorkOptions;

pub async fn work(_data: &[u8], _salt: Option<&[u8]>, _opts: WorkOptions) -> Result<String, SeaError> {
    Ok(String::new())
}
