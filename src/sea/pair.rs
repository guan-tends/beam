//! Key pair generation
use super::{KeyPair, SeaError};
use rand::rngs::OsRng;

pub async fn generate_pair() -> Result<KeyPair, SeaError> {
    Ok(KeyPair {
        pub_key: String::new(),
        priv_key: String::new(),
        epub_key: None,
        epriv_key: None,
    })
}
