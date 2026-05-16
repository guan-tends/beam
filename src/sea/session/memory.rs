//! In-memory session storage — ephemeral, test-safe, fast
use super::super::{KeyPair, SeaError, SessionStorage};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

/// Ephemeral session storage backed by an in-memory HashMap.
/// NOT for production — private keys live in process heap.
pub struct InMemorySessionStorage {
    data: Mutex<HashMap<String, KeyPair>>,
}

impl InMemorySessionStorage {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SessionStorage for InMemorySessionStorage {
    async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError> {
        self.data.lock().unwrap().insert(alias.to_string(), pair.clone());
        Ok(())
    }

    async fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError> {
        Ok(self.data.lock().unwrap().get(alias).cloned())
    }

    async fn clear(&self, alias: &str) -> Result<(), SeaError> {
        self.data.lock().unwrap().remove(alias);
        Ok(())
    }
}
