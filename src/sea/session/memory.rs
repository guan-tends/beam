//! In-memory session storage — ephemeral, test-safe, fast.
//!
//! [`InMemorySessionStorage`] stores keypairs in a `HashMap` in process memory.
//! Data is lost when the process exits. Suitable for testing, short-lived
//! CLI invocations, and scenarios where persistence is unnecessary.
//!
//! # Security
//!
//! Private keys live in the process heap for the duration of the session.
//! This is NOT suitable for production use — use [`super::EncryptedFileSessionStorage`]
//! for persistent, encrypted session storage.

use super::super::{KeyPair, SeaError, SessionStorage};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

/// Ephemeral session storage backed by an in-memory `HashMap`.
///
/// NOT for production — private keys live in process heap.
pub struct InMemorySessionStorage {
    data: Mutex<HashMap<String, KeyPair>>,
}

impl Default for InMemorySessionStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySessionStorage {
    /// Creates a new empty in-memory session store.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SessionStorage for InMemorySessionStorage {
    async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError> {
        self.data
            .lock()
            .unwrap()
            .insert(alias.to_string(), pair.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pair() -> KeyPair {
        KeyPair {
            pub_key: "test.pub".to_string(),
            priv_key: "test.priv".to_string(),
            epub_key: Some("test.epub".to_string()),
            epriv_key: Some("test.epriv".to_string()),
        }
    }

    #[tokio::test]
    async fn test_save_load_roundtrip() {
        let storage = InMemorySessionStorage::new();
        let pair = test_pair();
        storage.save("alice", &pair).await.unwrap();
        let loaded = storage.load("alice").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.pub_key, pair.pub_key);
        assert_eq!(loaded.priv_key, pair.priv_key);
    }

    #[tokio::test]
    async fn test_load_missing_returns_none() {
        let storage = InMemorySessionStorage::new();
        let loaded = storage.load("nobody").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_clear_removes_entry() {
        let storage = InMemorySessionStorage::new();
        let pair = test_pair();
        storage.save("bob", &pair).await.unwrap();
        assert!(storage.load("bob").await.unwrap().is_some());
        storage.clear("bob").await.unwrap();
        assert!(storage.load("bob").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_default_is_empty() {
        let storage = InMemorySessionStorage::default();
        let loaded = storage.load("anyone").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_overwrite_existing() {
        let storage = InMemorySessionStorage::new();
        let pair1 = test_pair();
        let mut pair2 = test_pair();
        pair2.pub_key = "other.pub".to_string();
        storage.save("alice", &pair1).await.unwrap();
        storage.save("alice", &pair2).await.unwrap();
        let loaded = storage.load("alice").await.unwrap().unwrap();
        assert_eq!(loaded.pub_key, "other.pub");
    }
}
