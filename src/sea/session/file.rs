//! Encrypted file session storage — production-grade, env-var master key
//!
//! Reads `BEAM_SEA_SESSION_KEY` (base64, 32 bytes) from environment.
//! Missing env var = sessions silently don't persist (safe fallback).
//! Files stored in `~/.config/beam/sessions/` with 0700 permissions.
//! Encryption: AES-256-GCM with random nonce per write.
//! Expiry: default 30 days, overridable via `BEAM_SEA_SESSION_EXPIRY_DAYS`.

use super::super::{KeyPair, SeaError, SessionStorage};
use async_trait::async_trait;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce as AesNonce,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Encrypted session file format
#[derive(Serialize, Deserialize)]
struct SessionFile {
    ct: String,        // base64 ciphertext
    iv: String,        // base64 nonce
    s: String,         // base64 salt (for future key rotation support)
    alias: String,
    expires_at: u64,   // Unix timestamp
}

/// Production session storage backed by encrypted files on disk.
pub struct EncryptedFileSessionStorage {
    dir: PathBuf,
    expiry_seconds: u64,
}

impl EncryptedFileSessionStorage {
    pub fn new() -> Result<Self, SeaError> {
        let dir = dirs::config_dir()
            .ok_or_else(|| SeaError::SessionStorage("no config dir".to_string()))?
            .join("beam")
            .join("sessions");

        Ok(Self {
            dir,
            expiry_seconds: Self::resolve_expiry(),
        })
    }

    fn resolve_expiry() -> u64 {
        if let Ok(days) = std::env::var("BEAM_SEA_SESSION_EXPIRY_DAYS") {
            if let Ok(d) = days.parse::<u64>() {
                return d * 86400;
            }
        }
        30 * 86400 // default 30 days
    }

    fn file_path(&self, alias: &str) -> PathBuf {
        self.dir.join(format!("{}.json", alias))
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn load_master_key() -> Result<Vec<u8>, SeaError> {
        let b64 = std::env::var("BEAM_SEA_SESSION_KEY")
            .map_err(|_| SeaError::SessionStorage("BEAM_SEA_SESSION_KEY not set".to_string()))?;
        let key = base64::decode_config(&b64, base64::STANDARD)
            .map_err(|_| SeaError::SessionStorage("bad base64 in BEAM_SEA_SESSION_KEY".to_string()))?;
        if key.len() != 32 {
            return Err(SeaError::SessionStorage(
                format!("BEAM_SEA_SESSION_KEY must be 32 bytes, got {}", key.len())
            ));
        }
        Ok(key)
    }
}

#[async_trait]
impl SessionStorage for EncryptedFileSessionStorage {
    async fn save(&self, alias: &str, pair: &KeyPair) -> Result<(), SeaError> {
        let key = Self::load_master_key()?;
        if self.dir.parent().is_some() {
            tokio::fs::create_dir_all(&self.dir).await
                .map_err(|e| SeaError::SessionStorage(format!("mkdir: {}", e)))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700)).await
                    .map_err(|e| SeaError::SessionStorage(format!("chmod: {}", e)))?;
            }
        }

        let data = json!({
            "pub": pair.pub_key,
            "priv": pair.priv_key,
            "epub": pair.epub_key,
            "epriv": pair.epriv_key,
        });
        let plaintext = serde_json::to_string(&data)
            .map_err(|e| SeaError::SessionStorage(format!("serialize: {}", e)))?;

        let mut nonce = [0u8; 12];
        let mut salt = [0u8; 9];
        rand::thread_rng().fill_bytes(&mut nonce);
        rand::thread_rng().fill_bytes(&mut salt);

        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|_| SeaError::SessionStorage("bad cipher key".to_string()))?;
        let ciphertext = cipher
            .encrypt(AesNonce::from_slice(&nonce), plaintext.as_bytes())
            .map_err(|_| SeaError::SessionStorage("encrypt failed".to_string()))?;

        let expires_at = Self::now() + self.expiry_seconds;
        let session_file = SessionFile {
            ct: base64::encode_config(&ciphertext, base64::STANDARD_NO_PAD),
            iv: base64::encode_config(&nonce, base64::STANDARD_NO_PAD),
            s: base64::encode_config(&salt, base64::STANDARD_NO_PAD),
            alias: alias.to_string(),
            expires_at,
        };

        let json = serde_json::to_string_pretty(&session_file)
            .map_err(|e| SeaError::SessionStorage(format!("json: {}", e)))?;
        tokio::fs::write(self.file_path(alias), json).await
            .map_err(|e| SeaError::SessionStorage(format!("write: {}", e)))?;

        Ok(())
    }

    async fn load(&self, alias: &str) -> Result<Option<KeyPair>, SeaError> {
        let key = match Self::load_master_key() {
            Ok(k) => k,
            Err(_) => return Ok(None), // env missing = safe silent failure
        };

        let path = self.file_path(alias);
        let json = match tokio::fs::read_to_string(path).await {
            Ok(s) => s,
            Err(_) => return Ok(None), // no session file
        };

        let session_file: SessionFile = serde_json::from_str(&json)
            .map_err(|e| SeaError::SessionStorage(format!("parse: {}", e)))?;

        if session_file.expires_at < Self::now() {
            let _ = tokio::fs::remove_file(self.file_path(alias)).await;
            return Ok(None); // expired, file cleaned up
        }

        let ciphertext = base64::decode_config(&session_file.ct, base64::STANDARD_NO_PAD)
            .map_err(|_| SeaError::SessionStorage("bad ct".to_string()))?;
        let nonce = base64::decode_config(&session_file.iv, base64::STANDARD_NO_PAD)
            .map_err(|_| SeaError::SessionStorage("bad iv".to_string()))?;

        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|_| SeaError::SessionStorage("bad cipher key".to_string()))?;
        let plaintext = cipher
            .decrypt(AesNonce::from_slice(&nonce), ciphertext.as_ref())
            .map_err(|_| SeaError::SessionStorage("decrypt failed".to_string()))?;

        let text = String::from_utf8(plaintext)
            .map_err(|_| SeaError::SessionStorage("bad utf8".to_string()))?;
        let data: JsonValue = serde_json::from_str(&text)
            .map_err(|_| SeaError::SessionStorage("bad json".to_string()))?;

        let pair = KeyPair {
            pub_key: data.get("pub").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
            priv_key: data.get("priv").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
            epub_key: data.get("epub").and_then(|v| v.as_str()).map(|s| s.to_string()),
            epriv_key: data.get("epriv").and_then(|v| v.as_str()).map(|s| s.to_string()),
        };

        Ok(Some(pair))
    }

    async fn clear(&self, alias: &str) -> Result<(), SeaError> {
        let _ = tokio::fs::remove_file(self.file_path(alias)).await;
        Ok(())
    }
}
