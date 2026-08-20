//! Session storage backends for SEA `recall()`.
//!
//! Session storage allows user keypairs to be persisted across restarts
//! without requiring the user to re-enter their password. The `SessionStorage`
//! trait defines the interface; two backends are provided:
//!
//! - `InMemorySessionStorage` — ephemeral, for testing and short-lived processes
//! - `EncryptedFileSessionStorage` — production-grade, AES-256-GCM encrypted files (native only)
//!
//! # Security
//!
//! Session files contain encrypted private keys. The master key is resolved
//! from (1) `BEAM_SEA_SESSION_KEY` env var, (2) `~/.config/beam/.session_key`
//! file, or (3) auto-generated with an `ERROR`-level log. Files are stored
//! with `0700` permissions on Unix.

#[cfg(not(target_arch = "wasm32"))]
pub mod file;
pub mod memory;

#[cfg(not(target_arch = "wasm32"))]
pub use file::EncryptedFileSessionStorage;
pub use memory::InMemorySessionStorage;
