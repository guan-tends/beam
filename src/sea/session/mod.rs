//! Session storage backends for SEA recall()
pub mod memory;
pub mod file;

pub use memory::InMemorySessionStorage;
pub use file::EncryptedFileSessionStorage;
