//! Session storage backends for SEA recall()
pub mod file;
pub mod memory;

pub use file::EncryptedFileSessionStorage;
pub use memory::InMemorySessionStorage;
