//! BEAM SEA Session Key Generator
//!
//! Generates a cryptographically random 32-byte master key for session encryption.
//! Output is base64-encoded, ready for the BEAM_SEA_SESSION_KEY environment variable.
//!
//! # Usage
//!
//! ```bash
//! # Generate a key and set it in your environment
//! export BEAM_SEA_SESSION_KEY=$(cargo run --bin beam-sea-keygen 2>/dev/null)
//!
//! # Or for systemd/docker:
//! BEAM_SEA_SESSION_KEY=$(cargo run --quiet --bin beam-sea-keygen)
//! ```
//!
//! # Deployment Checklist
//!
//! 1. Generate key once per deployment environment (dev/staging/prod)
//! 2. Store in secrets manager (e.g. systemd credentials, Docker secrets, Vault)
//! 3. Never commit the key to version control
//! 4. Rotate by clearing session directory and distributing new key
//!
use rand::RngCore;
use base64::prelude::*;

fn main() {
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    let encoded = BASE64_STANDARD.encode(key);
    println!("{}", encoded);
}
