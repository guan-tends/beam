//! User authentication system
use super::{KeyPair, SeaError};

pub struct User {
    pub pair: KeyPair,
    pub alias: Option<String>,
    pub is_authenticated: bool,
}
