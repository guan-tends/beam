//! Utility functions and data structures for BEAM.
//!
//! This module provides:
//!
//! - [`random_string`] — a cryptographically random alphanumeric string
//!   generator used for message IDs and actor addresses.
//! - [`BoundedHashMap`] — a fixed-capacity FIFO-eviction map used for
//!   deduplication tracking (seen message IDs, etc.).
//!
//! ## Security Note
//!
//! [`random_string`] uses [`rand::thread_rng`] which is backed by the OS
//! CSPRNG. This is suitable for message IDs and session tokens but is
//! **not** suitable for cryptographic key generation — use the [`crate::sea`]
//! module's `generate_pair()` for key generation.

use rand::distr::Alphanumeric;
use rand::{Rng, rng};
use std::collections::{HashMap, VecDeque};

/// Generates a random alphanumeric string of the given length.
///
/// Uses [`rand::thread_rng`] (OS CSPRNG) and the [`Alphanumeric`]
/// distribution (a–z, A–Z, 0–9). Each character provides ~5.95 bits of
/// entropy.
///
/// # Example
///
/// ```ignore
/// // Module is crate-private; use from within beam.
/// let id = beam::utils::random_string(32);
/// assert_eq!(id.len(), 32);
/// ```
pub fn random_string(len: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// A fixed-capacity hash map that evicts the oldest entries when full.
///
/// When the map is at capacity, each `insert` pushes out the oldest entry
/// (FIFO eviction order). This is used to track recently-seen message IDs
/// for deduplication, preventing unbounded memory growth in long-running
/// nodes.
///
/// # Example
///
/// ```ignore
/// // Module is crate-private; use from within beam.
/// use beam::utils::BoundedHashMap;
/// let mut map = BoundedHashMap::new(2);
/// map.insert("a", 1);
/// map.insert("b", 2);
/// ```
pub struct BoundedHashMap<K, V> {
    map: HashMap<K, V>,
    queue: VecDeque<K>,
    max_entries: usize,
}

impl<K: Clone + std::hash::Hash + std::cmp::Eq, V> BoundedHashMap<K, V> {
    /// Creates a new `BoundedHashMap` with the given maximum capacity.
    ///
    /// # Panics
    ///
    /// Does not panic; a capacity of 0 will simply evict on every insert.
    pub fn new(max_entries: usize) -> Self {
        BoundedHashMap {
            map: HashMap::new(),
            queue: VecDeque::new(),
            max_entries,
        }
    }

    /// Inserts a key-value pair, evicting the oldest entry if at capacity.
    ///
    /// If the key already exists, the value is updated in place and the
    /// eviction queue is not modified (the key's position is preserved).
    /// If capacity is 0, the insert is silently dropped.
    pub fn insert(&mut self, key: K, value: V) {
        if self.max_entries == 0 {
            return;
        }
        if self.queue.len() >= self.max_entries {
            if let Some(removed) = self.queue.pop_back() {
                self.map.remove(&removed);
            }
        }
        if !self.map.contains_key(&key) {
            self.queue.push_front(key.clone());
        }
        self.map.insert(key, value);
    }

    /// Returns a mutable reference to the value for the given key, or `None`.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    /// Returns a reference to the value for the given key, or `None`.
    #[allow(dead_code)] // Public API for Step 3 (router); module not yet `pub`
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    /// Returns the number of entries currently stored.
    #[allow(dead_code)] // Public API for Step 3 (router); module not yet `pub`
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the map contains no entries.
    #[allow(dead_code)] // Public API for Step 3 (router); module not yet `pub`
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the maximum number of entries before eviction begins.
    #[allow(dead_code)] // Public API for Step 3 (router); module not yet `pub`
    pub fn capacity(&self) -> usize {
        self.max_entries
    }

    /// Removes and returns the value for the given key, or `None`.
    ///
    /// Also removes the key from the eviction queue to prevent it from being
    /// re-inserted as a stale entry on the next FIFO eviction. If you re-insert
    /// the same key later, it goes to the front of the queue (most-recently-used).
    pub fn take(&mut self, key: &K) -> Option<V> {
        self.queue.retain(|k| k != key);
        self.map.remove(key)
    }

    /// Iterator over all (key, value) pairs.
    ///
    /// Used by periodic cleanup tasks (e.g., the quorum reaper) that need to
    /// scan all entries for expiration. Order is unspecified — typically the
    /// `HashMap`'s random iteration order. For FIFO-scoped iteration, callers
    /// should combine with `take()` to evict expired entries.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// for (key, value) in map.iter() {
    ///     if should_evict(&value) {
    ///         map.take(&key);
    ///     }
    /// }
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }
}

impl<K: Clone + std::hash::Hash + std::cmp::Eq, V> Default for BoundedHashMap<K, V> {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── random_string ──

    #[test]
    fn test_random_string_length() {
        let s = random_string(32);
        assert_eq!(s.len(), 32);
    }

    #[test]
    fn test_random_string_empty() {
        let s = random_string(0);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn test_random_string_alphanumeric() {
        let s = random_string(100);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_random_string_uniqueness() {
        let a = random_string(32);
        let b = random_string(32);
        // Astronomically unlikely to collide for 32 chars
        assert_ne!(a, b);
    }

    // ── BoundedHashMap ──

    #[test]
    fn test_bounded_insert_and_get() {
        let mut map = BoundedHashMap::new(10);
        map.insert("a", 1);
        map.insert("b", 2);
        assert_eq!(map.get(&"a"), Some(&1));
        assert_eq!(map.get(&"b"), Some(&2));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_bounded_eviction_fifo() {
        let mut map = BoundedHashMap::new(2);
        map.insert("a", 1);
        map.insert("b", 2);
        assert_eq!(map.len(), 2);
        map.insert("c", 3); // should evict "a" (oldest)
        assert_eq!(map.get(&"a"), None);
        assert_eq!(map.get(&"b"), Some(&2));
        assert_eq!(map.get(&"c"), Some(&3));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_bounded_update_existing_key() {
        let mut map = BoundedHashMap::new(2);
        map.insert("a", 1);
        map.insert("a", 99);
        assert_eq!(map.get(&"a"), Some(&99));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_bounded_get_mut() {
        let mut map = BoundedHashMap::new(10);
        map.insert("a", 1);
        if let Some(v) = map.get_mut(&"a") {
            *v = 42;
        }
        assert_eq!(map.get(&"a"), Some(&42));
    }

    #[test]
    fn test_bounded_is_empty() {
        let map: BoundedHashMap<&str, i32> = BoundedHashMap::new(10);
        assert!(map.is_empty());
    }

    #[test]
    fn test_bounded_capacity() {
        let map: BoundedHashMap<&str, i32> = BoundedHashMap::new(42);
        assert_eq!(map.capacity(), 42);
    }

    #[test]
    fn test_bounded_default() {
        let map: BoundedHashMap<&str, i32> = BoundedHashMap::default();
        assert_eq!(map.capacity(), 1024);
    }

    #[test]
    fn test_bounded_zero_capacity() {
        let mut map = BoundedHashMap::new(0);
        map.insert("a", 1);
        assert_eq!(map.get(&"a"), None); // immediately evicted
    }
}

// === Fire-and-forget observability (Follow-up B) ==========================

use crate::message::Message;
use crate::metrics::Metrics;

/// Try to send a message to an actor address. On full mailbox, increment
/// the [`Metrics::dropped_sends`] counter and log at debug level instead
/// of silently dropping the error.
///
/// This is the canonical BEAM pattern for fire-and-forget sends. It
/// converts the previously invisible `let _ = addr.send(msg)` pattern
/// into observable behavior without introducing a new abstraction layer.
///
/// # When to use
///
/// Use `try_send_or_log` when:
///
/// - The caller does **not** need ack confirmation
/// - The caller can tolerate message loss under actor back-pressure
/// - Observability of dropped messages is desirable (production telemetry)
///
/// Use `addr.send(msg).expect(...)` (or pattern-match on `Result`) when:
///
/// - The caller requires delivery confirmation
/// - The caller can handle back-pressure by retrying or propagating the error
/// - A silent drop would cause data loss (e.g. critical storage writes)
///
/// # Performance
///
/// On success this is a single `Addr::send` call. On failure it adds one
/// atomic increment and one `tracing::debug!` event — both lock-free and
/// negligible cost relative to the message-send attempt itself.
///
/// [`Metrics::dropped_sends`]: crate::metrics::Metrics::dropped_sends
pub(crate) fn try_send_or_log(
    addr: &crate::actor::Addr,
    msg: Message,
    metrics: &Metrics,
    ctx: &'static str,
) {
    if addr.send(msg).is_err() {
        metrics.record_dropped_send();
        log::debug!(target: "beam::send", "actor mailbox full or closed, dropped message (context={})", ctx);
    }
}

// Unit tests for `try_send_or_log` deferred to Phase 3 e2e integration
// tests in tests/send_metrics_e2e.rs. The helper is a thin wrapper around
// `Addr::send` + metric increment — full coverage via realistic actor
// scenarios is more valuable than mocked unit tests.
