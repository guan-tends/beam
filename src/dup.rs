//! Gun.js DAM-style message deduplication.
//!
//! This module implements [`Dup`] — a bounded, TTL-based deduplication
//! tracker that matches the semantics of Gun.js `dup.js`. It prevents
//! the same message from being processed or forwarded more than once
//! across the P2P mesh.
//!
//! ## How It Works
//!
//! Two layers of dedup, matching Gun.js:
//!
//! 1. **Message ID** (`#` field) — prevents echo and re-processing of
//!    messages this node has already seen.
//! 2. **Ack + hash** (`@` + `##` fields) — deduplicates identical responses
//!    to avoid redundant re-sends.
//!
//! Entries expire after a configurable TTL (default 9 seconds, matching
//! Gun.js `opt.age`). Eviction is both **lazy** (on `check`) and **periodic**
//! (on `track`), preventing unbounded memory growth.
//!
//! ## Example
//!
//! ```
//! use beam::Dup;
//!
//! let mut dup = Dup::default_gun();
//! assert!(!dup.check("msg-1"));    // not seen yet
//! dup.track("msg-1");              // mark as seen
//! assert!(dup.check("msg-1"));     // now seen
//! assert!(!dup.check("msg-2"));   // different message not seen
//! ```

use crate::utils::FxHashMap;
use web_time::{Duration, Instant};

/// A bounded, TTL-based deduplication tracker matching Gun.js `dup.js`.
///
/// Tracks message IDs with timestamps. Entries that haven't been seen
/// within the TTL are automatically evicted. The map is also bounded by
/// `max` entries — when exceeded, the oldest third is evicted.
pub struct Dup {
    entries: FxHashMap<String, DupEntry>,
    /// Maximum entries before forced eviction (default: 100,000).
    max: usize,
    /// Entry TTL (default: 9 seconds, matching Gun.js `opt.age`).
    age: Duration,
    /// Instant of last `drop()` call — rate-limits periodic cleanup.
    last_drop: Instant,
}

struct DupEntry {
    was: Instant,
}

impl Dup {
    /// Creates a new dedup tracker with the given capacity and TTL.
    ///
    /// # Arguments
    ///
    /// * `max` — Maximum number of entries before forced eviction.
    /// * `age_secs` — TTL in seconds. Entries older than this are evicted.
    pub fn new(max: usize, age_secs: u64) -> Self {
        Self {
            entries: FxHashMap::with_capacity_and_hasher(max, Default::default()),
            max,
            age: Duration::from_secs(age_secs),
            last_drop: Instant::now(),
        }
    }

    /// Creates a new dedup tracker with Gun.js defaults: 100,000 entries, 9s TTL.
    pub fn default_gun() -> Self {
        Self::new(100_000, 9)
    }

    /// Gun.js `dup.check(id)`: returns `true` if `id` has been seen
    /// within the TTL. If expired, removes it and returns `false`.
    ///
    /// This is lazy eviction — expired entries are cleaned up on access.
    pub fn check(&mut self, id: &str) -> bool {
        if let Some(entry) = self.entries.get(id) {
            if entry.was.elapsed() < self.age {
                return true;
            }
            // Expired — lazy removal
            self.entries.remove(id);
        }
        false
    }

    /// Gun.js `dup.track(id)`: marks `id` as seen now.
    ///
    /// Also performs periodic cleanup if enough time has passed:
    /// - If entries exceed `max`, evicts the oldest third.
    /// - If `last_drop` was more than `age / 2` ago, runs `drop()`.
    pub fn track(&mut self, id: &str) {
        self.entries.insert(
            id.to_string(),
            DupEntry {
                was: Instant::now(),
            },
        );
        if self.entries.len() > self.max {
            self.drop_oldest(self.max / 3);
        }
        // Periodic cleanup: every ~age/2 to keep map lean
        if self.last_drop.elapsed() > self.age / 2 {
            self.drop(None);
        }
    }

    /// Remove entries older than `age`. `force_age` overrides `self.age`.
    ///
    /// This is Gun.js `dup.drop(age)` — called periodically to clean
    /// expired entries from the map.
    pub fn drop(&mut self, force_age: Option<Duration>) {
        let cutoff = force_age.unwrap_or(self.age);
        let now = Instant::now();
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.was) > cutoff)
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            self.entries.remove(&k);
        }
        self.last_drop = Instant::now();
    }

    fn drop_oldest(&mut self, n: usize) {
        let n = n.min(self.entries.len());
        if n == 0 {
            return;
        }
        // Collect keys + timestamps, sort by age, remove oldest n
        let mut pairs: Vec<(String, Instant)> = self
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), v.was))
            .collect();
        pairs.sort_by_key(|a| a.1);
        for (k, _) in pairs.into_iter().take(n) {
            self.entries.remove(&k);
        }
    }

    /// Returns the number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no entries are currently tracked.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the maximum capacity.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Returns the TTL duration.
    pub fn age(&self) -> Duration {
        self.age
    }

    /// Removes all entries, resetting the tracker to empty.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.last_drop = Instant::now();
    }
}

impl Default for Dup {
    fn default() -> Self {
        Self::default_gun()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dup_basic() {
        let mut dup = Dup::new(10, 5);
        assert!(!dup.check("msg-1"));
        dup.track("msg-1");
        assert!(dup.check("msg-1"));
    }

    #[test]
    fn test_dup_expiration() {
        let mut dup = Dup::new(10, 1);
        dup.track("msg-1");
        assert!(dup.check("msg-1"));
        std::thread::sleep(Duration::from_secs(2));
        assert!(!dup.check("msg-1"));
    }

    #[test]
    fn test_dup_max_eviction() {
        let mut dup = Dup::new(3, 60);
        dup.track("a");
        dup.track("b");
        dup.track("c");
        assert_eq!(dup.len(), 3);
        dup.track("d");
        assert_eq!(dup.len(), 3);
        assert!(!dup.check("a")); // oldest third evicted
    }

    #[test]
    fn test_gun_default() {
        let dup = Dup::default_gun();
        assert_eq!(dup.max(), 100_000);
        assert_eq!(dup.age(), Duration::from_secs(9));
    }

    #[test]
    fn test_dup_default_trait() {
        let dup = Dup::default();
        assert_eq!(dup.max(), 100_000);
        assert_eq!(dup.age(), Duration::from_secs(9));
    }

    #[test]
    fn test_dup_is_empty() {
        let mut dup = Dup::new(10, 5);
        assert!(dup.is_empty());
        dup.track("x");
        assert!(!dup.is_empty());
    }

    #[test]
    fn test_dup_clear() {
        let mut dup = Dup::new(10, 60);
        dup.track("a");
        dup.track("b");
        assert_eq!(dup.len(), 2);
        dup.clear();
        assert_eq!(dup.len(), 0);
        assert!(dup.is_empty());
    }

    #[test]
    fn test_dup_drop_with_force_age() {
        let mut dup = Dup::new(100, 60); // long TTL
        dup.track("a");
        dup.track("b");
        assert_eq!(dup.len(), 2);
        // Force-drop with age 0 — should evict everything
        dup.drop(Some(Duration::from_secs(0)));
        assert_eq!(dup.len(), 0);
    }

    #[test]
    fn test_dup_retrack_updates_timestamp() {
        let mut dup = Dup::new(10, 1);
        dup.track("msg-1");
        std::thread::sleep(Duration::from_millis(500));
        dup.track("msg-1"); // re-track, should refresh timestamp
        std::thread::sleep(Duration::from_millis(600));
        // Total elapsed: 1.1s, but re-tracked at 0.5s, so only 0.6s since last track
        assert!(dup.check("msg-1")); // should still be alive
    }

    #[test]
    fn test_dup_different_ids_independent() {
        let mut dup = Dup::new(10, 60);
        dup.track("msg-1");
        assert!(dup.check("msg-1"));
        assert!(!dup.check("msg-2"));
        dup.track("msg-2");
        assert!(dup.check("msg-2"));
        assert!(dup.check("msg-1"));
    }

    #[test]
    fn test_dup_expired_entry_removed_on_check() {
        let mut dup = Dup::new(10, 1);
        dup.track("msg-1");
        assert_eq!(dup.len(), 1);
        std::thread::sleep(Duration::from_secs(2));
        assert!(!dup.check("msg-1"));
        assert_eq!(dup.len(), 0); // expired entry was removed
    }
}
