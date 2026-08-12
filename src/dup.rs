//! Message deduplication with generation-ring TTL.
//!
//! Uses [fastbloom](https://crates.io/crates/fastbloom) for the bloom
//! filter implementation — hash composition (one real hash, k derived
//! indices) for maximum throughput.
//!
//! ## How It Works
//!
//! **Bloom filter**: Each tracked ID is hashed once; k bit indices are
//! derived via hash composition (Kirsch-Mitzenmacher). `check()` tests
//! whether all k bits are set. False positives possible, false negatives
//! impossible.
//!
//! **Generation ring**: Time is divided into windows of `age / GENS`.
//! Each generation has its own bloom filter. `track()` writes to the
//! current generation. `check()` tests ALL active generations. When the
//! current generation's window expires, it is cleared and the ring
//! advances — TTL eviction without per-entry timestamps.
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

use fastbloom::BloomFilter;
use web_time::{Duration, Instant};

// ──────────────────────────────────────────────────────────
//  Constants
// ──────────────────────────────────────────────────────────

/// Number of generations in the ring.
const GENS: usize = 4;

/// Bits per generation bloom filter.
/// 65536 bits = 8 KiB per generation (32 KiB total) — fits in L1 cache
/// on most CPUs. Sized for <1% FPR at ~5000 entries per generation.
const BITS: usize = 65_536;

// ──────────────────────────────────────────────────────────
//  Generation
// ──────────────────────────────────────────────────────────

/// A single generation: a bloom filter and an entry count.
struct Gen {
    filter: BloomFilter,
    count: usize,
    since: Instant,
}

impl Gen {
    fn empty() -> Self {
        Self {
            filter: BloomFilter::with_num_bits(BITS).expected_items(5000),
            count: 0,
            since: Instant::now(),
        }
    }

    fn clear(&mut self) {
        self.filter.clear();
        self.count = 0;
    }
}

// ──────────────────────────────────────────────────────────
//  Dup
// ──────────────────────────────────────────────────────────

/// A bounded, TTL-based deduplication tracker matching Gun.js `dup.js`.
///
/// Uses a ring of bloom filters. O(k) check/track where k is the number
/// of hash functions derived from one real hash via composition.
pub struct Dup {
    gens: [Gen; GENS],
    current: usize,
    max: usize,
    age: Duration,
}

impl Dup {
    /// Creates a new dedup tracker with the given capacity and TTL.
    pub fn new(max: usize, age_secs: u64) -> Self {
        let now = Instant::now();
        Self {
            gens: std::array::from_fn(|_| {
                let mut g = Gen::empty();
                g.since = now;
                g
            }),
            current: 0,
            max,
            age: Duration::from_secs(age_secs),
        }
    }

    /// Creates a new dedup tracker with Gun.js defaults.
    /// TTL is 9s (Gun.js compatible).
    pub fn default_gun() -> Self {
        Self::new(100_000, 9)
    }

    fn maybe_advance(&mut self) {
        let window = self.age / GENS as u32;
        let now = Instant::now();
        let mut advanced = true;
        while advanced {
            let prev = (self.current + GENS - 1) % GENS;
            if now.duration_since(self.gens[prev].since) >= window {
                self.current = (self.current + 1) % GENS;
                self.gens[self.current].clear();
                self.gens[self.current].since = now;
            } else {
                advanced = false;
            }
        }
    }

    /// Gun.js `dup.check(id)`: returns `true` if `id` has been seen
    /// within the TTL.
    pub fn check(&mut self, id: &str) -> bool {
        let now = Instant::now();
        for g in &self.gens {
            if g.count > 0
                && now.duration_since(g.since) < self.age
                && g.filter.contains(id)
            {
                return true;
            }
        }
        false
    }

    /// Gun.js `dup.track(id)`: marks `id` as seen now.
    pub fn track(&mut self, id: &str) {
        self.maybe_advance();
        self.gens[self.current].filter.insert(id);
        self.gens[self.current].count += 1;
    }

    /// Remove entries older than `age`. `force_age` overrides `self.age`.
    pub fn drop(&mut self, force_age: Option<Duration>) {
        let cutoff = force_age.unwrap_or(self.age);
        let now = Instant::now();
        for g in &mut self.gens {
            if now.duration_since(g.since) > cutoff {
                g.clear();
            }
        }
    }

    /// Returns the approximate number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.gens.iter().map(|gen_slot| gen_slot.count).sum()
    }

    /// Returns `true` if no entries are currently tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the maximum capacity (config accessor).
    pub fn max(&self) -> usize {
        self.max
    }

    /// Returns the TTL duration.
    pub fn age(&self) -> Duration {
        self.age
    }

    /// Removes all entries, resetting the tracker to empty.
    pub fn clear(&mut self) {
        for g in &mut self.gens {
            g.clear();
            g.since = Instant::now();
        }
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
        assert!(!dup.is_empty());
        dup.clear();
        assert!(dup.is_empty());
        assert!(!dup.check("a"));
        assert!(!dup.check("b"));
    }

    #[test]
    fn test_dup_drop_with_force_age() {
        let mut dup = Dup::new(100, 60);
        dup.track("a");
        dup.track("b");
        assert!(!dup.is_empty());
        dup.drop(Some(Duration::from_secs(0)));
        assert!(dup.is_empty());
    }

    #[test]
    fn test_dup_no_false_negatives() {
        let mut dup = Dup::new(999, 9);
        for i in 0..500 {
            let id = format!("msg{:04x}", i);
            dup.track(&id);
        }
        for i in 0..500 {
            let id = format!("msg{:04x}", i);
            assert!(dup.check(&id), "false negative for {}", id);
        }
    }

    #[test]
    fn test_dup_low_false_positive_rate() {
        let mut dup = Dup::new(999, 60);
        for i in 0..250 {
            let id = format!("tracked{:04x}", i);
            dup.track(&id);
        }
        let mut false_positives = 0;
        for i in 0..250 {
            let id = format!("untracked{:04x}", i);
            if dup.check(&id) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / 250.0;
        assert!(
            fpr < 0.05,
            "FPR too high: {} ({:.2}%)",
            false_positives,
            fpr * 100.0
        );
    }

    #[test]
    fn test_dup_max_eviction_approximate() {
        let mut dup = Dup::new(3, 60);
        dup.track("a");
        dup.track("b");
        dup.track("c");
        dup.track("d");
        assert!(dup.check("a"));
        assert!(dup.check("b"));
        assert!(dup.check("c"));
        assert!(dup.check("d"));
    }
}
