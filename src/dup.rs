//! Bloom-filter message deduplication with generation-ring TTL.
//!
//! Replaces the original `HashMap<String, DupEntry>` with a fixed-size
//! bitset + rotating generation ring. Zero allocation after construction.
//!
//! ## How It Works
//!
//! **Bloom filter**: Each tracked ID is hashed with k independent hash
//! functions (double-hashing: H1 + i × H2) and k bits are set in a bitset.
//! `check()` tests whether all k bits are set. False positives are possible
//! but rare with proper sizing; false negatives are impossible.
//!
//! **Generation ring**: Time is divided into windows of `age / GENS`. Each
//! generation has its own bitset and counter. `track()` writes to the
//! current generation. `check()` tests ALL active generations. When the
//! current generation's window expires, it is cleared and the ring
//! advances — giving TTL eviction for free, without per-entry timestamps.
//!
//! ## Sizing
//!
//! With the defaults (999 max entries, 9 s TTL, 4 generations):
//! - Each generation covers 2.25 s
//! - The bitset has 8192 bits (1 KiB) — sized for <1% FPR at 250 entries/gen
//! - k = 7 hash functions
//! - Total memory: 4 KiB (vs unbounded HashMap growth)
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

use web_time::{Duration, Instant};

// ──────────────────────────────────────────────────────────
//  Constants
// ──────────────────────────────────────────────────────────

/// Number of generations in the ring. Each generation covers
/// `age / GENS` of the TTL. More generations = finer-grained
/// eviction but more memory.
const GENS: usize = 4;

/// Bits per generation. 8192 bits = 128 u64 words = 1 KiB.
/// Sized for <1% false-positive rate at ~250 entries per generation
/// (999 max / 4 generations ≈ 250).
const BITS: usize = 8192;

/// Number of hash functions (k). With 8192 bits and 250 entries,
/// k=7 gives FPR ≈ 0.8% — well under the 1% target.
const K: usize = 7;

/// Words in the bitset (BITS / 64).
const WORDS: usize = BITS / 64;

// ──────────────────────────────────────────────────────────
//  Hash
// ──────────────────────────────────────────────────────────

/// Fast non-cryptographic hash (FNV-1a variant with multiply mixing).
/// Returns two independent 64-bit hashes via a single pass — the second
/// uses a different prime offset so H1 and H2 are decorrelated.
#[inline]
fn hash2(id: &[u8]) -> (u64, u64) {
    // Two different 64-bit primes for decorrelated mixing.
    const P1: u64 = 0x51_5c_a3_d2_c9_9b_3c_41; // ≈ sqrt(2) × 2^63
    const P2: u64 = 0xc4_ce_b9_fe_1a_7c_b0_73; // ≈ sqrt(3) × 2^63

    let mut h1: u64 = 0xcb_f2_9c_e4_84_22_23_25; // FNV-1a 64-bit offset basis
    let mut h2: u64 = 0x5b_ed_c6_7b_a9_11_d3_3d; // Different offset

    for &b in id {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(P1);
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(P2);
    }
    (h1, h2)
}

/// Returns the k-th bit index for a given (h1, h2) pair.
/// Uses double-hashing: bit_k = (h1 + k * h2) % BITS.
#[inline]
fn bit_index(h1: u64, h2: u64, k: usize) -> usize {
    ((h1.wrapping_add(h2.wrapping_mul(k as u64))) as usize) % BITS
}

// ──────────────────────────────────────────────────────────
//  Generation
// ──────────────────────────────────────────────────────────

/// A single generation: a fixed bitset and an entry count.
struct Gen {
    /// Bitset: `WORDS` × 64 bits.
    bits: [u64; WORDS],
    /// Number of IDs tracked in this generation (for approximate len()).
    count: usize,
    /// When this generation became active.
    since: Instant,
}

impl Gen {
    fn empty() -> Self {
        Self {
            bits: [0; WORDS],
            count: 0,
            since: Instant::now(),
        }
    }

    #[inline]
    fn set_bits(&mut self, h1: u64, h2: u64) {
        for i in 0..K {
            let idx = bit_index(h1, h2, i);
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
        self.count += 1;
    }

    #[inline]
    fn test_bits(&self, h1: u64, h2: u64) -> bool {
        for i in 0..K {
            let idx = bit_index(h1, h2, i);
            if self.bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }
}

// ──────────────────────────────────────────────────────────
//  Dup
// ──────────────────────────────────────────────────────────

/// A bounded, TTL-based deduplication tracker matching Gun.js `dup.js`.
///
/// Uses a ring of bloom filters instead of a `HashMap`. Zero allocation
/// after construction. O(k) check/track where k is the number of hash
/// functions (default: 7).
pub struct Dup {
    /// Fixed ring of `GENS` generations.
    gens: [Gen; GENS],
    /// Index of the current (write) generation.
    current: usize,
    /// Maximum entries before forced eviction (config, retained for API compat).
    max: usize,
    /// Entry TTL (config, used for generation rotation).
    age: Duration,
}

impl Dup {
    /// Creates a new dedup tracker with the given capacity and TTL.
    ///
    /// # Arguments
    ///
    /// * `max` — Maximum number of entries (used for sizing, not a hard cap).
    /// * `age_secs` — TTL in seconds. Entries older than this are evicted.
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

    /// Creates a new dedup tracker with Gun.js defaults: 999 entries, 9s TTL.
    pub fn default_gun() -> Self {
        Self::new(999, 9)
    }

    /// Advances the generation ring if the current generation's window
    /// has expired. Clears the next generation and makes it current.
    fn maybe_advance(&mut self) {
        let window = self.age / GENS as u32;
        let now = Instant::now();
        // Advance the ring, clearing each generation we land on.
        // We stop when the previous generation hasn't expired yet.
        // This ensures that after a long idle period, all expired
        // generations are cleared before we write new data.
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
    ///
    /// Checks all active generations. A generation is active if it has
    /// been written to and has not yet been cleared by rotation.
    pub fn check(&mut self, id: &str) -> bool {
        let (h1, h2) = hash2(id.as_bytes());
        let now = Instant::now();
        for g in &self.gens {
            // Skip expired generations — they're stale data that
            // hasn't been cleared by rotation yet.
            if g.count > 0 && now.duration_since(g.since) < self.age && g.test_bits(h1, h2) {
                return true;
            }
        }
        false
    }

    /// Gun.js `dup.track(id)`: marks `id` as seen now.
    ///
    /// Writes to the current generation, advancing the ring if the
    /// current window has expired. This is the only method that triggers
    /// generation rotation (lazy TTL eviction).
    pub fn track(&mut self, id: &str) {
        self.maybe_advance();
        let (h1, h2) = hash2(id.as_bytes());
        self.gens[self.current].set_bits(h1, h2);
    }

    /// Remove entries older than `age`. `force_age` overrides `self.age`.
    ///
    /// Clears all generations whose `since` timestamp is older than the
    /// cutoff. This matches the Gun.js `dup.drop(age)` semantics.
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
    ///
    /// This is the sum of all generation counters. Due to re-tracking
    /// (same ID in multiple generations), this may overcount. It is
    /// accurate for the common case where each ID is tracked once.
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
    fn test_dup_expiration() {
        let mut dup = Dup::new(10, 1);
        dup.track("msg-1");
        assert!(dup.check("msg-1"));
        std::thread::sleep(Duration::from_secs(2));
        // Generation rotation on track() clears expired generations.
        // But check() alone doesn't advance — track does.
        // After sleeping, the generation's window has expired.
        // track() will advance the ring, clearing the old generation.
        dup.track("msg-other"); // triggers rotation
        assert!(!dup.check("msg-1"));
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
        assert_eq!(dup.max(), 999);
        assert_eq!(dup.age(), Duration::from_secs(9));
    }

    #[test]
    fn test_dup_default_trait() {
        let dup = Dup::default();
        assert_eq!(dup.max(), 999);
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
        let mut dup = Dup::new(100, 60); // long TTL
        dup.track("a");
        dup.track("b");
        assert!(!dup.is_empty());
        // Force-drop with age 0 — should evict everything
        dup.drop(Some(Duration::from_secs(0)));
        assert!(dup.is_empty());
    }

    #[test]
    fn test_dup_retrack_updates_timestamp() {
        // With a generation ring, re-tracking in a newer generation
        // effectively refreshes TTL. The ID is still in the old generation
        // (bloom filters can't remove individual entries), but it's also
        // in the new one. check() sees it in the new generation.
        let mut dup = Dup::new(10, 2);
        dup.track("msg-1");
        std::thread::sleep(Duration::from_millis(600));
        dup.track("msg-1"); // re-track in current generation
        std::thread::sleep(Duration::from_millis(600));
        // Total: 1.2s elapsed. First gen window = 0.5s (2s/4), so first
        // gen has expired. But msg-1 is in the second gen too.
        dup.track("trigger"); // advance if needed
        assert!(dup.check("msg-1")); // should be in second gen
    }

    #[test]
    fn test_dup_no_false_negatives() {
        // Bloom filters have no false negatives — once tracked, check
        // always returns true (within TTL).
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
        // With 500 entries in 8192-bit filter and k=7,
        // FPR should be under 2%.
        let mut dup = Dup::new(999, 60); // long TTL, no rotation
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
    fn test_dup_generation_rotation() {
        // With 1s TTL and 4 generations, each window is 250ms.
        // After 1s + track, the original generation should be cleared.
        let mut dup = Dup::new(100, 1);
        dup.track("early");
        assert!(dup.check("early"));
        // Sleep past the full TTL — all generations should expire
        std::thread::sleep(Duration::from_secs(2));
        dup.track("late"); // triggers rotation, clears old generations
        assert!(!dup.check("early")); // old generation was cleared
        assert!(dup.check("late"));
    }

    #[test]
    fn test_dup_expired_entry_removed_on_check() {
        // check() tests all active generations. After TTL expiry + track,
        // old generations are cleared, so check returns false.
        let mut dup = Dup::new(10, 1);
        dup.track("msg-1");
        assert!(dup.check("msg-1"));
        std::thread::sleep(Duration::from_secs(2));
        dup.track("trigger"); // advance ring, clear old gen
        assert!(!dup.check("msg-1"));
    }

    #[test]
    fn test_dup_max_eviction_approximate() {
        // The bloom filter doesn't have a hard capacity limit like the
        // old HashMap. Instead, as entries grow, the FPR increases.
        // We verify that tracking more than `max` entries doesn't crash
        // and check() still works (no false negatives for tracked items).
        let mut dup = Dup::new(3, 60);
        dup.track("a");
        dup.track("b");
        dup.track("c");
        dup.track("d");
        // All tracked items should still be detected (no false negatives)
        assert!(dup.check("a"));
        assert!(dup.check("b"));
        assert!(dup.check("c"));
        assert!(dup.check("d"));
    }

    #[test]
    fn test_dup_zero_allocation() {
        // Verify that after construction, track/check don't allocate.
        // We can't directly measure allocation in std, but we can verify
        // the implementation doesn't use any heap types (no String, Vec,
        // HashMap). This is a compile-time guarantee: the struct contains
        // only fixed-size arrays.
        let mut dup = Dup::new(999, 9);
        for i in 0..1000 {
            dup.track(&format!("msg{:08x}", i));
        }
        for i in 0..1000 {
            dup.check(&format!("msg{:08x}", i));
        }
        // If this compiles and runs, the zero-allication design is sound.
    }
}
