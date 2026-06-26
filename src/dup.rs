use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Gun.js DAM-style deduplication tracking.
///
/// Two-layer dedup matching Gun.js `dup.js`:
/// 1. Message ID (`#` field) — prevents echo and re-processing
/// 2. Ack + hash (`@` + `##` fields) — deduplicates identical responses
///
/// Entries expire after a configurable TTL (default 9s) to prevent
/// unbounded memory growth. Eviction is lazy (check) + periodic (drop).
pub struct Dup {
    entries: HashMap<String, DupEntry>,
    /// Maximum entries before forced eviction (default: 999)
    max: usize,
    /// Entry TTL (default: 9 seconds, matching Gun.js `opt.age`)
    age: Duration,
    /// Instant of last `drop()` call — rate-limits periodic cleanup
    last_drop: Instant,
}

struct DupEntry {
    was: Instant,
}

impl Dup {
    pub fn new(max: usize, age_secs: u64) -> Self {
        Self {
            entries: HashMap::with_capacity(max),
            max,
            age: Duration::from_secs(age_secs),
            last_drop: Instant::now(),
        }
    }

    pub fn default_gun() -> Self {
        Self::new(999, 9)
    }

    /// Gun.js `dup.check(id)`: returns true if `id` has been seen
    /// within the TTL. If expired, removes it and returns false.
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
    /// Also performs periodic cleanup if enough time has passed.
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

    /// Remove entries older than `age`. `force_age` overrides self.age.
    /// This is Gun.js `dup.drop(age)` — called periodically.
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

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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
        assert!(!dup.check("a"));
    }

    #[test]
    fn test_gun_default() {
        let dup = Dup::default_gun();
        assert_eq!(dup.max, 999);
        assert_eq!(dup.age, Duration::from_secs(9));
    }
}
