//! Eviction policy definitions.

use std::fmt;
use std::str::FromStr;

/// Eviction policy for memory management.
///
/// These policies determine which keys to evict when memory limit is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EvictionPolicy {
    /// No eviction - reject writes with OOM error when memory limit exceeded.
    #[default]
    NoEviction,

    /// Evict least recently used keys that have a TTL set.
    VolatileLru,

    /// Evict least recently used keys (any key, including persistent ones).
    AllkeysLru,

    /// Evict least frequently used keys that have a TTL set.
    VolatileLfu,

    /// Evict least frequently used keys (any key).
    AllkeysLfu,

    /// Evict random keys that have a TTL set.
    VolatileRandom,

    /// Evict random keys (any key).
    AllkeysRandom,

    /// Evict keys with the shortest remaining TTL first.
    VolatileTtl,
}

impl EvictionPolicy {
    /// Returns true if this policy only evicts keys with TTL (volatile keys).
    pub fn is_volatile(&self) -> bool {
        matches!(
            self,
            EvictionPolicy::VolatileLru
                | EvictionPolicy::VolatileLfu
                | EvictionPolicy::VolatileRandom
                | EvictionPolicy::VolatileTtl
        )
    }

    /// Returns true if this policy uses LRU (least recently used) ordering.
    pub fn uses_lru(&self) -> bool {
        matches!(
            self,
            EvictionPolicy::VolatileLru | EvictionPolicy::AllkeysLru
        )
    }

    /// Returns true if this policy uses LFU (least frequently used) ordering.
    pub fn uses_lfu(&self) -> bool {
        matches!(
            self,
            EvictionPolicy::VolatileLfu | EvictionPolicy::AllkeysLfu
        )
    }

    /// Returns true if this policy uses random selection.
    pub fn uses_random(&self) -> bool {
        matches!(
            self,
            EvictionPolicy::VolatileRandom | EvictionPolicy::AllkeysRandom
        )
    }

    /// Returns true if this policy uses TTL ordering.
    pub fn uses_ttl(&self) -> bool {
        matches!(self, EvictionPolicy::VolatileTtl)
    }

    /// Get the Redis-compatible name for this policy.
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::AllkeysLru => "allkeys-lru",
            EvictionPolicy::VolatileLfu => "volatile-lfu",
            EvictionPolicy::AllkeysLfu => "allkeys-lfu",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::AllkeysRandom => "allkeys-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    /// Get all valid policy names.
    pub fn all_names() -> &'static [&'static str] {
        &[
            "noeviction",
            "volatile-lru",
            "allkeys-lru",
            "volatile-lfu",
            "allkeys-lfu",
            "volatile-random",
            "allkeys-random",
            "volatile-ttl",
        ]
    }
}

impl fmt::Display for EvictionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Error when parsing an eviction policy string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEvictionPolicyError {
    invalid_value: String,
}

impl fmt::Display for ParseEvictionPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid eviction policy '{}', expected one of: {}",
            self.invalid_value,
            EvictionPolicy::all_names().join(", ")
        )
    }
}

impl std::error::Error for ParseEvictionPolicyError {}

impl FromStr for EvictionPolicy {
    type Err = ParseEvictionPolicyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "noeviction" => Ok(EvictionPolicy::NoEviction),
            "volatile-lru" => Ok(EvictionPolicy::VolatileLru),
            "allkeys-lru" => Ok(EvictionPolicy::AllkeysLru),
            "volatile-lfu" => Ok(EvictionPolicy::VolatileLfu),
            "allkeys-lfu" => Ok(EvictionPolicy::AllkeysLfu),
            "volatile-random" => Ok(EvictionPolicy::VolatileRandom),
            "allkeys-random" => Ok(EvictionPolicy::AllkeysRandom),
            "volatile-ttl" => Ok(EvictionPolicy::VolatileTtl),
            _ => Err(ParseEvictionPolicyError {
                invalid_value: s.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        assert_eq!(EvictionPolicy::default(), EvictionPolicy::NoEviction);
    }

    #[test]
    fn test_is_volatile() {
        assert!(!EvictionPolicy::NoEviction.is_volatile());
        assert!(EvictionPolicy::VolatileLru.is_volatile());
        assert!(!EvictionPolicy::AllkeysLru.is_volatile());
        assert!(EvictionPolicy::VolatileLfu.is_volatile());
        assert!(!EvictionPolicy::AllkeysLfu.is_volatile());
        assert!(EvictionPolicy::VolatileRandom.is_volatile());
        assert!(!EvictionPolicy::AllkeysRandom.is_volatile());
        assert!(EvictionPolicy::VolatileTtl.is_volatile());
    }

    #[test]
    fn test_uses_lru() {
        assert!(!EvictionPolicy::NoEviction.uses_lru());
        assert!(EvictionPolicy::VolatileLru.uses_lru());
        assert!(EvictionPolicy::AllkeysLru.uses_lru());
        assert!(!EvictionPolicy::VolatileLfu.uses_lru());
        assert!(!EvictionPolicy::AllkeysLfu.uses_lru());
        assert!(!EvictionPolicy::VolatileRandom.uses_lru());
        assert!(!EvictionPolicy::AllkeysRandom.uses_lru());
        assert!(!EvictionPolicy::VolatileTtl.uses_lru());
    }

    #[test]
    fn test_uses_lfu() {
        assert!(!EvictionPolicy::NoEviction.uses_lfu());
        assert!(!EvictionPolicy::VolatileLru.uses_lfu());
        assert!(!EvictionPolicy::AllkeysLru.uses_lfu());
        assert!(EvictionPolicy::VolatileLfu.uses_lfu());
        assert!(EvictionPolicy::AllkeysLfu.uses_lfu());
        assert!(!EvictionPolicy::VolatileRandom.uses_lfu());
        assert!(!EvictionPolicy::AllkeysRandom.uses_lfu());
        assert!(!EvictionPolicy::VolatileTtl.uses_lfu());
    }

    #[test]
    fn test_uses_random() {
        assert!(!EvictionPolicy::NoEviction.uses_random());
        assert!(!EvictionPolicy::VolatileLru.uses_random());
        assert!(!EvictionPolicy::AllkeysLru.uses_random());
        assert!(!EvictionPolicy::VolatileLfu.uses_random());
        assert!(!EvictionPolicy::AllkeysLfu.uses_random());
        assert!(EvictionPolicy::VolatileRandom.uses_random());
        assert!(EvictionPolicy::AllkeysRandom.uses_random());
        assert!(!EvictionPolicy::VolatileTtl.uses_random());
    }

    #[test]
    fn test_uses_ttl() {
        assert!(!EvictionPolicy::NoEviction.uses_ttl());
        assert!(!EvictionPolicy::VolatileLru.uses_ttl());
        assert!(!EvictionPolicy::AllkeysLru.uses_ttl());
        assert!(!EvictionPolicy::VolatileLfu.uses_ttl());
        assert!(!EvictionPolicy::AllkeysLfu.uses_ttl());
        assert!(!EvictionPolicy::VolatileRandom.uses_ttl());
        assert!(!EvictionPolicy::AllkeysRandom.uses_ttl());
        assert!(EvictionPolicy::VolatileTtl.uses_ttl());
    }

    #[test]
    fn test_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::VolatileLru.as_str(), "volatile-lru");
        assert_eq!(EvictionPolicy::AllkeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileLfu.as_str(), "volatile-lfu");
        assert_eq!(EvictionPolicy::AllkeysLfu.as_str(), "allkeys-lfu");
        assert_eq!(EvictionPolicy::VolatileRandom.as_str(), "volatile-random");
        assert_eq!(EvictionPolicy::AllkeysRandom.as_str(), "allkeys-random");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            "noeviction".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::NoEviction
        );
        assert_eq!(
            "volatile-lru".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::VolatileLru
        );
        assert_eq!(
            "allkeys-lru".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::AllkeysLru
        );
        assert_eq!(
            "volatile-lfu".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::VolatileLfu
        );
        assert_eq!(
            "allkeys-lfu".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::AllkeysLfu
        );
        assert_eq!(
            "volatile-random".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::VolatileRandom
        );
        assert_eq!(
            "allkeys-random".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::AllkeysRandom
        );
        assert_eq!(
            "volatile-ttl".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::VolatileTtl
        );
    }

    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!(
            "NoEviction".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::NoEviction
        );
        assert_eq!(
            "VOLATILE-LRU".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::VolatileLru
        );
        assert_eq!(
            "AllKeys-LFU".parse::<EvictionPolicy>().unwrap(),
            EvictionPolicy::AllkeysLfu
        );
    }

    #[test]
    fn test_from_str_invalid() {
        let err = "invalid".parse::<EvictionPolicy>().unwrap_err();
        assert_eq!(err.invalid_value, "invalid");
        assert!(err.to_string().contains("invalid eviction policy"));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", EvictionPolicy::NoEviction), "noeviction");
        assert_eq!(format!("{}", EvictionPolicy::VolatileLru), "volatile-lru");
    }
}
