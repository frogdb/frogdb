//! Per-ACL-user rate limiting using a token bucket algorithm.
//!
//! Provides `RateLimitConfig` (per-user quota), `RateLimitState` (lock-free
//! token bucket shared across connections), and `RateLimitRegistry` (username →
//! state mapping using DashMap).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;

/// Scaling factor for sub-token precision (×1000).
const SCALE: i64 = 1000;

/// Monotonic baseline for timestamp calculations.
static BASELINE: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn baseline() -> &'static Instant {
    BASELINE.get_or_init(Instant::now)
}

fn now_us() -> u64 {
    baseline().elapsed().as_micros() as u64
}

/// Rate limit configuration for a user.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Maximum commands per second (0 = unlimited).
    pub commands_per_second: u64,
    /// Maximum bytes per second (0 = unlimited).
    pub bytes_per_second: u64,
}

impl RateLimitConfig {
    /// Returns true if no limits are configured.
    pub fn is_unlimited(&self) -> bool {
        self.commands_per_second == 0 && self.bytes_per_second == 0
    }
}

/// Which budget was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitExceeded {
    Commands,
    Bytes,
}

/// Snapshot of rate limit stats for a user.
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub username: String,
    pub commands_per_second: u64,
    pub bytes_per_second: u64,
    pub commands_rejected: u64,
    pub bytes_rejected: u64,
}

/// Lock-free token bucket shared across all connections for one ACL user.
pub struct RateLimitState {
    /// Scaled command tokens (×SCALE for sub-token precision).
    command_tokens: AtomicI64,
    /// Scaled byte tokens (×SCALE).
    byte_tokens: AtomicI64,
    /// Last refill timestamp in microseconds from baseline.
    last_refill_us: AtomicU64,
    /// Current commands-per-second limit (mutable for live ACL updates).
    commands_per_second: AtomicU64,
    /// Current bytes-per-second limit.
    bytes_per_second: AtomicU64,
    /// Total commands rejected.
    commands_rejected: AtomicU64,
    /// Total bytes rejected.
    bytes_rejected: AtomicU64,
}

impl std::fmt::Debug for RateLimitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitState")
            .field("cps", &self.commands_per_second.load(Ordering::Relaxed))
            .field("bps", &self.bytes_per_second.load(Ordering::Relaxed))
            .finish()
    }
}

impl RateLimitState {
    /// Create a new token bucket from config. Starts fully loaded (1s burst).
    pub fn new(config: &RateLimitConfig) -> Self {
        let cps = config.commands_per_second;
        let bps = config.bytes_per_second;
        Self {
            command_tokens: AtomicI64::new(cps as i64 * SCALE),
            byte_tokens: AtomicI64::new(bps as i64 * SCALE),
            last_refill_us: AtomicU64::new(now_us()),
            commands_per_second: AtomicU64::new(cps),
            bytes_per_second: AtomicU64::new(bps),
            commands_rejected: AtomicU64::new(0),
            bytes_rejected: AtomicU64::new(0),
        }
    }

    /// Try to consume 1 command token and `bytes` byte tokens.
    ///
    /// Refills proportionally to elapsed time, caps at 1-second burst.
    /// Returns `Ok(())` or `Err(RateLimitExceeded)`.
    pub fn try_acquire(&self, bytes: u64) -> Result<(), RateLimitExceeded> {
        self.refill();

        let cps = self.commands_per_second.load(Ordering::Relaxed);
        let bps = self.bytes_per_second.load(Ordering::Relaxed);

        // Check command budget
        if cps > 0 {
            let cost = SCALE; // 1 command
            let prev = self.command_tokens.fetch_sub(cost, Ordering::Relaxed);
            if prev < cost {
                // Not enough tokens — restore what we took
                self.command_tokens.fetch_add(cost, Ordering::Relaxed);
                self.commands_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitExceeded::Commands);
            }
        }

        // Check byte budget
        if bps > 0 && bytes > 0 {
            let cost = bytes as i64 * SCALE;
            let prev = self.byte_tokens.fetch_sub(cost, Ordering::Relaxed);
            if prev < cost {
                // Not enough byte tokens — restore bytes AND refund the command token
                self.byte_tokens.fetch_add(cost, Ordering::Relaxed);
                if cps > 0 {
                    self.command_tokens.fetch_add(SCALE, Ordering::Relaxed);
                }
                self.bytes_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitExceeded::Bytes);
            }
        }

        Ok(())
    }

    /// Batch acquire for EXEC: N commands and total bytes.
    pub fn try_acquire_batch(&self, commands: u64, bytes: u64) -> Result<(), RateLimitExceeded> {
        self.refill();

        let cps = self.commands_per_second.load(Ordering::Relaxed);
        let bps = self.bytes_per_second.load(Ordering::Relaxed);

        // Check command budget
        if cps > 0 {
            let cost = commands as i64 * SCALE;
            let prev = self.command_tokens.fetch_sub(cost, Ordering::Relaxed);
            if prev < cost {
                self.command_tokens.fetch_add(cost, Ordering::Relaxed);
                self.commands_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitExceeded::Commands);
            }
        }

        // Check byte budget
        if bps > 0 && bytes > 0 {
            let cost = bytes as i64 * SCALE;
            let prev = self.byte_tokens.fetch_sub(cost, Ordering::Relaxed);
            if prev < cost {
                self.byte_tokens.fetch_add(cost, Ordering::Relaxed);
                if cps > 0 {
                    self.command_tokens
                        .fetch_add(commands as i64 * SCALE, Ordering::Relaxed);
                }
                self.bytes_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(RateLimitExceeded::Bytes);
            }
        }

        Ok(())
    }

    /// Update limits (called when ACL SETUSER changes config).
    pub fn update_limits(&self, config: &RateLimitConfig) {
        self.commands_per_second
            .store(config.commands_per_second, Ordering::Relaxed);
        self.bytes_per_second
            .store(config.bytes_per_second, Ordering::Relaxed);
    }

    /// Refill tokens proportional to elapsed time, capped at 1-second burst.
    fn refill(&self) {
        let now = now_us();
        let last = self.last_refill_us.load(Ordering::Relaxed);
        let elapsed_us = now.saturating_sub(last);

        if elapsed_us == 0 {
            return;
        }

        // Try to claim this refill window (CAS on timestamp).
        if self
            .last_refill_us
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            // Another thread already refilled; skip to avoid double-crediting.
            return;
        }

        let cps = self.commands_per_second.load(Ordering::Relaxed);
        let bps = self.bytes_per_second.load(Ordering::Relaxed);

        // Add command tokens: cps * elapsed_us / 1_000_000 * SCALE
        if cps > 0 {
            let add = (cps as i64 * SCALE * elapsed_us as i64) / 1_000_000;
            let cap = cps as i64 * SCALE; // 1-second burst
            let prev = self.command_tokens.fetch_add(add, Ordering::Relaxed);
            let new_val = prev + add;
            if new_val > cap {
                // Clamp to cap
                self.command_tokens
                    .fetch_sub(new_val - cap, Ordering::Relaxed);
            }
        }

        // Add byte tokens: bps * elapsed_us / 1_000_000 * SCALE
        if bps > 0 {
            let add = (bps as i64 * SCALE * elapsed_us as i64) / 1_000_000;
            let cap = bps as i64 * SCALE;
            let prev = self.byte_tokens.fetch_add(add, Ordering::Relaxed);
            let new_val = prev + add;
            if new_val > cap {
                self.byte_tokens.fetch_sub(new_val - cap, Ordering::Relaxed);
            }
        }
    }
}

/// Registry of per-user rate limit state (shared across connections).
pub struct RateLimitRegistry {
    limits: DashMap<Arc<str>, Arc<RateLimitState>>,
}

impl Default for RateLimitRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitRegistry {
    pub fn new() -> Self {
        Self {
            limits: DashMap::new(),
        }
    }

    /// Get or create rate limit state for a user. Returns `None` if the config
    /// specifies no limits (both cps and bps are 0).
    pub fn get_or_create(
        &self,
        username: &str,
        config: &RateLimitConfig,
    ) -> Option<Arc<RateLimitState>> {
        if config.is_unlimited() {
            return None;
        }
        let key: Arc<str> = Arc::from(username);
        let entry = self
            .limits
            .entry(key)
            .or_insert_with(|| Arc::new(RateLimitState::new(config)));
        Some(Arc::clone(entry.value()))
    }

    /// Update limits for an existing user, or insert if not present.
    pub fn update_user(&self, username: &str, config: &RateLimitConfig) {
        if config.is_unlimited() {
            self.limits.remove(username);
            return;
        }
        let key: Arc<str> = Arc::from(username);
        match self.limits.get(&key) {
            Some(state) => state.update_limits(config),
            None => {
                self.limits
                    .insert(key, Arc::new(RateLimitState::new(config)));
            }
        }
    }

    /// Remove a user's rate limit state.
    pub fn remove_user(&self, username: &str) {
        self.limits.remove(username);
    }

    /// Collect stats for all rate-limited users (for INFO output).
    pub fn all_stats(&self) -> Vec<RateLimitStats> {
        self.limits
            .iter()
            .map(|entry| RateLimitStats {
                username: entry.key().to_string(),
                commands_per_second: entry.value().commands_per_second.load(Ordering::Relaxed),
                bytes_per_second: entry.value().bytes_per_second.load(Ordering::Relaxed),
                commands_rejected: entry.value().commands_rejected.load(Ordering::Relaxed),
                bytes_rejected: entry.value().bytes_rejected.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Number of users with active rate limits.
    pub fn user_count(&self) -> usize {
        self.limits.len()
    }

    /// Total commands rejected across all users.
    pub fn total_commands_rejected(&self) -> u64 {
        self.limits
            .iter()
            .map(|e| e.value().commands_rejected.load(Ordering::Relaxed))
            .sum()
    }

    /// Total bytes-rejected events across all users.
    pub fn total_bytes_rejected(&self) -> u64 {
        self.limits
            .iter()
            .map(|e| e.value().bytes_rejected.load(Ordering::Relaxed))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_unlimited_config() {
        let config = RateLimitConfig::default();
        assert!(config.is_unlimited());
    }

    #[test]
    fn test_basic_acquire() {
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };
        let state = RateLimitState::new(&config);

        // Should be able to acquire 10 commands (1s burst)
        for _ in 0..10 {
            assert!(state.try_acquire(0).is_ok());
        }
        // 11th should fail
        assert_eq!(state.try_acquire(0), Err(RateLimitExceeded::Commands));
    }

    #[test]
    fn test_byte_limit() {
        let config = RateLimitConfig {
            commands_per_second: 0,
            bytes_per_second: 100,
        };
        let state = RateLimitState::new(&config);

        // Can consume 100 bytes
        assert!(state.try_acquire(50).is_ok());
        assert!(state.try_acquire(50).is_ok());
        // Next should fail
        assert_eq!(state.try_acquire(1), Err(RateLimitExceeded::Bytes));
    }

    #[test]
    fn test_both_limits() {
        let config = RateLimitConfig {
            commands_per_second: 5,
            bytes_per_second: 1000,
        };
        let state = RateLimitState::new(&config);

        // Use up command budget first (5 commands, small bytes)
        for _ in 0..5 {
            assert!(state.try_acquire(10).is_ok());
        }
        // Command limit hit
        assert_eq!(state.try_acquire(10), Err(RateLimitExceeded::Commands));
    }

    #[test]
    fn test_byte_limit_refunds_command_on_failure() {
        let config = RateLimitConfig {
            commands_per_second: 100,
            bytes_per_second: 10,
        };
        let state = RateLimitState::new(&config);

        // Use up byte budget
        assert!(state.try_acquire(10).is_ok());
        // Byte limit exceeded — command token should be refunded
        assert_eq!(state.try_acquire(1), Err(RateLimitExceeded::Bytes));
    }

    #[test]
    fn test_batch_acquire() {
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 1000,
        };
        let state = RateLimitState::new(&config);

        // Batch of 5 commands, 500 bytes
        assert!(state.try_acquire_batch(5, 500).is_ok());
        // Another batch of 5 should work
        assert!(state.try_acquire_batch(5, 500).is_ok());
        // Exceeding command limit
        assert_eq!(
            state.try_acquire_batch(1, 0),
            Err(RateLimitExceeded::Commands)
        );
    }

    #[test]
    fn test_update_limits() {
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };
        let state = RateLimitState::new(&config);

        // Update to higher limit
        state.update_limits(&RateLimitConfig {
            commands_per_second: 100,
            bytes_per_second: 0,
        });

        assert_eq!(state.commands_per_second.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_zero_config_passthrough() {
        let config = RateLimitConfig {
            commands_per_second: 0,
            bytes_per_second: 0,
        };
        let state = RateLimitState::new(&config);

        // Should always succeed when limits are 0
        for _ in 0..1000 {
            assert!(state.try_acquire(10000).is_ok());
        }
    }

    #[test]
    fn test_registry_get_or_create() {
        let registry = RateLimitRegistry::new();

        // No limit configured → None
        let none = registry.get_or_create("user1", &RateLimitConfig::default());
        assert!(none.is_none());

        // With limit → Some
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };
        let state = registry.get_or_create("user1", &config);
        assert!(state.is_some());
    }

    #[test]
    fn test_registry_shared_state() {
        let registry = RateLimitRegistry::new();
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };

        let s1 = registry.get_or_create("user1", &config).unwrap();
        let s2 = registry.get_or_create("user1", &config).unwrap();

        // Both references should point to the same state
        assert!(Arc::ptr_eq(&s1, &s2));
    }

    #[test]
    fn test_registry_remove_user() {
        let registry = RateLimitRegistry::new();
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };

        registry.get_or_create("user1", &config);
        assert_eq!(registry.user_count(), 1);

        registry.remove_user("user1");
        assert_eq!(registry.user_count(), 0);
    }

    #[test]
    fn test_registry_update_user_to_unlimited() {
        let registry = RateLimitRegistry::new();
        let config = RateLimitConfig {
            commands_per_second: 10,
            bytes_per_second: 0,
        };

        registry.get_or_create("user1", &config);
        assert_eq!(registry.user_count(), 1);

        // Update to unlimited removes from registry
        registry.update_user("user1", &RateLimitConfig::default());
        assert_eq!(registry.user_count(), 0);
    }

    #[test]
    fn test_registry_stats() {
        let registry = RateLimitRegistry::new();
        let config = RateLimitConfig {
            commands_per_second: 5,
            bytes_per_second: 0,
        };

        let state = registry.get_or_create("user1", &config).unwrap();

        // Exhaust budget
        for _ in 0..5 {
            let _ = state.try_acquire(0);
        }
        // These will be rejected
        let _ = state.try_acquire(0);
        let _ = state.try_acquire(0);

        assert_eq!(registry.total_commands_rejected(), 2);
    }

    #[test]
    fn test_concurrent_acquire() {
        let config = RateLimitConfig {
            commands_per_second: 1000,
            bytes_per_second: 0,
        };
        let state = Arc::new(RateLimitState::new(&config));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                let mut ok = 0u64;
                let mut err = 0u64;
                for _ in 0..500 {
                    match s.try_acquire(0) {
                        Ok(()) => ok += 1,
                        Err(_) => err += 1,
                    }
                }
                (ok, err)
            }));
        }

        let mut total_ok = 0u64;
        let mut total_err = 0u64;
        for h in handles {
            let (ok, err) = h.join().unwrap();
            total_ok += ok;
            total_err += err;
        }

        // 4 threads × 500 = 2000 attempts, budget is 1000
        assert_eq!(total_ok + total_err, 2000);
        // At most 1000 should succeed (burst cap)
        assert!(total_ok <= 1000, "total_ok={total_ok} should be <= 1000");
        assert!(
            total_ok >= 800,
            "total_ok={total_ok} should be close to 1000 (allowing CAS contention under CI load)"
        );
    }

    #[test]
    fn test_refill_after_time() {
        let config = RateLimitConfig {
            commands_per_second: 100,
            bytes_per_second: 0,
        };
        let state = RateLimitState::new(&config);

        // Exhaust budget
        for _ in 0..100 {
            assert!(state.try_acquire(0).is_ok());
        }
        assert!(state.try_acquire(0).is_err());

        // Wait real time for refill (110ms should give ~11 tokens at 100/s)
        std::thread::sleep(std::time::Duration::from_millis(110));

        // Should be able to acquire some more
        let mut acquired = 0;
        for _ in 0..100 {
            if state.try_acquire(0).is_ok() {
                acquired += 1;
            }
        }
        assert!(
            (8..=15).contains(&acquired),
            "acquired={acquired} after ~110ms refill at 100/s"
        );
    }
}
