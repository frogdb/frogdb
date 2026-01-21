//! LFU (Least Frequently Used) algorithms.
//!
//! This module implements Redis-compatible LFU algorithms for eviction.
//! Redis uses a probabilistic logarithmic counter that saturates at 255.
//!
//! The counter is incremented probabilistically:
//! - P(increment) = 1 / (counter * log_factor + 1)
//! - This means higher counters are less likely to increment
//! - New keys start with counter = 5 (not immediately evicted)
//!
//! The counter also decays over time:
//! - Counter decreases by 1 for every `decay_time` minutes of idle time
//! - This allows frequently accessed keys in the past to eventually be evicted

/// Probabilistically increment an LFU counter.
///
/// Uses Redis's logarithmic increment algorithm where higher counters
/// are exponentially less likely to increment.
///
/// # Arguments
///
/// * `counter` - Current LFU counter value (0-255)
/// * `log_factor` - Factor controlling increment probability (higher = less likely)
///
/// # Returns
///
/// The new counter value (may be unchanged or incremented by 1).
///
/// # Example
///
/// ```
/// use frogdb_core::eviction::lfu_log_incr;
///
/// let counter = 5;
/// let new_counter = lfu_log_incr(counter, 10);
/// assert!(new_counter >= counter); // Counter never decreases
/// assert!(new_counter <= 255);     // Counter is bounded
/// ```
pub fn lfu_log_incr(counter: u8, log_factor: u8) -> u8 {
    // Counter of 255 means it's already maxed out
    if counter == 255 {
        return 255;
    }

    // Calculate probability of increment
    // P = 1 / (counter * log_factor + 1)
    // Higher counters have lower probability of incrementing
    let base_val = counter.saturating_sub(5) as f64; // Subtract initial value
    let p = 1.0 / (base_val * log_factor as f64 + 1.0);

    // Generate random number and check if we should increment
    let r: f64 = rand::random();
    if r < p {
        counter.saturating_add(1)
    } else {
        counter
    }
}

/// Decay an LFU counter based on idle time.
///
/// Reduces the counter by 1 for every `decay_time` minutes since last access.
/// This ensures that keys that were frequently accessed in the past but are
/// no longer being accessed will eventually become candidates for eviction.
///
/// # Arguments
///
/// * `counter` - Current LFU counter value (0-255)
/// * `minutes_since_access` - Minutes since the key was last accessed
/// * `decay_time` - Number of minutes per counter decrement (0 = no decay)
///
/// # Returns
///
/// The decayed counter value.
///
/// # Example
///
/// ```
/// use frogdb_core::eviction::lfu_decay;
///
/// let counter = 10;
///
/// // After 1 minute with decay_time=1, counter decreases by 1
/// assert_eq!(lfu_decay(counter, 1, 1), 9);
///
/// // After 5 minutes with decay_time=1, counter decreases by 5
/// assert_eq!(lfu_decay(counter, 5, 1), 5);
///
/// // With decay_time=0, no decay occurs
/// assert_eq!(lfu_decay(counter, 100, 0), 10);
///
/// // Counter never goes below 0
/// assert_eq!(lfu_decay(counter, 100, 1), 0);
/// ```
pub fn lfu_decay(counter: u8, minutes_since_access: u64, decay_time: u64) -> u8 {
    // No decay if decay_time is 0
    if decay_time == 0 {
        return counter;
    }

    // Calculate how many decrements based on time
    let decrements = minutes_since_access / decay_time;

    // Saturating subtraction to avoid underflow
    counter.saturating_sub(decrements.min(255) as u8)
}

/// Get the effective LFU value for ranking (lower = more likely to be evicted).
///
/// This combines the counter with decay based on idle time to produce
/// a single value for comparison. Keys with lower values are better
/// candidates for eviction.
///
/// # Arguments
///
/// * `counter` - Current LFU counter value
/// * `minutes_since_access` - Minutes since last access
/// * `decay_time` - Minutes per counter decrement
///
/// # Returns
///
/// Effective LFU value for eviction ranking.
pub fn lfu_get_effective_value(counter: u8, minutes_since_access: u64, decay_time: u64) -> u8 {
    lfu_decay(counter, minutes_since_access, decay_time)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lfu_log_incr_max_value() {
        // Counter at 255 should never increase
        for _ in 0..100 {
            assert_eq!(lfu_log_incr(255, 10), 255);
        }
    }

    #[test]
    fn test_lfu_log_incr_bounds() {
        // Counter should never exceed 255
        let mut counter = 0u8;
        for _ in 0..10000 {
            counter = lfu_log_incr(counter, 10);
            assert!(counter <= 255);
        }
    }

    #[test]
    fn test_lfu_log_incr_probability() {
        // Low counters should increase frequently
        let mut low_increments = 0;
        for _ in 0..1000 {
            if lfu_log_incr(5, 10) > 5 {
                low_increments += 1;
            }
        }

        // High counters should increase rarely
        let mut high_increments = 0;
        for _ in 0..1000 {
            if lfu_log_incr(100, 10) > 100 {
                high_increments += 1;
            }
        }

        // Low counters should increment more often than high counters
        assert!(
            low_increments > high_increments,
            "Low counter increments ({}) should exceed high counter increments ({})",
            low_increments,
            high_increments
        );
    }

    #[test]
    fn test_lfu_decay_no_decay_time() {
        // With decay_time=0, counter should not change
        assert_eq!(lfu_decay(100, 1000, 0), 100);
        assert_eq!(lfu_decay(50, 999999, 0), 50);
    }

    #[test]
    fn test_lfu_decay_basic() {
        // 1 minute with decay_time=1 means 1 decrement
        assert_eq!(lfu_decay(10, 1, 1), 9);

        // 5 minutes with decay_time=1 means 5 decrements
        assert_eq!(lfu_decay(10, 5, 1), 5);

        // 3 minutes with decay_time=2 means 1 decrement (3/2 = 1)
        assert_eq!(lfu_decay(10, 3, 2), 9);
    }

    #[test]
    fn test_lfu_decay_bounds() {
        // Counter should never go below 0
        assert_eq!(lfu_decay(10, 100, 1), 0);
        assert_eq!(lfu_decay(5, 1000, 1), 0);
        assert_eq!(lfu_decay(0, 10, 1), 0);
    }

    #[test]
    fn test_lfu_decay_large_time() {
        // Very large time values should not overflow
        assert_eq!(lfu_decay(100, u64::MAX, 1), 0);
        assert_eq!(lfu_decay(100, 1000000, 1), 0);
    }

    #[test]
    fn test_lfu_get_effective_value() {
        // Should combine counter with decay
        assert_eq!(lfu_get_effective_value(10, 0, 1), 10);
        assert_eq!(lfu_get_effective_value(10, 5, 1), 5);
        assert_eq!(lfu_get_effective_value(10, 0, 0), 10);
    }
}
