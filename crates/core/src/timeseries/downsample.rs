//! Downsampling abstraction for time series.
//!
//! Provides a trait and noop implementation for automatic downsampling rules.
//! In a future implementation, the DownsampleManager would automatically
//! aggregate samples from source time series into destination time series.

use bytes::Bytes;
use std::collections::HashMap;
use thiserror::Error;

use crate::timeseries::aggregation::Aggregation;

/// Errors that can occur during downsampling operations.
#[derive(Debug, Error)]
pub enum DownsampleError {
    /// Rule already exists.
    #[error("Downsampling rule already exists")]
    RuleAlreadyExists,

    /// Rule not found.
    #[error("Downsampling rule not found")]
    RuleNotFound,

    /// Source key not found.
    #[error("Source time series not found")]
    SourceNotFound,

    /// Destination key not found.
    #[error("Destination time series not found")]
    DestinationNotFound,

    /// Invalid configuration.
    #[error("Invalid downsampling configuration: {0}")]
    InvalidConfig(String),
}

/// A downsampling rule configuration.
#[derive(Debug, Clone)]
pub struct DownsampleRule {
    /// Destination time series key.
    pub dest_key: Bytes,
    /// Time bucket size in milliseconds.
    pub bucket_duration_ms: i64,
    /// Aggregation type.
    pub aggregation: Aggregation,
}

impl DownsampleRule {
    /// Create a new downsampling rule.
    pub fn new(dest_key: Bytes, bucket_duration_ms: i64, aggregation: Aggregation) -> Self {
        Self {
            dest_key,
            bucket_duration_ms,
            aggregation,
        }
    }
}

/// Trait for managing downsampling rules.
///
/// Implementors can choose to either:
/// - Execute downsampling in real-time (on each sample add)
/// - Execute downsampling periodically in the background
/// - Do nothing (noop implementation)
pub trait DownsampleManager: Send + Sync {
    /// Called when a sample is added to a source time series.
    ///
    /// The implementation may aggregate and write to destination series.
    fn on_sample_add(&self, source_key: &Bytes, timestamp: i64, value: f64);

    /// Add a downsampling rule.
    fn add_rule(&mut self, source_key: Bytes, rule: DownsampleRule) -> Result<(), DownsampleError>;

    /// Remove a downsampling rule.
    fn remove_rule(&mut self, source_key: &Bytes, dest_key: &Bytes) -> Result<(), DownsampleError>;

    /// Get all rules for a source key.
    fn get_rules(&self, source_key: &Bytes) -> Vec<DownsampleRule>;

    /// Get all source keys that have rules.
    fn get_source_keys(&self) -> Vec<Bytes>;
}

/// Noop downsampling manager that stores rules but doesn't execute them.
///
/// This implementation maintains the rule configuration but does not
/// automatically aggregate samples. It follows the pattern from
/// `crates/core/src/noop.rs` for features to be implemented later.
#[derive(Debug, Default)]
pub struct NoopDownsampleManager {
    /// Rules by source key.
    rules: HashMap<Bytes, Vec<DownsampleRule>>,
}

impl NoopDownsampleManager {
    /// Create a new noop downsample manager.
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }
}

impl DownsampleManager for NoopDownsampleManager {
    fn on_sample_add(&self, _source_key: &Bytes, _timestamp: i64, _value: f64) {
        // Noop: we don't automatically downsample
        tracing::trace!("Noop downsample on_sample_add");
    }

    fn add_rule(&mut self, source_key: Bytes, rule: DownsampleRule) -> Result<(), DownsampleError> {
        let rules = self.rules.entry(source_key).or_default();

        // Check if rule to same destination already exists
        if rules.iter().any(|r| r.dest_key == rule.dest_key) {
            return Err(DownsampleError::RuleAlreadyExists);
        }

        rules.push(rule);
        tracing::trace!("Noop downsample add_rule");
        Ok(())
    }

    fn remove_rule(&mut self, source_key: &Bytes, dest_key: &Bytes) -> Result<(), DownsampleError> {
        if let Some(rules) = self.rules.get_mut(source_key) {
            let initial_len = rules.len();
            rules.retain(|r| r.dest_key != *dest_key);

            if rules.len() == initial_len {
                return Err(DownsampleError::RuleNotFound);
            }

            if rules.is_empty() {
                self.rules.remove(source_key);
            }

            tracing::trace!("Noop downsample remove_rule");
            Ok(())
        } else {
            Err(DownsampleError::RuleNotFound)
        }
    }

    fn get_rules(&self, source_key: &Bytes) -> Vec<DownsampleRule> {
        self.rules.get(source_key).cloned().unwrap_or_default()
    }

    fn get_source_keys(&self) -> Vec<Bytes> {
        self.rules.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_manager_basic() {
        let mut manager = NoopDownsampleManager::new();

        let rule = DownsampleRule::new(
            Bytes::from("dest:key"),
            60000, // 1 minute buckets
            Aggregation::Avg,
        );

        manager.add_rule(Bytes::from("source:key"), rule).unwrap();

        let rules = manager.get_rules(&Bytes::from("source:key"));
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].dest_key, Bytes::from("dest:key"));
        assert_eq!(rules[0].bucket_duration_ms, 60000);
    }

    #[test]
    fn test_noop_manager_duplicate_rule() {
        let mut manager = NoopDownsampleManager::new();

        let rule1 = DownsampleRule::new(Bytes::from("dest:key"), 60000, Aggregation::Avg);
        let rule2 = DownsampleRule::new(Bytes::from("dest:key"), 120000, Aggregation::Sum);

        manager.add_rule(Bytes::from("source:key"), rule1).unwrap();

        // Adding rule to same destination should fail
        let result = manager.add_rule(Bytes::from("source:key"), rule2);
        assert!(matches!(result, Err(DownsampleError::RuleAlreadyExists)));
    }

    #[test]
    fn test_noop_manager_multiple_rules() {
        let mut manager = NoopDownsampleManager::new();

        let rule1 = DownsampleRule::new(Bytes::from("dest:1min"), 60000, Aggregation::Avg);
        let rule2 = DownsampleRule::new(Bytes::from("dest:5min"), 300000, Aggregation::Avg);

        manager.add_rule(Bytes::from("source:key"), rule1).unwrap();
        manager.add_rule(Bytes::from("source:key"), rule2).unwrap();

        let rules = manager.get_rules(&Bytes::from("source:key"));
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_noop_manager_remove_rule() {
        let mut manager = NoopDownsampleManager::new();

        let rule = DownsampleRule::new(Bytes::from("dest:key"), 60000, Aggregation::Avg);
        manager.add_rule(Bytes::from("source:key"), rule).unwrap();

        manager
            .remove_rule(&Bytes::from("source:key"), &Bytes::from("dest:key"))
            .unwrap();

        let rules = manager.get_rules(&Bytes::from("source:key"));
        assert!(rules.is_empty());
    }

    #[test]
    fn test_noop_manager_remove_nonexistent() {
        let mut manager = NoopDownsampleManager::new();

        let result = manager.remove_rule(&Bytes::from("source:key"), &Bytes::from("dest:key"));
        assert!(matches!(result, Err(DownsampleError::RuleNotFound)));
    }

    #[test]
    fn test_noop_manager_get_source_keys() {
        let mut manager = NoopDownsampleManager::new();

        let rule1 = DownsampleRule::new(Bytes::from("dest:1"), 60000, Aggregation::Avg);
        let rule2 = DownsampleRule::new(Bytes::from("dest:2"), 60000, Aggregation::Avg);

        manager.add_rule(Bytes::from("source:a"), rule1).unwrap();
        manager.add_rule(Bytes::from("source:b"), rule2).unwrap();

        let sources = manager.get_source_keys();
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&Bytes::from("source:a")));
        assert!(sources.contains(&Bytes::from("source:b")));
    }

    #[test]
    fn test_on_sample_add_is_noop() {
        let manager = NoopDownsampleManager::new();
        // This should not panic or do anything
        manager.on_sample_add(&Bytes::from("key"), 1000, 42.0);
    }
}
