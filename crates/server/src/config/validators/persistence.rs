//! Persistence-related configuration validators.

use super::{ConfigValidator, ValidationResult};
use crate::config::Config;

/// Validates that snapshot and data directories are separate.
///
/// Rule: `snapshot.snapshot_dir` must not equal `persistence.data_dir`
pub struct DirectorySeparationValidator;

impl ConfigValidator for DirectorySeparationValidator {
    fn name(&self) -> &'static str {
        "directory-separation"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        // Only validate if both features are enabled
        if !config.persistence.enabled || config.snapshot.snapshot_interval_secs == 0 {
            return ValidationResult::Ok;
        }

        // Canonicalize paths for comparison (handle relative paths, trailing slashes, etc.)
        let data_dir = config.persistence.data_dir.as_path();
        let snapshot_dir = config.snapshot.snapshot_dir.as_path();

        // Simple equality check first
        if data_dir == snapshot_dir {
            return ValidationResult::Error(format!(
                "snapshot.snapshot_dir ({}) must be different from persistence.data_dir ({}); \
                 storing snapshots in the data directory can cause conflicts",
                snapshot_dir.display(),
                data_dir.display()
            ));
        }

        // Check if one is a subdirectory of the other
        if snapshot_dir.starts_with(data_dir) {
            return ValidationResult::Error(format!(
                "snapshot.snapshot_dir ({}) is inside persistence.data_dir ({}); \
                 this can cause conflicts and data corruption",
                snapshot_dir.display(),
                data_dir.display()
            ));
        }

        if data_dir.starts_with(snapshot_dir) {
            return ValidationResult::Error(format!(
                "persistence.data_dir ({}) is inside snapshot.snapshot_dir ({}); \
                 this can cause conflicts and data corruption",
                data_dir.display(),
                snapshot_dir.display()
            ));
        }

        ValidationResult::Ok
    }
}

/// Warns when sync_interval_ms is set but durability_mode is not "periodic".
///
/// Rule: `sync_interval_ms` is only meaningful when `durability_mode` = "periodic"
pub struct SyncIntervalIgnoredValidator;

impl ConfigValidator for SyncIntervalIgnoredValidator {
    fn name(&self) -> &'static str {
        "sync-interval-ignored"
    }

    fn validate(&self, config: &Config) -> ValidationResult {
        if !config.persistence.enabled {
            return ValidationResult::Ok;
        }

        let mode = config.persistence.durability_mode.to_lowercase();
        let default_interval = 1000; // default value from config

        // If mode is not periodic and interval is explicitly set to something other than default
        if mode != "periodic" && config.persistence.sync_interval_ms != default_interval {
            return ValidationResult::Info(format!(
                "persistence.sync_interval_ms ({}) is set but durability_mode is '{}'; \
                 sync_interval_ms is only used when durability_mode is 'periodic'",
                config.persistence.sync_interval_ms, mode
            ));
        }

        ValidationResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_directory_separation_different_dirs() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.data_dir = PathBuf::from("/data/frogdb");
        config.snapshot.snapshot_dir = PathBuf::from("/snapshots");
        config.snapshot.snapshot_interval_secs = 3600;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_directory_separation_same_dir() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.data_dir = PathBuf::from("/data");
        config.snapshot.snapshot_dir = PathBuf::from("/data");
        config.snapshot.snapshot_interval_secs = 3600;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_directory_separation_snapshot_inside_data() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.data_dir = PathBuf::from("/data");
        config.snapshot.snapshot_dir = PathBuf::from("/data/snapshots");
        config.snapshot.snapshot_interval_secs = 3600;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_directory_separation_data_inside_snapshot() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.data_dir = PathBuf::from("/snapshots/data");
        config.snapshot.snapshot_dir = PathBuf::from("/snapshots");
        config.snapshot.snapshot_interval_secs = 3600;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_error());
    }

    #[test]
    fn test_directory_separation_persistence_disabled() {
        let mut config = Config::default();
        config.persistence.enabled = false;
        config.persistence.data_dir = PathBuf::from("/data");
        config.snapshot.snapshot_dir = PathBuf::from("/data");
        config.snapshot.snapshot_interval_secs = 3600;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_directory_separation_snapshots_disabled() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.data_dir = PathBuf::from("/data");
        config.snapshot.snapshot_dir = PathBuf::from("/data");
        config.snapshot.snapshot_interval_secs = 0;

        let validator = DirectorySeparationValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_sync_interval_ignored_periodic_mode() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.durability_mode = "periodic".to_string();
        config.persistence.sync_interval_ms = 500;

        let validator = SyncIntervalIgnoredValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_sync_interval_ignored_async_mode_custom_interval() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.durability_mode = "async".to_string();
        config.persistence.sync_interval_ms = 500;

        let validator = SyncIntervalIgnoredValidator;
        assert!(validator.validate(&config).is_info());
    }

    #[test]
    fn test_sync_interval_ignored_sync_mode_custom_interval() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.durability_mode = "sync".to_string();
        config.persistence.sync_interval_ms = 500;

        let validator = SyncIntervalIgnoredValidator;
        assert!(validator.validate(&config).is_info());
    }

    #[test]
    fn test_sync_interval_ignored_async_mode_default_interval() {
        let mut config = Config::default();
        config.persistence.enabled = true;
        config.persistence.durability_mode = "async".to_string();
        // Keep default interval (1000)

        let validator = SyncIntervalIgnoredValidator;
        assert!(validator.validate(&config).is_ok());
    }

    #[test]
    fn test_sync_interval_ignored_persistence_disabled() {
        let mut config = Config::default();
        config.persistence.enabled = false;
        config.persistence.durability_mode = "async".to_string();
        config.persistence.sync_interval_ms = 500;

        let validator = SyncIntervalIgnoredValidator;
        assert!(validator.validate(&config).is_ok());
    }
}
