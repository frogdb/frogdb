//! File-based bundle storage with TTL and capacity limits.

use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::Serialize;

use super::BundleConfig;

/// Metadata about a stored bundle.
#[derive(Debug, Clone, Serialize)]
pub struct BundleInfo {
    /// Bundle ID.
    pub id: String,
    /// Creation timestamp (Unix seconds).
    pub created_at: u64,
    /// Size in bytes.
    pub size_bytes: u64,
}

/// File-based storage for diagnostic bundles.
pub struct BundleStore {
    config: BundleConfig,
}

impl BundleStore {
    /// Create a new bundle store, creating the directory if needed.
    pub fn new(config: BundleConfig) -> Self {
        let _ = fs::create_dir_all(&config.directory);
        Self { config }
    }

    /// List all stored bundles, sorted by creation time (newest first).
    pub fn list(&self) -> Vec<BundleInfo> {
        let dir = &self.config.directory;
        let Ok(entries) = fs::read_dir(dir) else {
            return Vec::new();
        };

        let mut bundles: Vec<BundleInfo> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "zip"))
            .filter_map(|e| {
                let path = e.path();
                let metadata = fs::metadata(&path).ok()?;
                let id = path.file_stem()?.to_string_lossy().to_string();
                let created = metadata.modified().ok()?;
                let created_at = created
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                Some(BundleInfo {
                    id,
                    created_at,
                    size_bytes: metadata.len(),
                })
            })
            .collect();

        // Sort newest first
        bundles.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        bundles
    }

    /// Get a stored bundle by ID.
    pub fn get(&self, id: &str) -> Option<Vec<u8>> {
        let path = self.bundle_path(id);
        fs::read(path).ok()
    }

    /// Store a bundle, enforcing capacity and TTL limits.
    pub fn store(&self, id: &str, data: &[u8]) -> Result<(), std::io::Error> {
        let _ = fs::create_dir_all(&self.config.directory);

        // Clean up expired bundles
        self.cleanup_expired();

        // Enforce max_bundles limit
        self.enforce_capacity();

        // Write the bundle
        let path = self.bundle_path(id);
        fs::write(path, data)
    }

    /// Get the file path for a bundle.
    fn bundle_path(&self, id: &str) -> PathBuf {
        self.config.directory.join(format!("{}.zip", id))
    }

    /// Remove bundles older than the TTL.
    fn cleanup_expired(&self) {
        let Ok(entries) = fs::read_dir(&self.config.directory) else {
            return;
        };

        let now = SystemTime::now();

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "zip")
                && let Ok(metadata) = fs::metadata(&path)
                && let Ok(modified) = metadata.modified()
                && let Ok(age) = now.duration_since(modified)
                && age.as_secs() > self.config.bundle_ttl_secs
            {
                let _ = fs::remove_file(&path);
            }
        }
    }

    /// Enforce the maximum bundle count by removing oldest bundles.
    fn enforce_capacity(&self) {
        let mut bundles = self.list();
        while bundles.len() >= self.config.max_bundles {
            if let Some(oldest) = bundles.pop() {
                let _ = fs::remove_file(self.bundle_path(&oldest.id));
            }
        }
    }
}
