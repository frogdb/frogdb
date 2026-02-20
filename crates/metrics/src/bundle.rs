//! Debug diagnostic bundles for FrogDB.
//!
//! This module provides functionality to generate diagnostic bundles - ZIP archives
//! containing comprehensive diagnostic information for troubleshooting.
//!
//! Inspired by CockroachDB statement diagnostics.
//!
//! ## Bundle Contents
//!
//! Each bundle contains:
//! - `manifest.json` - Metadata about the bundle
//! - `config.json` - Current server configuration
//! - `info.txt` - Full INFO command output
//! - `slowlog.json` - Recent slowlog entries
//! - `memory_stats.json` - Per-shard memory statistics
//! - `traces.json` - Recent trace entries
//! - `cluster.json` - Cluster state information
//! - `server_status.json` - STATUS JSON output

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write as IoWrite};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use zip::write::SimpleFileOptions;
use zip::ZipWriter;

use frogdb_core::{ShardMemoryStats, ShardMessage, SlowLogEntry};

use crate::tracing::{RecentTraceEntry, SharedTracer};
use crate::ServerStatus;

/// Default directory for bundle storage.
pub const DEFAULT_BUNDLE_DIRECTORY: &str = "/var/lib/frogdb/bundles";

/// Default maximum number of bundles to retain.
pub const DEFAULT_MAX_BUNDLES: usize = 10;

/// Default bundle TTL in seconds (1 hour).
pub const DEFAULT_BUNDLE_TTL_SECS: u64 = 3600;

/// Default maximum slowlog entries to include in bundle.
pub const DEFAULT_MAX_SLOWLOG_ENTRIES: usize = 1000;

/// Default maximum trace entries to include in bundle.
pub const DEFAULT_MAX_TRACE_ENTRIES: usize = 500;

/// Configuration for bundle generation and storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleConfig {
    /// Directory for storing bundles.
    pub directory: PathBuf,
    /// Maximum number of bundles to retain.
    pub max_bundles: usize,
    /// Bundle TTL in seconds before automatic cleanup.
    pub bundle_ttl_secs: u64,
    /// Maximum slowlog entries to include in bundles.
    pub max_slowlog_entries: usize,
    /// Maximum trace entries to include in bundles.
    pub max_trace_entries: usize,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from(DEFAULT_BUNDLE_DIRECTORY),
            max_bundles: DEFAULT_MAX_BUNDLES,
            bundle_ttl_secs: DEFAULT_BUNDLE_TTL_SECS,
            max_slowlog_entries: DEFAULT_MAX_SLOWLOG_ENTRIES,
            max_trace_entries: DEFAULT_MAX_TRACE_ENTRIES,
        }
    }
}

/// Manifest metadata for a bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleManifest {
    /// Unique bundle ID.
    pub id: String,
    /// ISO 8601 timestamp when the bundle was created.
    pub created_at: String,
    /// Unix timestamp when the bundle was created.
    pub created_at_unix: u64,
    /// FrogDB version.
    pub frogdb_version: String,
    /// Duration of collection in seconds (0 for instant).
    pub collection_duration_secs: u64,
    /// List of files in the bundle.
    pub files: Vec<String>,
    /// Collection mode (instant or duration).
    pub mode: String,
}

/// Information about a stored bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleInfo {
    /// Unique bundle ID.
    pub id: String,
    /// ISO 8601 timestamp when the bundle was created.
    pub created_at: String,
    /// Unix timestamp when the bundle was created.
    pub created_at_unix: u64,
    /// Size of the bundle in bytes.
    pub size_bytes: u64,
}

/// Metadata for a stored bundle (internal use).
#[derive(Debug, Clone)]
struct BundleMetadata {
    path: PathBuf,
    created_at: u64,
    size_bytes: u64,
}

/// Manages bundle storage on disk with TTL cleanup.
pub struct BundleStore {
    config: BundleConfig,
    /// Index of stored bundles: id -> metadata.
    index: RwLock<HashMap<String, BundleMetadata>>,
}

impl BundleStore {
    /// Create a new bundle store.
    pub fn new(config: BundleConfig) -> Self {
        let store = Self {
            config,
            index: RwLock::new(HashMap::new()),
        };
        // Load existing bundles from disk
        store.load_existing_bundles();
        store
    }

    /// Load existing bundles from the storage directory.
    fn load_existing_bundles(&self) {
        if !self.config.directory.exists() {
            return;
        }

        let Ok(entries) = std::fs::read_dir(&self.config.directory) else {
            return;
        };

        let mut index = self.index.write();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "zip").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(metadata) = entry.metadata() {
                        let created_at = metadata
                            .created()
                            .or_else(|_| metadata.modified())
                            .ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);

                        index.insert(
                            stem.to_string(),
                            BundleMetadata {
                                path: path.clone(),
                                created_at,
                                size_bytes: metadata.len(),
                            },
                        );
                    }
                }
            }
        }
    }

    /// Store a bundle on disk.
    pub fn store(&self, id: &str, data: &[u8]) -> std::io::Result<()> {
        // Ensure directory exists
        std::fs::create_dir_all(&self.config.directory)?;

        let path = self.config.directory.join(format!("{}.zip", id));
        std::fs::write(&path, data)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        {
            let mut index = self.index.write();
            index.insert(
                id.to_string(),
                BundleMetadata {
                    path,
                    created_at: now,
                    size_bytes: data.len() as u64,
                },
            );
        }

        // Cleanup expired and excess bundles
        self.cleanup();

        Ok(())
    }

    /// Get a stored bundle by ID.
    pub fn get(&self, id: &str) -> Option<Vec<u8>> {
        let index = self.index.read();
        let meta = index.get(id)?;
        std::fs::read(&meta.path).ok()
    }

    /// List all stored bundles.
    pub fn list(&self) -> Vec<BundleInfo> {
        let index = self.index.read();
        let mut bundles: Vec<_> = index
            .iter()
            .map(|(id, meta)| BundleInfo {
                id: id.clone(),
                created_at: format_iso8601_from_unix(meta.created_at),
                created_at_unix: meta.created_at,
                size_bytes: meta.size_bytes,
            })
            .collect();

        // Sort by creation time, newest first
        bundles.sort_by(|a, b| b.created_at_unix.cmp(&a.created_at_unix));
        bundles
    }

    /// Cleanup expired and excess bundles.
    fn cleanup(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let ttl_threshold = now.saturating_sub(self.config.bundle_ttl_secs);

        let mut index = self.index.write();

        // Remove expired bundles
        let expired_ids: Vec<_> = index
            .iter()
            .filter(|(_, meta)| meta.created_at < ttl_threshold)
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            if let Some(meta) = index.remove(&id) {
                let _ = std::fs::remove_file(&meta.path);
            }
        }

        // Remove excess bundles (keep only max_bundles newest)
        if index.len() > self.config.max_bundles {
            let mut entries: Vec<_> = index.iter().collect();
            entries.sort_by(|a, b| b.1.created_at.cmp(&a.1.created_at));

            let to_remove: Vec<_> = entries
                .into_iter()
                .skip(self.config.max_bundles)
                .map(|(id, _)| id.clone())
                .collect();

            for id in to_remove {
                if let Some(meta) = index.remove(&id) {
                    let _ = std::fs::remove_file(&meta.path);
                }
            }
        }
    }

    /// Delete a bundle by ID.
    pub fn delete(&self, id: &str) -> bool {
        let mut index = self.index.write();
        if let Some(meta) = index.remove(id) {
            let _ = std::fs::remove_file(&meta.path);
            true
        } else {
            false
        }
    }
}

/// Slowlog entry for JSON serialization.
#[derive(Debug, Clone, Serialize)]
pub struct SlowlogEntryJson {
    pub id: u64,
    pub timestamp: i64,
    pub duration_us: u64,
    pub command: String,
    pub client_addr: String,
    pub client_name: String,
}

impl From<SlowLogEntry> for SlowlogEntryJson {
    fn from(entry: SlowLogEntry) -> Self {
        // Convert command bytes to string
        let command = entry
            .command
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect::<Vec<_>>()
            .join(" ");

        Self {
            id: entry.id,
            timestamp: entry.timestamp,
            duration_us: entry.duration_us,
            command,
            client_addr: entry.client_addr,
            client_name: entry.client_name,
        }
    }
}

/// Trace entry for JSON serialization.
#[derive(Debug, Clone, Serialize)]
pub struct TraceEntryJson {
    pub trace_id: String,
    pub timestamp_ms: u64,
    pub command: String,
    pub sampled: bool,
}

impl From<RecentTraceEntry> for TraceEntryJson {
    fn from(entry: RecentTraceEntry) -> Self {
        Self {
            trace_id: entry.trace_id,
            timestamp_ms: entry.timestamp_ms,
            command: entry.command,
            sampled: entry.sampled,
        }
    }
}

/// Per-shard memory stats for JSON serialization.
#[derive(Debug, Clone, Serialize)]
pub struct ShardMemoryJson {
    pub shard_id: usize,
    pub keys: usize,
    pub data_memory: usize,
    pub peak_memory: u64,
    pub overhead_estimate: usize,
}

impl From<(usize, ShardMemoryStats)> for ShardMemoryJson {
    fn from((shard_id, stats): (usize, ShardMemoryStats)) -> Self {
        Self {
            shard_id,
            keys: stats.keys,
            data_memory: stats.data_memory,
            peak_memory: stats.peak_memory,
            overhead_estimate: stats.overhead_estimate,
        }
    }
}

/// Cluster state information for the bundle.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ClusterStateJson {
    pub mode: String,
    pub role: String,
    pub num_shards: usize,
    pub cluster_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<ClusterNodeJson>>,
}

/// Cluster node information.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterNodeJson {
    pub id: String,
    pub address: String,
    pub role: String,
    pub state: String,
}

/// Collected diagnostic data.
#[derive(Debug, Default)]
pub struct DiagnosticData {
    pub slowlog: Vec<SlowlogEntryJson>,
    pub memory_stats: Vec<ShardMemoryJson>,
    pub traces: Vec<TraceEntryJson>,
    pub cluster_state: ClusterStateJson,
    pub server_status: Option<ServerStatus>,
    pub config: serde_json::Value,
    pub info: String,
}

/// Collects diagnostic data from various sources.
pub struct DiagnosticCollector {
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    shared_tracer: Option<SharedTracer>,
    config: BundleConfig,
}

impl DiagnosticCollector {
    /// Create a new diagnostic collector.
    pub fn new(
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        shared_tracer: Option<SharedTracer>,
        config: BundleConfig,
    ) -> Self {
        Self {
            shard_senders,
            shared_tracer,
            config,
        }
    }

    /// Collect an instant snapshot of diagnostic data.
    pub async fn collect_instant(&self) -> DiagnosticData {
        let slowlog = self.collect_slowlog(self.config.max_slowlog_entries).await;
        let memory_stats = self.collect_memory_stats().await;
        let traces = self.collect_traces(self.config.max_trace_entries);

        DiagnosticData {
            slowlog,
            memory_stats,
            traces,
            ..Default::default()
        }
    }

    /// Collect diagnostic data over a duration, accumulating samples.
    pub async fn collect_with_duration(&self, duration_secs: u64) -> DiagnosticData {
        let end_time = Instant::now() + Duration::from_secs(duration_secs);
        let mut seen_slowlog_ids: HashSet<u64> = HashSet::new();
        let mut all_slowlog = Vec::new();
        let mut all_traces = Vec::new();

        // Sample periodically during the duration
        while Instant::now() < end_time {
            // Collect slowlog, deduplicating by ID
            let slowlog = self.collect_slowlog(self.config.max_slowlog_entries).await;
            for entry in slowlog {
                if !seen_slowlog_ids.contains(&entry.id) {
                    seen_slowlog_ids.insert(entry.id);
                    all_slowlog.push(entry);
                }
            }

            // Collect traces
            let traces = self.collect_traces(self.config.max_trace_entries);
            all_traces.extend(traces);

            // Sleep between samples
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Collect final memory stats snapshot
        let memory_stats = self.collect_memory_stats().await;

        // Deduplicate traces by trace_id
        let mut seen_trace_ids: HashSet<String> = HashSet::new();
        all_traces.retain(|t| seen_trace_ids.insert(t.trace_id.clone()));

        // Limit to max entries
        all_slowlog.truncate(self.config.max_slowlog_entries);
        all_traces.truncate(self.config.max_trace_entries);

        DiagnosticData {
            slowlog: all_slowlog,
            memory_stats,
            traces: all_traces,
            ..Default::default()
        }
    }

    /// Collect slowlog entries from all shards.
    async fn collect_slowlog(&self, count: usize) -> Vec<SlowlogEntryJson> {
        let per_shard_count = (count / self.shard_senders.len().max(1)) + 1;
        let mut all_entries = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogGet {
                    count: per_shard_count,
                    response_tx,
                })
                .await
                .is_ok()
            {
                if let Ok(entries) = response_rx.await {
                    all_entries.extend(entries.into_iter().map(SlowlogEntryJson::from));
                }
            }
        }

        // Sort by ID (newest first, since IDs are monotonically increasing)
        all_entries.sort_by(|a, b| b.id.cmp(&a.id));
        all_entries.truncate(count);
        all_entries
    }

    /// Collect memory stats from all shards.
    async fn collect_memory_stats(&self) -> Vec<ShardMemoryJson> {
        let mut stats = Vec::with_capacity(self.shard_senders.len());

        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
            {
                if let Ok(shard_stats) = response_rx.await {
                    stats.push(ShardMemoryJson::from((shard_id, shard_stats)));
                } else {
                    stats.push(ShardMemoryJson {
                        shard_id,
                        keys: 0,
                        data_memory: 0,
                        peak_memory: 0,
                        overhead_estimate: 0,
                    });
                }
            } else {
                stats.push(ShardMemoryJson {
                    shard_id,
                    keys: 0,
                    data_memory: 0,
                    peak_memory: 0,
                    overhead_estimate: 0,
                });
            }
        }

        stats
    }

    /// Collect recent traces.
    fn collect_traces(&self, count: usize) -> Vec<TraceEntryJson> {
        if let Some(ref tracer) = self.shared_tracer {
            tracer
                .get_recent_traces(count)
                .into_iter()
                .map(TraceEntryJson::from)
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// Generates diagnostic bundles as ZIP archives.
pub struct BundleGenerator {
    config: BundleConfig,
}

impl BundleGenerator {
    /// Create a new bundle generator.
    pub fn new(config: BundleConfig) -> Self {
        Self { config }
    }

    /// Generate a unique bundle ID.
    pub fn generate_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        // Generate a short random suffix
        let random: u32 = rand_simple();
        format!("{:x}{:04x}", timestamp, random & 0xFFFF)
    }

    /// Create a ZIP archive from collected diagnostic data.
    pub fn create_zip(
        &self,
        id: &str,
        data: &DiagnosticData,
        duration_secs: u64,
    ) -> std::io::Result<Vec<u8>> {
        let mut buffer = Cursor::new(Vec::new());
        let mut zip = ZipWriter::new(&mut buffer);
        let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        let now = SystemTime::now();
        let timestamp_unix = now
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut files = Vec::new();
        let prefix = format!("frogdb-bundle-{}", id);

        // Write manifest.json
        let manifest = BundleManifest {
            id: id.to_string(),
            created_at: format_iso8601(now),
            created_at_unix: timestamp_unix,
            frogdb_version: env!("CARGO_PKG_VERSION").to_string(),
            collection_duration_secs: duration_secs,
            files: vec![
                "manifest.json".to_string(),
                "config.json".to_string(),
                "info.txt".to_string(),
                "slowlog.json".to_string(),
                "memory_stats.json".to_string(),
                "traces.json".to_string(),
                "cluster.json".to_string(),
                "server_status.json".to_string(),
            ],
            mode: if duration_secs == 0 {
                "instant".to_string()
            } else {
                format!("duration_{}s", duration_secs)
            },
        };

        let manifest_path = format!("{}/manifest.json", prefix);
        zip.start_file(&manifest_path, options)?;
        let manifest_json = serde_json::to_string_pretty(&manifest).unwrap_or_default();
        zip.write_all(manifest_json.as_bytes())?;
        files.push(manifest_path);

        // Write config.json
        let config_path = format!("{}/config.json", prefix);
        zip.start_file(&config_path, options)?;
        let config_json = serde_json::to_string_pretty(&data.config).unwrap_or_default();
        zip.write_all(config_json.as_bytes())?;
        files.push(config_path);

        // Write info.txt
        let info_path = format!("{}/info.txt", prefix);
        zip.start_file(&info_path, options)?;
        zip.write_all(data.info.as_bytes())?;
        files.push(info_path);

        // Write slowlog.json
        let slowlog_path = format!("{}/slowlog.json", prefix);
        zip.start_file(&slowlog_path, options)?;
        let slowlog_json = serde_json::to_string_pretty(&data.slowlog).unwrap_or_default();
        zip.write_all(slowlog_json.as_bytes())?;
        files.push(slowlog_path);

        // Write memory_stats.json
        let memory_path = format!("{}/memory_stats.json", prefix);
        zip.start_file(&memory_path, options)?;
        let memory_json = serde_json::to_string_pretty(&data.memory_stats).unwrap_or_default();
        zip.write_all(memory_json.as_bytes())?;
        files.push(memory_path);

        // Write traces.json
        let traces_path = format!("{}/traces.json", prefix);
        zip.start_file(&traces_path, options)?;
        let traces_json = serde_json::to_string_pretty(&data.traces).unwrap_or_default();
        zip.write_all(traces_json.as_bytes())?;
        files.push(traces_path);

        // Write cluster.json
        let cluster_path = format!("{}/cluster.json", prefix);
        zip.start_file(&cluster_path, options)?;
        let cluster_json = serde_json::to_string_pretty(&data.cluster_state).unwrap_or_default();
        zip.write_all(cluster_json.as_bytes())?;
        files.push(cluster_path);

        // Write server_status.json
        let status_path = format!("{}/server_status.json", prefix);
        zip.start_file(&status_path, options)?;
        if let Some(ref status) = data.server_status {
            let status_json = serde_json::to_string_pretty(status).unwrap_or_default();
            zip.write_all(status_json.as_bytes())?;
        } else {
            zip.write_all(b"{}")?;
        }
        files.push(status_path);

        zip.finish()?;

        Ok(buffer.into_inner())
    }
}

/// Format a SystemTime as ISO 8601.
fn format_iso8601(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();

    // Simple ISO 8601 formatting without external dependency
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days_since_epoch as i64);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Format a Unix timestamp as ISO 8601.
fn format_iso8601_from_unix(timestamp: u64) -> String {
    let time = UNIX_EPOCH + Duration::from_secs(timestamp);
    format_iso8601(time)
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    // Algorithm from Howard Hinnant's date library
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m, d)
}

/// Simple pseudo-random number generator for bundle IDs.
fn rand_simple() -> u32 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};

    thread_local! {
        static STATE: Cell<u32> = Cell::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u32)
                .unwrap_or(12345)
        );
    }

    STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        state.set(x);
        x
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_id_generation() {
        let id1 = BundleGenerator::generate_id();
        let id2 = BundleGenerator::generate_id();

        // IDs should be unique
        assert_ne!(id1, id2);

        // IDs should be non-empty
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
    }

    #[test]
    fn test_format_iso8601() {
        let time = UNIX_EPOCH + Duration::from_secs(1704067200); // 2024-01-01 00:00:00 UTC
        let formatted = format_iso8601(time);
        assert_eq!(formatted, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_bundle_config_default() {
        let config = BundleConfig::default();
        assert_eq!(config.max_bundles, DEFAULT_MAX_BUNDLES);
        assert_eq!(config.bundle_ttl_secs, DEFAULT_BUNDLE_TTL_SECS);
    }

    #[tokio::test]
    async fn test_zip_creation() {
        let config = BundleConfig::default();
        let generator = BundleGenerator::new(config);

        let data = DiagnosticData {
            slowlog: vec![SlowlogEntryJson {
                id: 1,
                timestamp: 1704067200,
                duration_us: 1000,
                command: "GET key".to_string(),
                client_addr: "127.0.0.1:12345".to_string(),
                client_name: "test".to_string(),
            }],
            memory_stats: vec![ShardMemoryJson {
                shard_id: 0,
                keys: 100,
                data_memory: 1024,
                peak_memory: 2048,
                overhead_estimate: 512,
            }],
            traces: vec![],
            cluster_state: ClusterStateJson {
                mode: "standalone".to_string(),
                role: "primary".to_string(),
                num_shards: 4,
                cluster_enabled: false,
                nodes: None,
            },
            server_status: None,
            config: serde_json::json!({"test": "config"}),
            info: "# Server\r\nfrogdb_version:1.0.0\r\n".to_string(),
        };

        let id = BundleGenerator::generate_id();
        let zip_data = generator.create_zip(&id, &data, 0).unwrap();

        // Verify it's a valid ZIP
        assert!(!zip_data.is_empty());
        assert_eq!(&zip_data[0..2], b"PK"); // ZIP magic number
    }

    #[test]
    fn test_bundle_store_lifecycle() {
        let temp_dir = std::env::temp_dir().join(format!("frogdb-test-{}", rand_simple()));
        let config = BundleConfig {
            directory: temp_dir.clone(),
            max_bundles: 3,
            bundle_ttl_secs: 3600,
            ..Default::default()
        };

        let store = BundleStore::new(config);

        // Store some bundles
        let id1 = "test-bundle-1";
        let id2 = "test-bundle-2";
        let data = b"test zip data";

        store.store(id1, data).unwrap();
        store.store(id2, data).unwrap();

        // List bundles
        let bundles = store.list();
        assert_eq!(bundles.len(), 2);

        // Get a bundle
        let retrieved = store.get(id1).unwrap();
        assert_eq!(retrieved, data);

        // Delete a bundle
        assert!(store.delete(id1));
        assert!(store.get(id1).is_none());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
