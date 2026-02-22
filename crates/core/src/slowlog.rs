//! Slow query log for tracking commands that exceed execution time thresholds.
//!
//! Each shard maintains its own slowlog to avoid cross-shard locking.
//! Global monotonic IDs are shared across shards for consistent ordering.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

/// Default maximum entries per shard.
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;

/// Default maximum characters per argument before truncation.
pub const DEFAULT_SLOWLOG_MAX_ARG_LEN: usize = 128;

/// Default threshold in microseconds (10ms). Commands taking longer are logged.
/// Set to 0 to log all commands, -1 to disable.
pub const DEFAULT_SLOWLOG_LOG_SLOWER_THAN: i64 = 10000;

/// A single entry in the slow query log.
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Global monotonic ID (unique across all shards).
    pub id: u64,
    /// Unix timestamp in seconds when the command was logged.
    pub timestamp: i64,
    /// Duration of the command execution in microseconds.
    pub duration_us: u64,
    /// Command name and arguments (potentially truncated).
    pub command: Vec<Bytes>,
    /// Client IP:port address.
    pub client_addr: String,
    /// Client name from CLIENT SETNAME (empty if not set).
    pub client_name: String,
}

/// Per-shard slow query log.
pub struct SlowLog {
    /// Log entries, newest first.
    entries: VecDeque<SlowLogEntry>,
    /// Maximum number of entries to keep.
    max_len: usize,
    /// Maximum characters per argument for truncation.
    max_arg_len: usize,
    /// Shared counter for globally unique IDs.
    next_id: Arc<AtomicU64>,
}

impl SlowLog {
    /// Create a new slow query log.
    ///
    /// # Arguments
    /// * `max_len` - Maximum number of entries to keep per shard
    /// * `max_arg_len` - Maximum characters per argument before truncation
    /// * `next_id` - Shared atomic counter for global ID generation
    pub fn new(max_len: usize, max_arg_len: usize, next_id: Arc<AtomicU64>) -> Self {
        Self {
            entries: VecDeque::with_capacity(max_len.min(1024)),
            max_len,
            max_arg_len,
            next_id,
        }
    }

    /// Add a slow query entry to the log.
    ///
    /// If the log is at capacity, the oldest entry is evicted.
    /// Arguments are automatically truncated based on `max_arg_len`.
    pub fn add(
        &mut self,
        duration_us: u64,
        command: &[Bytes],
        client_addr: String,
        client_name: String,
    ) {
        // Generate globally unique ID
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Get current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // Truncate arguments
        let truncated_command = Self::truncate_args(command, self.max_arg_len);

        let entry = SlowLogEntry {
            id,
            timestamp,
            duration_us,
            command: truncated_command,
            client_addr,
            client_name,
        };

        // Evict oldest if at capacity
        if self.entries.len() >= self.max_len {
            self.entries.pop_back();
        }

        // Add new entry at front (newest first)
        self.entries.push_front(entry);
    }

    /// Get the most recent entries from the log.
    ///
    /// Returns up to `count` entries, newest first.
    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        self.entries.iter().take(count).cloned().collect()
    }

    /// Get the number of entries in the log.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries from the log.
    pub fn reset(&mut self) {
        self.entries.clear();
    }

    /// Update the maximum length setting.
    ///
    /// If the new max is smaller than current length, oldest entries are evicted.
    pub fn set_max_len(&mut self, max_len: usize) {
        self.max_len = max_len;
        while self.entries.len() > max_len {
            self.entries.pop_back();
        }
    }

    /// Update the maximum argument length setting.
    pub fn set_max_arg_len(&mut self, max_arg_len: usize) {
        self.max_arg_len = max_arg_len;
    }

    /// Truncate arguments to the specified maximum length.
    ///
    /// Arguments exceeding `max_arg_len` bytes are truncated and appended with
    /// "... (N more bytes)" to indicate truncation.
    pub fn truncate_args(args: &[Bytes], max_arg_len: usize) -> Vec<Bytes> {
        args.iter()
            .map(|arg| {
                if arg.len() <= max_arg_len {
                    arg.clone()
                } else {
                    // Truncate and add indicator
                    let truncated = &arg[..max_arg_len];
                    let remaining = arg.len() - max_arg_len;
                    let mut result = truncated.to_vec();
                    result.extend_from_slice(format!("... ({} more bytes)", remaining).as_bytes());
                    Bytes::from(result)
                }
            })
            .collect()
    }
}

impl std::fmt::Debug for SlowLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlowLog")
            .field("entries", &self.entries.len())
            .field("max_len", &self.max_len)
            .field("max_arg_len", &self.max_arg_len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_slowlog(max_len: usize) -> SlowLog {
        SlowLog::new(
            max_len,
            DEFAULT_SLOWLOG_MAX_ARG_LEN,
            Arc::new(AtomicU64::new(0)),
        )
    }

    #[test]
    fn test_add_and_get() {
        let mut log = create_test_slowlog(10);

        log.add(
            1000,
            &[Bytes::from("GET"), Bytes::from("key1")],
            "127.0.0.1:12345".to_string(),
            "client1".to_string(),
        );

        assert_eq!(log.len(), 1);

        let entries = log.get(10);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].duration_us, 1000);
        assert_eq!(entries[0].command.len(), 2);
        assert_eq!(entries[0].client_addr, "127.0.0.1:12345");
        assert_eq!(entries[0].client_name, "client1");
    }

    #[test]
    fn test_capacity_eviction() {
        let mut log = create_test_slowlog(3);

        for i in 0..5 {
            log.add(
                (i + 1) * 100,
                &[Bytes::from(format!("CMD{}", i))],
                "127.0.0.1:12345".to_string(),
                String::new(),
            );
        }

        // Should only have 3 entries (the newest ones)
        assert_eq!(log.len(), 3);

        let entries = log.get(10);
        // IDs should be 4, 3, 2 (newest first)
        assert_eq!(entries[0].id, 4);
        assert_eq!(entries[1].id, 3);
        assert_eq!(entries[2].id, 2);
    }

    #[test]
    fn test_newest_first_ordering() {
        let mut log = create_test_slowlog(10);

        log.add(
            100,
            &[Bytes::from("CMD1")],
            "addr".to_string(),
            String::new(),
        );
        log.add(
            200,
            &[Bytes::from("CMD2")],
            "addr".to_string(),
            String::new(),
        );
        log.add(
            300,
            &[Bytes::from("CMD3")],
            "addr".to_string(),
            String::new(),
        );

        let entries = log.get(10);
        assert_eq!(entries[0].duration_us, 300); // Newest
        assert_eq!(entries[1].duration_us, 200);
        assert_eq!(entries[2].duration_us, 100); // Oldest
    }

    #[test]
    fn test_id_monotonicity() {
        let next_id = Arc::new(AtomicU64::new(0));
        let mut log1 = SlowLog::new(10, DEFAULT_SLOWLOG_MAX_ARG_LEN, next_id.clone());
        let mut log2 = SlowLog::new(10, DEFAULT_SLOWLOG_MAX_ARG_LEN, next_id.clone());

        // Add to both logs interleaved
        log1.add(
            100,
            &[Bytes::from("CMD1")],
            "addr".to_string(),
            String::new(),
        );
        log2.add(
            100,
            &[Bytes::from("CMD2")],
            "addr".to_string(),
            String::new(),
        );
        log1.add(
            100,
            &[Bytes::from("CMD3")],
            "addr".to_string(),
            String::new(),
        );

        let entries1 = log1.get(10);
        let entries2 = log2.get(10);

        // IDs should be globally unique and monotonic
        assert_eq!(entries1[0].id, 2); // Last added to log1
        assert_eq!(entries1[1].id, 0); // First added to log1
        assert_eq!(entries2[0].id, 1); // Added to log2
    }

    #[test]
    fn test_argument_truncation() {
        let short_arg = Bytes::from("short");
        let long_arg = Bytes::from("a".repeat(200));

        let truncated = SlowLog::truncate_args(&[short_arg.clone(), long_arg], 128);

        assert_eq!(truncated[0], short_arg); // Unchanged
        assert!(truncated[1].len() > 128); // Truncated + suffix
        assert!(String::from_utf8_lossy(&truncated[1]).contains("more bytes"));
    }

    #[test]
    fn test_reset() {
        let mut log = create_test_slowlog(10);

        log.add(
            100,
            &[Bytes::from("CMD1")],
            "addr".to_string(),
            String::new(),
        );
        log.add(
            200,
            &[Bytes::from("CMD2")],
            "addr".to_string(),
            String::new(),
        );

        assert_eq!(log.len(), 2);

        log.reset();

        assert_eq!(log.len(), 0);
        assert!(log.is_empty());
    }

    #[test]
    fn test_get_limited_count() {
        let mut log = create_test_slowlog(10);

        for i in 0..5 {
            log.add(
                (i + 1) * 100,
                &[Bytes::from(format!("CMD{}", i))],
                "addr".to_string(),
                String::new(),
            );
        }

        let entries = log.get(2);
        assert_eq!(entries.len(), 2);
        // Should be the 2 newest
        assert_eq!(entries[0].id, 4);
        assert_eq!(entries[1].id, 3);
    }

    #[test]
    fn test_set_max_len() {
        let mut log = create_test_slowlog(10);

        for i in 0..5 {
            log.add(
                (i + 1) * 100,
                &[Bytes::from(format!("CMD{}", i))],
                "addr".to_string(),
                String::new(),
            );
        }

        assert_eq!(log.len(), 5);

        // Reduce max length
        log.set_max_len(3);

        assert_eq!(log.len(), 3);
        // Should keep the newest 3
        let entries = log.get(10);
        assert_eq!(entries[0].id, 4);
        assert_eq!(entries[1].id, 3);
        assert_eq!(entries[2].id, 2);
    }
}
