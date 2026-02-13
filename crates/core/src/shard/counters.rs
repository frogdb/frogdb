use std::time::{SystemTime, UNIX_EPOCH};

/// Number of 1-second buckets to keep for operation tracking (60 seconds).
const OPERATION_BUCKET_COUNT: usize = 60;

/// A single 1-second bucket of operation counts.
#[derive(Debug, Clone, Copy, Default)]
pub struct OperationBucket {
    /// Unix timestamp (seconds) for this bucket.
    pub timestamp: u64,
    /// Total operations in this second.
    pub total_ops: u64,
    /// Read operations in this second.
    pub read_ops: u64,
    /// Write operations in this second.
    pub write_ops: u64,
}

/// Windowed operation counters for hot shard detection.
/// Uses a ring buffer of 1-second buckets.
#[derive(Debug)]
pub struct OperationCounters {
    /// Ring buffer of 1-second buckets.
    buckets: [OperationBucket; OPERATION_BUCKET_COUNT],
    /// Current bucket index.
    current_index: usize,
    /// Timestamp of current bucket.
    current_second: u64,
}

impl Default for OperationCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationCounters {
    /// Create new operation counters.
    pub fn new() -> Self {
        Self {
            buckets: [OperationBucket::default(); OPERATION_BUCKET_COUNT],
            current_index: 0,
            current_second: 0,
        }
    }

    /// Get current unix timestamp in seconds.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Advance to current second, rolling over buckets as needed.
    fn advance_to_now(&mut self) {
        let now = Self::current_timestamp();

        if self.current_second == 0 {
            // First call - initialize
            self.current_second = now;
            self.buckets[self.current_index].timestamp = now;
            return;
        }

        // Advance buckets if time has passed
        while self.current_second < now {
            self.current_second += 1;
            self.current_index = (self.current_index + 1) % OPERATION_BUCKET_COUNT;
            // Clear the new bucket
            self.buckets[self.current_index] = OperationBucket {
                timestamp: self.current_second,
                ..Default::default()
            };
        }
    }

    /// Record an operation.
    pub fn record_op(&mut self, is_read: bool, is_write: bool) {
        self.advance_to_now();
        let bucket = &mut self.buckets[self.current_index];
        bucket.total_ops += 1;
        if is_read {
            bucket.read_ops += 1;
        }
        if is_write {
            bucket.write_ops += 1;
        }
    }

    /// Calculate ops/sec for the given period.
    /// Returns (total_ops_per_sec, read_ops_per_sec, write_ops_per_sec).
    pub fn calculate_ops_per_sec(&mut self, period_secs: u64) -> (f64, f64, f64) {
        self.advance_to_now();

        let period_secs = period_secs.min(OPERATION_BUCKET_COUNT as u64);
        if period_secs == 0 {
            return (0.0, 0.0, 0.0);
        }

        let now = self.current_second;
        let cutoff = now.saturating_sub(period_secs);

        let mut total_ops: u64 = 0;
        let mut read_ops: u64 = 0;
        let mut write_ops: u64 = 0;

        for bucket in &self.buckets {
            if bucket.timestamp > cutoff && bucket.timestamp <= now {
                total_ops += bucket.total_ops;
                read_ops += bucket.read_ops;
                write_ops += bucket.write_ops;
            }
        }

        let period = period_secs as f64;
        (
            total_ops as f64 / period,
            read_ops as f64 / period,
            write_ops as f64 / period,
        )
    }
}

/// Response for hot shard stats query.
#[derive(Debug, Clone)]
pub struct HotShardStatsResponse {
    /// Shard ID.
    pub shard_id: usize,
    /// Total operations per second.
    pub ops_per_sec: f64,
    /// Read operations per second.
    pub reads_per_sec: f64,
    /// Write operations per second.
    pub writes_per_sec: f64,
    /// Current queue depth.
    pub queue_depth: usize,
}
