//! Split-brain replication ring buffer.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Configuration for the split-brain replication ring buffer.
#[derive(Debug, Clone)]
pub struct SplitBrainBufferConfig {
    /// Whether split-brain logging is enabled.
    pub enabled: bool,
    /// Maximum number of recent commands to retain.
    pub max_entries: usize,
    /// Maximum memory in bytes for buffered commands.
    pub max_bytes: usize,
}

impl Default for SplitBrainBufferConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

struct BufferedCommand {
    offset: u64,
    resp_bytes: Bytes,
}

/// Bounded ring buffer that captures recent RESP-encoded commands with their
/// replication offsets. Used to recover divergent writes during split-brain detection.
pub struct ReplicationRingBuffer {
    entries: parking_lot::Mutex<VecDeque<BufferedCommand>>,
    max_entries: usize,
    current_bytes: AtomicUsize,
    max_bytes: usize,
}

impl ReplicationRingBuffer {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::with_capacity(max_entries.min(1024))),
            max_entries,
            current_bytes: AtomicUsize::new(0),
            max_bytes,
        }
    }

    pub fn push(&self, offset: u64, resp_bytes: Bytes) {
        let entry_size = resp_bytes.len();
        let mut entries = self.entries.lock();
        while entries.len() >= self.max_entries
            || (self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes
                && !entries.is_empty())
        {
            if let Some(evicted) = entries.pop_front() {
                self.current_bytes
                    .fetch_sub(evicted.resp_bytes.len(), Ordering::Relaxed);
            }
        }
        self.current_bytes.fetch_add(entry_size, Ordering::Relaxed);
        entries.push_back(BufferedCommand { offset, resp_bytes });
    }

    pub fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|cmd| cmd.offset > last_replicated_offset)
            .map(|cmd| (cmd.offset, cmd.resp_bytes.clone()))
            .collect()
    }
}
