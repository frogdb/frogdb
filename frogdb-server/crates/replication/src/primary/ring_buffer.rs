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
    /// Origin shard the command executed on, carried so a backlog-replayed frame
    /// tags the same shard the live frame did (see [`crate::frame::ReplicationFrame`]).
    shard_id: u16,
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

    pub fn push(&self, offset: u64, shard_id: u16, resp_bytes: Bytes) {
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
        entries.push_back(BufferedCommand {
            offset,
            shard_id,
            resp_bytes,
        });
    }

    pub fn extract_divergent_writes(&self, last_replicated_offset: u64) -> Vec<(u64, Bytes)> {
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|cmd| cmd.offset > last_replicated_offset)
            .map(|cmd| (cmd.offset, cmd.resp_bytes.clone()))
            .collect()
    }

    /// Oldest replication offset still retained in the backlog — the analogue of
    /// Redis `repl_backlog_off`. `None` when the backlog is empty.
    ///
    /// This is the *lower bound* of the continuable window:
    /// [`crate::state::ReplicationState::window_contains`] checks only the upper
    /// bound (`offset <= current`), so a replica whose requested offset is below
    /// this value has had its resume point evicted and must full-resync. Entries
    /// are pushed in offset order and evicted from the front (FIFO), so the front
    /// entry holds the oldest retained offset.
    pub fn oldest_offset(&self) -> Option<u64> {
        self.entries.lock().front().map(|c| c.offset)
    }

    /// Extract the backlog tail `(start, end]` in offset order — the RESP frames
    /// a reconnecting replica must replay to advance from `start` to `end`.
    ///
    /// The replay sibling of [`Self::extract_divergent_writes`]: same
    /// `offset > start` filter, but bounded above by `end` so a caller never
    /// streams past the offset it promised the replica. Only reached after the
    /// lower-bound (eviction) check has confirmed `start >= oldest_offset()`, so
    /// the returned range is contiguous from `start` with no silent truncation.
    /// Non-destructive.
    pub fn extract_backlog(&self, start: u64, end: u64) -> Vec<(u64, u16, Bytes)> {
        let entries = self.entries.lock();
        let tail: Vec<(u64, u16, Bytes)> = entries
            .iter()
            .filter(|cmd| cmd.offset > start && cmd.offset <= end)
            .map(|cmd| (cmd.offset, cmd.shard_id, cmd.resp_bytes.clone()))
            .collect();
        debug_assert!(
            tail.windows(2).all(|w| w[0].0 < w[1].0),
            "backlog must be offset-ordered for replay (got {:?})",
            tail.iter().map(|(o, _, _)| *o).collect::<Vec<_>>()
        );
        tail
    }
}
