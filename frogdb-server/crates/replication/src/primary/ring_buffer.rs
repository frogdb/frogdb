//! Split-brain replication ring buffer.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

/// `start` sentinel: this buffer has never been armed, so it claims no history.
const UNARMED: i64 = -1;

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
    /// Lowest offset this buffer claims history from — Redis `repl_backlog_off`.
    /// [`UNARMED`] until the buffer is armed (see [`Self::arm_start`]).
    start: AtomicI64,
}

impl ReplicationRingBuffer {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(VecDeque::with_capacity(max_entries.min(1024))),
            max_entries,
            current_bytes: AtomicUsize::new(0),
            max_bytes,
            start: AtomicI64::new(UNARMED),
        }
    }

    /// Claim history from `offset` onward — Redis's `createReplicationBacklog`,
    /// which sets `repl_backlog->offset = master_repl_offset + 1` so the empty
    /// backlog is a zero-length window at the current head rather than "no
    /// history". FrogDB's window check is `<=`-inclusive, so no `+1`.
    ///
    /// Called when a node starts a primary stint: at construction from the
    /// recovered offset, and again on promotion from the live applied offset.
    /// The floor only ever advances — arming it backwards would claim coverage of
    /// a range this node never buffered.
    pub fn arm_start(&self, offset: u64) {
        self.start.fetch_max(offset as i64, Ordering::AcqRel);
    }

    /// Drop every buffered command and close the window — this node claims no
    /// replication history again.
    ///
    /// Called at both ends of a primary stint (see
    /// [`crate::primary::PartialSyncReplay::reset_backlog`]). Entries buffered
    /// during a *previous* stint describe a history this node no longer heads:
    /// after a demotion it followed someone else's stream, and a full resync may
    /// have rewound its offset below them. Left in place they would be served as
    /// a `+CONTINUE` tail for offsets that mean something else entirely, and the
    /// `fetch_max` floor could never follow the rewind back down.
    ///
    /// Takes the entries lock for the whole reset so a concurrent [`Self::push`]
    /// cannot interleave an entry with the cleared window.
    pub fn reset(&self) {
        let mut entries = self.entries.lock();
        entries.clear();
        self.current_bytes.store(0, Ordering::Release);
        self.start.store(UNARMED, Ordering::Release);
    }

    /// Lowest offset a `+CONTINUE` may resume from, or `None` while unarmed.
    ///
    /// Unlike [`Self::oldest_offset`] (the *end* offset of the oldest retained
    /// entry) this is the *start* of the retained range, so a replica sitting
    /// exactly one command behind the oldest entry is still servable.
    pub fn start_offset(&self) -> Option<u64> {
        match self.start.load(Ordering::Acquire) {
            UNARMED => None,
            armed => Some(armed as u64),
        }
    }

    pub fn push(&self, offset: u64, shard_id: u16, resp_bytes: Bytes) {
        let entry_size = resp_bytes.len();
        let mut entries = self.entries.lock();
        // A push into a never-armed buffer implicitly opens the window at this
        // command's *start* offset, so the pushed entry itself is replayable.
        // `fetch_max` (not a store) for the same reason [`Self::arm_start`] uses
        // it: the floor only ever rises, and this path races an `arm_start` that
        // does not hold the entries lock.
        if self.start.load(Ordering::Acquire) == UNARMED {
            self.start.fetch_max(
                offset.saturating_sub(entry_size as u64) as i64,
                Ordering::AcqRel,
            );
        }
        while entries.len() >= self.max_entries
            || (self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes
                && !entries.is_empty())
        {
            if let Some(evicted) = entries.pop_front() {
                self.current_bytes
                    .fetch_sub(evicted.resp_bytes.len(), Ordering::Relaxed);
                // The evicted command's data is gone; the window floor rises to
                // where it ended, which is where the new front entry begins.
                self.start
                    .fetch_max(evicted.offset as i64, Ordering::AcqRel);
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

    /// **End** offset of the oldest retained entry. `None` when empty.
    ///
    /// Not the resume bound — that is [`Self::start_offset`], which sits one
    /// entry lower (an entry spanning `(a, b]` is replayable *from* `a`). Kept
    /// for diagnostics and eviction assertions. Entries are pushed in offset
    /// order and evicted from the front (FIFO), so the front entry is the oldest.
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
