//! The replication backlog: a bounded ring of recent commands, keyed by
//! replication offset, serving both partial-resync replay and split-brain
//! divergence capture.

use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

/// `start` sentinel: this buffer has never been armed, so it claims no history.
const UNARMED: i64 = -1;

/// Configuration for the replication backlog.
#[derive(Debug, Clone)]
pub struct BacklogConfig {
    /// Whether the backlog is populated at all. When `false` nothing is
    /// buffered, every PSYNC full-resyncs, and split-brain capture is empty.
    pub enabled: bool,
    /// Maximum number of recent commands to retain.
    pub max_entries: usize,
    /// Maximum memory in bytes for buffered commands.
    pub max_bytes: usize,
    /// Seconds with zero connected replicas after which the backlog is freed
    /// and its window closed — Redis `repl-backlog-ttl`. 0 = never free. See
    /// [`crate::primary::BacklogTtl`].
    pub ttl_secs: u64,
}

impl Default for BacklogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10_000,
            max_bytes: 64 * 1024 * 1024,
            ttl_secs: 3600,
        }
    }
}

/// The backlog's shape as `INFO replication` reports it, read as one triple so
/// the three fields cannot describe different instants of the same ring.
///
/// Every field is derived, never configured-and-forgotten: `size_bytes` is the
/// byte cap the ring was actually built with, and the other three come off the
/// live window. Rendered as `repl_backlog_active`, `repl_backlog_size`,
/// `repl_backlog_first_byte_offset` and `repl_backlog_histlen` by both INFO
/// renderers, through one shared field list (FM-REPLICATION-059).
///
/// `Default` is the honest reading for a node with no backlog at all: no
/// window, no capacity, nothing retained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BacklogGeometry {
    /// Whether a resume window is open — the same predicate PSYNC grants a
    /// `+CONTINUE` on ([`super::PartialSyncReplay::has_resume_history`]), not
    /// "some replica is attached".
    pub active: bool,
    /// Byte cap the ring was built with, from `replication.backlog-max-mb`.
    /// Reported whether or not the backlog is enabled or armed: it is the
    /// capacity an operator tuned, and reporting the default back at them is
    /// the bug this field exists to answer (issue 20).
    pub size_bytes: u64,
    /// Lowest offset a `+CONTINUE` may resume from — Redis `repl_backlog_off`.
    /// `0` when no window is open, which is also what Redis prints for an
    /// absent backlog.
    pub first_byte_offset: u64,
    /// Bytes of command stream the window still covers: `current_offset -
    /// first_byte_offset`, so `first_byte_offset + histlen` is the head. `0`
    /// when no window is open.
    pub histlen: u64,
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

    /// The window as INFO reports it, measured against `current_offset` (the
    /// node's live replication offset).
    ///
    /// Read here rather than assembled at the render site so the reported floor
    /// is *the* floor `extract_backlog` refuses below (FM-REPLICATION-014): the
    /// two cannot drift, because there is only one of them.
    pub fn geometry(&self, current_offset: u64) -> BacklogGeometry {
        let first_byte_offset = self.start_offset();
        BacklogGeometry {
            active: first_byte_offset.is_some(),
            size_bytes: self.max_bytes as u64,
            first_byte_offset: first_byte_offset.unwrap_or(0),
            // `saturating_sub` because the floor is armed from a recovered or
            // promoted offset that can momentarily sit above the live counter
            // (the same race `ReplicationTrackerImpl::replica_lag` saturates
            // for); a window "longer than the stream" is not reportable.
            histlen: first_byte_offset
                .map(|floor| current_offset.saturating_sub(floor))
                .unwrap_or(0),
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
        // `!entries.is_empty()` guards **both** caps, not just the byte one: an
        // eviction loop whose exit depends on `pop_front` succeeding can never
        // terminate once the deque is drained, and it spins holding
        // `self.entries` — so every later write parks on a lock that is never
        // released. `max_entries == 0` reached that state on the very first
        // push (`0 >= 0`). Config validation refuses `0`, but the loop is what
        // makes the hang unreachable from *any* caller, including the ones that
        // build a `BacklogConfig` in process (issue 14).
        while !entries.is_empty()
            && (entries.len() >= self.max_entries
                || self.current_bytes.load(Ordering::Relaxed) + entry_size > self.max_bytes)
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
    /// streams past the offset it promised the replica.
    ///
    /// **Contiguity is checked here, not assumed.** The caller's eviction check
    /// runs at PSYNC *grant* time, and a resume is streamed much later — after a
    /// checkpoint cut and a whole file transfer — so the window can close under
    /// it. The floor is therefore re-read under the entries lock (the same lock
    /// [`Self::push`] holds while it evicts and raises the floor) and a request
    /// that no longer starts inside the window is an [`BacklogTruncated`] error
    /// rather than a silently shorter vector: a short tail is indistinguishable
    /// from "nothing to replay", and the caller would seed the resume position
    /// from the last frame it *did* send, leaving the replica permanently
    /// missing the evicted range while its offset looks contiguous (round-2
    /// issue 52). Non-destructive.
    pub fn extract_backlog(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, u16, Bytes)>, BacklogTruncated> {
        let entries = self.entries.lock();
        // Under the lock: the floor rises only in `push`, which holds it, so
        // this pairs the window check with the contents it describes.
        let floor = match self.start.load(Ordering::Acquire) {
            UNARMED => None,
            armed => Some(armed as u64),
        };
        match floor {
            Some(floor) if floor <= start => {}
            // An empty range has nothing to truncate: a caught-up replica, and
            // the fresh-primary full sync whose snapshot offset *is* the head,
            // both ask for `(x, x]` and are served by the payload itself. The
            // window only has to cover a range that actually carries writes.
            _ if start >= end => return Ok(Vec::new()),
            floor => {
                return Err(BacklogTruncated {
                    requested: start,
                    floor,
                });
            }
        }
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
        debug_assert!(
            tail.first().is_none_or(|(offset, _, _)| *offset > start),
            "replay tail must begin strictly after the resume point"
        );
        Ok(tail)
    }
}

/// The backlog no longer holds the offset a replay was asked to start from.
///
/// Returned by [`ReplicationRingBuffer::extract_backlog`] when eviction (or a
/// TTL free, or a stint boundary) has raised the window floor above `requested`
/// since the resume was granted. The only correct answer is to abandon the
/// resume and let the replica come back for a full sync — streaming the short
/// tail would hand it a hole it can never detect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "the replication backlog no longer covers offset {requested} (window floor: {})",
    match floor { Some(f) => f.to_string(), None => "unarmed".to_string() }
)]
pub struct BacklogTruncated {
    /// The offset the replay was asked to resume from.
    pub requested: u64,
    /// The window floor now, or `None` if the window has been closed entirely.
    pub floor: Option<u64>,
}
