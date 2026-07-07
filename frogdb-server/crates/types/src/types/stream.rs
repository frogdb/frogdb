//! Stream value types.

use bytes::Bytes;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================================
// Stream Type
// ============================================================================

/// Stream entry ID consisting of millisecond timestamp and sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StreamId {
    /// Unix timestamp in milliseconds.
    pub ms: u64,
    /// Sequence number within the millisecond.
    pub seq: u64,
}

impl StreamId {
    /// Create a new stream ID.
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Minimum possible stream ID (0-0).
    pub fn min() -> Self {
        Self { ms: 0, seq: 0 }
    }

    /// Maximum possible stream ID.
    pub fn max() -> Self {
        Self {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    /// Check if this is the zero ID (0-0).
    pub fn is_zero(&self) -> bool {
        self.ms == 0 && self.seq == 0
    }

    /// Parse a stream ID from bytes.
    ///
    /// Format: "ms-seq" where ms and seq are unsigned integers.
    /// Also accepts just "ms" (sequence defaults to 0 for explicit IDs).
    pub fn parse(s: &[u8]) -> Result<Self, StreamIdParseError> {
        let s = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq })
        } else {
            // Just milliseconds, sequence is 0
            let ms = s
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(Self { ms, seq: 0 })
        }
    }

    /// Parse a stream ID for use as a range bound.
    ///
    /// Supports special values:
    /// - "-" means minimum ID
    /// - "+" means maximum ID
    /// - Otherwise parses as normal ID
    pub fn parse_range_bound(s: &[u8]) -> Result<StreamRangeBound, StreamIdParseError> {
        match s {
            b"-" => Ok(StreamRangeBound::Min),
            b"+" => Ok(StreamRangeBound::Max),
            _ => {
                // Check for exclusive prefix
                if s.starts_with(b"(") {
                    let id = Self::parse(&s[1..])?;
                    Ok(StreamRangeBound::Exclusive(id))
                } else {
                    let id = Self::parse(s)?;
                    Ok(StreamRangeBound::Inclusive(id))
                }
            }
        }
    }

    /// Parse a stream ID for XADD.
    ///
    /// Supports special values:
    /// - "*" means auto-generate
    /// - "ms-*" means auto-generate sequence for given timestamp
    /// - Otherwise parses as explicit ID
    pub fn parse_for_add(s: &[u8]) -> Result<StreamIdSpec, StreamIdParseError> {
        let s_str = std::str::from_utf8(s).map_err(|_| StreamIdParseError::InvalidFormat)?;

        if s_str == "*" {
            return Ok(StreamIdSpec::Auto);
        }

        if let Some((ms_str, seq_str)) = s_str.split_once('-') {
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            if seq_str == "*" {
                return Ok(StreamIdSpec::AutoSeq(ms));
            }
            let seq = seq_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq }))
        } else {
            let ms = s_str
                .parse::<u64>()
                .map_err(|_| StreamIdParseError::InvalidFormat)?;
            Ok(StreamIdSpec::Explicit(Self { ms, seq: 0 }))
        }
    }

    /// Generate a new stream ID based on current time and the last ID.
    /// Returns None if all IDs are exhausted.
    pub fn generate(last: &StreamId) -> Option<Self> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if now_ms > last.ms {
            Some(Self { ms: now_ms, seq: 0 })
        } else {
            // Same or earlier timestamp, increment sequence
            last.seq.checked_add(1).map(|seq| Self { ms: last.ms, seq })
        }
    }

    /// Generate a new stream ID with auto-sequence for a given timestamp.
    /// Returns None if the timestamp is in the past or sequence would overflow.
    pub fn generate_with_ms(ms: u64, last: &StreamId) -> Option<Self> {
        if ms > last.ms {
            Some(Self { ms, seq: 0 })
        } else if ms == last.ms {
            last.seq.checked_add(1).map(|seq| Self { ms, seq })
        } else {
            // Timestamp is in the past
            None
        }
    }

    /// Check if this ID is valid as a new entry after the last ID.
    pub fn is_valid_after(&self, last: &StreamId) -> bool {
        self > last
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.ms.cmp(&other.ms) {
            std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
            ord => ord,
        }
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Stream ID parsing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdParseError {
    /// Invalid format (not "ms-seq" or "ms").
    InvalidFormat,
}

impl std::fmt::Display for StreamIdParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamIdParseError::InvalidFormat => {
                write!(
                    f,
                    "ERR Invalid stream ID specified as stream command argument"
                )
            }
        }
    }
}

impl std::error::Error for StreamIdParseError {}

/// Stream ID specification for XADD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdSpec {
    /// Auto-generate ID based on current time.
    Auto,
    /// Auto-generate sequence for given millisecond timestamp.
    AutoSeq(u64),
    /// Explicit ID.
    Explicit(StreamId),
}

/// Stream range bound for XRANGE/XREVRANGE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRangeBound {
    /// Minimum ID ("-").
    Min,
    /// Maximum ID ("+").
    Max,
    /// Inclusive bound.
    Inclusive(StreamId),
    /// Exclusive bound (prefixed with "(").
    Exclusive(StreamId),
}

impl StreamRangeBound {
    /// Check if an ID satisfies this bound as a minimum.
    pub fn satisfies_min(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => true,
            StreamRangeBound::Max => false,
            StreamRangeBound::Inclusive(bound) => id >= bound,
            StreamRangeBound::Exclusive(bound) => id > bound,
        }
    }

    /// Check if an ID satisfies this bound as a maximum.
    pub fn satisfies_max(&self, id: &StreamId) -> bool {
        match self {
            StreamRangeBound::Min => false,
            StreamRangeBound::Max => true,
            StreamRangeBound::Inclusive(bound) => id <= bound,
            StreamRangeBound::Exclusive(bound) => id < bound,
        }
    }
}

/// Stream entry with ID and field-value pairs.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID.
    pub id: StreamId,
    /// Field-value pairs.
    pub fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    /// Create a new stream entry.
    pub fn new(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> Self {
        Self { id, fields }
    }

    /// Calculate memory size of this entry.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let fields_size: usize = self
            .fields
            .iter()
            .map(|(k, v)| k.len() + v.len() + 16) // 16 for Vec overhead
            .sum();
        base + fields_size
    }
}

/// Pending entry in a consumer group's PEL (Pending Entries List).
///
/// Fields are private: the PEL invariant `sum(consumer.pending_count) ==
/// pending.len()` is only sound if every mutation goes through [`ConsumerGroup`]
/// methods. Read access for command responses is via [`PendingInfo`].
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Consumer that owns this pending entry.
    consumer: Bytes,
    /// Time when the entry was delivered.
    delivery_time: Instant,
    /// Number of times this entry has been delivered.
    delivery_count: u32,
}

impl PendingEntry {
    /// Create a new pending entry.
    pub fn new(consumer: Bytes) -> Self {
        Self {
            consumer,
            delivery_time: Instant::now(),
            delivery_count: 1,
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.delivery_time.elapsed().as_millis() as u64
    }

    /// The consumer that currently owns this pending entry.
    pub fn consumer(&self) -> &Bytes {
        &self.consumer
    }

    /// Number of times this entry has been delivered.
    pub fn delivery_count(&self) -> u32 {
        self.delivery_count
    }
}

/// Read-only view of a single pending entry, decoupled from the internal
/// [`PendingEntry`] so command handlers build XPENDING / XINFO responses without
/// touching the PEL map. `idle_ms` is captured at query time.
#[derive(Debug, Clone)]
pub struct PendingInfo {
    /// Entry ID.
    pub id: StreamId,
    /// Owning consumer.
    pub consumer: Bytes,
    /// Idle time (ms since last delivery) at query time.
    pub idle_ms: u64,
    /// Number of deliveries.
    pub delivery_count: u32,
}

/// Summary of a group's PEL, for XPENDING summary mode.
#[derive(Debug, Clone)]
pub struct PendingSummary {
    /// Total number of pending entries.
    pub count: usize,
    /// Smallest pending ID.
    pub min_id: StreamId,
    /// Largest pending ID.
    pub max_id: StreamId,
    /// `(consumer, pending_count)` for each consumer owning at least one entry.
    pub per_consumer: Vec<(Bytes, u64)>,
}

/// Consumer in a consumer group.
///
/// Fields are private so that `pending_count` stays consistent with the group's
/// PEL — it is only ever adjusted by [`ConsumerGroup`] methods.
#[derive(Debug, Clone)]
pub struct Consumer {
    /// Consumer name.
    name: Bytes,
    /// Number of pending entries for this consumer.
    pending_count: usize,
    /// Last time this consumer was seen (read or claimed).
    last_seen: Instant,
    /// Last time this consumer actively consumed entries (XREADGROUP delivered data).
    /// `None` means the consumer has never actively consumed.
    active_time: Option<Instant>,
}

impl Consumer {
    /// Create a new consumer.
    pub fn new(name: Bytes) -> Self {
        Self {
            name,
            pending_count: 0,
            last_seen: Instant::now(),
            active_time: None,
        }
    }

    /// Consumer name.
    pub fn name(&self) -> &Bytes {
        &self.name
    }

    /// Number of pending entries currently owned by this consumer.
    pub fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Get idle time in milliseconds (time since last seen).
    pub fn idle_ms(&self) -> u64 {
        self.last_seen.elapsed().as_millis() as u64
    }

    /// Get inactive time in milliseconds (time since last active consumption).
    /// Returns -1 if the consumer has never actively consumed entries.
    pub fn inactive_ms(&self) -> i64 {
        match self.active_time {
            Some(t) => t.elapsed().as_millis() as i64,
            None => -1,
        }
    }

    /// Touch the consumer (update last_seen).
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }

    /// Mark the consumer as having actively consumed entries.
    pub fn touch_active(&mut self) {
        self.active_time = Some(Instant::now());
    }
}

/// How a claim adjusts a pending entry's delivery bookkeeping.
///
/// One place for the `IDLE` / `TIME` / `RETRYCOUNT` / `JUSTID` rules that used
/// to live inline — and divergently — in the XCLAIM and XAUTOCLAIM commands.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaimOpts {
    /// `XCLAIM IDLE <ms>`: set `delivery_time` so `idle_ms()` equals this.
    pub idle: Option<u64>,
    /// `XCLAIM TIME <unix-ms>`: set the last-delivery time. With the monotonic
    /// `Instant` clock this degrades to "now": the supplied timestamp cannot be
    /// represented, matching the previous behaviour where it was parsed then
    /// discarded. See the proposal's risks section.
    pub time: Option<u64>,
    /// `XCLAIM RETRYCOUNT <n>`: force `delivery_count` to this value.
    pub retrycount: Option<u32>,
    /// `JUSTID`: do not bump `delivery_count`.
    pub justid: bool,
}

impl ClaimOpts {
    /// XAUTOCLAIM (and the default XCLAIM re-stamp): set `delivery_time = now`.
    ///
    /// Expressed as `idle: Some(0)` because `now - 0ms == now`; this reuses the
    /// `idle` path in [`ClaimOpts::apply`] instead of a dedicated "now" sentinel.
    pub fn touch_now(justid: bool) -> Self {
        Self {
            idle: Some(0),
            time: None,
            retrycount: None,
            justid,
        }
    }

    /// Apply the `delivery_time` / `delivery_count` rules to a pending entry.
    ///
    /// Mirrors the previous inline order: `IDLE` then `TIME` (so `TIME` wins if
    /// both are given), then `RETRYCOUNT` overrides, else `!JUSTID` increments.
    fn apply(self, pe: &mut PendingEntry) {
        if let Some(idle_ms) = self.idle {
            pe.delivery_time = Instant::now() - Duration::from_millis(idle_ms);
        }
        if self.time.is_some() {
            pe.delivery_time = Instant::now();
        }
        if let Some(rc) = self.retrycount {
            pe.delivery_count = rc;
        } else if !self.justid {
            pe.delivery_count = pe.delivery_count.saturating_add(1);
        }
    }
}

/// Consumer group for a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group name.
    pub name: Bytes,
    /// Last delivered ID (entries after this are "new").
    ///
    /// Private: read via [`Self::last_delivered_id`]; mutation routes through
    /// this type's methods.
    last_delivered_id: StreamId,
    /// Pending entries list (PEL) - entries delivered but not acknowledged.
    ///
    /// Private: the PEL and the per-consumer `pending_count` fields form a single
    /// invariant (`sum(consumer.pending_count) == pending.len()`). All mutation
    /// routes through this type's methods so the invariant cannot be broken from
    /// the command layer. Read access is via [`Self::pending_entries`],
    /// [`Self::pending_summary`], [`Self::pending_idle`], etc.
    pending: BTreeMap<StreamId, PendingEntry>,
    /// Consumers in this group. Private (see `pending`).
    consumers: BTreeMap<Bytes, Consumer>,
    /// Number of entries read by this group (for XINFO).
    ///
    /// Private: read via [`Self::entries_read`].
    entries_read: Option<u64>,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: BTreeMap::new(),
            entries_read: None,
        }
    }

    /// Last delivered ID (entries after this are "new").
    #[inline]
    pub fn last_delivered_id(&self) -> StreamId {
        self.last_delivered_id
    }

    /// Number of entries read by this group (for XINFO).
    #[inline]
    pub fn entries_read(&self) -> Option<u64> {
        self.entries_read
    }

    /// Get or create a consumer.
    pub fn get_or_create_consumer(&mut self, name: Bytes) -> &mut Consumer {
        self.consumers
            .entry(name.clone())
            .or_insert_with(|| Consumer::new(name))
    }

    /// Create a consumer if it doesn't exist.
    ///
    /// Returns true if the consumer was created, false if it already existed.
    pub fn create_consumer(&mut self, name: Bytes) -> bool {
        if self.consumers.contains_key(&name) {
            false
        } else {
            self.consumers.insert(name.clone(), Consumer::new(name));
            true
        }
    }

    /// Delete a consumer.
    ///
    /// Returns the number of pending entries that were deleted with the consumer.
    pub fn delete_consumer(&mut self, name: &[u8]) -> usize {
        if self.consumers.remove(name).is_some() {
            // Remove all pending entries for this consumer
            let to_remove: Vec<StreamId> = self
                .pending
                .iter()
                .filter(|(_, pe)| pe.consumer.as_ref() == name)
                .map(|(id, _)| *id)
                .collect();
            let count = to_remove.len();
            for id in to_remove {
                self.pending.remove(&id);
            }
            count
        } else {
            0
        }
    }

    /// Add a pending entry for a consumer.
    pub fn add_pending(&mut self, id: StreamId, consumer: Bytes) {
        // Update consumer's pending count
        if let Some(c) = self.consumers.get_mut(&consumer) {
            c.pending_count += 1;
            c.touch();
        }
        self.pending.insert(id, PendingEntry::new(consumer));
    }

    /// Acknowledge entries (remove from PEL).
    ///
    /// Returns the number of entries acknowledged.
    pub fn ack(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(pe) = self.pending.remove(id) {
                count += 1;
                // Update consumer's pending count
                if let Some(c) = self.consumers.get_mut(&pe.consumer) {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
        count
    }

    /// Reassign (or, with FORCE semantics, create) a pending entry so that
    /// `consumer` owns it, keeping each consumer's `pending_count` correct.
    ///
    /// This is the single home of the "claim" half of the PEL invariant
    /// `sum(consumer.pending_count) == pending.len()`: it decrements the prior
    /// owner, (re)stamps the entry's delivery bookkeeping via `opts`, then
    /// increments the new owner. The target consumer is created if absent.
    ///
    /// If `id` is not already in the PEL the entry is created (the XCLAIM FORCE
    /// path); callers that must not create entries should branch on existence
    /// first (XAUTOCLAIM routes missing entries to [`Self::drop_missing_pending`]).
    pub fn claim_pending(&mut self, id: StreamId, consumer: &Bytes, opts: ClaimOpts) {
        // Decrement the prior owner's count, if the entry already exists.
        if let Some(pe) = self.pending.get(&id) {
            let old = pe.consumer.clone();
            if let Some(c) = self.consumers.get_mut(&old) {
                c.pending_count = c.pending_count.saturating_sub(1);
            }
        }
        // (Re)create the entry and reassign it to the new consumer.
        let pe = self
            .pending
            .entry(id)
            .or_insert_with(|| PendingEntry::new(consumer.clone()));
        pe.consumer = consumer.clone();
        opts.apply(pe);
        // Increment the new owner's count, creating the consumer if needed
        // (Consumer has no Default, so or_insert_with builds it from the name).
        let c = self
            .consumers
            .entry(consumer.clone())
            .or_insert_with(|| Consumer::new(consumer.clone()));
        c.pending_count += 1;
        c.touch();
    }

    /// Evict a PEL entry whose underlying stream message no longer exists,
    /// decrementing its current owner's count.
    ///
    /// Used by XAUTOCLAIM for entries deleted from the stream between the scan
    /// and the claim: they must be removed from the PEL (and reported in the
    /// deleted array), never reassigned. Mirrors the count-correct removal in
    /// [`Self::ack`] / `remove_all_pel_refs`.
    pub fn drop_missing_pending(&mut self, id: &StreamId) {
        if let Some(pe) = self.pending.remove(id)
            && let Some(c) = self.consumers.get_mut(&pe.consumer)
        {
            c.pending_count = c.pending_count.saturating_sub(1);
        }
    }

    /// Get pending entry count summary.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of consumers in this group (for XINFO).
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Iterate the group's consumers (read-only) for XINFO GROUPS/CONSUMERS/STREAM.
    pub fn consumers(&self) -> impl Iterator<Item = &Consumer> {
        self.consumers.values()
    }

    /// Idle time (ms since last delivery) of a single pending entry, or `None`
    /// if `id` is not in the PEL. Drives XCLAIM's min-idle / FORCE decision.
    pub fn pending_idle(&self, id: &StreamId) -> Option<u64> {
        self.pending.get(id).map(|pe| pe.idle_ms())
    }

    /// Summarise the PEL for XPENDING summary mode: total count, min/max ID, and
    /// per-consumer counts. Returns `None` when the PEL is empty.
    pub fn pending_summary(&self) -> Option<PendingSummary> {
        let min_id = *self.pending.first_key_value()?.0;
        let max_id = *self.pending.last_key_value()?.0;
        // Group by consumer deterministically (BTreeMap keeps a stable order).
        let mut counts: BTreeMap<Bytes, u64> = BTreeMap::new();
        for pe in self.pending.values() {
            *counts.entry(pe.consumer.clone()).or_insert(0) += 1;
        }
        Some(PendingSummary {
            count: self.pending.len(),
            min_id,
            max_id,
            per_consumer: counts.into_iter().collect(),
        })
    }

    /// Query pending entries (ascending by ID) for XPENDING detailed mode, the
    /// XINFO PEL arrays, and XREADGROUP's PEL re-read.
    ///
    /// Filters by `start`/`end` bounds, an optional minimum idle time, and an
    /// optional owning `consumer`, returning at most `count` entries.
    pub fn pending_entries(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: usize,
        min_idle_ms: Option<u64>,
        consumer: Option<&Bytes>,
    ) -> Vec<PendingInfo> {
        self.pending
            .iter()
            .filter(|(id, pe)| {
                start.satisfies_min(id)
                    && end.satisfies_max(id)
                    && min_idle_ms.is_none_or(|min| pe.idle_ms() >= min)
                    && consumer.is_none_or(|c| pe.consumer == *c)
            })
            .take(count)
            .map(|(id, pe)| PendingInfo {
                id: *id,
                consumer: pe.consumer.clone(),
                idle_ms: pe.idle_ms(),
                delivery_count: pe.delivery_count,
            })
            .collect()
    }

    /// Scan the PEL from `start` (inclusive) for XAUTOCLAIM: collect up to
    /// `count` IDs whose idle time is at least `min_idle_ms`, and compute the
    /// next cursor (`StreamId::default()` once the scan reaches the end).
    ///
    /// This owns XAUTOCLAIM's cursor arithmetic so the command layer never walks
    /// the PEL map itself.
    pub fn autoclaim_scan(
        &self,
        start: StreamId,
        min_idle_ms: u64,
        count: usize,
    ) -> (Vec<StreamId>, StreamId) {
        let mut to_claim: Vec<StreamId> = Vec::new();
        let mut scanned_to_end = true;

        for (id, pe) in self.pending.range(start..) {
            if pe.idle_ms() >= min_idle_ms {
                to_claim.push(*id);
                if to_claim.len() >= count {
                    // Are there more matching-or-not entries after this one?
                    let next_id = StreamId::new(id.ms, id.seq + 1);
                    scanned_to_end = self.pending.range(next_id..).next().is_none();
                    break;
                }
            }
        }

        let next_cursor = if scanned_to_end {
            StreamId::default()
        } else {
            to_claim
                .last()
                .map(|id| StreamId::new(id.ms, id.seq + 1))
                .unwrap_or_default()
        };

        (to_claim, next_cursor)
    }

    /// Calculate memory size of this group.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let pending_size: usize = self
            .pending
            .values()
            .map(|pe| std::mem::size_of::<StreamId>() + pe.consumer.len() + 32)
            .sum();
        let consumers_size: usize = self
            .consumers
            .keys()
            .map(|k| k.len() + std::mem::size_of::<Consumer>() + 32)
            .sum();
        base + pending_size + consumers_size + self.name.len()
    }
}

/// Strategy for handling consumer group PEL references during stream entry deletion.
///
/// Used by XDELEX and XACKDEL commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeleteRefStrategy {
    /// Delete entry, preserve PEL references (same as XDEL behavior).
    #[default]
    KeepRef,
    /// Delete entry AND remove all PEL references across all groups.
    DelRef,
    /// Only delete entries acknowledged by ALL consumer groups.
    Acked,
}

/// Trimming strategy for streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimStrategy {
    /// Trim by maximum length.
    MaxLen(u64),
    /// Trim by minimum ID (entries older than this ID are removed).
    MinId(StreamId),
}

/// Trimming mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTrimMode {
    /// Exact trimming (remove exactly to the threshold).
    Exact,
    /// Approximate trimming (may keep slightly more entries for efficiency).
    Approximate,
}

/// Trimming options for XTRIM and XADD.
#[derive(Debug, Clone, Copy)]
pub struct StreamTrimOptions {
    /// Trimming strategy.
    pub strategy: StreamTrimStrategy,
    /// Trimming mode.
    pub mode: StreamTrimMode,
    /// Maximum number of entries to trim in one operation (0 = unlimited).
    pub limit: usize,
}

/// Idempotency deduplication state for event sourcing streams.
///
/// Tracks recently seen idempotency keys with bounded FIFO eviction.
/// Default capacity: 10,000 keys.
#[derive(Debug, Clone)]
pub struct IdempotencyState {
    /// Set of active idempotency keys for O(1) lookup.
    keys: HashSet<Bytes>,
    /// FIFO order for eviction — oldest key at front.
    order: VecDeque<Bytes>,
    /// Maximum number of idempotency keys to retain.
    limit: usize,
}

impl IdempotencyState {
    /// Default maximum number of idempotency keys.
    pub const DEFAULT_LIMIT: usize = 10_000;

    /// Create a new idempotency state with the default limit.
    pub fn new() -> Self {
        Self {
            keys: HashSet::new(),
            order: VecDeque::new(),
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// Check if a key exists in the dedup set.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }

    /// Record an idempotency key, evicting the oldest if at capacity.
    pub fn record(&mut self, key: Bytes) {
        if self.keys.contains(&key) {
            return;
        }
        // Evict oldest if at capacity
        while self.order.len() >= self.limit {
            if let Some(old) = self.order.pop_front() {
                self.keys.remove(&old);
            }
        }
        self.keys.insert(key.clone());
        self.order.push_back(key);
    }

    /// Number of tracked idempotency keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the idempotency set is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Iterate over all idempotency keys in FIFO order.
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.order.iter()
    }

    /// Get the capacity limit.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for IdempotencyState {
    fn default() -> Self {
        Self::new()
    }
}

/// Stream value - an append-only log of entries with consumer groups.
#[derive(Debug, Clone)]
pub struct StreamValue {
    /// Entries ordered by ID.
    entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated/added ID (for auto-generation).
    last_id: StreamId,
    /// Consumer groups.
    groups: BTreeMap<Bytes, ConsumerGroup>,
    /// First entry ID (cached for efficiency).
    first_id: Option<StreamId>,
    /// Monotonic version counter — incremented on every append, never decremented.
    /// Used by ES.* commands for optimistic concurrency control.
    total_appended: u64,
    /// Total entries ever added (lifetime counter, never decremented).
    /// Used by XSETID ENTRIESADDED and XINFO STREAM.
    entries_added: u64,
    /// Highest ID ever deleted or trimmed. Used by XSETID MAXDELETEDID, XINFO STREAM,
    /// and lag computation (tombstones between a group's position and the stream end
    /// make lag indeterminate).
    max_deleted_id: Option<StreamId>,
    /// Idempotency deduplication state for ES.APPEND IF_NOT_EXISTS.
    /// Lazy-allocated: `None` for non-event-sourcing streams (zero overhead).
    idempotency: Option<Box<IdempotencyState>>,
}

impl Default for StreamValue {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamValue {
    /// Create a new empty stream.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::default(),
            groups: BTreeMap::new(),
            first_id: None,
            total_appended: 0,
            entries_added: 0,
            max_deleted_id: None,
            idempotency: None,
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the last entry ID.
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    /// Set the last entry ID (used by XSETID).
    pub fn set_last_id(&mut self, id: StreamId) {
        self.last_id = id;
    }

    /// Get the first entry ID.
    pub fn first_id(&self) -> Option<StreamId> {
        self.first_id
    }

    /// Get lifetime entries-added counter.
    pub fn entries_added(&self) -> u64 {
        self.entries_added
    }

    /// Set entries-added counter (used by XSETID ENTRIESADDED).
    pub fn set_entries_added(&mut self, n: u64) {
        self.entries_added = n;
    }

    /// Get the highest deleted entry ID.
    pub fn max_deleted_id(&self) -> Option<StreamId> {
        self.max_deleted_id
    }

    /// Set the max-deleted-entry-id (used by XSETID MAXDELETEDID).
    pub fn set_max_deleted_id(&mut self, id: StreamId) {
        self.max_deleted_id = Some(id);
    }

    /// Get the first entry.
    pub fn first_entry(&self) -> Option<StreamEntry> {
        self.entries
            .first_key_value()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
    }

    /// Get the last entry.
    pub fn last_entry(&self) -> Option<StreamEntry> {
        self.entries
            .last_key_value()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
    }

    /// Add an entry to the stream.
    ///
    /// Returns the ID of the added entry, or an error if the ID is invalid.
    pub fn add(
        &mut self,
        id_spec: StreamIdSpec,
        fields: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, StreamAddError> {
        let id = match id_spec {
            StreamIdSpec::Auto => {
                StreamId::generate(&self.last_id).ok_or(StreamAddError::IdOverflow)?
            }
            StreamIdSpec::AutoSeq(ms) => {
                StreamId::generate_with_ms(ms, &self.last_id).ok_or(if ms < self.last_id.ms {
                    StreamAddError::IdTooSmall
                } else {
                    StreamAddError::IdOverflow
                })?
            }
            StreamIdSpec::Explicit(id) => {
                if self.is_empty() {
                    // Allow any ID (including 0-0) on empty streams
                    id
                } else if !id.is_valid_after(&self.last_id) {
                    return Err(StreamAddError::IdTooSmall);
                } else {
                    id
                }
            }
        };

        self.entries.insert(id, fields);
        self.last_id = id;
        self.total_appended += 1;
        self.entries_added += 1;

        // Update first_id if this is the first entry
        if self.first_id.is_none() {
            self.first_id = Some(id);
        }

        Ok(id)
    }

    // ==================== Event Sourcing Methods ====================

    /// Get the monotonic append counter (used as version for OCC).
    pub fn total_appended(&self) -> u64 {
        self.total_appended
    }

    /// Set the total_appended counter (used during deserialization).
    pub fn set_total_appended(&mut self, value: u64) {
        self.total_appended = value;
    }

    /// Get a reference to the idempotency state, if initialized.
    pub fn idempotency(&self) -> Option<&IdempotencyState> {
        self.idempotency.as_deref()
    }

    /// Set the idempotency state (used during deserialization).
    pub fn set_idempotency(&mut self, state: IdempotencyState) {
        self.idempotency = Some(Box::new(state));
    }

    /// Check if an idempotency key exists.
    pub fn has_idempotency_key(&self, key: &[u8]) -> bool {
        self.idempotency
            .as_ref()
            .is_some_and(|state| state.contains(key))
    }

    /// Record an idempotency key, initializing the state if needed.
    pub fn record_idempotency_key(&mut self, key: Bytes) {
        self.idempotency
            .get_or_insert_with(|| Box::new(IdempotencyState::new()))
            .record(key);
    }

    /// Append with optimistic concurrency control (for ES.APPEND).
    ///
    /// Checks `expected_version` against `total_appended`. If they match,
    /// appends the entry and increments the version. Optionally checks
    /// and records an idempotency key for IF_NOT_EXISTS dedup.
    ///
    /// Returns `(stream_id, new_version)` on success.
    pub fn add_with_version_check(
        &mut self,
        expected_version: u64,
        fields: Vec<(Bytes, Bytes)>,
        idempotency_key: Option<&Bytes>,
    ) -> Result<(StreamId, u64), EsAppendError> {
        // Idempotency check: if key already seen, return current version
        if let Some(idem_key) = idempotency_key
            && self.has_idempotency_key(idem_key)
        {
            return Err(EsAppendError::DuplicateIdempotencyKey {
                version: self.total_appended,
            });
        }

        // Version check (OCC)
        if expected_version != self.total_appended {
            return Err(EsAppendError::VersionMismatch {
                expected: expected_version,
                actual: self.total_appended,
            });
        }

        // Append entry with auto-generated ID
        let id = self
            .add(StreamIdSpec::Auto, fields)
            .map_err(|_| EsAppendError::Internal("Failed to generate stream ID".to_string()))?;

        // Record idempotency key if provided
        if let Some(idem_key) = idempotency_key {
            self.record_idempotency_key(idem_key.clone());
        }

        Ok((id, self.total_appended))
    }

    /// Read entries by version range (1-based inclusive).
    ///
    /// Version 1 corresponds to the first entry ever appended, version 2 to the
    /// second, etc. This is O(N) skip for v1 — acceptable for initial implementation.
    pub fn range_by_version(
        &self,
        start: u64,
        end: Option<u64>,
        count: Option<usize>,
    ) -> Vec<(u64, StreamEntry)> {
        if start == 0 || start > self.total_appended {
            return vec![];
        }

        let end = end.unwrap_or(self.total_appended);

        // We walk all entries and assign version numbers sequentially.
        // Deleted entries create version gaps — we skip over them.
        // This is correct because total_appended never decrements.
        let mut version = 0u64;
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for (id, fields) in &self.entries {
            version += 1;
            if version > end || results.len() >= limit {
                break;
            }
            if version >= start {
                results.push((version, StreamEntry::new(*id, fields.clone())));
            }
        }

        results
    }

    /// Delete entries by ID.
    ///
    /// Returns the number of entries deleted.
    pub fn delete(&mut self, ids: &[StreamId]) -> usize {
        let mut count = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                count += 1;
                // Track the highest deleted ID
                if self.max_deleted_id.is_none_or(|m| *id > m) {
                    self.max_deleted_id = Some(*id);
                }
            }
        }

        // Update first_id if needed
        if count > 0 {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        count
    }

    /// Check if an entry is fully acknowledged by all consumer groups.
    ///
    /// Returns true if the ID is NOT in any group's PEL (i.e., either never delivered
    /// or already acked by all groups). Also returns true when there are no consumer groups.
    fn is_fully_acked(&self, id: &StreamId) -> bool {
        self.groups
            .values()
            .all(|group| !group.pending.contains_key(id))
    }

    /// Remove all PEL references for given IDs across all consumer groups.
    fn remove_all_pel_refs(&mut self, ids: &[StreamId]) {
        for group in self.groups.values_mut() {
            for id in ids {
                if let Some(pe) = group.pending.remove(id)
                    && let Some(c) = group.consumers.get_mut(&pe.consumer)
                {
                    c.pending_count = c.pending_count.saturating_sub(1);
                }
            }
        }
    }

    /// Extended delete with per-ID result array and reference control.
    ///
    /// Returns per-ID results: `-1` (not found), `1` (deleted), `2` (not deleted, ACKED mode only).
    pub fn delete_ex(&mut self, ids: &[StreamId], strategy: DeleteRefStrategy) -> Vec<i64> {
        let mut results = Vec::with_capacity(ids.len());
        let mut any_deleted = false;

        for id in ids {
            if !self.entries.contains_key(id) {
                results.push(-1);
                continue;
            }

            match strategy {
                DeleteRefStrategy::KeepRef => {
                    self.entries.remove(id);
                    if self.max_deleted_id.is_none_or(|m| *id > m) {
                        self.max_deleted_id = Some(*id);
                    }
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    if self.max_deleted_id.is_none_or(|m| *id > m) {
                        self.max_deleted_id = Some(*id);
                    }
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
                        if self.max_deleted_id.is_none_or(|m| *id > m) {
                            self.max_deleted_id = Some(*id);
                        }
                        any_deleted = true;
                        results.push(1);
                    } else {
                        results.push(2);
                    }
                }
            }
        }

        if any_deleted {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        results
    }

    /// Acknowledge in one group, then conditionally delete based on strategy.
    ///
    /// Returns per-ID results: `-1` (not found), `1` (acked+deleted), `2` (acked but not deleted).
    pub fn ack_and_delete(
        &mut self,
        group_name: &[u8],
        ids: &[StreamId],
        strategy: DeleteRefStrategy,
    ) -> Result<Vec<i64>, StreamGroupError> {
        if !self.groups.contains_key(group_name) {
            return Err(StreamGroupError::NoGroup);
        }

        let mut results = Vec::with_capacity(ids.len());
        let mut any_deleted = false;

        for id in ids {
            if !self.entries.contains_key(id) {
                // Still ack in the group even if entry doesn't exist (matching XACK behavior)
                if let Some(group) = self.groups.get_mut(group_name) {
                    group.ack(&[*id]);
                }
                results.push(-1);
                continue;
            }

            // Ack in the specified group
            if let Some(group) = self.groups.get_mut(group_name) {
                group.ack(&[*id]);
            }

            // Apply delete strategy
            match strategy {
                DeleteRefStrategy::KeepRef => {
                    self.entries.remove(id);
                    if self.max_deleted_id.is_none_or(|m| *id > m) {
                        self.max_deleted_id = Some(*id);
                    }
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    if self.max_deleted_id.is_none_or(|m| *id > m) {
                        self.max_deleted_id = Some(*id);
                    }
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
                        if self.max_deleted_id.is_none_or(|m| *id > m) {
                            self.max_deleted_id = Some(*id);
                        }
                        any_deleted = true;
                        results.push(1);
                    } else {
                        results.push(2);
                    }
                }
            }
        }

        if any_deleted {
            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
        }

        Ok(results)
    }

    /// Get entries in a range.
    pub fn range(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        let iter = self
            .entries
            .iter()
            .filter(|(id, _)| start.satisfies_min(id) && end.satisfies_max(id));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Get entries in a range (reverse order).
    pub fn range_rev(
        &self,
        start: StreamRangeBound,
        end: StreamRangeBound,
        count: Option<usize>,
    ) -> Vec<StreamEntry> {
        // For reverse range, start and end are swapped (end is the "start" bound)
        let iter = self
            .entries
            .iter()
            .rev()
            .filter(|(id, _)| end.satisfies_min(id) && start.satisfies_max(id));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Read entries after a given ID (for XREAD).
    pub fn read_after(&self, after: &StreamId, count: Option<usize>) -> Vec<StreamEntry> {
        let iter = self.entries.range((
            std::ops::Bound::Excluded(*after),
            std::ops::Bound::Unbounded,
        ));

        let entries: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        } else {
            iter.map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
                .collect()
        };

        entries
    }

    /// Trim the stream.
    ///
    /// Returns the number of entries removed.
    pub fn trim(&mut self, options: StreamTrimOptions) -> usize {
        let mut removed = 0;
        let limit = if options.limit == 0 {
            usize::MAX
        } else {
            options.limit
        };

        match options.strategy {
            StreamTrimStrategy::MaxLen(max_len) => {
                let max_len = max_len as usize;
                let excess = self.entries.len().saturating_sub(max_len);

                // Approximate mode with LIMIT: simulate Redis's radix-tree node
                // granularity. Redis only trims whole nodes (default ~100 entries).
                // When the stream is small enough to fit in a single node, a small
                // LIMIT can't cover the whole node, so the trim is a no-op.
                if options.mode == StreamTrimMode::Approximate && options.limit > 0 {
                    const APPROX_NODE_SIZE: usize = 100;
                    if self.entries.len() < APPROX_NODE_SIZE && options.limit < excess {
                        return 0;
                    }
                }

                while self.entries.len() > max_len && removed < limit {
                    if let Some((id, _)) = self.entries.pop_first() {
                        removed += 1;
                        // Note: trim does NOT update max_deleted_id — only XDEL does (Redis compat)
                        // Update first_id
                        if Some(id) == self.first_id {
                            self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                        }
                    } else {
                        break;
                    }
                }
            }
            StreamTrimStrategy::MinId(min_id) => {
                // Approximate mode with LIMIT: simulate node granularity.
                if options.mode == StreamTrimMode::Approximate && options.limit > 0 {
                    const APPROX_NODE_SIZE: usize = 100;
                    let excess = self.entries.range(..min_id).count();
                    if self.entries.len() < APPROX_NODE_SIZE && options.limit < excess {
                        return 0;
                    }
                }

                // Remove all entries with ID < min_id
                let to_remove: Vec<StreamId> = self
                    .entries
                    .range(..min_id)
                    .take(limit)
                    .map(|(id, _)| *id)
                    .collect();
                for id in to_remove {
                    self.entries.remove(&id);
                    removed += 1;
                    // Note: trim does NOT update max_deleted_id — only XDEL does (Redis compat)
                }
                // Update first_id
                if removed > 0 {
                    self.first_id = self.entries.first_key_value().map(|(id, _)| *id);
                }
            }
        }

        removed
    }

    // ==================== Consumer Group Methods ====================

    /// Create a consumer group.
    ///
    /// Returns an error if the group already exists.
    pub fn create_group(
        &mut self,
        name: Bytes,
        last_delivered_id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupError> {
        if self.groups.contains_key(&name) {
            return Err(StreamGroupError::GroupExists);
        }
        let mut group = ConsumerGroup::new(name.clone(), last_delivered_id);
        group.entries_read = entries_read;
        self.groups.insert(name, group);
        Ok(())
    }

    /// Destroy a consumer group.
    ///
    /// Returns true if the group was destroyed, false if it didn't exist.
    pub fn destroy_group(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    /// Get a consumer group.
    pub fn get_group(&self, name: &[u8]) -> Option<&ConsumerGroup> {
        self.groups.get(name)
    }

    /// Get a mutable consumer group.
    pub fn get_group_mut(&mut self, name: &[u8]) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(name)
    }

    /// Get all consumer groups.
    pub fn groups(&self) -> impl Iterator<Item = &ConsumerGroup> {
        self.groups.values()
    }

    /// Number of consumer groups.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Set a consumer group's last delivered ID.
    pub fn set_group_id(
        &mut self,
        name: &[u8],
        id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<(), StreamGroupError> {
        let group = self.groups.get_mut(name).ok_or(StreamGroupError::NoGroup)?;
        group.last_delivered_id = id;
        if entries_read.is_some() {
            group.entries_read = entries_read;
        }
        Ok(())
    }

    /// Compute the lag for a consumer group (number of unconsumed entries).
    ///
    /// Returns `None` when lag is indeterminate (tombstones create ambiguity).
    pub fn compute_lag(&self, group: &ConsumerGroup) -> Option<u64> {
        // Group has consumed everything
        if group.last_delivered_id >= self.last_id {
            return Some(0);
        }
        // Empty stream — nothing to consume
        if self.is_empty() {
            return Some(0);
        }

        let first = self.first_id.unwrap(); // safe: not empty
        let max_del = self.max_deleted_id.unwrap_or_default();
        let has_tombstones_in_stream = max_del >= first;

        if group.last_delivered_id < first {
            // Group is behind the stream start (entries before it were trimmed)
            return if has_tombstones_in_stream {
                None // tombstones make the count unreliable
            } else {
                Some(self.len() as u64)
            };
        }

        // Group is within stream range
        if let Some(entries_read) = group.entries_read {
            if max_del > group.last_delivered_id && has_tombstones_in_stream {
                None // active tombstones after group position
            } else {
                Some(self.entries_added.saturating_sub(entries_read))
            }
        } else {
            None // can't compute without entries_read
        }
    }

    /// Record that XREADGROUP delivered entries to a consumer group.
    ///
    /// Updates last_delivered_id, entries_read, PEL, and consumer timestamps.
    /// Called from both the non-blocking XREADGROUP path and the blocking satisfy path.
    pub fn record_group_delivery(
        &mut self,
        group_name: &[u8],
        consumer_name: &Bytes,
        delivered_entries: &[StreamEntry],
        noack: bool,
    ) {
        if delivered_entries.is_empty() {
            return;
        }

        let new_last_delivered = delivered_entries.last().unwrap().id;
        let num_delivered = delivered_entries.len() as u64;

        let group = match self.groups.get_mut(group_name) {
            Some(g) => g,
            None => return,
        };

        let old_last_delivered = group.last_delivered_id;
        group.last_delivered_id = new_last_delivered;

        // Update entries_read
        if let Some(n) = group.entries_read {
            group.entries_read = Some(n + num_delivered);
        } else if let Some(first) = self.first_id
            && old_last_delivered < first
            && self.max_deleted_id.unwrap_or_default() < first
        {
            // Group was behind stream start with no tombstones — we know the exact count
            group.entries_read = Some(num_delivered);
        }
        // If group caught up, override entries_read to entries_added
        if new_last_delivered >= self.last_id {
            group.entries_read = Some(self.entries_added);
        }

        // Add to PEL
        if !noack {
            for entry in delivered_entries {
                group.add_pending(entry.id, consumer_name.clone());
            }
        }

        // Touch consumer timestamps
        let consumer = group.get_or_create_consumer(consumer_name.clone());
        consumer.touch();
        consumer.touch_active();
    }

    /// Get an entry by ID.
    pub fn get(&self, id: &StreamId) -> Option<StreamEntry> {
        self.entries
            .get(id)
            .map(|fields| StreamEntry::new(*id, fields.clone()))
    }

    /// Check if an entry exists.
    pub fn contains(&self, id: &StreamId) -> bool {
        self.entries.contains_key(id)
    }

    /// Calculate memory size of this stream.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();

        let entries_size: usize = self
            .entries
            .values()
            .map(|fields| {
                let fields_size: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 16).sum();
                std::mem::size_of::<StreamId>() + fields_size + 32
            })
            .sum();

        let groups_size: usize = self.groups.values().map(|g| g.memory_size()).sum();

        base + entries_size + groups_size
    }

    /// Convert to vec for serialization.
    pub fn to_vec(&self) -> Vec<StreamEntry> {
        self.entries
            .iter()
            .map(|(id, fields)| StreamEntry::new(*id, fields.clone()))
            .collect()
    }
}

/// Error when adding to a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAddError {
    /// The ID is equal to or smaller than the last ID.
    IdTooSmall,
    /// The stream ID would overflow (all IDs exhausted).
    IdOverflow,
}

impl std::fmt::Display for StreamAddError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamAddError::IdTooSmall => {
                write!(
                    f,
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                )
            }
            StreamAddError::IdOverflow => {
                write!(
                    f,
                    "ERR The stream has exhausted the last possible ID, unable to add more items"
                )
            }
        }
    }
}

impl std::error::Error for StreamAddError {}

/// Error for ES.APPEND operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EsAppendError {
    /// OCC version mismatch.
    VersionMismatch { expected: u64, actual: u64 },
    /// Idempotency key already exists (not a real error — returns current version).
    DuplicateIdempotencyKey { version: u64 },
    /// Internal failure (stream ID generation, etc.).
    Internal(String),
}

impl std::fmt::Display for EsAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EsAppendError::VersionMismatch { expected, actual } => {
                write!(f, "VERSIONMISMATCH expected {expected} actual {actual}")
            }
            EsAppendError::DuplicateIdempotencyKey { version } => {
                write!(f, "duplicate idempotency key at version {version}")
            }
            EsAppendError::Internal(msg) => {
                write!(f, "ERR {msg}")
            }
        }
    }
}

impl std::error::Error for EsAppendError {}

/// Error for consumer group operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamGroupError {
    /// Consumer group already exists.
    GroupExists,
    /// Consumer group doesn't exist.
    NoGroup,
}

impl std::fmt::Display for StreamGroupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamGroupError::GroupExists => {
                write!(f, "BUSYGROUP Consumer Group name already exists")
            }
            StreamGroupError::NoGroup => {
                write!(f, "NOGROUP No such consumer group")
            }
        }
    }
}

impl std::error::Error for StreamGroupError {}

#[cfg(test)]
mod claim_tests {
    use super::*;

    fn name(s: &[u8]) -> Bytes {
        Bytes::copy_from_slice(s)
    }

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId::new(ms, seq)
    }

    /// The invariant this proposal protects:
    /// `sum(consumer.pending_count) == pending.len()`.
    fn assert_invariant(g: &ConsumerGroup) {
        let counted: usize = g.consumers.values().map(|c| c.pending_count).sum();
        assert_eq!(
            counted,
            g.pending.len(),
            "PEL count invariant broken: sum(pending_count)={counted}, pending.len()={}",
            g.pending.len()
        );
    }

    /// Build a group with the given consumers, each owning the listed entries.
    fn group_with(owned: &[(&[u8], &[StreamId])]) -> ConsumerGroup {
        let mut g = ConsumerGroup::new(name(b"g"), StreamId::default());
        for (consumer, ids) in owned {
            g.create_consumer(name(consumer));
            for id in *ids {
                g.add_pending(*id, name(consumer));
            }
        }
        assert_invariant(&g);
        g
    }

    #[test]
    fn claim_reassigns_between_consumers() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i]), (b"b", &[])]);

        g.claim_pending(i, &name(b"b"), ClaimOpts::default());

        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 0);
        assert_eq!(g.consumers[b"b".as_slice()].pending_count, 1);
        assert_eq!(g.pending[&i].consumer, name(b"b"));
        // add_pending started delivery_count at 1; default (!justid) bumps to 2.
        assert_eq!(g.pending[&i].delivery_count, 2);
        assert_invariant(&g);
    }

    #[test]
    fn claim_self_reclaim_is_net_zero() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);

        g.claim_pending(i, &name(b"a"), ClaimOpts::default());

        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 1);
        assert_eq!(g.pending[&i].consumer, name(b"a"));
        assert_invariant(&g);
    }

    #[test]
    fn claim_force_creates_entry() {
        let i = id(5, 0);
        let mut g = group_with(&[(b"a", &[])]);
        assert!(g.pending.is_empty());

        // FORCE path: id absent, entry is created and owned by the claimant.
        g.claim_pending(i, &name(b"a"), ClaimOpts::default());

        assert!(g.pending.contains_key(&i));
        assert_eq!(g.pending[&i].consumer, name(b"a"));
        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 1);
        // Created at delivery_count 1, then bumped by the default (!justid) claim.
        assert_eq!(g.pending[&i].delivery_count, 2);
        assert_invariant(&g);
    }

    #[test]
    fn claim_force_create_with_justid_keeps_count_one() {
        let i = id(5, 0);
        let mut g = group_with(&[(b"a", &[])]);

        g.claim_pending(i, &name(b"a"), ClaimOpts::touch_now(true));

        assert_eq!(g.pending[&i].delivery_count, 1);
        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 1);
        assert_invariant(&g);
    }

    #[test]
    fn claim_creates_missing_target_consumer() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);
        assert!(!g.consumers.contains_key(b"new".as_slice()));

        // The target consumer does not exist yet; claim_pending must create it.
        g.claim_pending(i, &name(b"new"), ClaimOpts::default());

        assert!(g.consumers.contains_key(b"new".as_slice()));
        assert_eq!(g.consumers[b"new".as_slice()].pending_count, 1);
        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 0);
        assert_invariant(&g);
    }

    #[test]
    fn claim_justid_does_not_bump_delivery_count() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]); // delivery_count == 1

        g.claim_pending(
            i,
            &name(b"b"),
            ClaimOpts {
                justid: true,
                ..Default::default()
            },
        );

        assert_eq!(g.pending[&i].delivery_count, 1);
        assert_invariant(&g);
    }

    #[test]
    fn claim_retrycount_sets_exact_value_overriding_justid() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);

        g.claim_pending(
            i,
            &name(b"b"),
            ClaimOpts {
                retrycount: Some(7),
                justid: true,
                ..Default::default()
            },
        );

        assert_eq!(g.pending[&i].delivery_count, 7);
        assert_invariant(&g);
    }

    #[test]
    fn claim_idle_sets_delivery_time_in_the_past() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);

        g.claim_pending(
            i,
            &name(b"b"),
            ClaimOpts {
                idle: Some(1000),
                ..Default::default()
            },
        );

        let idle = g.pending[&i].idle_ms();
        assert!(
            (1000..2000).contains(&idle),
            "idle_ms() = {idle}, want ~1000"
        );
        assert_invariant(&g);
    }

    #[test]
    fn claim_default_leaves_delivery_time_unchanged() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);

        // Push delivery_time ~2s into the past via IDLE.
        g.claim_pending(
            i,
            &name(b"b"),
            ClaimOpts {
                idle: Some(2000),
                ..Default::default()
            },
        );
        // A claim without IDLE/TIME must NOT reset delivery_time.
        g.claim_pending(i, &name(b"a"), ClaimOpts::default());

        let idle = g.pending[&i].idle_ms();
        assert!(idle >= 2000, "delivery_time was reset: idle_ms() = {idle}");
        assert_invariant(&g);
    }

    #[test]
    fn claim_time_resets_delivery_time_to_now() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);

        // Age the entry, then claim with TIME — which maps to "now".
        g.claim_pending(
            i,
            &name(b"b"),
            ClaimOpts {
                idle: Some(2000),
                ..Default::default()
            },
        );
        g.claim_pending(
            i,
            &name(b"a"),
            ClaimOpts {
                time: Some(99_999),
                ..Default::default()
            },
        );

        let idle = g.pending[&i].idle_ms();
        assert!(
            idle < 1000,
            "TIME did not reset delivery_time: idle_ms() = {idle}"
        );
        assert_invariant(&g);
    }

    #[test]
    fn touch_now_resets_delivery_time_and_bumps() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);
        g.claim_pending(
            i,
            &name(b"a"),
            ClaimOpts {
                idle: Some(2000),
                justid: true,
                ..Default::default()
            },
        );

        g.claim_pending(i, &name(b"b"), ClaimOpts::touch_now(false));

        assert!(g.pending[&i].idle_ms() < 1000);
        assert_eq!(g.pending[&i].delivery_count, 2);
        assert_invariant(&g);
    }

    #[test]
    fn drop_missing_decrements_owner() {
        let (i1, i2) = (id(1, 0), id(2, 0));
        let mut g = group_with(&[(b"a", &[i1, i2])]);

        g.drop_missing_pending(&i1);

        assert!(!g.pending.contains_key(&i1));
        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 1);
        assert_eq!(g.pending.len(), 1);
        assert_invariant(&g);
    }

    #[test]
    fn drop_missing_absent_id_is_noop() {
        let i1 = id(1, 0);
        let mut g = group_with(&[(b"a", &[i1])]);

        g.drop_missing_pending(&id(99, 0)); // not in the PEL

        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 1);
        assert_eq!(g.pending.len(), 1);
        assert_invariant(&g);
    }

    /// Type-level analogue of the XAUTOCLAIM deleted-entry bug: claiming a live
    /// entry while evicting a stream-deleted one must keep the invariant — the
    /// new consumer must not over-count the dropped entry.
    #[test]
    fn claim_live_and_drop_deleted_keeps_invariant() {
        let (live, deleted) = (id(1, 0), id(2, 0));
        let mut g = group_with(&[(b"a", &[live, deleted]), (b"b", &[])]);

        // `deleted` is gone from the stream: evict, don't claim.
        g.claim_pending(live, &name(b"b"), ClaimOpts::touch_now(false));
        g.drop_missing_pending(&deleted);

        assert_eq!(g.consumers[b"a".as_slice()].pending_count, 0);
        assert_eq!(g.consumers[b"b".as_slice()].pending_count, 1);
        assert_eq!(g.pending.len(), 1);
        assert_invariant(&g);
    }

    // ---- PEL read/query interface (XPENDING / XINFO / XAUTOCLAIM / XREADGROUP) ----

    #[test]
    fn pending_summary_counts_bounds_and_per_consumer() {
        let (i1, i2, i3) = (id(1, 0), id(2, 0), id(3, 0));
        let g = group_with(&[(b"a", &[i1, i3]), (b"b", &[i2])]);

        let s = g.pending_summary().expect("non-empty PEL");
        assert_eq!(s.count, 3);
        assert_eq!(s.min_id, i1);
        assert_eq!(s.max_id, i3);
        // Deterministic order (BTreeMap by consumer name): a=2, b=1.
        assert_eq!(s.per_consumer, vec![(name(b"a"), 2), (name(b"b"), 1)]);
    }

    #[test]
    fn pending_summary_empty_is_none() {
        let g = group_with(&[(b"a", &[])]);
        assert!(g.pending_summary().is_none());
    }

    #[test]
    fn pending_entries_respects_bounds_and_count() {
        let (i1, i2, i3) = (id(1, 0), id(2, 0), id(3, 0));
        let g = group_with(&[(b"a", &[i1, i2, i3])]);

        let ids = |v: Vec<PendingInfo>| v.into_iter().map(|p| p.id).collect::<Vec<_>>();

        // Inclusive [i1, i2].
        assert_eq!(
            ids(g.pending_entries(
                StreamRangeBound::Inclusive(i1),
                StreamRangeBound::Inclusive(i2),
                10,
                None,
                None,
            )),
            vec![i1, i2]
        );

        // COUNT limits the result, ascending order preserved.
        assert_eq!(
            ids(g.pending_entries(StreamRangeBound::Min, StreamRangeBound::Max, 2, None, None)),
            vec![i1, i2]
        );

        // Exclusive lower bound drops i1.
        assert_eq!(
            ids(g.pending_entries(
                StreamRangeBound::Exclusive(i1),
                StreamRangeBound::Max,
                10,
                None,
                None,
            )),
            vec![i2, i3]
        );
    }

    #[test]
    fn pending_entries_filters_by_consumer() {
        let (i1, i2, i3) = (id(1, 0), id(2, 0), id(3, 0));
        let g = group_with(&[(b"a", &[i1, i3]), (b"b", &[i2])]);

        let got = g.pending_entries(
            StreamRangeBound::Min,
            StreamRangeBound::Max,
            10,
            None,
            Some(&name(b"a")),
        );
        assert_eq!(got.iter().map(|p| p.id).collect::<Vec<_>>(), vec![i1, i3]);
        assert!(got.iter().all(|p| p.consumer == name(b"a")));
    }

    #[test]
    fn pending_entries_filters_by_min_idle() {
        let (fresh, aged) = (id(1, 0), id(2, 0));
        let mut g = group_with(&[(b"a", &[fresh, aged])]);
        // Age `aged` ~1s into the past; `fresh` stays ~0ms idle.
        g.claim_pending(
            aged,
            &name(b"a"),
            ClaimOpts {
                idle: Some(1000),
                justid: true,
                ..Default::default()
            },
        );

        let got = g.pending_entries(
            StreamRangeBound::Min,
            StreamRangeBound::Max,
            10,
            Some(500),
            None,
        );
        assert_eq!(got.iter().map(|p| p.id).collect::<Vec<_>>(), vec![aged]);
    }

    #[test]
    fn pending_idle_present_and_absent() {
        let i = id(1, 0);
        let mut g = group_with(&[(b"a", &[i])]);
        g.claim_pending(
            i,
            &name(b"a"),
            ClaimOpts {
                idle: Some(750),
                justid: true,
                ..Default::default()
            },
        );

        let idle = g.pending_idle(&i).expect("present in PEL");
        assert!((750..1750).contains(&idle), "idle_ms() = {idle}");
        assert!(g.pending_idle(&id(9, 0)).is_none());
    }

    #[test]
    fn autoclaim_scan_filters_min_idle_and_paginates() {
        let (i1, i2, i3) = (id(1, 0), id(2, 0), id(3, 0));
        let mut g = group_with(&[(b"a", &[i1, i2, i3])]);
        for entry in [i1, i2, i3] {
            g.claim_pending(
                entry,
                &name(b"a"),
                ClaimOpts {
                    idle: Some(1000),
                    justid: true,
                    ..Default::default()
                },
            );
        }

        // Page 1: COUNT=2 stops at i2; cursor points just past i2.
        let (ids, cursor) = g.autoclaim_scan(StreamId::default(), 500, 2);
        assert_eq!(ids, vec![i1, i2]);
        assert_eq!(cursor, StreamId::new(i2.ms, i2.seq + 1));

        // Page 2 from cursor: i3, then the scan reaches the end -> cursor 0-0.
        let (ids, cursor) = g.autoclaim_scan(cursor, 500, 2);
        assert_eq!(ids, vec![i3]);
        assert_eq!(cursor, StreamId::default());
    }

    #[test]
    fn autoclaim_scan_skips_below_min_idle() {
        let (fresh, aged) = (id(1, 0), id(2, 0));
        let mut g = group_with(&[(b"a", &[fresh, aged])]);
        g.claim_pending(
            aged,
            &name(b"a"),
            ClaimOpts {
                idle: Some(1000),
                justid: true,
                ..Default::default()
            },
        );

        // `fresh` (idle ~0) is filtered out; only `aged` qualifies and the scan
        // reaches the end, so the cursor resets to 0-0.
        let (ids, cursor) = g.autoclaim_scan(StreamId::default(), 500, 10);
        assert_eq!(ids, vec![aged]);
        assert_eq!(cursor, StreamId::default());
    }
}
