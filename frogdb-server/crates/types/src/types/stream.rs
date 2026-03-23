//! Stream value types.

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    pub fn generate(last: &StreamId) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        if now_ms > last.ms {
            Self { ms: now_ms, seq: 0 }
        } else {
            // Same or earlier timestamp, increment sequence
            Self {
                ms: last.ms,
                seq: last.seq.saturating_add(1),
            }
        }
    }

    /// Generate a new stream ID with auto-sequence for a given timestamp.
    pub fn generate_with_ms(ms: u64, last: &StreamId) -> Option<Self> {
        if ms > last.ms {
            Some(Self { ms, seq: 0 })
        } else if ms == last.ms {
            Some(Self {
                ms,
                seq: last.seq.saturating_add(1),
            })
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
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// Consumer that owns this pending entry.
    pub consumer: Bytes,
    /// Time when the entry was delivered.
    pub delivery_time: Instant,
    /// Number of times this entry has been delivered.
    pub delivery_count: u32,
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
}

/// Consumer in a consumer group.
#[derive(Debug, Clone)]
pub struct Consumer {
    /// Consumer name.
    pub name: Bytes,
    /// Number of pending entries for this consumer.
    pub pending_count: usize,
    /// Last time this consumer was seen (read or claimed).
    pub last_seen: Instant,
}

impl Consumer {
    /// Create a new consumer.
    pub fn new(name: Bytes) -> Self {
        Self {
            name,
            pending_count: 0,
            last_seen: Instant::now(),
        }
    }

    /// Get idle time in milliseconds.
    pub fn idle_ms(&self) -> u64 {
        self.last_seen.elapsed().as_millis() as u64
    }

    /// Touch the consumer (update last_seen).
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }
}

/// Consumer group for a stream.
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Group name.
    pub name: Bytes,
    /// Last delivered ID (entries after this are "new").
    pub last_delivered_id: StreamId,
    /// Pending entries list (PEL) - entries delivered but not acknowledged.
    pub pending: BTreeMap<StreamId, PendingEntry>,
    /// Consumers in this group.
    pub consumers: HashMap<Bytes, Consumer>,
    /// Number of entries read by this group (for XINFO).
    pub entries_read: Option<u64>,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(name: Bytes, last_delivered_id: StreamId) -> Self {
        Self {
            name,
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: HashMap::new(),
            entries_read: None,
        }
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

    /// Get pending entry count summary.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get the smallest and largest pending IDs.
    pub fn pending_range(&self) -> Option<(StreamId, StreamId)> {
        let first = self.pending.first_key_value()?.0;
        let last = self.pending.last_key_value()?.0;
        Some((*first, *last))
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
    groups: HashMap<Bytes, ConsumerGroup>,
    /// First entry ID (cached for efficiency).
    first_id: Option<StreamId>,
    /// Monotonic version counter — incremented on every append, never decremented.
    /// Used by ES.* commands for optimistic concurrency control.
    total_appended: u64,
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
            groups: HashMap::new(),
            first_id: None,
            total_appended: 0,
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

    /// Get the first entry ID.
    pub fn first_id(&self) -> Option<StreamId> {
        self.first_id
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
            StreamIdSpec::Auto => StreamId::generate(&self.last_id),
            StreamIdSpec::AutoSeq(ms) => {
                StreamId::generate_with_ms(ms, &self.last_id).ok_or(StreamAddError::IdTooSmall)?
            }
            StreamIdSpec::Explicit(id) => {
                if !id.is_valid_after(&self.last_id) {
                    return Err(StreamAddError::IdTooSmall);
                }
                id
            }
        };

        // Check for zero ID (not allowed as first entry)
        if self.is_empty() && id.is_zero() {
            return Err(StreamAddError::IdTooSmall);
        }

        self.entries.insert(id, fields);
        self.last_id = id;
        self.total_appended += 1;

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
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
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
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::DelRef => {
                    self.entries.remove(id);
                    self.remove_all_pel_refs(&[*id]);
                    any_deleted = true;
                    results.push(1);
                }
                DeleteRefStrategy::Acked => {
                    if self.is_fully_acked(id) {
                        self.entries.remove(id);
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
                while self.entries.len() > max_len && removed < limit {
                    if let Some((id, _)) = self.entries.pop_first() {
                        removed += 1;
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
