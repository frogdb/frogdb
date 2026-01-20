//! Value types and key metadata.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Value types stored in FrogDB.
#[derive(Debug, Clone)]
pub enum Value {
    /// String value.
    String(StringValue),
    /// Sorted set value.
    SortedSet(SortedSetValue),
    // Future types:
    // List(ListValue),
    // Set(SetValue),
    // Hash(HashValue),
    // Stream(StreamValue),
}

impl Value {
    /// Create a string value from bytes.
    pub fn string(data: impl Into<Bytes>) -> Self {
        Value::String(StringValue::new(data))
    }

    /// Create a sorted set value.
    pub fn sorted_set() -> Self {
        Value::SortedSet(SortedSetValue::new())
    }

    /// Get the key type.
    pub fn key_type(&self) -> KeyType {
        match self {
            Value::String(_) => KeyType::String,
            Value::SortedSet(_) => KeyType::SortedSet,
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        match self {
            Value::String(s) => s.memory_size(),
            Value::SortedSet(z) => z.memory_size(),
        }
    }

    /// Try to get as a string value.
    pub fn as_string(&self) -> Option<&StringValue> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a mutable string value.
    pub fn as_string_mut(&mut self) -> Option<&mut StringValue> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a sorted set value.
    pub fn as_sorted_set(&self) -> Option<&SortedSetValue> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    /// Try to get as a mutable sorted set value.
    pub fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSetValue> {
        match self {
            Value::SortedSet(z) => Some(z),
            _ => None,
        }
    }
}

/// String value with optional integer encoding.
#[derive(Debug, Clone)]
pub struct StringValue {
    data: StringData,
}

#[derive(Debug, Clone)]
enum StringData {
    /// Raw byte string.
    Raw(Bytes),
    /// Integer value (for efficient INCR/DECR).
    Integer(i64),
}

impl StringValue {
    /// Create a new string value from bytes.
    pub fn new(data: impl Into<Bytes>) -> Self {
        let bytes = data.into();
        // Try to parse as integer for efficient storage
        if let Ok(s) = std::str::from_utf8(&bytes) {
            if let Ok(i) = s.parse::<i64>() {
                return Self {
                    data: StringData::Integer(i),
                };
            }
        }
        Self {
            data: StringData::Raw(bytes),
        }
    }

    /// Create a string value from an integer.
    pub fn from_integer(i: i64) -> Self {
        Self {
            data: StringData::Integer(i),
        }
    }

    /// Get the value as bytes.
    pub fn as_bytes(&self) -> Bytes {
        match &self.data {
            StringData::Raw(b) => b.clone(),
            StringData::Integer(i) => Bytes::from(i.to_string()),
        }
    }

    /// Try to get the value as an integer.
    pub fn as_integer(&self) -> Option<i64> {
        match &self.data {
            StringData::Integer(i) => Some(*i),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }

    /// Calculate memory size.
    pub fn memory_size(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(_) => 8, // i64
        }
    }

    /// Get the byte length of the string.
    pub fn len(&self) -> usize {
        match &self.data {
            StringData::Raw(b) => b.len(),
            StringData::Integer(i) => i.to_string().len(),
        }
    }

    /// Check if the string is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append bytes to the string.
    ///
    /// Returns the new length of the string.
    /// Note: This converts integer-encoded values to raw bytes.
    pub fn append(&mut self, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();
        current.extend_from_slice(data);
        let new_len = current.len();

        // After append, we might still have an integer but it's unlikely,
        // and appending typically makes it non-integer anyway
        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Get a substring by byte indices.
    ///
    /// Supports negative indices (like Python/Redis):
    /// - -1 is the last character
    /// - -2 is second-to-last, etc.
    pub fn get_range(&self, start: i64, end: i64) -> Bytes {
        let bytes = self.as_bytes();
        let len = bytes.len() as i64;

        if len == 0 {
            return Bytes::new();
        }

        // Convert negative indices to positive
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            (start as usize).min(len as usize)
        };

        let end = if end < 0 {
            (len + end).max(0) as usize
        } else {
            (end as usize).min(len as usize - 1)
        };

        // If start > end after conversion, return empty
        if start > end {
            return Bytes::new();
        }

        // end is inclusive in Redis GETRANGE
        bytes.slice(start..=end)
    }

    /// Overwrite part of the string at the given offset.
    ///
    /// If offset is past the end, the string is padded with null bytes.
    /// Returns the new length of the string.
    pub fn set_range(&mut self, offset: usize, data: &[u8]) -> usize {
        let mut current = self.as_bytes().to_vec();

        // Pad with zeros if offset is past end
        if offset > current.len() {
            current.resize(offset, 0);
        }

        // Replace or extend
        let end_pos = offset + data.len();
        if end_pos > current.len() {
            current.resize(end_pos, 0);
        }

        current[offset..end_pos].copy_from_slice(data);
        let new_len = current.len();

        *self = Self::new(Bytes::from(current));
        new_len
    }

    /// Increment the value by delta.
    ///
    /// Returns the new value or an error if not an integer.
    pub fn increment(&mut self, delta: i64) -> Result<i64, IncrementError> {
        let current = self.as_integer().ok_or(IncrementError::NotInteger)?;
        let new_val = current
            .checked_add(delta)
            .ok_or(IncrementError::Overflow)?;
        self.data = StringData::Integer(new_val);
        Ok(new_val)
    }

    /// Increment the value by a float delta.
    ///
    /// Returns the new value or an error if not a valid float.
    pub fn increment_float(&mut self, delta: f64) -> Result<f64, IncrementError> {
        let current = self.as_float().ok_or(IncrementError::NotFloat)?;
        let new_val = current + delta;

        // Check for infinity or NaN
        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        // Store as raw string (floats are always stored as strings in Redis)
        self.data = StringData::Raw(Bytes::from(format_float(new_val)));
        Ok(new_val)
    }

    /// Try to get the value as a float.
    pub fn as_float(&self) -> Option<f64> {
        match &self.data {
            StringData::Integer(i) => Some(*i as f64),
            StringData::Raw(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        }
    }
}

/// Error type for increment operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementError {
    /// Value is not an integer.
    NotInteger,
    /// Value is not a valid float.
    NotFloat,
    /// Operation would overflow.
    Overflow,
}

/// Format a float for Redis compatibility.
///
/// Redis uses specific formatting rules:
/// - No trailing zeros after decimal point
/// - Scientific notation for very large/small numbers
fn format_float(f: f64) -> String {
    // Handle special cases
    if f == 0.0 {
        return "0".to_string();
    }

    // Check if it's a whole number
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{:.0}", f);
    }

    // Use standard formatting, then trim trailing zeros
    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

/// Type identifier for TYPE command and WRONGTYPE errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    /// Key doesn't exist.
    None,
    /// String type.
    String,
    /// List type.
    List,
    /// Set type.
    Set,
    /// Hash type.
    Hash,
    /// Sorted set type.
    SortedSet,
    /// Stream type.
    Stream,
}

impl KeyType {
    /// Get the Redis type name.
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::None => "none",
            KeyType::String => "string",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::Hash => "hash",
            KeyType::SortedSet => "zset",
            KeyType::Stream => "stream",
        }
    }
}

/// Metadata tracked per key.
#[derive(Debug, Clone)]
pub struct KeyMetadata {
    /// Expiration time (None = no expiry).
    pub expires_at: Option<Instant>,

    /// Last access time (for LRU eviction).
    pub last_access: Instant,

    /// Access frequency counter (for LFU eviction).
    pub lfu_counter: u8,

    /// Approximate memory size of this entry.
    pub memory_size: usize,
}

impl KeyMetadata {
    /// Create new metadata with defaults.
    pub fn new(memory_size: usize) -> Self {
        Self {
            expires_at: None,
            last_access: Instant::now(),
            lfu_counter: 5, // New keys start at 5 (not immediately evicted)
            memory_size,
        }
    }

    /// Check if the key is expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| exp <= Instant::now())
            .unwrap_or(false)
    }

    /// Update last access time.
    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

// ============================================================================
// SET Command Options
// ============================================================================

/// Options for the SET command.
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Expiration setting.
    pub expiry: Option<Expiry>,
    /// Conditional set (NX/XX).
    pub condition: SetCondition,
    /// Keep existing TTL when updating.
    pub keep_ttl: bool,
    /// Return the old value (GET flag).
    pub return_old: bool,
}

/// Expiration specification for SET command.
#[derive(Debug, Clone, Copy)]
pub enum Expiry {
    /// Expire in N seconds (EX).
    Ex(u64),
    /// Expire in N milliseconds (PX).
    Px(u64),
    /// Expire at Unix timestamp in seconds (EXAT).
    ExAt(u64),
    /// Expire at Unix timestamp in milliseconds (PXAT).
    PxAt(u64),
}

impl Expiry {
    /// Convert to an absolute Instant for storage.
    pub fn to_instant(&self) -> Option<Instant> {
        let now = Instant::now();
        let system_now = SystemTime::now();

        match self {
            Expiry::Ex(secs) => Some(now + Duration::from_secs(*secs)),
            Expiry::Px(ms) => Some(now + Duration::from_millis(*ms)),
            Expiry::ExAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_secs(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
            Expiry::PxAt(ts) => {
                let target = UNIX_EPOCH + Duration::from_millis(*ts);
                if let Ok(duration) = target.duration_since(system_now) {
                    Some(now + duration)
                } else {
                    // Already expired
                    Some(now)
                }
            }
        }
    }
}

/// Conditional set behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SetCondition {
    /// Always set (default behavior).
    #[default]
    Always,
    /// Only set if key does Not eXist.
    NX,
    /// Only set if key already eXists.
    XX,
}

/// Result of a SET operation.
#[derive(Debug, Clone)]
pub enum SetResult {
    /// Set was successful, returns OK.
    Ok,
    /// Set was successful, returns the old value (for GET flag).
    OkWithOldValue(Option<Value>),
    /// Set was not performed (NX/XX condition not met).
    NotSet,
}

// ============================================================================
// Sorted Set Types
// ============================================================================

/// Score boundary for range queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Inclusive bound.
    Inclusive(f64),
    /// Exclusive bound.
    Exclusive(f64),
    /// Negative infinity.
    NegInf,
    /// Positive infinity.
    PosInf,
}

impl ScoreBound {
    /// Check if a score satisfies this bound as a minimum.
    pub fn satisfies_min(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => false,
            ScoreBound::Inclusive(bound) => score >= *bound,
            ScoreBound::Exclusive(bound) => score > *bound,
        }
    }

    /// Check if a score satisfies this bound as a maximum.
    pub fn satisfies_max(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => false,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(bound) => score <= *bound,
            ScoreBound::Exclusive(bound) => score < *bound,
        }
    }

    /// Get the value for BTreeMap range queries (minimum bound).
    pub fn start_bound_value(&self) -> Option<OrderedFloat<f64>> {
        match self {
            ScoreBound::NegInf => None,
            ScoreBound::PosInf => Some(OrderedFloat(f64::INFINITY)),
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => Some(OrderedFloat(*v)),
        }
    }
}

/// Lexicographic boundary for range queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Inclusive bound.
    Inclusive(Bytes),
    /// Exclusive bound.
    Exclusive(Bytes),
    /// Minimum (unbounded).
    Min,
    /// Maximum (unbounded).
    Max,
}

impl LexBound {
    /// Check if a member satisfies this bound as a minimum.
    pub fn satisfies_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => true,
            LexBound::Max => false,
            LexBound::Inclusive(bound) => member >= bound.as_ref(),
            LexBound::Exclusive(bound) => member > bound.as_ref(),
        }
    }

    /// Check if a member satisfies this bound as a maximum.
    pub fn satisfies_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::Min => false,
            LexBound::Max => true,
            LexBound::Inclusive(bound) => member <= bound.as_ref(),
            LexBound::Exclusive(bound) => member < bound.as_ref(),
        }
    }
}

/// Result of adding a member to a sorted set.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZAddResult {
    /// Whether a new member was added.
    pub added: bool,
    /// Whether the score was changed (for existing members).
    pub changed: bool,
    /// The previous score (if member existed).
    pub old_score: Option<f64>,
}

/// Sorted set value with dual indexing for O(1) score lookup and O(log n) range queries.
#[derive(Debug, Clone)]
pub struct SortedSetValue {
    /// O(1) lookup: member -> score
    members: HashMap<Bytes, f64>,
    /// O(log n) range queries: (score, member) -> ()
    /// Using (OrderedFloat, Bytes) ensures proper ordering by score, then member.
    scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
}

impl Default for SortedSetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetValue {
    /// Create a new empty sorted set.
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            scores: BTreeMap::new(),
        }
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Add or update a member with a score.
    ///
    /// Returns information about what changed.
    pub fn add(&mut self, member: Bytes, score: f64) -> ZAddResult {
        if let Some(&old_score) = self.members.get(&member) {
            if (old_score - score).abs() < f64::EPSILON
                || (old_score.is_nan() && score.is_nan())
                || (old_score == score)
            {
                // Score unchanged
                return ZAddResult {
                    added: false,
                    changed: false,
                    old_score: Some(old_score),
                };
            }
            // Remove old entry from scores index
            self.scores.remove(&(OrderedFloat(old_score), member.clone()));
            // Insert new entry
            self.scores.insert((OrderedFloat(score), member.clone()), ());
            self.members.insert(member, score);
            ZAddResult {
                added: false,
                changed: true,
                old_score: Some(old_score),
            }
        } else {
            // New member
            self.members.insert(member.clone(), score);
            self.scores.insert((OrderedFloat(score), member), ());
            ZAddResult {
                added: true,
                changed: false,
                old_score: None,
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns the score if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        if let Some(score) = self.members.remove(member) {
            self.scores
                .remove(&(OrderedFloat(score), Bytes::copy_from_slice(member)));
            Some(score)
        } else {
            None
        }
    }

    /// Get the score of a member.
    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        self.members.contains_key(member)
    }

    /// Get the 0-based rank of a member (ascending by score).
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.members.get(member)?;
        let key = (OrderedFloat(*score), Bytes::copy_from_slice(member));
        Some(self.scores.range(..&key).count())
    }

    /// Get the 0-based rank of a member (descending by score).
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Increment the score of a member.
    ///
    /// If the member doesn't exist, it's created with the given increment as its score.
    /// Returns the new score.
    pub fn incr(&mut self, member: Bytes, increment: f64) -> f64 {
        let old_score = self.members.get(&member).copied().unwrap_or(0.0);
        let new_score = old_score + increment;

        // Check for overflow to infinity
        if new_score.is_infinite() && !old_score.is_infinite() && !increment.is_infinite() {
            // This would be an error in Redis, but we'll handle it
            return new_score;
        }

        // Remove old entry if exists
        if self.members.contains_key(&member) {
            self.scores
                .remove(&(OrderedFloat(old_score), member.clone()));
        }

        // Insert new entry
        self.members.insert(member.clone(), new_score);
        self.scores.insert((OrderedFloat(new_score), member), ());

        new_score
    }

    /// Get members by rank range (inclusive).
    ///
    /// `start` and `end` are 0-based indices. Negative indices count from the end.
    pub fn range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1) as i64
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.scores
            .iter()
            .skip(start)
            .take(end - start + 1)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by rank range in reverse order (descending by score).
    pub fn rev_range_by_rank(&self, start: i64, end: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        if len == 0 {
            return vec![];
        }

        // Convert negative indices
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start.min(len) as usize
        };

        let end = if end < 0 {
            (len + end).max(-1) as i64
        } else {
            end.min(len - 1)
        };

        if end < 0 || start > end as usize {
            return vec![];
        }

        let end = end as usize;

        self.scores
            .iter()
            .rev()
            .skip(start)
            .take(end - start + 1)
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }

    /// Get members by score range.
    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().filter(|((score, _), _)| {
            min.satisfies_min(score.0) && max.satisfies_max(score.0)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by score range in reverse order.
    pub fn rev_range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().rev().filter(|((score, _), _)| {
            min.satisfies_min(score.0) && max.satisfies_max(score.0)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by lexicographic range (requires all scores to be equal).
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        // For lex range, we iterate in (score, member) order
        // This naturally gives us lexicographic order for same scores
        let iter = self.scores.iter().filter(|((_, member), _)| {
            min.satisfies_min(member) && max.satisfies_max(member)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Get members by lexicographic range in reverse order.
    pub fn rev_range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<(Bytes, f64)> {
        let iter = self.scores.iter().rev().filter(|((_, member), _)| {
            min.satisfies_min(member) && max.satisfies_max(member)
        });

        let iter = iter.skip(offset);

        let results: Vec<_> = if let Some(count) = count {
            iter.take(count)
                .map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|((score, member), _)| (member.clone(), score.0))
                .collect()
        };

        results
    }

    /// Count members in score range.
    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> usize {
        self.scores
            .iter()
            .filter(|((score, _), _)| min.satisfies_min(score.0) && max.satisfies_max(score.0))
            .count()
    }

    /// Count members in lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.scores
            .iter()
            .filter(|((_, member), _)| min.satisfies_min(member) && max.satisfies_max(member))
            .count()
    }

    /// Pop members with minimum scores.
    pub fn pop_min(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some(((score, member), _)) = self.scores.pop_first() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Pop members with maximum scores.
    pub fn pop_max(&mut self, count: usize) -> Vec<(Bytes, f64)> {
        let mut result = Vec::with_capacity(count.min(self.len()));
        for _ in 0..count {
            if let Some(((score, member), _)) = self.scores.pop_last() {
                self.members.remove(&member);
                result.push((member, score.0));
            } else {
                break;
            }
        }
        result
    }

    /// Remove members by rank range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_rank(&mut self, start: i64, end: i64) -> usize {
        let to_remove = self.range_by_rank(start, end);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Remove members by score range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_score(&mut self, min: &ScoreBound, max: &ScoreBound) -> usize {
        let to_remove = self.range_by_score(min, max, 0, None);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Remove members by lex range.
    ///
    /// Returns the number of members removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let to_remove = self.range_by_lex(min, max, 0, None);
        let count = to_remove.len();
        for (member, _) in to_remove {
            self.remove(&member);
        }
        count
    }

    /// Get random members.
    ///
    /// If `count` is positive, returns that many unique members.
    /// If `count` is negative, returns abs(count) members with possible duplicates.
    pub fn random_members(&self, count: i64) -> Vec<(Bytes, f64)> {
        if self.is_empty() {
            return vec![];
        }

        let members: Vec<_> = self
            .scores
            .iter()
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect();

        if count == 0 {
            return vec![];
        }

        if count > 0 {
            // Return unique members
            let count = (count as usize).min(members.len());
            // Simple reservoir sampling or just take first N for now
            // In production, we'd use proper random sampling
            members.into_iter().take(count).collect()
        } else {
            // Allow duplicates
            let count = (-count) as usize;
            // Return members (allowing duplicates)
            let mut result = Vec::with_capacity(count);
            for i in 0..count {
                let idx = i % members.len();
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();

        // HashMap overhead + entries
        let members_size: usize = self
            .members
            .iter()
            .map(|(k, _)| k.len() + std::mem::size_of::<f64>() + 32) // 32 for HashMap node overhead
            .sum();

        // BTreeMap overhead + entries
        let scores_size: usize = self
            .scores
            .iter()
            .map(|((_, member), _)| member.len() + std::mem::size_of::<OrderedFloat<f64>>() + 32)
            .sum();

        base_size + members_size + scores_size
    }

    /// Iterate over all members in score order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, f64)> {
        self.scores
            .iter()
            .map(|((score, member), _)| (member, score.0))
    }

    /// Get all members and scores as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, f64)> {
        self.scores
            .iter()
            .map(|((score, member), _)| (member.clone(), score.0))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_value_raw() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.as_bytes().as_ref(), b"hello");
        assert!(sv.as_integer().is_none());
    }

    #[test]
    fn test_string_value_integer() {
        let sv = StringValue::new("42");
        assert_eq!(sv.as_integer(), Some(42));
        assert_eq!(sv.as_bytes().as_ref(), b"42");
    }

    #[test]
    fn test_string_value_from_integer() {
        let sv = StringValue::from_integer(-100);
        assert_eq!(sv.as_integer(), Some(-100));
        assert_eq!(sv.as_bytes().as_ref(), b"-100");
    }

    #[test]
    fn test_key_type_as_str() {
        assert_eq!(KeyType::String.as_str(), "string");
        assert_eq!(KeyType::None.as_str(), "none");
    }

    #[test]
    fn test_string_value_len() {
        let sv = StringValue::new("hello");
        assert_eq!(sv.len(), 5);
        assert!(!sv.is_empty());

        let sv_int = StringValue::from_integer(12345);
        assert_eq!(sv_int.len(), 5);

        let sv_empty = StringValue::new("");
        assert_eq!(sv_empty.len(), 0);
        assert!(sv_empty.is_empty());
    }

    #[test]
    fn test_string_value_append() {
        let mut sv = StringValue::new("hello");
        let new_len = sv.append(b" world");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello world");
    }

    #[test]
    fn test_string_value_get_range() {
        let sv = StringValue::new("hello world");

        // Positive indices
        assert_eq!(sv.get_range(0, 4).as_ref(), b"hello");
        assert_eq!(sv.get_range(6, 10).as_ref(), b"world");

        // Negative indices
        assert_eq!(sv.get_range(-5, -1).as_ref(), b"world");
        assert_eq!(sv.get_range(0, -1).as_ref(), b"hello world");

        // Out of range
        assert_eq!(sv.get_range(0, 100).as_ref(), b"hello world");

        // Empty result
        assert_eq!(sv.get_range(5, 2).as_ref(), b"");
    }

    #[test]
    fn test_string_value_set_range() {
        let mut sv = StringValue::new("hello world");
        let new_len = sv.set_range(6, b"WORLD");
        assert_eq!(new_len, 11);
        assert_eq!(sv.as_bytes().as_ref(), b"hello WORLD");

        // Extend past end
        let mut sv2 = StringValue::new("hello");
        let new_len = sv2.set_range(10, b"world");
        assert_eq!(new_len, 15);
        // Should have null padding between "hello" and "world"
        let bytes = sv2.as_bytes();
        assert_eq!(&bytes[0..5], b"hello");
        assert_eq!(&bytes[10..15], b"world");
    }

    #[test]
    fn test_string_value_increment() {
        let mut sv = StringValue::from_integer(10);
        assert_eq!(sv.increment(5).unwrap(), 15);
        assert_eq!(sv.as_integer(), Some(15));

        // Negative increment
        assert_eq!(sv.increment(-20).unwrap(), -5);
        assert_eq!(sv.as_integer(), Some(-5));
    }

    #[test]
    fn test_string_value_increment_overflow() {
        let mut sv = StringValue::from_integer(i64::MAX);
        assert_eq!(sv.increment(1), Err(IncrementError::Overflow));
    }

    #[test]
    fn test_string_value_increment_not_integer() {
        let mut sv = StringValue::new("not a number");
        assert_eq!(sv.increment(1), Err(IncrementError::NotInteger));
    }

    #[test]
    fn test_string_value_increment_float() {
        let mut sv = StringValue::from_integer(10);
        let result = sv.increment_float(0.5).unwrap();
        assert!((result - 10.5).abs() < 0.001);

        // Increment a float value
        let result = sv.increment_float(1.5).unwrap();
        assert!((result - 12.0).abs() < 0.001);
    }

    #[test]
    fn test_string_value_as_float() {
        let sv_int = StringValue::from_integer(42);
        assert_eq!(sv_int.as_float(), Some(42.0));

        let sv_float = StringValue::new("3.14159");
        assert!((sv_float.as_float().unwrap() - 3.14159).abs() < 0.0001);

        let sv_invalid = StringValue::new("not a float");
        assert!(sv_invalid.as_float().is_none());
    }
}
