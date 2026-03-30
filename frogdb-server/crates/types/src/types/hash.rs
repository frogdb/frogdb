use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::time::Instant;

use super::{EitherIter, ListpackThresholds};
use crate::types::string_value::IncrementError;
use crate::types::string_value::format_float;

use rand::RngExt;
use rand::seq::SliceRandom;

// ============================================================================
// Hash Listpack Helpers
// ============================================================================

/// Iterator over field-value pairs in a hash listpack buffer.
/// Layout: [flen:u16][field_bytes][vlen:u16][value_bytes]... repeated.
struct ListpackHashIter<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> ListpackHashIter<'a> {
    fn new(buf: &'a Bytes) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Iterator for ListpackHashIter<'_> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<(Bytes, Bytes)> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let flen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let field = self.buf.slice(self.pos..self.pos + flen);
        self.pos += flen;
        let vlen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let value = self.buf.slice(self.pos..self.pos + vlen);
        self.pos += vlen;
        Some((field, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Find a field in a hash listpack buffer, returning its value as a zero-copy slice.
fn lp_hash_get(buf: &Bytes, field: &[u8]) -> Option<Bytes> {
    let mut pos = 0;
    while pos < buf.len() {
        let flen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        let matches = &buf[pos..pos + flen] == field;
        pos += flen;
        let vlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if matches {
            return Some(buf.slice(pos..pos + vlen));
        }
        pos += vlen;
    }
    None
}

/// Check if a field exists in a hash listpack buffer.
fn lp_hash_contains(buf: &[u8], field: &[u8]) -> bool {
    let mut pos = 0;
    while pos < buf.len() {
        let flen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if &buf[pos..pos + flen] == field {
            return true;
        }
        pos += flen;
        let vlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2 + vlen;
    }
    false
}

/// Rebuild a hash listpack buffer with a field set or updated.
/// Returns (new_buf, new_len, was_new_field).
fn lp_hash_set(old_buf: &[u8], old_len: u16, field: &[u8], value: &[u8]) -> (Bytes, u16, bool) {
    let mut new_buf = BytesMut::with_capacity(old_buf.len() + 2 + field.len() + 2 + value.len());
    let mut pos = 0;
    let mut found = false;

    while pos < old_buf.len() {
        let flen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;
        let existing_field = &old_buf[pos..pos + flen];
        pos += flen;
        let vlen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;

        if existing_field == field {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
            new_buf.extend_from_slice(value);
            found = true;
        } else {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(vlen as u16).to_le_bytes());
            new_buf.extend_from_slice(&old_buf[pos..pos + vlen]);
        }
        pos += vlen;
    }

    if !found {
        new_buf.extend_from_slice(&(field.len() as u16).to_le_bytes());
        new_buf.extend_from_slice(field);
        new_buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
        new_buf.extend_from_slice(value);
    }

    let new_len = if found { old_len } else { old_len + 1 };
    (new_buf.freeze(), new_len, !found)
}

/// Rebuild a hash listpack buffer with a field removed.
/// Returns (new_buf, new_len, was_removed).
fn lp_hash_remove(old_buf: &[u8], old_len: u16, field: &[u8]) -> (Bytes, u16, bool) {
    let mut new_buf = BytesMut::with_capacity(old_buf.len());
    let mut pos = 0;
    let mut found = false;

    while pos < old_buf.len() {
        let flen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;
        let existing_field = &old_buf[pos..pos + flen];
        pos += flen;
        let vlen = u16::from_le_bytes([old_buf[pos], old_buf[pos + 1]]) as usize;
        pos += 2;

        if existing_field == field {
            found = true;
        } else {
            new_buf.extend_from_slice(&(flen as u16).to_le_bytes());
            new_buf.extend_from_slice(existing_field);
            new_buf.extend_from_slice(&(vlen as u16).to_le_bytes());
            new_buf.extend_from_slice(&old_buf[pos..pos + vlen]);
        }
        pos += vlen;
    }

    let new_len = if found { old_len - 1 } else { old_len };
    (new_buf.freeze(), new_len, found)
}

// ============================================================================
// Hash Type
// ============================================================================

/// Internal encoding for hash values.
#[derive(Debug, Clone)]
enum HashEncoding {
    /// Contiguous byte buffer for small hashes.
    /// Layout: [flen:u16][field_bytes][vlen:u16][value_bytes]... repeated per entry.
    /// O(n) lookups — fast for small N due to cache locality.
    /// Per-entry overhead: 4 bytes (two u16 length prefixes).
    Listpack { buf: Bytes, len: u16 },

    /// Standard hash table for large hashes. O(1) lookups.
    HashMap(HashMap<Bytes, Bytes>),
}

impl Default for HashEncoding {
    fn default() -> Self {
        HashEncoding::Listpack {
            buf: Bytes::new(),
            len: 0,
        }
    }
}

/// Hash value - a mapping from field names to values.
#[derive(Debug, Clone)]
pub struct HashValue {
    data: HashEncoding,
    field_expiries: Option<HashMap<Bytes, Instant>>,
}

impl Default for HashValue {
    fn default() -> Self {
        Self::new()
    }
}

impl HashValue {
    /// Create a new empty hash (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: HashEncoding::default(),
            field_expiries: None,
        }
    }

    /// Create a hash from an iterator of field-value pairs, choosing encoding
    /// based on thresholds.
    pub fn from_entries(
        entries: impl IntoIterator<Item = (Bytes, Bytes)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes)> = entries.into_iter().collect();
        let use_listpack = entries.len() <= thresholds.max_entries
            && entries.iter().all(|(k, v)| {
                k.len() <= thresholds.max_value_bytes && v.len() <= thresholds.max_value_bytes
            });

        if use_listpack {
            let mut buf = BytesMut::new();
            for (k, v) in &entries {
                buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(&(v.len() as u16).to_le_bytes());
                buf.extend_from_slice(v);
            }
            Self {
                data: HashEncoding::Listpack {
                    buf: buf.freeze(),
                    len: entries.len() as u16,
                },
                field_expiries: None,
            }
        } else {
            Self {
                data: HashEncoding::HashMap(entries.into_iter().collect()),
                field_expiries: None,
            }
        }
    }

    /// Whether this hash uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, HashEncoding::Listpack { .. })
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        match &self.data {
            HashEncoding::Listpack { len, .. } => *len as usize,
            HashEncoding::HashMap(map) => map.len(),
        }
    }

    /// Check if the hash is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Set a field value. Promotes to HashMap if thresholds are exceeded.
    ///
    /// Returns true if the field is new, false if it was updated.
    pub fn set(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        self.remove_field_expiry(&field);
        let data = std::mem::take(&mut self.data);
        match data {
            HashEncoding::Listpack { buf, len } => {
                let would_be_new = !lp_hash_contains(&buf, &field);
                let new_count = if would_be_new {
                    len as usize + 1
                } else {
                    len as usize
                };

                if new_count > thresholds.max_entries
                    || field.len() > thresholds.max_value_bytes
                    || value.len() > thresholds.max_value_bytes
                {
                    // Promote to HashMap
                    let mut map: HashMap<Bytes, Bytes> = ListpackHashIter::new(&buf).collect();
                    let was_new = map.insert(field, value).is_none();
                    self.data = HashEncoding::HashMap(map);
                    was_new
                } else {
                    let (new_buf, new_len, was_new) = lp_hash_set(&buf, len, &field, &value);
                    self.data = HashEncoding::Listpack {
                        buf: new_buf,
                        len: new_len,
                    };
                    was_new
                }
            }
            HashEncoding::HashMap(mut map) => {
                let was_new = map.insert(field, value).is_none();
                self.data = HashEncoding::HashMap(map);
                was_new
            }
        }
    }

    /// Set a field value only if it doesn't exist.
    ///
    /// Returns true if the field was set, false if it already existed.
    pub fn set_nx(&mut self, field: Bytes, value: Bytes, thresholds: ListpackThresholds) -> bool {
        if self.contains(&field) {
            return false;
        }
        self.set(field, value, thresholds)
    }

    /// Get a field value (zero-copy for listpack encoding).
    pub fn get(&self, field: &[u8]) -> Option<Bytes> {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => lp_hash_get(buf, field),
            HashEncoding::HashMap(map) => map.get(field).cloned(),
        }
    }

    /// Remove a field.
    ///
    /// Returns true if the field existed.
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.remove_field_expiry(field);
        let data = std::mem::take(&mut self.data);
        match data {
            HashEncoding::Listpack { buf, len } => {
                let (new_buf, new_len, was_removed) = lp_hash_remove(&buf, len, field);
                self.data = HashEncoding::Listpack {
                    buf: new_buf,
                    len: new_len,
                };
                was_removed
            }
            HashEncoding::HashMap(mut map) => {
                let was_removed = map.remove(field).is_some();
                self.data = HashEncoding::HashMap(map);
                was_removed
            }
        }
    }

    /// Check if a field exists.
    pub fn contains(&self, field: &[u8]) -> bool {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => lp_hash_contains(buf, field),
            HashEncoding::HashMap(map) => map.contains_key(field),
        }
    }

    /// Get all field names.
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => {
                EitherIter::Left(ListpackHashIter::new(buf).map(|(k, _)| k))
            }
            HashEncoding::HashMap(map) => EitherIter::Right(map.keys().cloned()),
        }
    }

    /// Get all values.
    pub fn values(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => {
                EitherIter::Left(ListpackHashIter::new(buf).map(|(_, v)| v))
            }
            HashEncoding::HashMap(map) => EitherIter::Right(map.values().cloned()),
        }
    }

    /// Iterate over all field-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        match &self.data {
            HashEncoding::Listpack { buf, .. } => EitherIter::Left(ListpackHashIter::new(buf)),
            HashEncoding::HashMap(map) => {
                EitherIter::Right(map.iter().map(|(k, v)| (k.clone(), v.clone())))
            }
        }
    }

    /// Increment an integer field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid integer.
    pub fn incr_by(
        &mut self,
        field: Bytes,
        delta: i64,
        thresholds: ListpackThresholds,
    ) -> Result<i64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(IncrementError::NotInteger)?,
            None => 0,
        };

        let new_val = current.checked_add(delta).ok_or(IncrementError::Overflow)?;
        self.set(field, Bytes::from(new_val.to_string()), thresholds);
        Ok(new_val)
    }

    /// Increment a float field by delta.
    ///
    /// If the field doesn't exist, it's created with the delta value.
    /// Returns the new value or an error if the field is not a valid float.
    pub fn incr_by_float(
        &mut self,
        field: Bytes,
        delta: f64,
        thresholds: ListpackThresholds,
    ) -> Result<f64, IncrementError> {
        let current = match self.get(&field) {
            Some(val) => std::str::from_utf8(&val)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(IncrementError::NotFloat)?,
            None => 0.0,
        };

        let new_val = current + delta;

        if new_val.is_infinite() || new_val.is_nan() {
            return Err(IncrementError::Overflow);
        }

        self.set(field, Bytes::from(format_float(new_val)), thresholds);
        Ok(new_val)
    }

    /// Get random fields from the hash.
    ///
    /// If count > 0: return up to count unique fields
    /// If count < 0: return |count| fields, allowing duplicates
    pub fn random_fields(&self, count: i64, with_values: bool) -> Vec<(Bytes, Option<Bytes>)> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let entries: Vec<(Bytes, Bytes)> = self.iter().collect();
        let mut rng = rand::rng();

        if count > 0 {
            let count = (count as usize).min(entries.len());
            let mut indices: Vec<usize> = (0..entries.len()).collect();
            indices.shuffle(&mut rng);
            indices
                .into_iter()
                .take(count)
                .map(|i| {
                    let (ref k, ref v) = entries[i];
                    (k.clone(), if with_values { Some(v.clone()) } else { None })
                })
                .collect()
        } else {
            let abs_count = count.unsigned_abs() as usize;
            let mut result = Vec::with_capacity(abs_count);
            for _ in 0..abs_count {
                let idx = rand::rng().random_range(0..entries.len());
                let (ref k, ref v) = entries[idx];
                result.push((k.clone(), if with_values { Some(v.clone()) } else { None }));
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let data_size = match &self.data {
            HashEncoding::Listpack { buf, .. } => buf.len(),
            HashEncoding::HashMap(map) => map.iter().map(|(k, v)| k.len() + v.len() + 32).sum(),
        };
        let expiry_size = self
            .field_expiries
            .as_ref()
            .map(|expiries| expiries.keys().map(|k| k.len() + 16 + 32).sum::<usize>())
            .unwrap_or(0);
        base_size + data_size + expiry_size
    }

    /// Get all field-value pairs as a vec for serialization.
    pub fn to_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.iter().collect()
    }

    /// Create a hash from entries with per-field expiry times (for deserialization).
    pub fn from_entries_with_expiries(
        entries: impl IntoIterator<Item = (Bytes, Bytes, Option<Instant>)>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let entries: Vec<(Bytes, Bytes, Option<Instant>)> = entries.into_iter().collect();
        let mut field_expiries: HashMap<Bytes, Instant> = HashMap::new();
        let mut data_entries = Vec::with_capacity(entries.len());

        for (field, value, expiry) in entries {
            if let Some(expires_at) = expiry {
                field_expiries.insert(field.clone(), expires_at);
            }
            data_entries.push((field, value));
        }

        let mut hash = Self::from_entries(data_entries, thresholds);
        if !field_expiries.is_empty() {
            hash.field_expiries = Some(field_expiries);
        }
        hash
    }

    /// Set field expiry time.
    pub fn set_field_expiry(&mut self, field: &[u8], expires_at: Instant) {
        let expiries = self.field_expiries.get_or_insert_with(HashMap::new);
        expiries.insert(Bytes::copy_from_slice(field), expires_at);
    }

    /// Remove field expiry. Returns true if the field had an expiry.
    pub fn remove_field_expiry(&mut self, field: &[u8]) -> bool {
        if let Some(ref mut expiries) = self.field_expiries {
            let removed = expiries.remove(field).is_some();
            if expiries.is_empty() {
                self.field_expiries = None;
            }
            removed
        } else {
            false
        }
    }

    /// Get expiry time for a field.
    pub fn get_field_expiry(&self, field: &[u8]) -> Option<Instant> {
        self.field_expiries.as_ref()?.get(field).copied()
    }

    /// Check if any field has an expiry set.
    pub fn has_field_expiries(&self) -> bool {
        self.field_expiries.as_ref().is_some_and(|e| !e.is_empty())
    }

    /// Access the field expiries map.
    pub fn field_expiries(&self) -> Option<&HashMap<Bytes, Instant>> {
        self.field_expiries.as_ref()
    }

    /// Remove all expired fields from data and field_expiries.
    /// Returns the names of removed fields.
    pub fn remove_expired_fields(&mut self, now: Instant) -> Vec<Bytes> {
        let expiries = match self.field_expiries.take() {
            Some(e) => e,
            None => return vec![],
        };

        let mut removed = Vec::new();
        let mut remaining = HashMap::new();

        for (field, expires_at) in expiries {
            if expires_at <= now {
                self.remove(&field);
                removed.push(field);
            } else {
                remaining.insert(field, expires_at);
            }
        }

        if !remaining.is_empty() {
            self.field_expiries = Some(remaining);
        }

        removed
    }

    /// Get all field-value pairs with their expiry times, for serialization.
    pub fn to_vec_with_expiries(&self) -> Vec<(Bytes, Bytes, Option<Instant>)> {
        self.iter()
            .map(|(field, value)| {
                let expiry = self.get_field_expiry(&field);
                (field, value, expiry)
            })
            .collect()
    }
}
