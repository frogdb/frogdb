use bytes::{Bytes, BytesMut};
use rand::Rng;
use rand::seq::SliceRandom;
use std::collections::HashSet;

use super::{EitherIter, ListpackThresholds};

// ============================================================================
// Set Listpack Helpers
// ============================================================================

/// Iterator over members in a set listpack buffer.
/// Layout: [mlen:u16][member_bytes]... repeated.
struct ListpackSetIter<'a> {
    buf: &'a Bytes,
    pos: usize,
}

impl<'a> ListpackSetIter<'a> {
    fn new(buf: &'a Bytes) -> Self {
        Self { buf, pos: 0 }
    }
}

impl Iterator for ListpackSetIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let mlen = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]) as usize;
        self.pos += 2;
        let member = self.buf.slice(self.pos..self.pos + mlen);
        self.pos += mlen;
        Some(member)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Check if a member exists in a set listpack buffer.
fn lp_set_contains(buf: &[u8], member: &[u8]) -> bool {
    let mut pos = 0;
    while pos < buf.len() {
        let mlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;
        if &buf[pos..pos + mlen] == member {
            return true;
        }
        pos += mlen;
    }
    false
}

// ============================================================================
// Set Type
// ============================================================================

/// Internal encoding for set values.
#[derive(Debug, Clone)]
enum SetEncoding {
    /// Contiguous byte buffer for small sets.
    /// Layout: [mlen:u16][member_bytes]... repeated per member.
    /// Per-entry overhead: 2 bytes (one u16 length prefix).
    Listpack { buf: Bytes, len: u16 },

    /// Standard hash set for large sets. O(1) lookups.
    HashSet(HashSet<Bytes>),
}

impl Default for SetEncoding {
    fn default() -> Self {
        SetEncoding::Listpack {
            buf: Bytes::new(),
            len: 0,
        }
    }
}

/// Set value - an unordered collection of unique members.
#[derive(Debug, Clone)]
pub struct SetValue {
    data: SetEncoding,
}

impl Default for SetValue {
    fn default() -> Self {
        Self::new()
    }
}

impl SetValue {
    /// Create a new empty set (starts as listpack).
    pub fn new() -> Self {
        Self {
            data: SetEncoding::default(),
        }
    }

    /// Create a set from an iterator of members, choosing encoding
    /// based on thresholds.
    pub fn from_members(
        members: impl IntoIterator<Item = Bytes>,
        thresholds: ListpackThresholds,
    ) -> Self {
        let members: Vec<Bytes> = members.into_iter().collect();
        let use_listpack = members.len() <= thresholds.max_entries
            && members
                .iter()
                .all(|m| m.len() <= thresholds.max_value_bytes);

        if use_listpack {
            let mut buf = BytesMut::new();
            for m in &members {
                buf.extend_from_slice(&(m.len() as u16).to_le_bytes());
                buf.extend_from_slice(m);
            }
            Self {
                data: SetEncoding::Listpack {
                    buf: buf.freeze(),
                    len: members.len() as u16,
                },
            }
        } else {
            Self {
                data: SetEncoding::HashSet(members.into_iter().collect()),
            }
        }
    }

    /// Whether this set uses listpack encoding.
    pub fn is_listpack(&self) -> bool {
        matches!(self.data, SetEncoding::Listpack { .. })
    }

    /// Get the number of members.
    pub fn len(&self) -> usize {
        match &self.data {
            SetEncoding::Listpack { len, .. } => *len as usize,
            SetEncoding::HashSet(set) => set.len(),
        }
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a member to the set. Promotes to HashSet if thresholds are exceeded.
    ///
    /// Returns true if the member was new, false if it already existed.
    pub fn add(&mut self, member: Bytes, thresholds: ListpackThresholds) -> bool {
        let data = std::mem::take(&mut self.data);
        match data {
            SetEncoding::Listpack { buf, len } => {
                if lp_set_contains(&buf, &member) {
                    self.data = SetEncoding::Listpack { buf, len };
                    return false;
                }
                let new_count = len as usize + 1;
                if new_count > thresholds.max_entries || member.len() > thresholds.max_value_bytes {
                    // Promote to HashSet
                    let mut set: HashSet<Bytes> = ListpackSetIter::new(&buf).collect();
                    set.insert(member);
                    self.data = SetEncoding::HashSet(set);
                } else {
                    let mut new_buf = BytesMut::with_capacity(buf.len() + 2 + member.len());
                    new_buf.extend_from_slice(&buf);
                    new_buf.extend_from_slice(&(member.len() as u16).to_le_bytes());
                    new_buf.extend_from_slice(&member);
                    self.data = SetEncoding::Listpack {
                        buf: new_buf.freeze(),
                        len: len + 1,
                    };
                }
                true
            }
            SetEncoding::HashSet(mut set) => {
                let was_new = set.insert(member);
                self.data = SetEncoding::HashSet(set);
                was_new
            }
        }
    }

    /// Remove a member from the set.
    ///
    /// Returns true if the member existed.
    pub fn remove(&mut self, member: &[u8]) -> bool {
        let data = std::mem::take(&mut self.data);
        match data {
            SetEncoding::Listpack { buf, len } => {
                if !lp_set_contains(&buf, member) {
                    self.data = SetEncoding::Listpack { buf, len };
                    return false;
                }
                let mut new_buf = BytesMut::with_capacity(buf.len());
                let mut pos = 0;
                while pos < buf.len() {
                    let mlen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                    pos += 2;
                    if &buf[pos..pos + mlen] != member {
                        new_buf.extend_from_slice(&(mlen as u16).to_le_bytes());
                        new_buf.extend_from_slice(&buf[pos..pos + mlen]);
                    }
                    pos += mlen;
                }
                self.data = SetEncoding::Listpack {
                    buf: new_buf.freeze(),
                    len: len - 1,
                };
                true
            }
            SetEncoding::HashSet(mut set) => {
                let was_removed = set.remove(member);
                self.data = SetEncoding::HashSet(set);
                was_removed
            }
        }
    }

    /// Check if a member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match &self.data {
            SetEncoding::Listpack { buf, .. } => lp_set_contains(buf, member),
            SetEncoding::HashSet(set) => set.contains(member),
        }
    }

    /// Get all members.
    pub fn members(&self) -> impl Iterator<Item = Bytes> + '_ {
        match &self.data {
            SetEncoding::Listpack { buf, .. } => EitherIter::Left(ListpackSetIter::new(buf)),
            SetEncoding::HashSet(set) => EitherIter::Right(set.iter().cloned()),
        }
    }

    /// Compute the union of this set with others.
    pub fn union<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            for member in other.members() {
                result.insert(member);
            }
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Compute the intersection of this set with others.
    pub fn intersection<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| other.contains(m));
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Compute the difference of this set minus others.
    pub fn difference<'a>(&'a self, others: impl Iterator<Item = &'a SetValue>) -> SetValue {
        let mut result: HashSet<Bytes> = self.members().collect();
        for other in others {
            result.retain(|m| !other.contains(m));
        }
        SetValue {
            data: SetEncoding::HashSet(result),
        }
    }

    /// Pop a random member from the set.
    ///
    /// Returns None if the set is empty.
    pub fn pop(&mut self) -> Option<Bytes> {
        if self.is_empty() {
            return None;
        }
        let members: Vec<Bytes> = self.members().collect();
        let idx = rand::rng().random_range(0..members.len());
        let member = members[idx].clone();
        self.remove(&member);
        Some(member)
    }

    /// Pop multiple random members from the set.
    pub fn pop_many(&mut self, count: usize) -> Vec<Bytes> {
        let count = count.min(self.len());
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(member) = self.pop() {
                result.push(member);
            } else {
                break;
            }
        }
        result
    }

    /// Get random members without removing them.
    ///
    /// If count > 0: return up to count unique members
    /// If count < 0: return |count| members, allowing duplicates
    pub fn random_members(&self, count: i64) -> Vec<Bytes> {
        if self.is_empty() || count == 0 {
            return vec![];
        }

        let members: Vec<Bytes> = self.members().collect();
        let mut rng = rand::rng();

        if count > 0 {
            let count = (count as usize).min(members.len());
            let mut shuffled = members;
            shuffled.shuffle(&mut rng);
            shuffled.into_iter().take(count).collect()
        } else {
            let count = (-count) as usize;
            let mut result = Vec::with_capacity(count);
            for _ in 0..count {
                let idx = rng.random_range(0..members.len());
                result.push(members[idx].clone());
            }
            result
        }
    }

    /// Calculate approximate memory size.
    pub fn memory_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        match &self.data {
            SetEncoding::Listpack { buf, .. } => base_size + buf.len(),
            SetEncoding::HashSet(set) => {
                let entries_size: usize = set.iter().map(|m| m.len() + 24).sum();
                base_size + entries_size
            }
        }
    }

    /// Get all members as a vec for serialization.
    pub fn to_vec(&self) -> Vec<Bytes> {
        self.members().collect()
    }
}
