//! Listpack — a contiguous encoding for a sequence of byte strings.
//!
//! One heap allocation holds many elements back to back, so element data never
//! gets an allocation (or a refcount header) of its own. This is the block
//! primitive the quicklist chain in [`crate::types::ListValue`] is built from,
//! and the shared encoding the other composite types move onto as they convert
//! to block forms.
//!
//! # Wire format
//!
//! Entries are laid out end to end with no header:
//!
//! ```text
//! [len: varint][payload: len bytes][backlen: reverse varint]
//! ```
//!
//! * `len` is an LEB128 varint (7 bits per byte, low group first, high bit =
//!   "another byte follows"), readable **forwards**.
//! * `backlen` encodes `len_prefix_bytes + payload_len` — the distance from the
//!   first byte of `backlen` back to the first byte of the entry — using the
//!   same 7-bit groups written **most-significant group first**, with the high
//!   bit set on every byte except the first one written. That makes it readable
//!   **backwards**, so the previous entry can be found in O(1) from the end of
//!   the buffer.
//!
//! A 10-byte element therefore costs 12 bytes (1 + 10 + 1): 20% overhead,
//! against the ~48 bytes of allocator plus `Bytes` header a per-element
//! `Bytes` pays.
//!
//! # Deviations from Redis `listpack.c`
//!
//! * **No integer encoding.** Redis stores an element that parses as an integer
//!   in 2–9 bytes rather than as its decimal text. FrogDB stores every element
//!   as raw bytes. Elements are byte-identical on the way out either way; the
//!   deviation costs bytes on numeric workloads and buys a much smaller codec.
//! * **No total-bytes/num-elements header.** Redis puts a 6-byte header at the
//!   front of every listpack so it can be memcpy'd around as a self-describing
//!   blob (it is one, in RDB). FrogDB keeps the element count beside the buffer
//!   in [`Listpack`] and re-derives everything else, so a block carries no
//!   per-block wire header at all.
//! * **No end-of-listpack terminator byte.** The buffer length is the terminator.

/// Number of 7-bit groups needed to encode `v` — also the byte length of both
/// the forward length prefix and the reverse backlen for that value.
#[inline]
const fn varint_len(v: u64) -> usize {
    let mut v = v;
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Write an LEB128 varint at `dst[0..]`, returning the bytes written.
#[inline]
fn write_varint(dst: &mut [u8], v: u64) -> usize {
    let mut v = v;
    let mut n = 0;
    while v >= 0x80 {
        dst[n] = (v as u8) | 0x80;
        v >>= 7;
        n += 1;
    }
    dst[n] = v as u8;
    n + 1
}

/// Read an LEB128 varint starting at `buf[pos]`, returning `(value, bytes)`.
#[inline]
fn read_varint(buf: &[u8], pos: usize) -> (u64, usize) {
    let mut v = 0u64;
    let mut shift = 0;
    let mut n = 0;
    loop {
        let b = buf[pos + n];
        v |= ((b & 0x7f) as u64) << shift;
        n += 1;
        if b & 0x80 == 0 {
            return (v, n);
        }
        shift += 7;
    }
}

/// Write the backwards-readable length at `dst[0..]`, returning bytes written.
#[inline]
fn write_backlen(dst: &mut [u8], v: u64) -> usize {
    let n = varint_len(v);
    for (i, slot) in dst.iter_mut().enumerate().take(n) {
        // Most significant group first; continuation bit on all but the first.
        let group = ((v >> (7 * (n - 1 - i))) & 0x7f) as u8;
        *slot = if i == 0 { group } else { group | 0x80 };
    }
    n
}

/// Read the backwards-readable length whose last byte is `buf[end - 1]`,
/// returning `(value, bytes)`.
#[inline]
fn read_backlen(buf: &[u8], end: usize) -> (u64, usize) {
    let mut pos = end - 1;
    let mut v = (buf[pos] & 0x7f) as u64;
    let mut shift = 7;
    let mut n = 1;
    while buf[pos] & 0x80 != 0 {
        pos -= 1;
        v |= ((buf[pos] & 0x7f) as u64) << shift;
        shift += 7;
        n += 1;
    }
    (v, n)
}

/// A sequence of byte strings packed into one contiguous buffer.
///
/// Every mutation is a memmove inside that buffer — the Redis trade: edits cost
/// bytes moved, reads and storage cost no pointer chasing and no per-element
/// allocation. Callers keep listpacks small (a quicklist block caps at 128
/// entries / 8 KiB) so the memmove stays bounded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listpack {
    buf: Vec<u8>,
    len: usize,
}

impl Listpack {
    /// An empty listpack that has not allocated.
    pub const fn new() -> Self {
        Self {
            buf: Vec::new(),
            len: 0,
        }
    }

    /// An empty listpack with room for `bytes` of encoded entries.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
            len: 0,
        }
    }

    /// Encoded size of an entry holding a `value_len`-byte element.
    ///
    /// Use this to decide whether an element still fits a block *before*
    /// committing to the insert.
    #[inline]
    pub const fn entry_size(value_len: usize) -> usize {
        let head = varint_len(value_len as u64) + value_len;
        head + varint_len(head as u64)
    }

    /// Number of elements.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the listpack holds no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Encoded byte length of all entries.
    #[inline]
    pub fn byte_len(&self) -> usize {
        self.buf.len()
    }

    /// Drop every element.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.len = 0;
    }

    /// Byte offset of entry `index`, or `None` when out of range.
    ///
    /// Scans from whichever end is closer, so the walk is at most `len / 2`
    /// entries of trivial varint arithmetic over a cache-resident buffer.
    fn offset_of(&self, index: usize) -> Option<usize> {
        if index > self.len {
            return None;
        }
        if index == self.len {
            return Some(self.buf.len());
        }
        if index <= self.len / 2 {
            let mut pos = 0;
            for _ in 0..index {
                pos += self.entry_total_at(pos);
            }
            Some(pos)
        } else {
            let mut pos = self.buf.len();
            for _ in index..self.len {
                let (back, back_bytes) = read_backlen(&self.buf, pos);
                pos -= back_bytes + back as usize;
            }
            Some(pos)
        }
    }

    /// Total encoded bytes of the entry starting at `pos`.
    #[inline]
    fn entry_total_at(&self, pos: usize) -> usize {
        let (value_len, prefix) = read_varint(&self.buf, pos);
        let head = prefix + value_len as usize;
        head + varint_len(head as u64)
    }

    /// Payload of the entry starting at `pos`.
    #[inline]
    fn payload_at(&self, pos: usize) -> &[u8] {
        let (value_len, prefix) = read_varint(&self.buf, pos);
        &self.buf[pos + prefix..pos + prefix + value_len as usize]
    }

    /// Splice an encoded entry for `value` in at byte offset `at`.
    fn splice_in(&mut self, at: usize, value: &[u8]) {
        let size = Self::entry_size(value.len());
        let old_len = self.buf.len();
        self.buf.resize(old_len + size, 0);
        if at < old_len {
            self.buf.copy_within(at..old_len, at + size);
        }
        let dst = &mut self.buf[at..at + size];
        let prefix = write_varint(dst, value.len() as u64);
        dst[prefix..prefix + value.len()].copy_from_slice(value);
        let head = prefix + value.len();
        write_backlen(&mut dst[head..], head as u64);
        self.len += 1;
    }

    /// Append an element.
    pub fn push_back(&mut self, value: &[u8]) {
        let at = self.buf.len();
        self.splice_in(at, value);
    }

    /// Prepend an element.
    pub fn push_front(&mut self, value: &[u8]) {
        self.splice_in(0, value);
    }

    /// Insert an element so that it lands at `index`.
    ///
    /// # Panics
    /// Panics if `index > len()`.
    pub fn insert(&mut self, index: usize, value: &[u8]) {
        let at = self
            .offset_of(index)
            .expect("listpack insert index out of range");
        self.splice_in(at, value);
    }

    /// Remove the element at `index`. Returns false when out of range.
    pub fn remove(&mut self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let at = self.offset_of(index).expect("index checked above");
        let size = self.entry_total_at(at);
        self.buf.copy_within(at + size.., at);
        let new_len = self.buf.len() - size;
        self.buf.truncate(new_len);
        self.len -= 1;
        true
    }

    /// Replace the element at `index`. Returns false when out of range.
    pub fn replace(&mut self, index: usize, value: &[u8]) -> bool {
        if index >= self.len {
            return false;
        }
        self.remove(index);
        self.insert(index, value);
        true
    }

    /// Element at `index`.
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.len {
            return None;
        }
        let pos = self.offset_of(index)?;
        Some(self.payload_at(pos))
    }

    /// First element.
    pub fn first(&self) -> Option<&[u8]> {
        (!self.is_empty()).then(|| self.payload_at(0))
    }

    /// Last element.
    pub fn last(&self) -> Option<&[u8]> {
        if self.is_empty() {
            return None;
        }
        let (back, back_bytes) = read_backlen(&self.buf, self.buf.len());
        Some(self.payload_at(self.buf.len() - back_bytes - back as usize))
    }

    /// Iterate elements front to back.
    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            lp: self,
            pos: 0,
            remaining: self.len,
        }
    }

    /// Iterate elements back to front.
    pub fn iter_rev(&self) -> ListpackRevIter<'_> {
        ListpackRevIter {
            lp: self,
            end: self.buf.len(),
            remaining: self.len,
        }
    }

    /// Split off the elements from `index` onwards into a new listpack.
    ///
    /// Entries are self-contained, so this is a byte split at an entry
    /// boundary — no re-encoding.
    ///
    /// # Panics
    /// Panics if `index > len()`.
    pub fn split_off(&mut self, index: usize) -> Listpack {
        let at = self
            .offset_of(index)
            .expect("listpack split index out of range");
        let tail = self.buf.split_off(at);
        let tail_len = self.len - index;
        self.len = index;
        Listpack {
            buf: tail,
            len: tail_len,
        }
    }

    /// Append every element of `other`.
    pub fn append(&mut self, other: &Listpack) {
        self.buf.extend_from_slice(&other.buf);
        self.len += other.len;
    }

    /// Drop the first `n` elements (all of them if `n >= len()`).
    pub fn drain_front(&mut self, n: usize) {
        if n >= self.len {
            self.clear();
            return;
        }
        let at = self.offset_of(n).expect("n < len");
        self.buf.drain(..at);
        self.len -= n;
    }

    /// Drop the last `n` elements (all of them if `n >= len()`).
    pub fn drain_back(&mut self, n: usize) {
        if n >= self.len {
            self.clear();
            return;
        }
        let at = self.offset_of(self.len - n).expect("n < len");
        self.buf.truncate(at);
        self.len -= n;
    }
}

impl FromIterator<Vec<u8>> for Listpack {
    fn from_iter<I: IntoIterator<Item = Vec<u8>>>(iter: I) -> Self {
        let mut lp = Listpack::new();
        for v in iter {
            lp.push_back(&v);
        }
        lp
    }
}

/// Forward iterator over listpack elements.
pub struct ListpackIter<'a> {
    lp: &'a Listpack,
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        let value = self.lp.payload_at(self.pos);
        self.pos += self.lp.entry_total_at(self.pos);
        self.remaining -= 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for ListpackIter<'_> {}

/// Backward iterator over listpack elements.
pub struct ListpackRevIter<'a> {
    lp: &'a Listpack,
    end: usize,
    remaining: usize,
}

impl<'a> Iterator for ListpackRevIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        let (back, back_bytes) = read_backlen(&self.lp.buf, self.end);
        self.end -= back_bytes + back as usize;
        self.remaining -= 1;
        Some(self.lp.payload_at(self.end))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for ListpackRevIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(lp: &Listpack) -> Vec<Vec<u8>> {
        lp.iter().map(|e| e.to_vec()).collect()
    }

    #[test]
    fn varint_roundtrips_across_group_boundaries() {
        for v in [
            0u64,
            1,
            127,
            128,
            16383,
            16384,
            1 << 20,
            1 << 34,
            u32::MAX as u64,
        ] {
            let mut buf = [0u8; 10];
            let n = write_varint(&mut buf, v);
            assert_eq!(n, varint_len(v), "forward length for {v}");
            assert_eq!(read_varint(&buf, 0), (v, n), "forward roundtrip for {v}");

            let mut back = [0u8; 10];
            let bn = write_backlen(&mut back, v);
            assert_eq!(bn, varint_len(v), "backlen length for {v}");
            assert_eq!(
                read_backlen(&back, bn),
                (v, bn),
                "backlen roundtrip for {v}"
            );
        }
    }

    #[test]
    fn push_back_and_iterate() {
        let mut lp = Listpack::new();
        for i in 0..50u32 {
            lp.push_back(format!("value-{i}").as_bytes());
        }
        assert_eq!(lp.len(), 50);
        let items = collect(&lp);
        for (i, item) in items.iter().enumerate() {
            assert_eq!(item, format!("value-{i}").as_bytes());
        }
    }

    #[test]
    fn push_front_reverses_order() {
        let mut lp = Listpack::new();
        for i in 0..10u32 {
            lp.push_front(format!("{i}").as_bytes());
        }
        let items = collect(&lp);
        assert_eq!(items[0], b"9");
        assert_eq!(items[9], b"0");
    }

    #[test]
    fn reverse_iteration_matches_forward() {
        let mut lp = Listpack::new();
        for i in 0..200u32 {
            lp.push_back(format!("x{i}").as_bytes());
        }
        let fwd = collect(&lp);
        let mut rev: Vec<Vec<u8>> = lp.iter_rev().map(|e| e.to_vec()).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
    }

    #[test]
    fn get_scans_from_the_closer_end() {
        let mut lp = Listpack::new();
        for i in 0..101u32 {
            lp.push_back(format!("{i}").as_bytes());
        }
        // Both the forward (index <= len/2) and backward branches.
        assert_eq!(lp.get(0).unwrap(), b"0");
        assert_eq!(lp.get(50).unwrap(), b"50");
        assert_eq!(lp.get(100).unwrap(), b"100");
        assert_eq!(lp.get(101), None);
        assert_eq!(lp.first().unwrap(), b"0");
        assert_eq!(lp.last().unwrap(), b"100");
    }

    #[test]
    fn insert_remove_replace_in_the_middle() {
        let mut lp = Listpack::new();
        for i in 0..6u32 {
            lp.push_back(format!("{i}").as_bytes());
        }
        lp.insert(3, b"new");
        assert_eq!(
            collect(&lp),
            vec![
                b"0".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
                b"new".to_vec(),
                b"3".to_vec(),
                b"4".to_vec(),
                b"5".to_vec()
            ]
        );
        assert!(lp.remove(3));
        assert_eq!(lp.len(), 6);
        assert!(lp.replace(0, b"zero"));
        assert_eq!(lp.get(0).unwrap(), b"zero");
        assert!(!lp.remove(6));
        assert!(!lp.replace(6, b"nope"));
    }

    #[test]
    fn insert_at_len_appends() {
        let mut lp = Listpack::new();
        lp.push_back(b"a");
        lp.insert(1, b"b");
        assert_eq!(collect(&lp), vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn split_off_and_append_roundtrip() {
        let mut lp = Listpack::new();
        for i in 0..20u32 {
            lp.push_back(format!("{i}").as_bytes());
        }
        let tail = lp.split_off(7);
        assert_eq!(lp.len(), 7);
        assert_eq!(tail.len(), 13);
        assert_eq!(tail.first().unwrap(), b"7");
        assert_eq!(lp.last().unwrap(), b"6");
        lp.append(&tail);
        assert_eq!(lp.len(), 20);
        assert_eq!(lp.get(19).unwrap(), b"19");
        // Reverse traversal still works after the concatenation.
        assert_eq!(lp.iter_rev().count(), 20);
    }

    #[test]
    fn drain_from_both_ends() {
        let mut lp = Listpack::new();
        for i in 0..10u32 {
            lp.push_back(format!("{i}").as_bytes());
        }
        lp.drain_front(3);
        assert_eq!(lp.first().unwrap(), b"3");
        lp.drain_back(3);
        assert_eq!(lp.last().unwrap(), b"6");
        assert_eq!(lp.len(), 4);
        lp.drain_front(99);
        assert!(lp.is_empty());
        assert_eq!(lp.byte_len(), 0);
    }

    #[test]
    fn empty_and_large_payloads() {
        let mut lp = Listpack::new();
        lp.push_back(b"");
        lp.push_back(&vec![b'z'; 100_000]);
        lp.push_back(b"tail");
        assert_eq!(lp.len(), 3);
        assert_eq!(lp.get(0).unwrap(), b"");
        assert_eq!(lp.get(1).unwrap().len(), 100_000);
        assert_eq!(lp.get(2).unwrap(), b"tail");
        assert_eq!(lp.last().unwrap(), b"tail");
        let mut rev = lp.iter_rev();
        assert_eq!(rev.next().unwrap(), b"tail");
        assert_eq!(rev.next().unwrap().len(), 100_000);
        assert_eq!(rev.next().unwrap(), b"");
    }

    #[test]
    fn entry_size_matches_encoded_bytes() {
        for len in [0usize, 1, 63, 127, 128, 1000, 20_000] {
            let mut lp = Listpack::new();
            lp.push_back(&vec![b'a'; len]);
            assert_eq!(lp.byte_len(), Listpack::entry_size(len), "len {len}");
        }
    }
}
