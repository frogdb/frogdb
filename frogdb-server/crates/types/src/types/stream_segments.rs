//! Packed segment storage for stream entries.
//!
//! Entries are stored in ~128-entry segments keyed by their first ID. Each
//! segment holds one [`Listpack`] with one blob element per entry, plus a
//! `master` listpack of field names shared by entries whose field names match
//! the first entry appended to the segment (Redis's listpack-master trick).
//! This removes the per-entry `Vec` and per-field `Bytes` allocations of the
//! previous `BTreeMap<StreamId, Vec<(Bytes, Bytes)>>` representation.
//!
//! # Entry blob format
//!
//! ```text
//! [flags: u8][count: varint][payload]
//! ```
//!
//! * `flags == 1`: field names match `master` — payload is `count` values,
//!   each `[len: varint][bytes]`.
//! * `flags == 0`: payload is `count` pairs, each
//!   `[flen: varint][field][vlen: varint][value]`.
//!
//! # Invariants
//!
//! * Storage is strictly append-only: `XADD`/`ES.APPEND` IDs are validated
//!   against the stream's top item before reaching [`SegmentedEntries::append`]
//!   (an empty stream accepts any ID — trivially an append).
//! * A segment's map key always equals `ids[0]` — removal of a segment's first
//!   entry re-keys the segment.
//! * `XDEL` removes entries physically (a bounded memmove inside one segment),
//!   so dead space is bounded by construction — no tombstones, no compaction.

use super::stream::StreamId;
use crate::listpack::{Listpack, ListpackIter, ListpackRevIter};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::Bound;

/// Entries per segment. Matches Redis's `stream-node-max-entries` default
/// order of magnitude; bounds the memmove cost of a mid-segment removal.
const SEGMENT_MAX_ENTRIES: usize = 128;

/// Append a LEB128 varint to `buf`.
#[inline]
fn push_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Read a LEB128 varint at `*pos`, advancing `*pos` past it.
#[inline]
fn take_varint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*pos];
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

/// Encode one entry's fields against `master` (see module docs for layout).
fn encode_entry(fields: &[(Bytes, Bytes)], master: &Listpack) -> Vec<u8> {
    let dedup = master.len() == fields.len()
        && master
            .iter()
            .zip(fields)
            .all(|(name, (f, _))| name == f.as_ref());
    let payload: usize = fields
        .iter()
        .map(|(f, v)| v.len() + if dedup { 0 } else { f.len() } + 10)
        .sum();
    let mut blob = Vec::with_capacity(1 + 10 + payload);
    blob.push(dedup as u8);
    push_varint(&mut blob, fields.len() as u64);
    for (f, v) in fields {
        if !dedup {
            push_varint(&mut blob, f.len() as u64);
            blob.extend_from_slice(f);
        }
        push_varint(&mut blob, v.len() as u64);
        blob.extend_from_slice(v);
    }
    blob
}

/// Decode an entry blob produced by [`encode_entry`] against the same master.
fn decode_entry(blob: &[u8], master: &Listpack) -> Vec<(Bytes, Bytes)> {
    let mut pos = 0;
    let flags = blob[pos];
    pos += 1;
    let count = take_varint(blob, &mut pos) as usize;
    let mut fields = Vec::with_capacity(count);
    if flags == 1 {
        let mut names = master.iter();
        for _ in 0..count {
            let name = names
                .next()
                .expect("dedup entry field count matches master");
            let vlen = take_varint(blob, &mut pos) as usize;
            fields.push((
                Bytes::copy_from_slice(name),
                Bytes::copy_from_slice(&blob[pos..pos + vlen]),
            ));
            pos += vlen;
        }
    } else {
        for _ in 0..count {
            let flen = take_varint(blob, &mut pos) as usize;
            let f = Bytes::copy_from_slice(&blob[pos..pos + flen]);
            pos += flen;
            let vlen = take_varint(blob, &mut pos) as usize;
            let v = Bytes::copy_from_slice(&blob[pos..pos + vlen]);
            pos += vlen;
            fields.push((f, v));
        }
    }
    fields
}

/// One packed run of consecutive entries.
#[derive(Debug, Clone, Default)]
struct Segment {
    /// Entry IDs, ascending. `ids[i]` pairs with `entries` element `i`.
    ids: Vec<StreamId>,
    /// One encoded blob per entry.
    entries: Listpack,
    /// Field names of the first entry ever appended to this segment. Kept for
    /// the segment's lifetime — later dedup entries reference it even after
    /// that first entry is deleted.
    master: Listpack,
}

/// Stream entry storage: packed segments keyed by first ID.
#[derive(Debug, Clone, Default)]
pub(crate) struct SegmentedEntries {
    segments: BTreeMap<StreamId, Segment>,
    len: usize,
}

impl SegmentedEntries {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Number of live entries.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Smallest stored ID.
    pub(crate) fn first_id(&self) -> Option<StreamId> {
        self.segments.first_key_value().map(|(id, _)| *id)
    }

    /// Largest stored ID.
    pub(crate) fn last_id(&self) -> Option<StreamId> {
        self.segments
            .last_key_value()
            .and_then(|(_, seg)| seg.ids.last().copied())
    }

    /// First entry, decoded.
    pub(crate) fn first(&self) -> Option<(StreamId, Vec<(Bytes, Bytes)>)> {
        let (_, seg) = self.segments.first_key_value()?;
        Some((seg.ids[0], decode_entry(seg.entries.first()?, &seg.master)))
    }

    /// Last entry, decoded.
    pub(crate) fn last(&self) -> Option<(StreamId, Vec<(Bytes, Bytes)>)> {
        let (_, seg) = self.segments.last_key_value()?;
        Some((
            *seg.ids.last()?,
            decode_entry(seg.entries.last()?, &seg.master),
        ))
    }

    /// Append an entry. `id` must be greater than every stored ID (the stream
    /// validates IDs against its top item before calling; an empty store
    /// accepts any ID).
    pub(crate) fn append(&mut self, id: StreamId, fields: &[(Bytes, Bytes)]) {
        debug_assert!(
            self.last_id().is_none_or(|last| id > last),
            "stream storage is append-only"
        );
        if let Some(mut entry) = self.segments.last_entry()
            && entry.get().ids.len() < SEGMENT_MAX_ENTRIES
        {
            let seg = entry.get_mut();
            let blob = encode_entry(fields, &seg.master);
            seg.ids.push(id);
            seg.entries.push_back(&blob);
            self.len += 1;
            return;
        }
        let mut master = Listpack::new();
        for (f, _) in fields {
            master.push_back(f);
        }
        let mut seg = Segment {
            ids: vec![id],
            entries: Listpack::new(),
            master,
        };
        let blob = encode_entry(fields, &seg.master);
        seg.entries.push_back(&blob);
        self.segments.insert(id, seg);
        self.len += 1;
    }

    /// Segment that could contain `id` (last segment whose key is `<= id`).
    fn segment_key_for(&self, id: &StreamId) -> Option<StreamId> {
        self.segments.range(..=*id).next_back().map(|(k, _)| *k)
    }

    /// Entry fields by ID, decoded.
    pub(crate) fn get(&self, id: &StreamId) -> Option<Vec<(Bytes, Bytes)>> {
        let seg = self.segments.get(&self.segment_key_for(id)?)?;
        let idx = seg.ids.binary_search(id).ok()?;
        Some(decode_entry(seg.entries.get(idx)?, &seg.master))
    }

    /// Whether an entry with `id` exists.
    pub(crate) fn contains(&self, id: &StreamId) -> bool {
        self.segment_key_for(id)
            .and_then(|k| self.segments.get(&k))
            .is_some_and(|seg| seg.ids.binary_search(id).is_ok())
    }

    /// Physically remove one entry. Returns false when absent.
    pub(crate) fn remove(&mut self, id: &StreamId) -> bool {
        let Some(key) = self.segment_key_for(id) else {
            return false;
        };
        let seg = self.segments.get_mut(&key).expect("key came from the map");
        let Ok(idx) = seg.ids.binary_search(id) else {
            return false;
        };
        seg.ids.remove(idx);
        seg.entries.remove(idx);
        self.len -= 1;
        if seg.ids.is_empty() {
            self.segments.remove(&key);
        } else if idx == 0 {
            // Map key must stay equal to ids[0].
            let seg = self.segments.remove(&key).expect("key came from the map");
            self.segments.insert(seg.ids[0], seg);
        }
        true
    }

    /// Remove the `n` smallest entries (all of them if `n >= len()`), whole
    /// segments at a time. Returns the number removed.
    pub(crate) fn drain_front(&mut self, n: usize) -> usize {
        let removed = n.min(self.len);
        let mut remaining = removed;
        while remaining > 0 {
            let seg_len = self
                .segments
                .first_key_value()
                .map(|(_, seg)| seg.ids.len())
                .expect("remaining > 0 implies a segment exists");
            if seg_len <= remaining {
                self.segments.pop_first();
                remaining -= seg_len;
            } else {
                let (_, mut seg) = self.segments.pop_first().expect("checked above");
                seg.ids.drain(..remaining);
                seg.entries.drain_front(remaining);
                self.segments.insert(seg.ids[0], seg);
                remaining = 0;
            }
        }
        self.len -= removed;
        removed
    }

    /// Number of entries with ID strictly below `min_id`. Because IDs are
    /// ascending, these are exactly the first `count_below` entries.
    pub(crate) fn count_below(&self, min_id: &StreamId) -> usize {
        self.segments
            .range(..*min_id)
            .map(|(_, seg)| match seg.ids.binary_search(min_id) {
                Ok(i) | Err(i) => i,
            })
            .sum()
    }

    /// Iterate all entries in ID order.
    pub(crate) fn iter(&self) -> Iter<'_> {
        Iter {
            segs: self.segments.range(..),
            cur: None,
        }
    }

    /// Iterate entries with IDs inside `lower`, in ID order.
    pub(crate) fn iter_from(&self, lower: Bound<StreamId>) -> Iter<'_> {
        let (id, inclusive) = match lower {
            Bound::Unbounded => return self.iter(),
            Bound::Included(id) => (id, true),
            Bound::Excluded(id) => (id, false),
        };
        let Some(key) = self.segment_key_for(&id) else {
            return self.iter();
        };
        let mut it = Iter {
            segs: self.segments.range(key..),
            cur: None,
        };
        let (_, seg) = it.segs.next().expect("key came from the map");
        let skip = match seg.ids.binary_search(&id) {
            Ok(i) => i + usize::from(!inclusive),
            Err(i) => i,
        };
        let mut ids = seg.ids.iter();
        let mut blobs = seg.entries.iter();
        for _ in 0..skip {
            ids.next();
            blobs.next();
        }
        it.cur = Some(CurSeg {
            ids,
            blobs,
            master: &seg.master,
        });
        it
    }

    /// Iterate all entries in reverse ID order.
    pub(crate) fn iter_rev(&self) -> RevIter<'_> {
        RevIter {
            segs: self.segments.range(..).rev(),
            cur: None,
        }
    }

    /// Iterate entries with IDs inside `upper`, in reverse ID order.
    pub(crate) fn iter_rev_from(&self, upper: Bound<StreamId>) -> RevIter<'_> {
        let (id, inclusive) = match upper {
            Bound::Unbounded => return self.iter_rev(),
            Bound::Included(id) => (id, true),
            Bound::Excluded(id) => (id, false),
        };
        let Some(key) = self.segment_key_for(&id) else {
            // Every stored ID is above the bound: empty iteration.
            return RevIter {
                segs: self.segments.range(..StreamId::min()).rev(),
                cur: None,
            };
        };
        let mut it = RevIter {
            segs: self.segments.range(..=key).rev(),
            cur: None,
        };
        let (_, seg) = it.segs.next().expect("key came from the map");
        // Entries above the bound sit at the tail of this segment.
        let keep = match seg.ids.binary_search(&id) {
            Ok(i) => i + usize::from(inclusive),
            Err(i) => i,
        };
        let skip = seg.ids.len() - keep;
        let mut ids = seg.ids.iter().rev();
        let mut blobs = seg.entries.iter_rev();
        for _ in 0..skip {
            ids.next();
            blobs.next();
        }
        it.cur = Some(CurSegRev {
            ids,
            blobs,
            master: &seg.master,
        });
        it
    }

    /// Heap bytes attributable to entry storage. Deterministic for a given
    /// operation history: derived from logical lengths and encoded bytes,
    /// never from `Vec` capacities.
    pub(crate) fn memory_size(&self) -> usize {
        self.segments
            .values()
            .map(|seg| {
                std::mem::size_of::<StreamId>()
                    + std::mem::size_of::<Segment>()
                    + seg.ids.len() * std::mem::size_of::<StreamId>()
                    + seg.entries.byte_len()
                    + seg.master.byte_len()
            })
            .sum()
    }
}

struct CurSeg<'a> {
    ids: std::slice::Iter<'a, StreamId>,
    blobs: ListpackIter<'a>,
    master: &'a Listpack,
}

/// Forward iterator over `(id, decoded fields)`.
pub(crate) struct Iter<'a> {
    segs: Range<'a, StreamId, Segment>,
    cur: Option<CurSeg<'a>>,
}

impl Iterator for Iter<'_> {
    type Item = (StreamId, Vec<(Bytes, Bytes)>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = &mut self.cur
                && let (Some(id), Some(blob)) = (cur.ids.next(), cur.blobs.next())
            {
                return Some((*id, decode_entry(blob, cur.master)));
            }
            let (_, seg) = self.segs.next()?;
            self.cur = Some(CurSeg {
                ids: seg.ids.iter(),
                blobs: seg.entries.iter(),
                master: &seg.master,
            });
        }
    }
}

struct CurSegRev<'a> {
    ids: std::iter::Rev<std::slice::Iter<'a, StreamId>>,
    blobs: ListpackRevIter<'a>,
    master: &'a Listpack,
}

/// Reverse iterator over `(id, decoded fields)`.
pub(crate) struct RevIter<'a> {
    segs: std::iter::Rev<Range<'a, StreamId, Segment>>,
    cur: Option<CurSegRev<'a>>,
}

impl Iterator for RevIter<'_> {
    type Item = (StreamId, Vec<(Bytes, Bytes)>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = &mut self.cur
                && let (Some(id), Some(blob)) = (cur.ids.next(), cur.blobs.next())
            {
                return Some((*id, decode_entry(blob, cur.master)));
            }
            let (_, seg) = self.segs.next()?;
            self.cur = Some(CurSegRev {
                ids: seg.ids.iter().rev(),
                blobs: seg.entries.iter_rev(),
                master: &seg.master,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId { ms, seq }
    }

    fn fields(pairs: &[(&str, &str)]) -> Vec<(Bytes, Bytes)> {
        pairs
            .iter()
            .map(|(f, v)| {
                (
                    Bytes::copy_from_slice(f.as_bytes()),
                    Bytes::copy_from_slice(v.as_bytes()),
                )
            })
            .collect()
    }

    /// n entries with ids (i, 0) and uniform field names.
    fn build(n: u64) -> SegmentedEntries {
        let mut se = SegmentedEntries::new();
        for i in 0..n {
            se.append(id(i, 0), &fields(&[("temp", "21"), ("hum", "40")]));
        }
        se
    }

    #[test]
    fn append_get_and_rollover() {
        let se = build(300);
        assert_eq!(se.len(), 300);
        assert_eq!(se.segments.len(), 300usize.div_ceil(SEGMENT_MAX_ENTRIES));
        for i in [0u64, 1, 127, 128, 255, 299] {
            assert_eq!(
                se.get(&id(i, 0)).unwrap(),
                fields(&[("temp", "21"), ("hum", "40")]),
                "entry {i}"
            );
        }
        assert!(!se.contains(&id(300, 0)));
        assert!(!se.contains(&id(0, 1)));
        assert_eq!(se.first_id(), Some(id(0, 0)));
        assert_eq!(se.last_id(), Some(id(299, 0)));
    }

    #[test]
    fn mixed_field_names_round_trip() {
        let mut se = SegmentedEntries::new();
        se.append(id(1, 0), &fields(&[("a", "1")]));
        se.append(id(2, 0), &fields(&[("b", "2"), ("c", "3")])); // no dedup
        se.append(id(3, 0), &fields(&[("a", "x")])); // dedup again
        se.append(id(4, 0), &fields(&[])); // empty fields
        assert_eq!(se.get(&id(1, 0)).unwrap(), fields(&[("a", "1")]));
        assert_eq!(
            se.get(&id(2, 0)).unwrap(),
            fields(&[("b", "2"), ("c", "3")])
        );
        assert_eq!(se.get(&id(3, 0)).unwrap(), fields(&[("a", "x")]));
        assert_eq!(se.get(&id(4, 0)).unwrap(), vec![]);
    }

    #[test]
    fn remove_rekeys_segment_on_first_entry() {
        let mut se = build(10);
        assert!(se.remove(&id(0, 0)));
        assert_eq!(se.first_id(), Some(id(1, 0)));
        assert_eq!(*se.segments.first_key_value().unwrap().0, id(1, 0));
        assert!(!se.remove(&id(0, 0)));
        assert!(se.remove(&id(5, 0)));
        assert_eq!(se.len(), 8);
        // Master survives deletion of the entry that defined it.
        assert_eq!(
            se.get(&id(2, 0)).unwrap(),
            fields(&[("temp", "21"), ("hum", "40")])
        );
        for i in 1..10 {
            se.remove(&id(i, 0));
        }
        assert!(se.is_empty());
        assert!(se.segments.is_empty());
        assert_eq!(se.memory_size(), 0);
    }

    #[test]
    fn drain_front_whole_and_partial_segments() {
        let mut se = build(300);
        assert_eq!(se.drain_front(150), 150); // one whole segment + 22 partial
        assert_eq!(se.len(), 150);
        assert_eq!(se.first_id(), Some(id(150, 0)));
        assert_eq!(*se.segments.first_key_value().unwrap().0, id(150, 0));
        assert_eq!(se.drain_front(1000), 150);
        assert!(se.is_empty());
        assert_eq!(se.drain_front(1), 0);
    }

    #[test]
    fn count_below_matches_linear_scan() {
        let mut se = build(300);
        se.remove(&id(17, 0));
        se.remove(&id(200, 0));
        for probe in [id(0, 0), id(17, 0), id(18, 0), id(128, 0), id(500, 0)] {
            let expected = se.iter().filter(|(i, _)| *i < probe).count();
            assert_eq!(se.count_below(&probe), expected, "probe {probe:?}");
        }
    }

    #[test]
    fn forward_and_reverse_iteration_agree() {
        let mut se = build(300);
        se.remove(&id(128, 0)); // segment-head removal in the middle
        let fwd: Vec<StreamId> = se.iter().map(|(i, _)| i).collect();
        let mut rev: Vec<StreamId> = se.iter_rev().map(|(i, _)| i).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
        assert_eq!(fwd.len(), 299);
        assert!(fwd.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn iter_from_seeks_correctly() {
        let se = build(300);
        let from = |b| se.iter_from(b).map(|(i, _)| i).collect::<Vec<_>>();
        assert_eq!(from(Bound::Unbounded).len(), 300);
        assert_eq!(from(Bound::Included(id(150, 0)))[0], id(150, 0));
        assert_eq!(from(Bound::Excluded(id(150, 0)))[0], id(151, 0));
        assert_eq!(from(Bound::Included(id(150, 0))).len(), 150);
        // Bound below every stored ID.
        assert_eq!(from(Bound::Included(id(0, 0)))[0], id(0, 0));
        // Bound between IDs (no exact match).
        assert_eq!(from(Bound::Included(id(150, 1)))[0], id(151, 0));
        // Bound above every stored ID.
        assert!(from(Bound::Included(id(400, 0))).is_empty());
        // Bound at the last entry of a segment (skip == everything in it).
        assert_eq!(from(Bound::Excluded(id(127, 0)))[0], id(128, 0));
    }

    #[test]
    fn iter_rev_from_seeks_correctly() {
        let se = build(300);
        let from = |b| se.iter_rev_from(b).map(|(i, _)| i).collect::<Vec<_>>();
        assert_eq!(from(Bound::Unbounded).len(), 300);
        assert_eq!(from(Bound::Included(id(150, 0)))[0], id(150, 0));
        assert_eq!(from(Bound::Excluded(id(150, 0)))[0], id(149, 0));
        assert_eq!(from(Bound::Included(id(150, 0))).len(), 151);
        // Bound below every stored ID.
        assert!(from(Bound::Excluded(id(0, 0))).is_empty());
        // Bound between IDs.
        assert_eq!(from(Bound::Included(id(150, 1)))[0], id(150, 0));
        // Bound above every stored ID.
        assert_eq!(from(Bound::Included(id(400, 0))).len(), 300);
        // Excluded bound exactly at a segment key: that segment contributes nothing.
        assert_eq!(from(Bound::Excluded(id(128, 0)))[0], id(127, 0));
    }

    #[test]
    fn memory_size_is_run_stable_and_shrinks_on_removal() {
        let build_with_churn = || {
            let mut se = build(1000);
            for i in (0..1000).step_by(3) {
                se.remove(&id(i, 0));
            }
            se.drain_front(100);
            se
        };
        let a = build_with_churn();
        let b = build_with_churn();
        assert_eq!(a.memory_size(), b.memory_size());
        let full = build(1000);
        assert!(a.memory_size() < full.memory_size());
        assert_eq!(a.len(), b.len());
    }

    /// XDEL churn bounds dead space by construction: removal is physical, so
    /// encoded bytes track live entries, not historical peak.
    #[test]
    fn churn_does_not_accumulate_dead_space() {
        let mut se = build(1000);
        let full_size = se.memory_size();
        for i in 0..900 {
            se.remove(&id(i, 0));
        }
        assert_eq!(se.len(), 100);
        // 100 live entries must cost roughly 10% of 1000 — allow generous
        // slack for per-segment constants.
        assert!(
            se.memory_size() < full_size / 5,
            "dead space accumulated: {} vs full {}",
            se.memory_size(),
            full_size
        );
    }

    /// Memory shape at 100k entries: overhead beyond payload is per-segment
    /// plus small per-entry constants, and master dedup beats the no-dedup
    /// encoding. The measured sizes get recorded in the issue resolution.
    #[test]
    fn memory_shape_100k_entries_and_dedup_benefit() {
        let entry = fields(&[("temperature", "21.5"), ("humidity", "40")]);
        let payload_per_entry: usize = entry.iter().map(|(f, v)| f.len() + v.len()).sum();
        let n: usize = 100_000;
        let mut se = SegmentedEntries::new();
        for i in 0..n as u64 {
            se.append(id(i, 0), &entry);
        }
        let packed = se.memory_size();
        let segments = se.segments.len();
        assert_eq!(segments, n.div_ceil(SEGMENT_MAX_ENTRIES));

        // No-dedup cost of the same data: encode every entry against an
        // empty master (forces flags == 0, field names inline per entry).
        let empty = Listpack::new();
        let nodedup_blob = Listpack::entry_size(encode_entry(&entry, &empty).len());
        let nodedup = segments * (std::mem::size_of::<StreamId>() + std::mem::size_of::<Segment>())
            + n * (std::mem::size_of::<StreamId>() + nodedup_blob);

        // Old representation: BTreeMap<StreamId, Vec<(Bytes, Bytes)>> as
        // counted by the old memory_size(): per-field k+v+16, plus 32/entry.
        let old = n
            * (std::mem::size_of::<StreamId>()
                + entry
                    .iter()
                    .map(|(f, v)| f.len() + v.len() + 16)
                    .sum::<usize>()
                + 32);

        println!(
            "100k entries: payload {} B, packed {} B ({:.2} B/entry), \
             no-dedup {} B, old-representation {} B, segments {}",
            n * payload_per_entry,
            packed,
            packed as f64 / n as f64,
            nodedup,
            old,
            segments
        );
        assert!(packed < nodedup, "dedup must beat inline field names");
        assert!(
            packed < old / 2,
            "packed form must halve the old accounting"
        );
        // Overhead beyond raw payload: ids (16 B) + blob framing (~5 B) per
        // entry, plus per-segment constants — well under 32 B/entry here.
        assert!(packed - n * payload_per_entry < n * 32);
    }

    mod model {
        use super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Op {
            /// Append with the next auto ID; field-name set chosen from a pool.
            Add { shape: u8, value: u8 },
            /// Delete the ID at `index % (len + 1)` (may miss).
            Del { index: usize },
            /// Trim to at most `max_len` entries.
            TrimMaxLen { max_len: usize },
            /// Range query, compared against the model.
            Range { lo: u64, hi: u64, rev: bool },
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                4 => (0u8..3, any::<u8>()).prop_map(|(shape, value)| Op::Add { shape, value }),
                2 => any::<usize>().prop_map(|index| Op::Del { index }),
                1 => (0usize..600).prop_map(|max_len| Op::TrimMaxLen { max_len }),
                2 => (any::<u64>(), any::<u64>(), any::<bool>())
                    .prop_map(|(lo, hi, rev)| Op::Range { lo: lo % 700, hi: hi % 700, rev }),
            ]
        }

        fn shape_fields(shape: u8, value: u8) -> Vec<(Bytes, Bytes)> {
            let v = value.to_string();
            match shape {
                0 => fields(&[("temp", &v), ("hum", &v)]),
                1 => fields(&[("only", &v)]),
                _ => fields(&[("a", &v), ("b", &v), ("c", &v)]),
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]
            #[test]
            fn matches_btreemap_model(ops in proptest::collection::vec(op_strategy(), 1..400)) {
                let mut se = SegmentedEntries::new();
                let mut model: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>> =
                    Default::default();
                let mut next_ms = 0u64;

                for op in ops {
                    match op {
                        Op::Add { shape, value } => {
                            let entry = shape_fields(shape, value);
                            let new_id = id(next_ms, 0);
                            next_ms += 1;
                            se.append(new_id, &entry);
                            model.insert(new_id, entry);
                        }
                        Op::Del { index } => {
                            let target = model
                                .keys()
                                .nth(index % (model.len() + 1))
                                .copied()
                                .unwrap_or(id(next_ms + 1, 0));
                            prop_assert_eq!(se.remove(&target), model.remove(&target).is_some());
                        }
                        Op::TrimMaxLen { max_len } => {
                            let excess = model.len().saturating_sub(max_len);
                            prop_assert_eq!(se.drain_front(excess), excess);
                            for _ in 0..excess {
                                model.pop_first();
                            }
                        }
                        Op::Range { lo, hi, rev } => {
                            let (lo_id, hi_id) = (id(lo, 0), id(hi, 0));
                            if lo_id > hi_id {
                                // Inverted range: must yield nothing.
                                prop_assert_eq!(
                                    se.iter_from(Bound::Included(lo_id))
                                        .take_while(|(i, _)| *i <= hi_id)
                                        .count(),
                                    0
                                );
                                prop_assert_eq!(
                                    se.iter_rev_from(Bound::Included(hi_id))
                                        .take_while(|(i, _)| *i >= lo_id)
                                        .count(),
                                    0
                                );
                            } else if rev {
                                let got: Vec<_> = se
                                    .iter_rev_from(Bound::Included(hi_id))
                                    .take_while(|(i, _)| *i >= lo_id)
                                    .collect();
                                let want: Vec<_> = model
                                    .range(lo_id..=hi_id)
                                    .rev()
                                    .map(|(i, f)| (*i, f.clone()))
                                    .collect();
                                prop_assert_eq!(got, want);
                            } else {
                                let got: Vec<_> = se
                                    .iter_from(Bound::Included(lo_id))
                                    .take_while(|(i, _)| *i <= hi_id)
                                    .collect();
                                let want: Vec<_> = model
                                    .range(lo_id..=hi_id)
                                    .map(|(i, f)| (*i, f.clone()))
                                    .collect();
                                prop_assert_eq!(got, want);
                            }
                        }
                    }
                }

                // Full-state agreement after every op sequence.
                prop_assert_eq!(se.len(), model.len());
                let got: Vec<_> = se.iter().collect();
                let want: Vec<_> = model.iter().map(|(i, f)| (*i, f.clone())).collect();
                prop_assert_eq!(got, want);
                prop_assert_eq!(se.first_id(), model.keys().next().copied());
                prop_assert_eq!(se.last_id(), model.keys().next_back().copied());
            }
        }
    }
}
