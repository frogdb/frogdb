//! Tagged 8-byte slot words.
//!
//! The spike ruled the width and the thresholds and this module implements them
//! verbatim: one 8-byte word per key and per value, byte strings up to **7 bytes**
//! inline, integers up to **61 significant bits** inline, everything else an
//! 8-aligned pointer to a [`Record`].
//!
//! ```text
//! bits 0..3   tag       000 record pointer, 001 inline int, 010 inline string
//! bits 3..7   length    inline string only, 0..=7
//! bits 8..64  payload   inline string bytes 1..8, or the integer shifted left 3
//! ```
//!
//! A record pointer is 8-aligned, so its low three bits are already the `PTR`
//! tag: a pointer word **is** the pointer and the lookup fast path does no
//! masking.
//!
//! Two word types, because keys and values are not the same question. [`KeyWord`]
//! carries byte strings only (a Redis key is bytes). [`ValueWord`] carries byte
//! strings or integers, and is what makes R6's "inline small values" real. Both
//! own their record and free it on drop, so a slot is dropped by dropping its
//! fields — there is no manual free path to get wrong.

use std::ptr::NonNull;

use crate::record::Record;

const TAG_MASK: u64 = 0b111;
const TAG_PTR: u64 = 0b000;
const TAG_INT: u64 = 0b001;
const TAG_STR: u64 = 0b010;

/// Longest byte string that fits in a word.
pub const INLINE_STR_MAX: usize = 7;

/// Significant bits an inline integer keeps (sign included).
pub const INLINE_INT_BITS: u32 = 61;

/// What a word holds, borrowed from the word itself.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

/// Scratch an inline decode borrows from. Sixteen bytes so the caller can hold
/// one on the stack for either word type without thinking about it.
pub type InlineBuf = [u8; 16];

/// Whether `v` keeps all its significant bits in an inline integer word.
#[inline]
pub const fn int_fits_inline(v: i64) -> bool {
    (v << 3) >> 3 == v
}

/// An owning 8-byte word holding a byte string: inline when short, otherwise a
/// pointer to a [`Record`].
#[repr(transparent)]
pub struct KeyWord(u64);

impl KeyWord {
    /// Encodes `key`, allocating a record when it does not inline.
    #[inline]
    pub fn new(key: &[u8]) -> KeyWord {
        if key.len() <= INLINE_STR_MAX {
            KeyWord(pack_inline_str(key))
        } else {
            KeyWord(pack_record(Record::new(key)))
        }
    }

    /// Whether the key lives in the word rather than in a record.
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.0 & TAG_MASK != TAG_PTR
    }

    /// The key's bytes. Inline keys are copied into `buf`; out-of-line keys are
    /// borrowed from their record.
    #[inline]
    pub fn bytes<'a>(&'a self, buf: &'a mut InlineBuf) -> &'a [u8] {
        if self.is_inline() {
            unpack_inline_str(self.0, buf)
        } else {
            // SAFETY: the tag says `PTR`, so the word is a live record pointer
            // this word owns; the borrow is tied to `&self`.
            unsafe { record_ref(&self.0).as_bytes() }
        }
    }

    /// Whether the key equals `other`, without materialising it.
    #[inline]
    pub fn eq_bytes(&self, other: &[u8]) -> bool {
        if self.is_inline() {
            let mut buf: InlineBuf = [0; 16];
            unpack_inline_str(self.0, &mut buf) == other
        } else {
            // SAFETY: as `bytes`.
            unsafe { record_ref(&self.0).as_bytes() == other }
        }
    }

    /// Heap bytes this word owns, allocator header included; `0` when inline.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        if self.is_inline() {
            0
        } else {
            // SAFETY: as `bytes`.
            unsafe { record_ref(&self.0).requested_bytes() }
        }
    }
}

impl Drop for KeyWord {
    fn drop(&mut self) {
        // SAFETY: a non-inline word owns exactly one reference to its record,
        // taken in `KeyWord::new` and released exactly here.
        unsafe { drop_record(self.0) };
    }
}

impl Clone for KeyWord {
    /// Shares the record (an `rc` bump) rather than copying its bytes.
    fn clone(&self) -> KeyWord {
        if self.is_inline() {
            KeyWord(self.0)
        } else {
            // SAFETY: as `bytes`.
            KeyWord(pack_record(unsafe { record_ref(&self.0) }.clone()))
        }
    }
}

impl std::fmt::Debug for KeyWord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf: InlineBuf = [0; 16];
        write!(
            f,
            "KeyWord({:?})",
            String::from_utf8_lossy(self.bytes(&mut buf))
        )
    }
}

/// An owning 8-byte word holding a byte string or an integer.
///
/// This is the word R6's "inline small values" ruling is about: 59 % of
/// `redis-feel` values and 100 % of counter values never leave it.
#[repr(transparent)]
pub struct ValueWord(u64);

impl ValueWord {
    /// Encodes a byte string, inlining when it fits.
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> ValueWord {
        if bytes.len() <= INLINE_STR_MAX {
            ValueWord(pack_inline_str(bytes))
        } else {
            ValueWord(pack_record(Record::new(bytes)))
        }
    }

    /// Encodes an integer, inlining when it keeps all its bits. Wider integers
    /// fall back to an 8-byte little-endian record, as the spike's control does.
    #[inline]
    pub fn from_int(v: i64) -> ValueWord {
        if int_fits_inline(v) {
            ValueWord(((v << 3) as u64) | TAG_INT)
        } else {
            ValueWord(pack_record(Record::new(&v.to_le_bytes())))
        }
    }

    /// Whether the value lives in the word rather than in a record.
    #[inline]
    pub fn is_inline(&self) -> bool {
        self.0 & TAG_MASK != TAG_PTR
    }

    /// The value, borrowed from the word or from its record.
    #[inline]
    pub fn decode<'a>(&'a self, buf: &'a mut InlineBuf) -> Decoded<'a> {
        match self.0 & TAG_MASK {
            TAG_INT => Decoded::Int((self.0 as i64) >> 3),
            TAG_STR => Decoded::Bytes(unpack_inline_str(self.0, buf)),
            // SAFETY: the remaining tag is `PTR`, so the word is a live record
            // pointer this word owns.
            _ => Decoded::Bytes(unsafe { record_ref(&self.0).as_bytes() }),
        }
    }

    /// Heap bytes this word owns, allocator header included; `0` when inline.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        if self.is_inline() {
            0
        } else {
            // SAFETY: as `decode`.
            unsafe { record_ref(&self.0).requested_bytes() }
        }
    }

    /// The record behind an out-of-line value, for COW: the caller clones it to
    /// hand a snapshot the current bytes, or `make_mut`s it to write.
    #[inline]
    pub fn record_mut(&mut self) -> Option<&mut Record> {
        if self.is_inline() {
            return None;
        }
        // SAFETY: the tag says `PTR`. A `Record` is `repr`-compatible with the
        // word only in that the word *is* the pointer, so the reference is
        // produced by reinterpreting the word's storage as the handle it holds —
        // exactly what `pack_record` wrote there. `&mut self` rules out aliasing.
        Some(unsafe { &mut *(std::ptr::from_mut(&mut self.0).cast::<Record>()) })
    }

    /// The record behind an out-of-line value, shared.
    #[inline]
    pub fn record(&self) -> Option<&Record> {
        if self.is_inline() {
            return None;
        }
        // SAFETY: as `record_mut`, with a shared borrow.
        Some(unsafe { record_ref(&self.0) })
    }
}

impl Drop for ValueWord {
    fn drop(&mut self) {
        // SAFETY: a non-inline word owns exactly one reference to its record.
        unsafe { drop_record(self.0) };
    }
}

impl Clone for ValueWord {
    /// Shares the record (an `rc` bump) rather than copying its bytes — the read
    /// half of COW.
    fn clone(&self) -> ValueWord {
        if self.is_inline() {
            ValueWord(self.0)
        } else {
            // SAFETY: as `decode`.
            ValueWord(pack_record(unsafe { record_ref(&self.0) }.clone()))
        }
    }
}

impl std::fmt::Debug for ValueWord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf: InlineBuf = [0; 16];
        write!(f, "ValueWord({:?})", self.decode(&mut buf))
    }
}

// ---------------------------------------------------------------------------
// Shared bit twiddling. Private: nothing outside this module should have to
// know which bit means what.
// ---------------------------------------------------------------------------

#[inline]
fn pack_inline_str(b: &[u8]) -> u64 {
    debug_assert!(b.len() <= INLINE_STR_MAX);
    let mut w = [0u8; 8];
    w[0] = (TAG_STR as u8) | ((b.len() as u8) << 3);
    w[1..1 + b.len()].copy_from_slice(b);
    u64::from_le_bytes(w)
}

#[inline]
fn unpack_inline_str(word: u64, buf: &mut InlineBuf) -> &[u8] {
    let bytes = word.to_le_bytes();
    let len = (bytes[0] >> 3) as usize;
    debug_assert!(len <= INLINE_STR_MAX);
    buf[..len].copy_from_slice(&bytes[1..1 + len]);
    &buf[..len]
}

#[inline]
fn pack_record(record: Record) -> u64 {
    let ptr = record.into_raw().as_ptr() as usize as u64;
    debug_assert_eq!(ptr & TAG_MASK, TAG_PTR, "record pointer must be 8-aligned");
    ptr
}

/// # Safety
/// `word`'s tag must be `PTR` and its record must still be live.
///
/// Takes the word **by reference**: the returned `&Record` borrows the word's own
/// storage, so tying it to a temporary would hand back a dangling reference.
#[inline]
unsafe fn record_ref(word: &u64) -> &Record {
    debug_assert_eq!(*word & TAG_MASK, TAG_PTR);
    // SAFETY: `Record` is `repr(transparent)` over `NonNull<u8>`, so a word whose
    // tag is `PTR` is bit-identical to the handle `pack_record` moved into it.
    // Reading it back as a reference is the inverse of that move, and the
    // lifetime is the word's.
    unsafe { &*(std::ptr::from_ref(word).cast::<Record>()) }
}

/// # Safety
/// If `word`'s tag is `PTR`, it must own exactly one live reference to a record,
/// and the caller must not use `word` afterwards.
#[inline]
unsafe fn drop_record(word: u64) {
    if word & TAG_MASK != TAG_PTR {
        return;
    }
    let ptr = NonNull::new(word as usize as *mut u8).expect("record pointer is never null");
    // SAFETY: the word owns the reference `pack_record` put there; rebuilding
    // the handle and dropping it releases exactly that one reference.
    drop(unsafe { Record::from_raw(ptr) });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_word_is_eight_bytes() {
        assert_eq!(std::mem::size_of::<KeyWord>(), 8);
        assert_eq!(std::mem::size_of::<ValueWord>(), 8);
        assert_eq!(std::mem::align_of::<KeyWord>(), 8);
    }

    #[test]
    fn keys_inline_up_to_seven_bytes() {
        for len in 0..=INLINE_STR_MAX {
            let key = vec![b'k'; len];
            let w = KeyWord::new(&key);
            assert!(w.is_inline(), "{len}-byte key should inline");
            assert_eq!(w.heap_bytes(), 0);
            let mut buf: InlineBuf = [0; 16];
            assert_eq!(w.bytes(&mut buf), &key[..]);
        }
    }

    #[test]
    fn keys_over_seven_bytes_take_a_record() {
        for len in [8usize, 9, 28, 512] {
            let key = vec![b'k'; len];
            let w = KeyWord::new(&key);
            assert!(!w.is_inline(), "{len}-byte key should not inline");
            assert_eq!(w.heap_bytes(), crate::record::HEADER_BYTES + len);
            let mut buf: InlineBuf = [0; 16];
            assert_eq!(w.bytes(&mut buf), &key[..]);
            assert!(w.eq_bytes(&key));
            assert!(!w.eq_bytes(b"other"));
        }
    }

    #[test]
    fn inline_and_out_of_line_keys_compare_the_same_way() {
        assert!(KeyWord::new(b"short").eq_bytes(b"short"));
        assert!(!KeyWord::new(b"short").eq_bytes(b"shorter"));
        assert!(KeyWord::new(b"a-much-longer-key").eq_bytes(b"a-much-longer-key"));
        assert!(!KeyWord::new(b"a-much-longer-key").eq_bytes(b"a-much-longer-keY"));
        assert!(KeyWord::new(b"").eq_bytes(b""));
    }

    #[test]
    fn values_inline_integers_to_sixty_one_bits() {
        let widest = (1i64 << (INLINE_INT_BITS - 1)) - 1;
        for v in [0i64, 1, -1, 42, -42, widest, -widest - 1] {
            let w = ValueWord::from_int(v);
            assert!(w.is_inline(), "{v} should inline");
            let mut buf: InlineBuf = [0; 16];
            assert_eq!(w.decode(&mut buf), Decoded::Int(v));
        }
    }

    #[test]
    fn values_wider_than_sixty_one_bits_spill_to_a_record() {
        for v in [i64::MAX, i64::MIN, 1i64 << 61] {
            let w = ValueWord::from_int(v);
            assert!(!w.is_inline(), "{v} must not inline");
            let mut buf: InlineBuf = [0; 16];
            assert_eq!(w.decode(&mut buf), Decoded::Bytes(&v.to_le_bytes()));
        }
    }

    #[test]
    fn values_inline_byte_strings_up_to_seven_bytes() {
        for len in 0..=16 {
            let v = vec![b'v'; len];
            let w = ValueWord::from_bytes(&v);
            assert_eq!(w.is_inline(), len <= INLINE_STR_MAX, "len {len}");
            let mut buf: InlineBuf = [0; 16];
            assert_eq!(w.decode(&mut buf), Decoded::Bytes(&v));
        }
    }

    #[test]
    fn cloning_an_out_of_line_word_shares_one_record() {
        let a = ValueWord::from_bytes(b"a long enough value to need a record");
        let b = a.clone();
        assert_eq!(a.record().unwrap().ref_count(), 2);
        drop(b);
        assert_eq!(a.record().unwrap().ref_count(), 1);
    }

    /// The COW forcing test at the word level: a snapshot word keeps the old
    /// bytes while the table's word is rewritten in place.
    #[test]
    fn writing_through_a_shared_value_word_copies_first() {
        let mut live = ValueWord::from_bytes(b"the original payload");
        let snapshot = live.clone();

        live.record_mut().unwrap().make_mut()[4..12].copy_from_slice(b"REWRITTE");

        let mut buf: InlineBuf = [0; 16];
        let mut snap_buf: InlineBuf = [0; 16];
        assert_eq!(
            snapshot.decode(&mut snap_buf),
            Decoded::Bytes(b"the original payload")
        );
        assert_eq!(
            live.decode(&mut buf),
            Decoded::Bytes(b"the REWRITTE payload")
        );
        assert!(live.record().unwrap().is_unique());
        assert!(snapshot.record().unwrap().is_unique());
    }

    #[test]
    fn inline_words_have_no_record_to_share() {
        assert!(ValueWord::from_int(7).record().is_none());
        assert!(ValueWord::from_bytes(b"tiny").record().is_none());
    }
}
