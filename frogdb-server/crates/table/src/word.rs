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
//! tag: a pointer word **is** the pointer's address and the lookup fast path does
//! no masking. The word stores that address through
//! [`expose_provenance`](std::ptr::expose_provenance) and rebuilds a handle from
//! it with [`with_exposed_provenance_mut`](std::ptr::with_exposed_provenance_mut),
//! which is what keeps a tagged-integer pointer dereferenceable under Miri and
//! the strict-provenance rules it models.
//!
//! Two word types, because keys and values are not the same question. [`KeyWord`]
//! carries byte strings only (a Redis key is bytes). [`ValueWord`] carries byte
//! strings or integers, and is what makes R6's "inline small values" real. Both
//! own their record and free it on drop, so a slot is dropped by dropping its
//! fields — there is no manual free path to get wrong.

use std::mem::ManuallyDrop;
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
            unsafe { record_bytes(&self.0) }
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
            unsafe { record_bytes(&self.0) == other }
        }
    }

    /// Heap bytes this word owns, allocator header included; `0` when inline.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        if self.is_inline() {
            0
        } else {
            // SAFETY: as `bytes`.
            unsafe { record_handle(self.0) }.requested_bytes()
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
            let record = unsafe { record_handle(self.0) };
            KeyWord(pack_record(Record::clone(&record)))
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
            _ => Decoded::Bytes(unsafe { record_bytes(&self.0) }),
        }
    }

    /// Heap bytes this word owns, allocator header included; `0` when inline.
    #[inline]
    pub fn heap_bytes(&self) -> usize {
        if self.is_inline() {
            0
        } else {
            // SAFETY: as `decode`.
            unsafe { record_handle(self.0) }.requested_bytes()
        }
    }

    /// Runs `f` on the record behind an out-of-line value — the COW hook: `f`
    /// clones the handle to hand a snapshot the current bytes, or `make_mut`s it
    /// to write. `None` when the value is inline.
    ///
    /// A closure rather than a `&mut Record`, because the handle lives in the
    /// word as a bare address: it has to be rebuilt for the call and written back
    /// afterwards, since `make_mut` may move the record.
    #[inline]
    pub fn with_record_mut<R>(&mut self, f: impl FnOnce(&mut Record) -> R) -> Option<R> {
        if self.is_inline() {
            return None;
        }
        // SAFETY: the tag says `PTR`, so the word is a live record pointer this
        // word owns, and `&mut self` rules out any other handle to it here.
        let mut record = unsafe { record_handle(self.0) };
        let out = f(&mut record);
        // The word still owns the reference the handle was rebuilt from, but
        // `make_mut` may have replaced the record with a private copy, so the word
        // follows the handle rather than keeping a stale address.
        self.0 = pack_record(ManuallyDrop::into_inner(record));
        Some(out)
    }

    /// Runs `f` on the record behind an out-of-line value, shared. `None` when
    /// the value is inline.
    #[inline]
    pub fn with_record<R>(&self, f: impl FnOnce(&Record) -> R) -> Option<R> {
        if self.is_inline() {
            return None;
        }
        // SAFETY: as `with_record_mut`, with a shared borrow.
        let record = unsafe { record_handle(self.0) };
        Some(f(&record))
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
            let record = unsafe { record_handle(self.0) };
            ValueWord(pack_record(Record::clone(&record)))
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
    // `expose_provenance`, not a plain `as` cast: the word is an integer, and a
    // pointer rebuilt from an integer is only dereferenceable if the address it
    // came from was exposed. Skipping this is undefined behaviour that no test
    // can see and Miri catches immediately.
    let ptr = record.into_raw().as_ptr().expose_provenance() as u64;
    debug_assert_eq!(ptr & TAG_MASK, TAG_PTR, "record pointer must be 8-aligned");
    ptr
}

/// The pointer a `PTR` word holds, with the provenance [`pack_record`] exposed.
#[inline]
fn record_ptr(word: u64) -> NonNull<u8> {
    debug_assert_eq!(word & TAG_MASK, TAG_PTR);
    NonNull::new(std::ptr::with_exposed_provenance_mut::<u8>(word as usize))
        .expect("record pointer is never null")
}

/// Rebuilds the handle a `PTR` word holds **without** taking its reference: the
/// word still owns it, so the handle is [`ManuallyDrop`] and must not escape.
///
/// # Safety
/// `word`'s tag must be `PTR` and its record must still be live.
#[inline]
unsafe fn record_handle(word: u64) -> ManuallyDrop<Record> {
    // SAFETY: the caller guarantees the tag and liveness, and `pack_record`
    // exposed this address, so the pointer is dereferenceable. The handle is
    // `ManuallyDrop` because dropping it would release the word's reference.
    ManuallyDrop::new(unsafe { Record::from_raw(record_ptr(word)) })
}

/// A record word's payload, borrowed for as long as the word that owns it.
///
/// # Safety
/// As [`record_handle`].
#[inline]
unsafe fn record_bytes(word: &u64) -> &[u8] {
    // SAFETY: the caller guarantees the tag and liveness.
    let record = unsafe { record_handle(*word) };
    let bytes = record.as_bytes();
    // SAFETY: the payload lives in the record's allocation, which the word owns a
    // reference to and so outlives the word's borrow. Only the handle is
    // temporary, so the slice is re-formed to borrow from the word instead of it.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr(), bytes.len()) }
}

/// # Safety
/// If `word`'s tag is `PTR`, it must own exactly one live reference to a record,
/// and the caller must not use `word` afterwards.
#[inline]
unsafe fn drop_record(word: u64) {
    if word & TAG_MASK != TAG_PTR {
        return;
    }
    // SAFETY: the word owns the reference `pack_record` put there; rebuilding
    // the handle and dropping it releases exactly that one reference.
    drop(unsafe { Record::from_raw(record_ptr(word)) });
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
        assert_eq!(a.with_record(Record::ref_count), Some(2));
        drop(b);
        assert_eq!(a.with_record(Record::ref_count), Some(1));
    }

    /// The COW forcing test at the word level: a snapshot word keeps the old
    /// bytes while the table's word is rewritten in place.
    #[test]
    fn writing_through_a_shared_value_word_copies_first() {
        let mut live = ValueWord::from_bytes(b"the original payload");
        let snapshot = live.clone();

        live.with_record_mut(|r| r.make_mut()[4..12].copy_from_slice(b"REWRITTE"))
            .expect("the value is out of line");

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
        assert_eq!(live.with_record(Record::is_unique), Some(true));
        assert_eq!(snapshot.with_record(Record::is_unique), Some(true));
    }

    #[test]
    fn inline_words_have_no_record_to_share() {
        assert_eq!(ValueWord::from_int(7).with_record(Record::ref_count), None);
        assert_eq!(
            ValueWord::from_bytes(b"tiny").with_record(Record::ref_count),
            None
        );
    }
}
