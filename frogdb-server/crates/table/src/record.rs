//! Out-of-line byte records — R6's heap half.
//!
//! One allocation per payload: an 8-byte `{len: u32, rc: u32}` header followed by
//! the bytes. Alignment is 8, so the low three bits of every record pointer are
//! zero and belong to the slot word's tag ([`crate::word`]).
//!
//! `rc` is a **non-atomic, same-core** refcount. Per PRD R2/R3 a table is owned by
//! exactly one shard thread and is never shared, so an atomic RMW on every clone
//! would buy nothing. That is enforced by construction rather than by convention:
//! [`Record`] holds a `*mut u8` and a `PhantomData<*mut u8>`, so it is neither
//! `Send` nor `Sync` and a record cannot cross a thread boundary at all.
//!
//! Copy-on-write is the point of the refcount. A snapshot or a replication feed
//! takes a [`Record::clone`] (an `rc` bump, no copy); a writer that wants to
//! mutate calls [`Record::make_mut`], which copies first when anyone else still
//! holds the record. The snapshot therefore keeps observing the bytes it was
//! taken from while the table moves on.

use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::marker::PhantomData;
use std::ptr::NonNull;

/// Bytes of header in front of every payload.
pub const HEADER_BYTES: usize = 8;

/// Alignment of a record, and therefore the number of tag bits a pointer to one
/// leaves free in a slot word.
pub const RECORD_ALIGN: usize = 8;

/// The header a record carries. `#[repr(C)]` so the field order is the one the
/// layout arithmetic above assumes.
#[repr(C)]
struct Header {
    len: u32,
    /// Non-atomic, same-core refcount. `1` for a freshly allocated record.
    rc: u32,
}

/// An owned handle to one refcounted byte record.
///
/// Dropping decrements; the record is freed when the last handle goes. Not
/// `Send`/`Sync` — see the module docs. That is the property the non-atomic `rc`
/// rests on, so it is asserted rather than assumed:
///
/// ```compile_fail
/// fn needs_send<T: Send>(_: T) {}
/// needs_send(frogdb_table::Record::new(b"nope"));
/// ```
///
/// `repr(transparent)` keeps a handle exactly one pointer wide, which is what
/// lets [`crate::word`] keep a record in a single 8-byte slot word. The word
/// stores the pointer's *address*, not the handle's bytes, and rebuilds a handle
/// from it with [`Record::from_raw`] — see that module for why the round trip
/// goes through exposed provenance.
#[repr(transparent)]
pub struct Record {
    ptr: NonNull<u8>,
    /// Pins `Record` to one thread, which is what makes the non-atomic `rc`
    /// sound rather than merely conventional.
    _not_send: PhantomData<*mut u8>,
}

impl Record {
    /// Allocates a record holding `bytes`, with `rc == 1`.
    pub fn new(bytes: &[u8]) -> Record {
        assert!(
            bytes.len() <= u32::MAX as usize,
            "record payload exceeds 4 GiB"
        );
        let layout = layout_for(bytes.len());
        // SAFETY: `layout_for` never produces a zero size (the header alone is
        // 8 bytes), which is `alloc`'s only precondition.
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            handle_alloc_error(layout);
        };
        // SAFETY: `raw` is a fresh allocation of `HEADER_BYTES + len` bytes,
        // aligned to 8, so both the header write and the payload copy are within
        // it and correctly aligned. The regions do not overlap `bytes`, which the
        // caller owns.
        unsafe {
            ptr.cast::<Header>().write(Header {
                len: bytes.len() as u32,
                rc: 1,
            });
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                ptr.as_ptr().add(HEADER_BYTES),
                bytes.len(),
            );
        }
        Record {
            ptr,
            _not_send: PhantomData,
        }
    }

    /// Rebuilds a handle from a raw record pointer, taking ownership of one
    /// reference.
    ///
    /// # Safety
    /// `ptr` must have come from [`Record::into_raw`] and the reference it
    /// represents must not have been reclaimed since.
    pub unsafe fn from_raw(ptr: NonNull<u8>) -> Record {
        Record {
            ptr,
            _not_send: PhantomData,
        }
    }

    /// Surrenders the handle's reference to a raw pointer, to be reclaimed later
    /// with [`Record::from_raw`]. Leaks if that never happens.
    pub fn into_raw(self) -> NonNull<u8> {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }

    /// The record's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: `self.ptr` is a live record, so its header is initialised and
        // `len` bytes of payload follow it. The borrow is tied to `&self`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(HEADER_BYTES), self.len()) }
    }

    /// Payload length in bytes.
    pub fn len(&self) -> usize {
        self.header().len as usize
    }

    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many handles currently share this record.
    pub fn ref_count(&self) -> u32 {
        self.header().rc
    }

    /// Whether this handle is the only owner, and so may be mutated in place.
    pub fn is_unique(&self) -> bool {
        self.ref_count() == 1
    }

    /// Bytes this record asks the allocator for, header included. The allocator
    /// rounds this up to a size class; the store seam charges the rounded figure.
    pub fn requested_bytes(&self) -> usize {
        HEADER_BYTES + self.len()
    }

    /// Mutable access to the payload, copying first if anyone else holds it.
    ///
    /// This is the write half of COW: after it returns, `self` is the sole owner
    /// and every other handle still observes the bytes as they were.
    pub fn make_mut(&mut self) -> &mut [u8] {
        if !self.is_unique() {
            *self = Record::new(self.as_bytes());
        }
        // SAFETY: `self` is now uniquely owned — either it always was, or the
        // line above replaced it with a fresh `rc == 1` record — so handing out a
        // `&mut` to its payload cannot alias another handle's `&[u8]`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(HEADER_BYTES), self.len()) }
    }

    fn header(&self) -> &Header {
        // SAFETY: a live record's first 8 bytes are its initialised header.
        unsafe { self.ptr.cast::<Header>().as_ref() }
    }

    fn header_mut(&mut self) -> &mut Header {
        // SAFETY: as `header`, and `&mut self` rules out a concurrent `&Header`.
        unsafe { self.ptr.cast::<Header>().as_mut() }
    }
}

impl Clone for Record {
    /// Bumps the refcount. No bytes are copied — this is the read half of COW.
    fn clone(&self) -> Record {
        let mut copy = Record {
            ptr: self.ptr,
            _not_send: PhantomData,
        };
        copy.header_mut().rc = copy
            .header()
            .rc
            .checked_add(1)
            .expect("record refcount overflowed 2^32 handles");
        copy
    }
}

impl Drop for Record {
    /// Mutation note: `rc > 1` survives being widened to `rc >= 1`. The widened
    /// form takes the decrement branch on the *last* handle too, writing `rc = 0`
    /// and returning without the `dealloc` — a pure leak. A leak changes no value
    /// this crate can read back and raises no signal any in-process test can
    /// assert on, so no unit test kills it; catching it needs an allocator-level
    /// checker (Miri, LeakSanitizer, heaptrack) rather than a test case.
    fn drop(&mut self) {
        let rc = self.header().rc;
        if rc > 1 {
            self.header_mut().rc = rc - 1;
            return;
        }
        let layout = layout_for(self.len());
        // SAFETY: this was the last handle, so no other reference to the
        // allocation exists; `layout` reproduces the one `Record::new` used
        // because it is a pure function of the (immutable) payload length.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
    }
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("len", &self.len())
            .field("rc", &self.ref_count())
            .finish()
    }
}

fn layout_for(len: usize) -> Layout {
    Layout::from_size_align(HEADER_BYTES + len, RECORD_ALIGN).expect("record layout")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_its_bytes() {
        let r = Record::new(b"frogdb");
        assert_eq!(r.as_bytes(), b"frogdb");
        assert_eq!(r.len(), 6);
        assert_eq!(r.requested_bytes(), 14);
        assert!(!r.is_empty());
        assert!(Record::new(b"").is_empty());
    }

    /// The allocation is a header *plus* a payload, and `Drop` reproduces the
    /// same layout from the same length to hand back to `dealloc`. Pinning the
    /// arithmetic here rather than only through `Record::new` keeps the free
    /// side honest: any other shape (a product, a payload with no header) would
    /// still round-trip within this module while handing the allocator a layout
    /// that does not match the one it gave out.
    #[test]
    fn the_layout_is_the_header_plus_the_payload() {
        assert_eq!(layout_for(0).size(), HEADER_BYTES);
        assert_eq!(layout_for(6).size(), HEADER_BYTES + 6);
        assert_eq!(layout_for(4096).size(), HEADER_BYTES + 4096);
        assert_eq!(layout_for(6).align(), RECORD_ALIGN);
    }

    #[test]
    fn empty_payload_is_legal() {
        let r = Record::new(b"");
        assert!(r.is_empty());
        assert_eq!(r.as_bytes(), b"");
    }

    #[test]
    fn record_pointers_are_eight_aligned_so_the_tag_bits_are_free() {
        for len in 0..40 {
            let r = Record::new(&vec![7u8; len]);
            let raw = r.into_raw();
            assert_eq!(raw.as_ptr() as usize % RECORD_ALIGN, 0, "len {len}");
            // SAFETY: `raw` came from `into_raw` on the line above and has not
            // been reclaimed.
            drop(unsafe { Record::from_raw(raw) });
        }
    }

    #[test]
    fn clone_shares_and_drop_releases() {
        let a = Record::new(b"shared");
        assert_eq!(a.ref_count(), 1);
        let b = a.clone();
        assert_eq!(a.ref_count(), 2);
        assert_eq!(b.ref_count(), 2);
        assert_eq!(a.as_bytes().as_ptr(), b.as_bytes().as_ptr());
        drop(b);
        assert_eq!(a.ref_count(), 1);
        assert!(a.is_unique());
    }

    #[test]
    fn make_mut_writes_in_place_when_unique() {
        let mut a = Record::new(b"abc");
        let before = a.as_bytes().as_ptr();
        a.make_mut()[0] = b'x';
        assert_eq!(a.as_bytes(), b"xbc");
        assert_eq!(a.as_bytes().as_ptr(), before, "unique record must not copy");
    }

    /// The COW forcing case: a snapshot holds the old bytes, the writer sees the
    /// new ones, and the two no longer share an allocation.
    #[test]
    fn make_mut_copies_away_from_a_snapshot() {
        let mut live = Record::new(b"v1-bytes");
        let snapshot = live.clone();

        live.make_mut().copy_from_slice(b"v2-bytes");

        assert_eq!(snapshot.as_bytes(), b"v1-bytes");
        assert_eq!(live.as_bytes(), b"v2-bytes");
        assert!(live.is_unique());
        assert!(snapshot.is_unique());
        assert_ne!(live.as_bytes().as_ptr(), snapshot.as_bytes().as_ptr());
    }
}
