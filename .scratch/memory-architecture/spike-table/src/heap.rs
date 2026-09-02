//! Out-of-line payload records — the thing a non-inline slot word points at.
//!
//! Models R6's heap-value shape: one allocation per payload, an 8-byte header
//! carrying the length and a **non-atomic** (same-core) refcount, then the bytes.
//! Alignment is 8, so the low three bits of every payload pointer are zero and are
//! free for the slot word's tag.
//!
//! Spike shortcut: the refcount is stored but never used — COW/refcount semantics
//! are R6's heap half, drafted at issue-11 time. It is present so the header size
//! (and therefore every bytes/entry number) is the production one.

use std::alloc::{alloc, dealloc, Layout};

/// Bytes of header in front of every out-of-line payload.
pub const HEADER_BYTES: usize = 8;

#[repr(C)]
struct Header {
    len: u32,
    /// R6 non-atomic, same-core refcount. Written once by the spike.
    rc: u32,
}

fn layout_for(len: usize) -> Layout {
    Layout::from_size_align(HEADER_BYTES + len, 8).expect("payload layout")
}

/// Allocates a payload record holding `bytes` and returns a thin, 8-aligned pointer.
///
/// # Safety
/// The caller owns the returned pointer and must release it with [`free`] exactly once.
pub fn alloc_payload(bytes: &[u8]) -> *mut u8 {
    assert!(bytes.len() <= u32::MAX as usize);
    unsafe {
        let p = alloc(layout_for(bytes.len()));
        assert!(!p.is_null(), "payload allocation failed");
        (p as *mut Header).write(Header {
            len: bytes.len() as u32,
            rc: 1,
        });
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), p.add(HEADER_BYTES), bytes.len());
        p
    }
}

/// # Safety
/// `p` must come from [`alloc_payload`] and still be live.
pub unsafe fn payload<'a>(p: *mut u8) -> &'a [u8] {
    let len = (*(p as *const Header)).len as usize;
    std::slice::from_raw_parts(p.add(HEADER_BYTES), len)
}

/// # Safety
/// `p` must come from [`alloc_payload`] and not have been freed.
pub unsafe fn free(p: *mut u8) {
    let len = (*(p as *const Header)).len as usize;
    dealloc(p, layout_for(len));
}

/// Requested bytes for a payload of `len` — what the allocator is asked for, before
/// size-class rounding. The measured numbers come from jemalloc, not from this.
pub fn requested_bytes(len: usize) -> usize {
    HEADER_BYTES + len
}
