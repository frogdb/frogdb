//! Tagged slot words — R5's "entry slots are tagged 8-byte words".
//!
//! Four encodings are swept, plus one hybrid. Every one of them is a `#[repr(C)]`
//! POD whose all-zero bit pattern is valid, so a segment can be `alloc_zeroed`.
//!
//! ```text
//! W8Ptr    8 B   control: always a pointer, nothing ever inlines
//! W8Int    8 B   integers inline (61-bit signed), everything else a pointer
//! W8       8 B   integers inline + byte strings up to  7 B inline
//! W16     16 B   integers inline (full 64-bit) + byte strings up to 15 B inline
//! ```
//!
//! Tag lives in the low three bits of byte 0. Payload records are 8-aligned
//! (see [`crate::heap`]), so a pointer word has tag `0b000` and needs no masking on
//! the lookup fast path — the word *is* the pointer.

use crate::heap;

pub const TAG_PTR: u8 = 0b000;
pub const TAG_INT: u8 = 0b001;
pub const TAG_STR: u8 = 0b010;

/// Scratch big enough for any inline payload.
pub type InlineBuf = [u8; 16];

/// What a live slot word holds.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

/// One tagged slot word — a key word or a value word.
///
/// # Safety
/// A word that reports `is_inline() == false` owns a [`heap`] payload. Every
/// non-inline word must be released with [`Word::free`] exactly once.
pub trait Word: Copy + Sized + 'static {
    /// Name used in report tables.
    const NAME: &'static str;
    /// Longest byte string that inlines, in bytes. `0` means no string inlining.
    const INLINE_STR_MAX: usize;
    /// Whether integers inline at all, and how many significant bits if so.
    const INLINE_INT_BITS: u32;

    fn encode_bytes(b: &[u8]) -> Self;
    fn encode_int(v: i64) -> Self;
    fn is_inline(&self) -> bool;

    /// # Safety
    /// The word must be live (its slot's occupied bit set).
    unsafe fn decode<'a>(&'a self, buf: &'a mut InlineBuf) -> Decoded<'a>;

    /// # Safety
    /// The word must be live.
    unsafe fn eq_bytes(&self, other: &[u8]) -> bool;

    /// # Safety
    /// The word must be live, and must not be used again afterwards.
    unsafe fn free(&mut self);
}

// ---------------------------------------------------------------------------
// 8-byte words
// ---------------------------------------------------------------------------

/// The 8-byte word body shared by `W8`, `W8Int` and `W8Ptr`; the three differ only
/// in which encodings they are *allowed* to produce.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Raw8(u64);

impl Raw8 {
    #[inline]
    fn tag(self) -> u8 {
        (self.0 & 0b111) as u8
    }

    #[inline]
    fn ptr(self) -> *mut u8 {
        self.0 as usize as *mut u8
    }

    #[inline]
    fn from_ptr(p: *mut u8) -> Self {
        debug_assert_eq!(p as usize & 0b111, 0, "payload must be 8-aligned");
        Raw8(p as usize as u64)
    }

    /// Inline string: byte 0 = tag | len<<3, bytes 1..=7 = payload.
    #[inline]
    fn from_str(b: &[u8]) -> Self {
        debug_assert!(b.len() <= 7);
        let mut w = [0u8; 8];
        w[0] = TAG_STR | ((b.len() as u8) << 3);
        w[1..1 + b.len()].copy_from_slice(b);
        Raw8(u64::from_le_bytes(w))
    }

    #[inline]
    fn from_int(v: i64) -> Self {
        Raw8(((v << 3) as u64) | TAG_INT as u64)
    }

    #[inline]
    fn fits_int(v: i64) -> bool {
        (v << 3) >> 3 == v
    }

    #[inline]
    unsafe fn decode<'a>(&'a self, buf: &'a mut InlineBuf) -> Decoded<'a> {
        match self.tag() {
            TAG_INT => Decoded::Int((self.0 as i64) >> 3),
            TAG_STR => {
                let bytes = self.0.to_le_bytes();
                let len = ((bytes[0] >> 3) & 0x0f) as usize;
                buf[..len].copy_from_slice(&bytes[1..1 + len]);
                Decoded::Bytes(&buf[..len])
            }
            _ => Decoded::Bytes(heap::payload(self.ptr())),
        }
    }

    #[inline]
    unsafe fn eq_bytes(&self, other: &[u8]) -> bool {
        match self.tag() {
            TAG_INT => false,
            TAG_STR => {
                let bytes = self.0.to_le_bytes();
                let len = ((bytes[0] >> 3) & 0x0f) as usize;
                len == other.len() && bytes[1..1 + len] == *other
            }
            _ => heap::payload(self.ptr()) == other,
        }
    }

    #[inline]
    unsafe fn free(&mut self) {
        if self.tag() == TAG_PTR {
            heap::free(self.ptr());
        }
    }
}

macro_rules! word8 {
    ($name:ident, $label:literal, $str_max:expr, $int_bits:expr, $enc_str:expr, $enc_int:expr) => {
        #[derive(Clone, Copy)]
        #[repr(transparent)]
        pub struct $name(Raw8);

        impl Word for $name {
            const NAME: &'static str = $label;
            const INLINE_STR_MAX: usize = $str_max;
            const INLINE_INT_BITS: u32 = $int_bits;

            #[inline]
            fn encode_bytes(b: &[u8]) -> Self {
                let inline: bool = $enc_str && b.len() <= $str_max;
                if inline {
                    $name(Raw8::from_str(b))
                } else {
                    $name(Raw8::from_ptr(heap::alloc_payload(b)))
                }
            }

            #[inline]
            fn encode_int(v: i64) -> Self {
                let inline: bool = $enc_int && Raw8::fits_int(v);
                if inline {
                    $name(Raw8::from_int(v))
                } else {
                    // A word that cannot inline integers stores the value the way
                    // R5's control does: an out-of-line 8-byte value record.
                    $name(Raw8::from_ptr(heap::alloc_payload(&v.to_le_bytes())))
                }
            }

            #[inline]
            fn is_inline(&self) -> bool {
                self.0.tag() != TAG_PTR
            }

            #[inline]
            unsafe fn decode<'a>(&'a self, buf: &'a mut InlineBuf) -> Decoded<'a> {
                self.0.decode(buf)
            }

            #[inline]
            unsafe fn eq_bytes(&self, other: &[u8]) -> bool {
                self.0.eq_bytes(other)
            }

            #[inline]
            unsafe fn free(&mut self) {
                self.0.free()
            }
        }
    };
}

word8!(W8Ptr, "ptr8", 0, 0, false, false);
word8!(W8Int, "int8", 0, 61, false, true);
word8!(W8, "str7", 7, 61, true, true);

// ---------------------------------------------------------------------------
// 16-byte word
// ---------------------------------------------------------------------------

/// Wide slot word: byte 0 is `tag | len<<3`, bytes 1..=15 are the inline string,
/// and bytes 8..16 hold the pointer or the full 64-bit integer when not a string.
#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct W16([u8; 16]);

impl W16 {
    #[inline]
    fn tag(&self) -> u8 {
        self.0[0] & 0b111
    }

    #[inline]
    fn tail(&self) -> u64 {
        u64::from_le_bytes(self.0[8..16].try_into().unwrap())
    }

    #[inline]
    fn inline_len(&self) -> usize {
        ((self.0[0] >> 3) & 0x0f) as usize
    }
}

impl Word for W16 {
    const NAME: &'static str = "str15w";
    const INLINE_STR_MAX: usize = 15;
    const INLINE_INT_BITS: u32 = 64;

    #[inline]
    fn encode_bytes(b: &[u8]) -> Self {
        let mut w = [0u8; 16];
        if b.len() <= Self::INLINE_STR_MAX {
            w[0] = TAG_STR | ((b.len() as u8) << 3);
            w[1..1 + b.len()].copy_from_slice(b);
        } else {
            let p = heap::alloc_payload(b);
            w[0] = TAG_PTR;
            w[8..16].copy_from_slice(&(p as usize as u64).to_le_bytes());
        }
        W16(w)
    }

    #[inline]
    fn encode_int(v: i64) -> Self {
        let mut w = [0u8; 16];
        w[0] = TAG_INT;
        w[8..16].copy_from_slice(&v.to_le_bytes());
        W16(w)
    }

    #[inline]
    fn is_inline(&self) -> bool {
        self.tag() != TAG_PTR
    }

    #[inline]
    unsafe fn decode<'a>(&'a self, buf: &'a mut InlineBuf) -> Decoded<'a> {
        match self.tag() {
            TAG_INT => Decoded::Int(self.tail() as i64),
            TAG_STR => {
                let len = self.inline_len();
                buf[..len].copy_from_slice(&self.0[1..1 + len]);
                Decoded::Bytes(&buf[..len])
            }
            _ => Decoded::Bytes(heap::payload(self.tail() as usize as *mut u8)),
        }
    }

    #[inline]
    unsafe fn eq_bytes(&self, other: &[u8]) -> bool {
        match self.tag() {
            TAG_INT => false,
            TAG_STR => {
                let len = self.inline_len();
                len == other.len() && self.0[1..1 + len] == *other
            }
            _ => heap::payload(self.tail() as usize as *mut u8) == other,
        }
    }

    #[inline]
    unsafe fn free(&mut self) {
        if self.tag() == TAG_PTR {
            heap::free(self.tail() as usize as *mut u8);
        }
    }
}

/// Bytes an out-of-line payload of `len` asks the allocator for — reported alongside
/// the measured jemalloc numbers so the report can separate model from measurement.
pub fn out_of_line_request(len: usize) -> usize {
    heap::requested_bytes(len)
}
