//! A segmented extendible-hash keyspace table.
//!
//! This is the production form of the memory-architecture spike: a directory of
//! segments, each one allocator-size-class shaped, each holding buckets of tagged
//! 8-byte slot words. It exists to fix three things about a plain hash map of
//! `Bytes -> Entry`:
//!
//! - **Growth does not stall.** A directory of segments splits one segment at a
//!   time, so growth costs a fixed 16 KB of work instead of a whole-table rehash
//!   whose stall grows with the keyspace.
//! - **Small keys and values do not allocate.** A byte string up to 7 bytes and
//!   an integer up to 61 significant bits live in the slot word itself
//!   ([`word`]); anything larger takes a refcounted [`Record`], shared on clone
//!   and copied on write.
//! - **SCAN survives a split.** The cursor is advanced in reverse-binary order at
//!   the *scanned segment's* local depth, which is what gives Redis's
//!   exactly-once-for-stable-keys guarantee across splits.
//!
//! # Threading
//!
//! A table belongs to exactly one shard thread, per PRD R2/R3. That is enforced
//! by construction rather than by convention: [`Record`] is neither `Send` nor
//! `Sync`, so nothing holding one — a word, a slot, a bucket, a segment, a
//! table — can cross a thread boundary, and the refcount is free to be a plain
//! `u32` rather than an atomic.
//!
//! # Module order
//!
//! [`layout`] fixes the geometry, [`record`] and [`word`] the storage,
//! [`hasher`] the placement inputs, [`bucket`] and [`segment`] the placement
//! rules.

pub mod bucket;
pub mod hasher;
pub mod layout;
pub mod record;
pub mod segment;
pub mod word;

pub use bucket::{Bucket, Slot};
pub use hasher::{TableHasher, TableSeed};
pub use record::Record;
pub use segment::{Segment, SplitStats};
pub use word::{Decoded, InlineBuf, KeyWord, ValueWord};
