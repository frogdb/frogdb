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
//! A table belongs to exactly one shard thread, per PRD R2/R3, and its records
//! are refcounted with a plain `u32` rather than an atomic. What makes that
//! sound is the type system, not convention.
//!
//! [`KeyWord`] and [`ValueWord`] each carry a `PhantomData<*mut u8>`, so both are
//! `!Send` and `!Sync` — and so is everything built out of them: [`Slot`],
//! [`Bucket`], [`Segment`], [`Table`]. The marker is on the *word* rather than
//! on [`Record`], because a word holds an address, and an address is `Send`
//! however the thing it points at is declared; putting the bound on `Record`
//! alone (as an earlier version of this crate did) left `Table` `Send + Sync`
//! and safe code able to race the refcount.
//!
//! A whole table is the one thing that may move, through an explicit
//! `unsafe impl<V: Send> Send for Table<V, N>`: the table and every record it
//! owns move together, so after the move exactly one thread can still reach any
//! of those refcounts. There is deliberately no `Sync` — a shared `&Table` would
//! let two threads clone the same record at once — and no blanket `Send`, since
//! `V` is the half of a slot a caller can take out by value.
//!
//! The `compile_fail` doctests on [`Record`], [`KeyWord`], [`ValueWord`] and
//! [`Table`] are what keep that from rotting.
//!
//! # Module order
//!
//! [`layout`] fixes the geometry, [`record`] and [`word`] the storage,
//! [`hasher`] the placement inputs, [`bucket`] and [`segment`] the placement
//! rules, and [`table`] the directory and the cursor over it.

pub mod bucket;
pub mod hasher;
pub mod layout;
pub mod record;
pub mod segment;
pub mod table;
pub mod word;

pub use bucket::{Bucket, Slot};
pub use hasher::{TableHasher, TableSeed};
pub use record::Record;
pub use segment::{Segment, SplitStats};
pub use table::{Table, TableStats};
pub use word::{Decoded, InlineBuf, KeyWord, ValueWord};
