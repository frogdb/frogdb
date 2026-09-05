//! The map the keyspace itself lives in, behind one seam.
//!
//! [`HashMapStore`](super::HashMapStore) is ~3 000 lines of expiry, eviction,
//! keysize and warm-tier logic sitting on top of one container. That container
//! is the thing the memory-architecture work replaces, and everything else in
//! the file is indifferent to which one it is — so it goes behind a trait with
//! exactly the operations the store performs, and the store names the chosen
//! one through a type alias.
//!
//! Two backends:
//!
//! - [`GriddleKeyspace`] — `griddle::HashMap<Bytes, Entry>`, the incumbent.
//!   Default, and unchanged in behaviour: its SCAN is the same content-hash
//!   ordering the store has always used.
//! - [`TableKeyspace`] — the segmented table (`frogdb-table`), behind the
//!   `table-keyspace` cargo feature. Its SCAN is Redis's real reverse-binary
//!   cursor over the directory, which is the point of the exercise.
//!
//! # Choosing one
//!
//! [`Selected`] is the alias the store uses. Flipping the default is one line
//! (see the `cfg` at the bottom of this file), gated on the lookup measurement
//! recorded in the issue-11 report — not on this code compiling.
//!
//! # Why a callback and not an iterator
//!
//! The table stores keys as tagged 8-byte words: a key of 7 bytes or fewer
//! lives *in* the word, and reading it needs a caller-supplied scratch buffer
//! ([`frogdb_table::InlineBuf`]). An iterator would have to hand out a
//! reference into a buffer it owns, which is a lending iterator and not
//! expressible. A visitor closure borrows the scratch for exactly the length of
//! the call, so both backends can hand out a plain `&[u8]`.

use std::ops::ControlFlow;

use bytes::Bytes;

use super::hashmap::Entry;

/// A key as the backend holds it.
///
/// Griddle owns a [`Bytes`] per key and can clone it for the price of a
/// refcount bump; the table holds a packed word and has to materialise the
/// bytes. Both hand out `&[u8]` for free, so callers that only look at a key
/// (glob match, slot number, length) pay nothing either way, and only the
/// callers that keep one ([`Keyspace::scan`]'s results, `all_keys`) pay the
/// backend's real cost.
pub(super) enum KeyRef<'a> {
    /// Griddle's stored `Bytes`, cloneable for a refcount bump. Only the
    /// griddle backend produces one.
    #[cfg_attr(feature = "table-keyspace", allow(dead_code))]
    Shared(&'a Bytes),
    /// Bytes decoded from a table slot, valid for the visit only. Only the
    /// segmented-table backend produces one.
    #[cfg_attr(not(feature = "table-keyspace"), allow(dead_code))]
    Borrowed(&'a [u8]),
}

impl KeyRef<'_> {
    /// The key's bytes. Free for both backends.
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            KeyRef::Shared(b) => b,
            KeyRef::Borrowed(b) => b,
        }
    }

    /// An owned handle on the key: a refcount bump on griddle, a copy on the
    /// table.
    pub(super) fn to_bytes(&self) -> Bytes {
        match self {
            KeyRef::Shared(b) => (*b).clone(),
            KeyRef::Borrowed(b) => Bytes::copy_from_slice(b),
        }
    }
}

impl std::ops::Deref for KeyRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// The operations [`HashMapStore`](super::HashMapStore) performs on its
/// keyspace, and nothing else.
///
/// Deliberately concrete in [`Entry`] rather than generic over a value type:
/// there is one keyspace in this server and one entry in it, and a generic
/// parameter would buy nothing but a type argument at every mention.
pub(super) trait Keyspace {
    fn new() -> Self;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn contains_key(&self, key: &[u8]) -> bool;

    fn get(&self, key: &[u8]) -> Option<&Entry>;

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry>;

    /// Inserts, returning the entry that was displaced.
    fn insert(&mut self, key: Bytes, entry: Entry) -> Option<Entry>;

    fn remove(&mut self, key: &[u8]) -> Option<Entry>;

    fn clear(&mut self);

    /// Visits every entry, in whatever order the backend holds them, until the
    /// visitor breaks. Used by the audit and recompute paths, which want the
    /// whole keyspace and do not care about order.
    fn visit(&self, f: impl FnMut(KeyRef<'_>, &Entry) -> ControlFlow<()>);

    /// One SCAN step: feeds the visitor every unexpired key the step covers and
    /// returns the cursor to resume from, or 0 when the walk is complete.
    ///
    /// `count` is a hint with a permitted range, not a limit. A backend may
    /// bound **either** the keys it examines **or** the keys the visitor keeps
    /// (the visitor applies MATCH/TYPE and returns whether it kept the key), and
    /// may overshoot whichever it bounds in order to finish a unit it can only
    /// walk atomically. Redis permits all of that; what a caller may rely on is
    /// the cursor, not the reply size.
    ///
    /// Concretely, of the two backends here: `GriddleKeyspace` bounds keys
    /// *kept*, which is the store's long-standing behaviour and is preserved
    /// rather than quietly changed by the seam; `TableKeyspace` bounds keys
    /// *examined*, because a step emits whole segments and so overshoots by up
    /// to one segment. (Only the selected backend is compiled, which is why
    /// neither is a doc link.)
    ///
    /// Each backend owns its own cursor encoding. The only contract they share
    /// is Redis's: a key present for the whole walk is returned at least once.
    fn scan(&self, cursor: u64, count: usize, visit: impl FnMut(KeyRef<'_>, &Entry) -> bool)
    -> u64;

    /// Nominates up to `want` keys the backend considers coldest, taking only
    /// entries `accept` passes — or `None` when the backend has no cold
    /// ordering and the store should fall back to sampling.
    ///
    /// The split of responsibility is deliberate. The backend owns *which
    /// entries are coldest*, which is a property of how it stores them; the
    /// store owns *which entries the policy may take*, which is a property of
    /// `maxmemory-policy` and of the entry's own metadata. Neither knows the
    /// other's half, so `volatile-*` confinement is a closure rather than a
    /// flag threaded through the container.
    ///
    /// Nominating is not removing: the caller deletes what it is handed,
    /// because a deletion here is also a keyspace notification, an eviction
    /// metric, a WAL tombstone and a replicated `DEL`. `Some(vec![])` means the
    /// backend has a cold ordering and nothing in it the policy may take —
    /// which is an OOM verdict, not a cue to ask again.
    ///
    /// `epoch` is a coarse tick from the clock seam. Backends that age their
    /// ordering use it; ones that do not ignore it. It is a parameter rather
    /// than a clock read so the container never has a clock of its own.
    ///
    /// `volatile_only` is the one axis `accept` varies on that a backend may
    /// need to *remember* — a closure is opaque, so a backend caching "this
    /// walk found nothing" cannot otherwise tell whether the next caller is
    /// asking the same question. It is a hint for that cache and nothing else:
    /// confinement is still applied by `accept`, per entry.
    fn cold_candidates(
        &mut self,
        want: usize,
        epoch: u16,
        volatile_only: bool,
        accept: impl Fn(&Entry) -> bool,
    ) -> Option<Vec<Bytes>>;
}

// Each backend is compiled only when it is the selected one: the unselected
// one would be dead code, and a warning nobody can act on is worse than not
// building it.
#[cfg(not(feature = "table-keyspace"))]
mod griddle_keyspace;
#[cfg(not(feature = "table-keyspace"))]
pub(super) use griddle_keyspace::GriddleKeyspace;

#[cfg(feature = "table-keyspace")]
mod table_keyspace;
#[cfg(feature = "table-keyspace")]
pub(super) use table_keyspace::TableKeyspace;

// The one line the swap turns on. The default is griddle until the lookup gate
// in `.scratch/memory-architecture/` is met on production-shaped hardware;
// building with `--features table-keyspace` runs the whole server on the
// segmented table today.
#[cfg(not(feature = "table-keyspace"))]
pub(super) type Selected = GriddleKeyspace;
#[cfg(feature = "table-keyspace")]
pub(super) type Selected = TableKeyspace;
