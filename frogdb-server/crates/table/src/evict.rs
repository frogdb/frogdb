//! 2Q over *segments*: the queues, and the rule that picks the next victim.
//!
//! PRD R9 rules segment-granularity eviction with no per-key LRU field. The
//! whole of the eviction state therefore lives in the 64-byte segment header —
//! `q_state`, `q_prev`/`q_next`, `hits`, `misses`, `last_touch`,
//! `victim_cursor` — 22 bytes for a segment holding some 700 live entries,
//! against the 4–16 bytes *per key* any per-key scheme would need. The price is
//! that recency is only known to the granularity of a segment; that coarseness
//! is R9's explicit trade, not an accident of this implementation.
//!
//! # The three queues
//!
//! Each is an intrusive doubly-linked list threaded through the headers by
//! segment *index* — never a pointer, because [`crate::Table`]'s segment vector
//! reallocates as the table grows and the links must not care.
//!
//! - **A1in** — segments that have not proven themselves. A fresh segment
//!   enters here, and so does the new half of a split (immediately behind its
//!   parent: it holds half the parent's entries but has no history of its own).
//!   Victims are taken from this tail first.
//! - **Am** — segments 2Q has seen referenced. A1in tail promotes here when its
//!   counters say the lookups landing on it are productive; an Am segment at the
//!   tail that is still being hit gets a second chance and returns to the head.
//! - **A1out** — the ghost queue. A segment eviction has emptied: membership
//!   only, no evictable content, so victim selection skips it in O(1) instead of
//!   walking it every pass. It leaves the moment an insert lands in it, exactly
//!   as 2Q re-admits a ghost on reference. The classic "a hit on a ghost proves
//!   the eviction was premature" promotion has no analogue here and is not
//!   implemented: a segment's identity is its slots, so an empty one cannot be
//!   hit — only missed through.
//!
//! # Why the counters, and not a timestamp
//!
//! The read path holds `&self` (see [`crate::Table::get`]), so all it can do is
//! bump a `Cell`. Queue maintenance needs `&mut`, and the only caller that has
//! it is eviction — which is also the only caller that needs the answer. So a
//! lookup costs one non-atomic increment and nothing else, and the queues are
//! reconciled lazily, from the counters, at the moment a victim is chosen.
//!
//! `misses` earns its place there: a segment that is probed constantly and
//! answers rarely is not hot data, it is a segment other keys route through.
//! Promotion requires `hits > misses` as well as a hit floor.
//!
//! # Termination
//!
//! Selection walks a queue from the tail, and a segment it moves is stamped with
//! the caller's epoch. A segment whose `last_touch` already equals the current
//! epoch gets no further chance in that epoch, so within one selection each
//! segment can be moved at most once and the walk is bounded by the segment
//! count. [`crate::Table::cold_candidates`] bounds its own retry loop the same
//! way. Nothing here can spin: it nominates a candidate or reports that it has
//! none, which is what turns a full keyspace into an OOM verdict rather than a
//! livelock.

use crate::segment::Segment;

/// The segment index standing for "no link".
///
/// Safe as a sentinel because [`crate::Table`] already refuses to grow past
/// `u32::MAX` segments (`expect("more than 4 G segments")` in `Table::split`),
/// so no live segment can ever carry this index.
pub(crate) const NIL: u32 = u32::MAX;

/// Hits an A1in segment needs before 2Q will call it hot.
///
/// Two, which is 2Q's own "a second reference promotes" rule. A single hit is
/// what any segment gets the moment it holds one live key; the second one is
/// the first evidence of re-use.
pub(crate) const PROMOTE_HITS: u32 = 2;

/// Which queue a segment is linked into.
///
/// The discriminants are the values stored in `SegmentHeader::q_state`, and `0`
/// is deliberately not one of them: a segment allocation is zeroed, so a header
/// that has never been linked reads back as "in no queue".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueId {
    /// Unproven segments. Victims come from here first.
    A1in = 1,
    /// Ghosts: segments eviction emptied.
    A1out = 2,
    /// Segments 2Q has seen referenced productively.
    Am = 3,
}

/// The `q_state` byte of a segment in no queue.
pub(crate) const QUEUE_NONE: u8 = 0;

impl QueueId {
    /// The queue a `q_state` byte names, or `None` for [`QUEUE_NONE`].
    pub(crate) fn from_state(state: u8) -> Option<QueueId> {
        match state {
            1 => Some(QueueId::A1in),
            2 => Some(QueueId::A1out),
            3 => Some(QueueId::Am),
            _ => None,
        }
    }

    fn slot(self) -> usize {
        self as usize - 1
    }
}

/// The head and tail of one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ends {
    head: u32,
    tail: u32,
}

impl Ends {
    const EMPTY: Ends = Ends {
        head: NIL,
        tail: NIL,
    };
}

/// The three queue endpoints. The links themselves live in the headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Queues {
    ends: [Ends; 3],
}

impl Queues {
    pub(crate) fn new() -> Queues {
        Queues {
            ends: [Ends::EMPTY; 3],
        }
    }

    /// The coldest segment in `q`, or `None` when it is empty.
    pub(crate) fn tail(&self, q: QueueId) -> Option<u32> {
        match self.ends[q.slot()].tail {
            NIL => None,
            i => Some(i),
        }
    }

    /// Links `i` at the head of `q`. `i` must not already be linked.
    pub(crate) fn push_head<V, const N: usize>(
        &mut self,
        segs: &mut [Box<Segment<V, N>>],
        i: u32,
        q: QueueId,
    ) {
        debug_assert_eq!(segs[i as usize].q_state(), QUEUE_NONE, "already linked");
        let old_head = self.ends[q.slot()].head;
        segs[i as usize].set_links(NIL, old_head);
        segs[i as usize].set_q_state(q as u8);
        if old_head == NIL {
            self.ends[q.slot()].tail = i;
        } else {
            segs[old_head as usize].set_q_prev(i);
        }
        self.ends[q.slot()].head = i;
    }

    /// Links `i` immediately behind `after`, in `after`'s own queue — the
    /// position a split's new half takes, one step colder than the parent it
    /// was carved out of.
    pub(crate) fn insert_after<V, const N: usize>(
        &mut self,
        segs: &mut [Box<Segment<V, N>>],
        after: u32,
        i: u32,
    ) {
        let Some(q) = QueueId::from_state(segs[after as usize].q_state()) else {
            debug_assert!(false, "split parent {after} is in no queue");
            return self.push_head(segs, i, QueueId::A1in);
        };
        debug_assert_eq!(segs[i as usize].q_state(), QUEUE_NONE, "already linked");
        let next = segs[after as usize].q_next();
        segs[i as usize].set_links(after, next);
        segs[i as usize].set_q_state(q as u8);
        segs[after as usize].set_q_next(i);
        if next == NIL {
            self.ends[q.slot()].tail = i;
        } else {
            segs[next as usize].set_q_prev(i);
        }
    }

    /// Unlinks `i` from whichever queue holds it. A no-op if it is in none.
    pub(crate) fn unlink<V, const N: usize>(
        &mut self,
        segs: &mut [Box<Segment<V, N>>],
        i: u32,
    ) -> Option<QueueId> {
        let q = QueueId::from_state(segs[i as usize].q_state())?;
        let (prev, next) = (segs[i as usize].q_prev(), segs[i as usize].q_next());
        if prev == NIL {
            self.ends[q.slot()].head = next;
        } else {
            segs[prev as usize].set_q_next(next);
        }
        if next == NIL {
            self.ends[q.slot()].tail = prev;
        } else {
            segs[next as usize].set_q_prev(prev);
        }
        segs[i as usize].set_links(NIL, NIL);
        segs[i as usize].set_q_state(QUEUE_NONE);
        Some(q)
    }

    /// Moves `i` to the head of `q`, wherever it is now.
    pub(crate) fn move_to_head<V, const N: usize>(
        &mut self,
        segs: &mut [Box<Segment<V, N>>],
        i: u32,
        q: QueueId,
    ) {
        self.unlink(segs, i);
        self.push_head(segs, i, q);
    }

    /// The segments in `q`, head (hottest) first.
    ///
    /// Introspection for the invariant tests, O(queue length). Nothing on a hot
    /// path walks a queue, so this is deliberately test-only rather than a
    /// method the table is tempted to call.
    #[cfg(test)]
    pub(crate) fn members<V, const N: usize>(
        &self,
        segs: &[Box<Segment<V, N>>],
        q: QueueId,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        let mut i = self.ends[q.slot()].head;
        while i != NIL {
            out.push(i);
            i = segs[i as usize].q_next();
        }
        out
    }

    /// The same walk from the tail, so a test can prove the two directions
    /// agree rather than trusting one of them.
    #[cfg(test)]
    pub(crate) fn members_reversed<V, const N: usize>(
        &self,
        segs: &[Box<Segment<V, N>>],
        q: QueueId,
    ) -> Vec<u32> {
        let mut out = Vec::new();
        let mut i = self.ends[q.slot()].tail;
        while i != NIL {
            out.push(i);
            i = segs[i as usize].q_prev();
        }
        out
    }
}
