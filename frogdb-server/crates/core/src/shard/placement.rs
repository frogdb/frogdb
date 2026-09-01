//! What kind of thread a shard worker is running on.
//!
//! A shard worker used to be one task among many on a shared multi-thread
//! runtime. Under thread-per-core (PRD R2–R4) it instead owns an OS thread and
//! a `current_thread` runtime of its own. Almost nothing inside the worker
//! cares — but one thing does: a synchronous cross-shard wait.
//!
//! On the shared runtime, blocking the calling thread would hold a worker slot
//! that the *target* shard's task might need in order to reply, so such a wait
//! must hand the slot back with `tokio::task::block_in_place`. On a dedicated
//! shard thread there is no slot to hand back — `block_in_place` panics — and
//! no starvation to cause either: the target shard is a different shard by
//! construction, running on a different thread with a different runtime.
//!
//! Which of those two worlds we are in is a property of the *executor*, not of
//! the worker, so the server declares it once at startup. Simulation never
//! declares it: under turmoil every shard is a task on one thread, where a
//! blocking wait for another shard would deadlock, so the pre-existing
//! "cross-shard call needs a multi-thread runtime" error remains the answer
//! there.

use std::sync::atomic::{AtomicBool, Ordering};

/// Set once at startup, read for the lifetime of the process.
static SHARDS_OWN_THREADS: AtomicBool = AtomicBool::new(false);

/// Declare that every shard worker runs on its own OS thread with its own
/// runtime.
///
/// Called once by the server after launching shards, from the one place that
/// knows which executor produced them. Never called under simulation.
pub fn declare_shards_own_threads() {
    SHARDS_OWN_THREADS.store(true, Ordering::Relaxed);
}

/// Whether shard workers own their threads — see [`declare_shards_own_threads`].
///
/// Defaults to `false`, which is the conservative answer: it selects the
/// `block_in_place` path that was correct before thread-per-core existed, and
/// that degrades to an explicit error rather than a deadlock when it is not
/// available.
pub fn shards_own_threads() -> bool {
    SHARDS_OWN_THREADS.load(Ordering::Relaxed)
}
