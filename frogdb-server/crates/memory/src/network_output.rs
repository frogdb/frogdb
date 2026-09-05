//! This core's [`Subsystem::NetworkOutput`] budget.
//!
//! A thread local. On FrogDB's default deployment that is the same thing as
//! per-core: `colocate-connections` (on by default) pins every connection to
//! its shard's thread, so the bytes a connection buffers for its client are
//! *this core's* network output, and there is no handle to pass down and no way
//! to charge a foreign core's allowance.
//!
//! **With `colocate-connections = false`** connections run on the ambient
//! multi-threaded runtime instead, and this becomes per *worker thread*: each
//! worker gets its own budget, the ceiling applies per worker rather than per
//! core, and no shard broker adopts those budgets — the bytes are still capped
//! and still enforced, but they do not appear in the per-subsystem breakdown.
//! That is the honest cost of unpinning connections, and it is why the pinned
//! layout is the default.
//!
//! It lives here, in the zero-dependency memory crate, rather than beside the
//! buffer pool in `frogdb-net`, because both of its callers must reach it and
//! they sit on opposite sides of that crate: the connection code in
//! `frogdb-server` and the shard's broker in `frogdb-core`. This crate is the
//! one they already share.
//!
//! # Who reaches it
//!
//! Two callers, from opposite directions, and the thread local is what lets
//! them meet without a plumbing chain between them:
//!
//! - a **connection**, on every write, charging what it has buffered
//!   (`connection::output_buffer`);
//! - the **shard's memory broker**, once, adopting the budget so it appears in
//!   the per-subsystem breakdown an operator reads out of `INFO` and the
//!   `frogdb_memory_budget_*` metrics.
//!
//! The broker must adopt from *its own* thread for that to be the same budget
//! the connections charge — which it is under the pinned layout, since the
//! shard's run loop and its connections share a thread.
//!
//! # Disposition
//!
//! [`Disposition::Shed`]: buffered replies are the one thing the server is
//! entitled to drop when it runs out of room. A refusal here closes the
//! connection that asked for the bytes, which is also what Redis's
//! `client-output-buffer-limit` does to a client that will not read.

use crate::{Budget, Disposition, Subsystem};

/// The per-core ceiling on buffered client output.
///
/// Generous on purpose: it is a backstop against a core's worth of connections
/// collectively pinning memory, not the mechanism that disciplines one slow
/// client — that is `client-output-buffer-limit`, which is per connection and
/// per class. A limit tight enough to matter per connection would shed innocent
/// clients on a busy core.
///
/// Not configurable: nothing in the config surface drives it, and a setter no
/// parameter reaches is a seam that rots. `client-output-buffer-limit` is the
/// knob operators actually have, and it is the one that disciplines a client.
pub const NETWORK_OUTPUT_BYTES: u64 = 512 * 1024 * 1024;

thread_local! {
    static BUDGET: Budget = Budget::new(
        Subsystem::NetworkOutput,
        Disposition::Shed,
        NETWORK_OUTPUT_BYTES,
    );
}

/// This core's `NetworkOutput` budget.
///
/// The returned [`Budget`] is a handle onto the same allowance every other
/// caller on this thread gets; cloning it is how a connection keeps one.
pub fn current() -> Budget {
    BUDGET.with(|budget| budget.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    // FM-MEMORY-002
    #[test]
    fn a_core_has_one_budget() {
        let a = current();
        let b = current();
        let mut charge = a.charge(1_024).expect("within the default limit");
        assert_eq!(
            b.charged(),
            1_024,
            "both handles must name the same per-core allowance"
        );
        charge.shrink(1_024);
        assert_eq!(b.charged(), 0);
    }

    /// The per-core ceiling is not configurable, so this constant *is* the
    /// contract — there is no config key an operator could read it back from.
    // FM-MEMORY-002
    #[test]
    fn the_per_core_ceiling_is_the_documented_size() {
        assert_eq!(NETWORK_OUTPUT_BYTES, 536_870_912, "512 MiB");
        assert_eq!(
            current().limit(),
            NETWORK_OUTPUT_BYTES,
            "the thread-local budget opens at the ceiling"
        );
        assert_eq!(current().disposition(), Disposition::Shed);
        assert_eq!(current().subsystem(), Subsystem::NetworkOutput);
    }

    // FM-MEMORY-002
    #[test]
    fn budgets_do_not_cross_cores() {
        let mine = current();
        let _held = mine.charge(4_096).expect("within the default limit");

        let theirs_charged = std::thread::spawn(|| current().charged())
            .join()
            .expect("thread");

        assert_eq!(
            theirs_charged, 0,
            "another core's buffered output is not charged to this one"
        );
        assert_eq!(mine.charged(), 4_096);
    }
}
