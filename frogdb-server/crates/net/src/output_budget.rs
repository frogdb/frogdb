//! This core's [`Subsystem::NetworkOutput`] budget.
//!
//! Sibling of [`crate::buffers`], and per-core for the same reason: a
//! connection runs on its shard's thread, so the bytes it buffers for its
//! client are *this core's* network output. The budget is a thread local, which
//! makes that true by construction — there is no handle to pass down, and no
//! way to charge a foreign core's allowance.
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
//! the connections charge — which it is, since the shard's run loop and its
//! connections share a thread.
//!
//! # Disposition
//!
//! [`Disposition::Shed`]: buffered replies are the one thing the server is
//! entitled to drop when it runs out of room. A refusal here closes the
//! connection that asked for the bytes, which is also what Redis's
//! `client-output-buffer-limit` does to a client that will not read.

use std::sync::atomic::{AtomicU64, Ordering};

use frogdb_memory::{Budget, Disposition, Subsystem};

/// Default per-core ceiling on buffered client output.
///
/// Generous on purpose: it is a backstop against a core's worth of connections
/// collectively pinning memory, not the mechanism that disciplines one slow
/// client — that is `client-output-buffer-limit`, which is per connection and
/// per class. A limit tight enough to matter per connection would shed innocent
/// clients on a busy core.
pub const DEFAULT_NETWORK_OUTPUT_BYTES: u64 = 512 * 1024 * 1024;

/// Process-wide configured ceiling, applied to each core's budget.
static CONFIGURED_LIMIT: AtomicU64 = AtomicU64::new(DEFAULT_NETWORK_OUTPUT_BYTES);

/// Set the per-core ceiling. Takes effect on a core the next time
/// [`current`] is called there.
pub fn set_limit(bytes: u64) {
    CONFIGURED_LIMIT.store(bytes, Ordering::Relaxed);
}

/// The configured per-core ceiling.
pub fn limit() -> u64 {
    CONFIGURED_LIMIT.load(Ordering::Relaxed)
}

thread_local! {
    static BUDGET: Budget = Budget::new(
        Subsystem::NetworkOutput,
        Disposition::Shed,
        CONFIGURED_LIMIT.load(Ordering::Relaxed),
    );
}

/// This core's `NetworkOutput` budget.
///
/// The returned [`Budget`] is a handle onto the same allowance every other
/// caller on this thread gets; cloning it is how a connection keeps one.
/// Re-reads the configured limit so a `CONFIG SET` reaches a core without
/// having to walk the runtimes.
pub fn current() -> Budget {
    BUDGET.with(|budget| {
        let configured = CONFIGURED_LIMIT.load(Ordering::Relaxed);
        if budget.limit() != configured {
            budget.set_limit(configured);
        }
        budget.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_reconfigured_limit_reaches_the_core() {
        std::thread::spawn(|| {
            let before = current();
            assert_eq!(before.limit(), DEFAULT_NETWORK_OUTPUT_BYTES);

            set_limit(8_192);
            let after = current();
            assert_eq!(after.limit(), 8_192);
            assert_eq!(
                before.limit(),
                8_192,
                "the handle already held names the same budget, so it sees the new limit"
            );

            set_limit(DEFAULT_NETWORK_OUTPUT_BYTES);
        })
        .join()
        .expect("thread");
    }
}
