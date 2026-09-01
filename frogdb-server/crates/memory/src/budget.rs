//! The [`Budget`] handle and the [`Charge`] it issues.
//!
//! A `Budget` is the only thing that can authorize growth of a non-keyspace
//! buffer. The vocabulary is [`specs/memory.md`]'s "Invariant vocabulary" and
//! is not re-defined here: **charge** is permission taken *before* the bytes
//! exist, a failed charge is a **refusal** handled at that seam, and every
//! budget declares whether that seam **sheds** or **backpressures**.
//!
//! [`specs/memory.md`]: ../../../../../specs/memory.md

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The non-keyspace subsystems that hold a budget, as
/// [adr/0006](../../../../../adr/0006-memory-architecture-seams.md) §2 names
/// them.
///
/// A closed enum rather than a free `&'static str` for two reasons: the
/// per-subsystem breakdown an operator reads is a fixed label set (so a
/// Prometheus series cannot appear because someone typed a new string at a
/// call site), and [`Subsystem::as_str`] is the *stable* name — it is
/// deliberately not derived from the Rust type that happens to own the buffer
/// today, so a refactor of that type does not rename an operator's metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Subsystem {
    /// Per-connection output buffering (`client-output-buffer-limit` classes).
    NetworkOutput,
    /// The replication backlog ring and the replica feed's hold buffers.
    ReplicationBacklog,
    /// The client-side-caching tracking table (`CLIENT TRACKING`).
    ClientTracking,
    /// The WAL's in-flight channel between a shard and its writer.
    WalChannel,
    /// Full-sync staging: a dataset materialized for a joining replica.
    FullsyncStaging,
    /// Transaction buffering: queued `MULTI` commands, staged write batches,
    /// replica-side pending groups.
    TxnBuffering,
    /// Persistence engine memory (RocksDB block cache + write buffers). Held
    /// so [issue 06] can size a `WriteBufferManager` and a strict-capacity
    /// block cache from a budget rather than from a free-standing constant.
    ///
    /// [issue 06]: ../../../../../.scratch/memory-architecture/issues/
    Persistence,
}

impl Subsystem {
    /// Every subsystem, in declaration order. The breakdown iterates this, so
    /// adding a variant adds it to the operator's view without a second list.
    pub const ALL: [Subsystem; 7] = [
        Subsystem::NetworkOutput,
        Subsystem::ReplicationBacklog,
        Subsystem::ClientTracking,
        Subsystem::WalChannel,
        Subsystem::FullsyncStaging,
        Subsystem::TxnBuffering,
        Subsystem::Persistence,
    ];

    /// The stable name used in metric labels, `INFO` fields and log lines.
    /// Changing one of these strings is an operator-visible break.
    pub const fn as_str(self) -> &'static str {
        match self {
            Subsystem::NetworkOutput => "network_output",
            Subsystem::ReplicationBacklog => "replication_backlog",
            Subsystem::ClientTracking => "client_tracking",
            Subsystem::WalChannel => "wal_channel",
            Subsystem::FullsyncStaging => "fullsync_staging",
            Subsystem::TxnBuffering => "txn_buffering",
            Subsystem::Persistence => "persistence",
        }
    }

    /// Index into a dense per-subsystem array.
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

impl fmt::Display for Subsystem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a subsystem does when its charge is refused.
///
/// Declared at construction and never inferred: `specs/memory.md` requires
/// every buffer to state which of the two it does and forbids neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Disposition {
    /// Discard work the server is entitled to drop — close the connection,
    /// throw away buffered output, evict the oldest tracked key. Shedding is
    /// observable: it produces a metric naming the subsystem.
    Shed,
    /// Make the producer wait — decline the write, stop polling the reader,
    /// block the channel. Backpressure preserves the work; shedding discards
    /// it.
    Backpressure,
}

impl Disposition {
    /// The stable name used in metric labels and `INFO` fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Disposition::Shed => "shed",
            Disposition::Backpressure => "backpressure",
        }
    }
}

impl fmt::Display for Disposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A refused charge: the budget could not authorize `requested` more bytes.
///
/// This is a value the caller must handle at its seam — there is deliberately
/// no constructor that turns it into a warning and a charge taken anyway,
/// because that is exactly what today's unbounded buffers already do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused {
    /// Which subsystem asked.
    pub subsystem: Subsystem,
    /// What that subsystem does about it — the disposition it declared when
    /// the budget was opened.
    pub disposition: Disposition,
    /// Bytes the caller asked to be allowed to allocate.
    pub requested: u64,
    /// Bytes already charged when the request arrived.
    pub charged: u64,
    /// The budget's limit.
    pub limit: u64,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} budget refused {} bytes ({} of {} charged); disposition: {}",
            self.subsystem, self.requested, self.charged, self.limit, self.disposition
        )
    }
}

impl std::error::Error for Refused {}

#[derive(Debug)]
struct BudgetInner {
    subsystem: Subsystem,
    disposition: Disposition,
    /// Atomic so the limit can follow a live `CONFIG SET` without the budget
    /// having to be re-issued to every holder.
    limit: AtomicU64,
    charged: AtomicU64,
    peak: AtomicU64,
    refusals: AtomicU64,
}

/// A handle, held by one subsystem on one core, carrying a limit and a current
/// charge.
///
/// Cheap to clone — a clone is an `Arc` bump and shares the same counters, so
/// two holders of "the network output budget" are charging one pool rather
/// than two.
///
/// # Why atomics on a per-core object
///
/// A budget belongs to exactly one core and is never charged from another, so
/// the counters are uncontended by construction and a non-atomic `Cell` would
/// be correct. They are atomics anyway because a `Cell` makes every structure
/// that holds a budget `!Send`, and a shard is *built* on one thread and
/// *moved* to its own thread by the `ShardExecutor`. The relaxed ordering on
/// an uncontended cache line is the same instruction a plain load would be;
/// this is a `Send` accommodation, not cross-core sharing, and nothing here
/// may be read as a licence to charge a budget from a foreign core.
#[derive(Debug, Clone)]
pub struct Budget {
    inner: Arc<BudgetInner>,
}

impl Budget {
    /// Open a budget. Prefer [`crate::MemoryBroker::open`] — going through the
    /// broker is what makes the budget appear in the per-subsystem breakdown
    /// an operator sees. This constructor exists for tests and for the broker
    /// itself.
    pub fn new(subsystem: Subsystem, disposition: Disposition, limit: u64) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                subsystem,
                disposition,
                limit: AtomicU64::new(limit),
                charged: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                refusals: AtomicU64::new(0),
            }),
        }
    }

    /// Ask for `bytes` **before** allocating them.
    ///
    /// The returned [`Charge`] holds the allowance until it is dropped or
    /// released. An `Err` is a refusal the caller handles at its seam
    /// according to the disposition it declared.
    pub fn charge(&self, bytes: u64) -> Result<Charge, Refused> {
        self.take(bytes)?;
        Ok(Charge {
            budget: self.clone(),
            bytes,
        })
    }

    /// A zero-byte charge, for a structure that holds one charge for its whole
    /// lifetime and grows and shrinks it in place ([`Charge::grow`] /
    /// [`Charge::shrink`]). Cannot be refused: it authorizes nothing.
    pub fn open_charge(&self) -> Charge {
        Charge {
            budget: self.clone(),
            bytes: 0,
        }
    }

    /// The accounting half of `charge`, shared with [`Charge::grow`].
    fn take(&self, bytes: u64) -> Result<(), Refused> {
        let inner = &self.inner;
        let limit = inner.limit.load(Ordering::Relaxed);
        let mut charged = inner.charged.load(Ordering::Relaxed);
        loop {
            let Some(next) = charged.checked_add(bytes) else {
                inner.refusals.fetch_add(1, Ordering::Relaxed);
                return Err(self.refusal(bytes, charged, limit));
            };
            if next > limit {
                inner.refusals.fetch_add(1, Ordering::Relaxed);
                return Err(self.refusal(bytes, charged, limit));
            }
            match inner.charged.compare_exchange_weak(
                charged,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    inner.peak.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(actual) => charged = actual,
            }
        }
    }

    fn refusal(&self, requested: u64, charged: u64, limit: u64) -> Refused {
        Refused {
            subsystem: self.inner.subsystem,
            disposition: self.inner.disposition,
            requested,
            charged,
            limit,
        }
    }

    /// Give bytes back. Not public: bytes are returned by dropping or
    /// shrinking a [`Charge`], so a subsystem cannot release an allowance it
    /// never held.
    fn give_back(&self, bytes: u64) {
        // `saturating` rather than a wrapping `fetch_sub`: an accounting bug
        // that double-releases must not wrap the counter to `u64::MAX` and
        // hand the subsystem an unlimited budget. It is clamped and stays
        // visible in the breakdown instead.
        let _ = self
            .inner
            .charged
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                Some(c.saturating_sub(bytes))
            });
    }

    /// Which subsystem this budget belongs to.
    pub fn subsystem(&self) -> Subsystem {
        self.inner.subsystem
    }

    /// What this subsystem does when a charge is refused.
    pub fn disposition(&self) -> Disposition {
        self.inner.disposition
    }

    /// The current limit in bytes.
    pub fn limit(&self) -> u64 {
        self.inner.limit.load(Ordering::Relaxed)
    }

    /// Move the limit (a live `CONFIG SET`). Lowering it below the current
    /// charge does not claw anything back — it refuses the next growth, which
    /// is the only thing a budget can promise without unwinding work already
    /// done.
    pub fn set_limit(&self, limit: u64) {
        self.inner.limit.store(limit, Ordering::Relaxed);
    }

    /// Bytes currently charged. This is an exact count of what this budget
    /// authorized — not an allocator reading, and not the subsystem's true
    /// heap footprint, which no counter can know.
    pub fn charged(&self) -> u64 {
        self.inner.charged.load(Ordering::Relaxed)
    }

    /// High-water mark of [`Budget::charged`] since the budget was opened.
    pub fn peak(&self) -> u64 {
        self.inner.peak.load(Ordering::Relaxed)
    }

    /// How many charges this budget has refused. The observable half of
    /// "shedding is observable": a subsystem at its limit moves this counter
    /// every time it declines to grow.
    pub fn refusals(&self) -> u64 {
        self.inner.refusals.load(Ordering::Relaxed)
    }

    /// Bytes still available before the next charge is refused.
    pub fn available(&self) -> u64 {
        self.limit().saturating_sub(self.charged())
    }
}

/// An authorized allowance, held until dropped.
///
/// Release is on drop by choice, not by omission: a subsystem that has to
/// remember to release will eventually not, and the resulting leak is a budget
/// that refuses forever with nothing allocated behind it.
#[derive(Debug)]
#[must_use = "a Charge that is dropped immediately releases the bytes it just authorized"]
pub struct Charge {
    budget: Budget,
    bytes: u64,
}

impl Charge {
    /// Bytes this charge currently holds.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The budget this charge is drawn on.
    pub fn budget(&self) -> &Budget {
        &self.budget
    }

    /// Grow the allowance by `extra` bytes, **before** those bytes exist.
    ///
    /// On refusal the charge is unchanged, so a caller that sheds and retries
    /// does not have to reason about a partially applied growth.
    pub fn grow(&mut self, extra: u64) -> Result<(), Refused> {
        self.budget.take(extra)?;
        self.bytes += extra;
        Ok(())
    }

    /// Give `bytes` back, after the memory they covered is gone. Clamped at
    /// zero, so an over-shrink cannot make the charge lend bytes to the pool.
    pub fn shrink(&mut self, bytes: u64) {
        let released = bytes.min(self.bytes);
        self.bytes -= released;
        self.budget.give_back(released);
    }

    /// Release the whole allowance now. Identical to dropping it; spelled out
    /// where the release is the point of the statement.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.give_back(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charge_succeeds_under_limit_and_is_refused_over_it() {
        let budget = Budget::new(Subsystem::ClientTracking, Disposition::Shed, 100);

        let held = budget.charge(60).expect("60 of 100 fits");
        assert_eq!(budget.charged(), 60);

        let refused = budget.charge(50).expect_err("60 + 50 > 100");
        assert_eq!(refused.subsystem, Subsystem::ClientTracking);
        assert_eq!(refused.disposition, Disposition::Shed);
        assert_eq!(refused.requested, 50);
        assert_eq!(refused.charged, 60);
        assert_eq!(refused.limit, 100);
        assert_eq!(budget.refusals(), 1);
        // A refusal charges nothing.
        assert_eq!(budget.charged(), 60);

        drop(held);
        assert_eq!(budget.charged(), 0, "the charge is released on drop");
        assert_eq!(budget.peak(), 60, "the peak survives the release");
    }

    #[test]
    fn explicit_release_matches_drop() {
        let budget = Budget::new(Subsystem::WalChannel, Disposition::Backpressure, 100);
        let held = budget.charge(40).unwrap();
        held.release();
        assert_eq!(budget.charged(), 0);
    }

    #[test]
    fn exact_limit_is_allowed() {
        let budget = Budget::new(Subsystem::NetworkOutput, Disposition::Shed, 100);
        let _held = budget.charge(100).expect("the limit itself is inside it");
        assert_eq!(budget.available(), 0);
        assert!(budget.charge(1).is_err());
    }

    #[test]
    fn clones_share_one_pool() {
        let budget = Budget::new(Subsystem::TxnBuffering, Disposition::Backpressure, 100);
        let other = budget.clone();
        let _a = budget.charge(60).unwrap();
        assert!(other.charge(60).is_err(), "the clone sees the same charge");
        assert_eq!(other.charged(), 60);
    }

    #[test]
    fn grow_and_shrink_move_the_same_pool() {
        let budget = Budget::new(Subsystem::ClientTracking, Disposition::Shed, 100);
        let mut charge = budget.open_charge();
        assert_eq!(charge.bytes(), 0);

        charge.grow(70).unwrap();
        assert_eq!(budget.charged(), 70);

        let refused = charge.grow(40).expect_err("70 + 40 > 100");
        assert_eq!(refused.requested, 40);
        assert_eq!(
            charge.bytes(),
            70,
            "a refused growth leaves the charge alone"
        );

        charge.shrink(50);
        assert_eq!(budget.charged(), 20);
        charge.grow(40).expect("room again after the shrink");
        assert_eq!(budget.charged(), 60);

        drop(charge);
        assert_eq!(budget.charged(), 0);
    }

    #[test]
    fn over_shrink_cannot_lend_bytes_to_the_pool() {
        let budget = Budget::new(Subsystem::FullsyncStaging, Disposition::Shed, 100);
        let mut charge = budget.charge(10).unwrap();
        charge.shrink(9_999);
        assert_eq!(charge.bytes(), 0);
        assert_eq!(budget.charged(), 0);
        let _all = budget.charge(100).expect("the pool is whole");
    }

    #[test]
    fn overflowing_request_is_a_refusal_not_a_wrap() {
        let budget = Budget::new(Subsystem::NetworkOutput, Disposition::Shed, u64::MAX);
        let _held = budget.charge(u64::MAX - 1).unwrap();
        assert!(budget.charge(u64::MAX).is_err());
        assert_eq!(budget.charged(), u64::MAX - 1);
    }

    #[test]
    fn lowering_the_limit_refuses_the_next_growth() {
        let budget = Budget::new(
            Subsystem::ReplicationBacklog,
            Disposition::Backpressure,
            100,
        );
        let _held = budget.charge(80).unwrap();
        budget.set_limit(50);
        assert_eq!(budget.charged(), 80, "nothing is clawed back");
        assert!(budget.charge(1).is_err());
    }

    #[test]
    fn subsystem_names_are_unique_and_stable() {
        let mut seen = std::collections::HashSet::new();
        for s in Subsystem::ALL {
            assert!(seen.insert(s.as_str()), "duplicate subsystem name {s}");
            assert_eq!(Subsystem::ALL[s.index()], s, "index() disagrees with ALL");
        }
        assert_eq!(seen.len(), Subsystem::ALL.len());
    }
}
