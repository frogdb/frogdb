//! The per-core memory broker.

use std::sync::Arc;

use crate::arena::{ArenaSampler, NoArenaReading};
use crate::budget::{Budget, Disposition, Subsystem};

/// Default budget limits, per core, applied when a caller does not name one.
///
/// These are starting points, not a contract: the limits become configuration
/// as each subsystem is converted, and a subsystem that has not been converted
/// yet holds an *unopened* budget, which is not the same as an infinite one —
/// it simply is not charged by anything and reports zero in the breakdown.
pub mod defaults {
    /// Client tracking. Redis caps this table by *key count*
    /// (`tracking-table-max-keys`, 1 M) and FrogDB keeps that ceiling for
    /// compatibility; this is the byte budget that sits underneath it, sized
    /// so a million small keys fit comfortably and a table of large keys binds
    /// on bytes long before it binds on count.
    pub const CLIENT_TRACKING_BYTES: u64 = 128 * 1024 * 1024;
}

/// One budget slot in the broker's dense table.
#[derive(Debug, Clone)]
struct Slot {
    budget: Budget,
    /// Whether a subsystem has actually taken this budget. An unopened slot is
    /// reported so an operator can see the subsystem exists and is not yet
    /// charging, rather than seeing nothing and concluding it is free.
    opened: bool,
}

/// The per-core owner of every [`Budget`] on that core, and the one component
/// that reads allocator truth for the core.
///
/// Deliberately minimal. It hands out budgets, tracks their charges, and
/// reports a per-subsystem breakdown. It does **not** drive eviction and does
/// not answer the `maxmemory` question — those are later phases of
/// [the memory architecture][prd], and building them now would guess at a
/// table design that does not exist yet.
///
/// One broker per core; brokers do not share state, consistent with the
/// shared-nothing topology.
///
/// [prd]: ../../../../../.scratch/memory-architecture/PRD.md
#[derive(Debug)]
pub struct MemoryBroker {
    core: usize,
    slots: Vec<Slot>,
    arena: Arc<dyn ArenaSampler>,
}

impl MemoryBroker {
    /// A broker for `core`, reading its arena figure through `arena`.
    ///
    /// Pass [`NoArenaReading`] where no arena is bound — the simulation
    /// executor, and every real shard until [issue 03] lands. See
    /// [`crate::arena`] for how that swap works.
    ///
    /// [issue 03]: ../../../../../.scratch/memory-architecture/issues/
    pub fn new(core: usize, arena: Arc<dyn ArenaSampler>) -> Self {
        let slots = Subsystem::ALL
            .iter()
            .map(|&subsystem| Slot {
                // An unopened budget has a zero limit: nothing may grow
                // against a budget nobody has opened, so a charge site that
                // forgets to open its budget fails loudly at its first byte
                // instead of running unbounded.
                budget: Budget::new(subsystem, Disposition::Backpressure, 0),
                opened: false,
            })
            .collect();
        Self { core, slots, arena }
    }

    /// A broker with no arena reading, for tests and for the simulation
    /// executor.
    pub fn detached(core: usize) -> Self {
        Self::new(core, Arc::new(NoArenaReading))
    }

    /// Which core this broker belongs to.
    pub fn core(&self) -> usize {
        self.core
    }

    /// Open `subsystem`'s budget with a limit and a declared disposition, and
    /// return the handle. Opening a subsystem twice replaces its budget, so a
    /// reconfiguration is a re-open rather than a second pool; existing
    /// holders keep charging the old one until they take a fresh handle.
    pub fn open(&mut self, subsystem: Subsystem, limit: u64, disposition: Disposition) -> Budget {
        let budget = Budget::new(subsystem, disposition, limit);
        self.slots[subsystem.index()] = Slot {
            budget: budget.clone(),
            opened: true,
        };
        budget
    }

    /// The handle for `subsystem`, whether or not it has been opened. An
    /// unopened budget has a zero limit and refuses everything.
    pub fn budget(&self, subsystem: Subsystem) -> Budget {
        self.slots[subsystem.index()].budget.clone()
    }

    /// Whether `subsystem` has taken its budget yet — false for every
    /// subsystem still on the seam lint's allowlist.
    pub fn is_open(&self, subsystem: Subsystem) -> bool {
        self.slots[subsystem.index()].opened
    }

    /// A **sampled upper bound**, in bytes, on what this core's arena is
    /// holding — or `None` when the core has no arena figure.
    ///
    /// Read [`crate::arena`] before using this for anything: it overstates
    /// live bytes by whatever is parked in the thread cache and is refreshed
    /// on the sampler's schedule, not on the caller's. It is not a live
    /// reading and there is deliberately no API here that offers one.
    pub fn arena_sampled_upper_bound_bytes(&self) -> Option<u64> {
        self.arena.sampled_upper_bound_bytes()
    }

    /// The per-subsystem charge breakdown an operator sees.
    ///
    /// Every subsystem appears, opened or not, so the view answers "who is
    /// using this core's memory" and "who is not yet accounted for" with the
    /// same list.
    pub fn breakdown(&self) -> Breakdown {
        Breakdown {
            core: self.core,
            arena_sampled_upper_bound_bytes: self.arena_sampled_upper_bound_bytes(),
            subsystems: self
                .slots
                .iter()
                .map(|slot| SubsystemCharge {
                    subsystem: slot.budget.subsystem(),
                    disposition: slot.budget.disposition(),
                    opened: slot.opened,
                    charged_bytes: slot.budget.charged(),
                    limit_bytes: slot.budget.limit(),
                    peak_bytes: slot.budget.peak(),
                    refusals: slot.budget.refusals(),
                })
                .collect(),
        }
    }

    /// Total bytes charged across every budget on this core.
    ///
    /// This is what the broker *authorized*, which is a different quantity
    /// from what the allocator holds — the keyspace is not budgeted, and an
    /// unconverted buffer contributes nothing. Never present it as the core's
    /// memory use.
    pub fn total_charged_bytes(&self) -> u64 {
        self.slots
            .iter()
            .map(|slot| slot.budget.charged())
            .sum::<u64>()
    }
}

/// One row of the per-subsystem breakdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsystemCharge {
    /// The subsystem, whose [`Subsystem::as_str`] is the stable label.
    pub subsystem: Subsystem,
    /// What it does when refused.
    pub disposition: Disposition,
    /// Whether it has taken its budget. `false` means "not yet converted",
    /// which is why the numbers below are zero.
    pub opened: bool,
    /// Bytes it currently holds authorization for.
    pub charged_bytes: u64,
    /// Its limit.
    pub limit_bytes: u64,
    /// Its high-water charge.
    pub peak_bytes: u64,
    /// How many charges it has been refused.
    pub refusals: u64,
}

/// A core's memory picture: every subsystem's charge, plus the core's arena
/// figure — which is a sampled upper bound, and is `None` until [issue 03]
/// binds an arena.
///
/// [issue 03]: ../../../../../.scratch/memory-architecture/issues/
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Breakdown {
    /// The core this breakdown describes.
    pub core: usize,
    /// The core's arena figure — a **sampled upper bound**, never a live
    /// reading. `None` when the core has no arena (simulation, or before
    /// arena binding lands).
    pub arena_sampled_upper_bound_bytes: Option<u64>,
    /// One row per [`Subsystem`], in [`Subsystem::ALL`] order.
    pub subsystems: Vec<SubsystemCharge>,
}

impl Breakdown {
    /// The row for `subsystem`.
    pub fn get(&self, subsystem: Subsystem) -> &SubsystemCharge {
        &self.subsystems[subsystem.index()]
    }

    /// Total bytes charged across the core's subsystems.
    pub fn total_charged_bytes(&self) -> u64 {
        self.subsystems.iter().map(|s| s.charged_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unopened_budget_refuses_its_first_byte() {
        let broker = MemoryBroker::detached(0);
        let budget = broker.budget(Subsystem::FullsyncStaging);
        assert!(!broker.is_open(Subsystem::FullsyncStaging));
        assert_eq!(budget.limit(), 0);
        assert!(
            budget.charge(1).is_err(),
            "nothing may grow against a budget nobody opened"
        );
    }

    #[test]
    fn opening_hands_out_a_chargeable_budget() {
        let mut broker = MemoryBroker::detached(3);
        assert_eq!(broker.core(), 3);
        let budget = broker.open(Subsystem::ClientTracking, 1_000, Disposition::Shed);
        let _held = budget.charge(400).unwrap();

        assert!(broker.is_open(Subsystem::ClientTracking));
        let row = broker.breakdown().get(Subsystem::ClientTracking).clone();
        assert_eq!(row.charged_bytes, 400);
        assert_eq!(row.limit_bytes, 1_000);
        assert_eq!(row.disposition, Disposition::Shed);
        assert!(row.opened);
    }

    #[test]
    fn the_breakdown_names_every_subsystem_opened_or_not() {
        let mut broker = MemoryBroker::detached(0);
        broker.open(Subsystem::ClientTracking, 1_000, Disposition::Shed);
        let breakdown = broker.breakdown();

        assert_eq!(breakdown.subsystems.len(), Subsystem::ALL.len());
        for (row, expected) in breakdown.subsystems.iter().zip(Subsystem::ALL) {
            assert_eq!(row.subsystem, expected, "breakdown order must match ALL");
        }
        assert!(!breakdown.get(Subsystem::WalChannel).opened);
        assert_eq!(breakdown.get(Subsystem::WalChannel).charged_bytes, 0);
    }

    #[test]
    fn refusals_show_up_in_the_breakdown() {
        let mut broker = MemoryBroker::detached(0);
        let budget = broker.open(Subsystem::NetworkOutput, 10, Disposition::Shed);
        assert!(budget.charge(11).is_err());
        assert_eq!(broker.breakdown().get(Subsystem::NetworkOutput).refusals, 1);
    }

    #[test]
    fn totals_add_across_subsystems() {
        let mut broker = MemoryBroker::detached(0);
        let a = broker.open(Subsystem::ClientTracking, 1_000, Disposition::Shed);
        let b = broker.open(Subsystem::TxnBuffering, 1_000, Disposition::Backpressure);
        let _x = a.charge(100).unwrap();
        let _y = b.charge(250).unwrap();
        assert_eq!(broker.total_charged_bytes(), 350);
        assert_eq!(broker.breakdown().total_charged_bytes(), 350);
    }

    #[test]
    fn a_detached_broker_reports_no_arena_figure() {
        let broker = MemoryBroker::detached(0);
        assert_eq!(broker.arena_sampled_upper_bound_bytes(), None);
        assert_eq!(broker.breakdown().arena_sampled_upper_bound_bytes, None);
    }

    #[test]
    fn an_arena_sampler_reaches_the_breakdown() {
        #[derive(Debug)]
        struct Fixed(u64);
        impl ArenaSampler for Fixed {
            fn sampled_upper_bound_bytes(&self) -> Option<u64> {
                Some(self.0)
            }
        }
        let broker = MemoryBroker::new(1, Arc::new(Fixed(8_192)));
        assert_eq!(broker.arena_sampled_upper_bound_bytes(), Some(8_192));
        assert_eq!(
            broker.breakdown().arena_sampled_upper_bound_bytes,
            Some(8_192)
        );
    }
}
