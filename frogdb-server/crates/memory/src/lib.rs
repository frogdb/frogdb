//! Per-core memory broker and the `Budget` handle every non-keyspace buffer
//! charges before it grows.
//!
//! # The invariant
//!
//! **A structure that cannot charge cannot grow.**
//! [adr/0006](../../../../adr/0006-memory-architecture-seams.md) §2: each
//! non-keyspace subsystem — network output, the replication backlog, the
//! tracking table, the WAL channel, full-sync staging, transaction buffering —
//! holds a [`Budget`] and charges growth against it *before* the bytes exist.
//! A charge that fails is a refusal handled at that seam: the subsystem
//! [sheds](Disposition::Shed) or [backpressures](Disposition::Backpressure),
//! and it declares which.
//!
//! There is no "charge anyway and log a warning" path in this crate, because
//! that is what today's unbounded buffers already do.
//!
//! The vocabulary — budget, charge, shed, backpressure, ceiling vs budget — is
//! defined once in [`specs/memory.md`](../../../../specs/memory.md)'s
//! "Invariant vocabulary" and used here without redefinition.
//!
//! # What this crate is not
//!
//! - **Not an allocator reading.** [`Budget::charged`] is an exact count of
//!   what a budget authorized. The core's actual allocator figure comes
//!   through [`arena::ArenaSampler`] and is a **sampled upper bound**, not a
//!   live measurement — read [`arena`] before using it.
//! - **Not the `maxmemory` gate, and not eviction.** The broker is
//!   deliberately minimal in this phase: it hands out budgets, tracks their
//!   charges, and reports a breakdown. Refusal verdicts and eviction are later
//!   phases with their own designs.
//! - **Not the keyspace.** Keyspace bytes are the arena's business. Budgets
//!   cover everything else.
//!
//! # The chokepoint and its lint
//!
//! [`Budget`] is the chokepoint the `lint-budget-growth` seam gate
//! (`scripts/budget-growth.py`, `agents/seam-lints.md`) pins: a growth site in
//! a budgeted subsystem's source that is not preceded by a charge is a lint
//! failure, and unconverted buffers ride a count-pinned allowlist that burns
//! down in batches.
//!
//! # Example
//!
//! ```
//! use frogdb_memory::{Disposition, MemoryBroker, Subsystem};
//!
//! let mut broker = MemoryBroker::detached(0);
//! let budget = broker.open(Subsystem::ClientTracking, 1024, Disposition::Shed);
//!
//! // Ask before the bytes exist.
//! let charge = budget.charge(512).expect("512 of 1024");
//! assert_eq!(budget.charged(), 512);
//!
//! // A refusal is a value the caller handles at its seam.
//! let refused = budget.charge(1024).unwrap_err();
//! assert_eq!(refused.disposition, Disposition::Shed);
//!
//! // Release is on drop.
//! drop(charge);
//! assert_eq!(budget.charged(), 0);
//! ```

pub mod arena;
mod broker;
mod budget;

pub use arena::{ArenaSampler, NoArenaReading};
pub use broker::{Breakdown, MemoryBroker, SubsystemCharge, defaults};
pub use budget::{Budget, Charge, Disposition, Refused, Subsystem};
