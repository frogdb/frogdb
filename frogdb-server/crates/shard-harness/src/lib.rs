//! Deterministic shard-worker driver harness against the real command set.
//!
//! `frogdb-core`'s own test tree cannot exercise its shard-worker dispatch
//! path against the *real* production command registry: `frogdb-commands`
//! depends on `frogdb-core`, so pulling it in as a dev-dependency of
//! `frogdb-core` itself would compile two rlib variants of the crate and trip
//! `E0308` the moment unit-test code touched both. This crate breaks that
//! cycle by living one level up: it depends on `frogdb-core` (with its
//! `shard-driver`/`fake-wal` test-only seam features enabled) and on
//! `frogdb-commands` as ordinary dependencies, and re-exports a harness that
//! drives a real [`frogdb_core::ShardWorker`] — built with the real
//! `CommandRegistry` — directly, with controlled message ordering.
//!
//! Modules:
//! - [`harness`]: [`harness::ShardDriver`], the core driver owning real shard
//!   workers plus direct-dispatch and pumped-mode helpers.
//! - [`sink`]: a harness-local [`frogdb_vll::ShardSink`] implementation
//!   ([`sink::ChannelSink`]) plus a failure-injecting wrapper
//!   ([`sink::FaultSink`]) for VLL scatter/gather scenarios.
//! - [`generator`]: a proptest schedule generator enforcing the
//!   permutation-constraint model so illegal message orders are
//!   unrepresentable.
//! - [`notify_capture`]: a keyspace-notification capture seam plus an
//!   order-consistency checker.
//!
//! The scenario tests that exercise this harness live under `tests/` in this
//! same crate (moved from `frogdb-core`'s integration-test tree, which no
//! longer depends on `frogdb-commands` at all).

pub mod generator;
pub mod harness;
pub mod notify_capture;
pub mod sink;
