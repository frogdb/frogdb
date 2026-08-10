//! Explicit-state models of the replication protocol (`stateright`).
//!
//! One module per model, each against its own slice of the protocol and each
//! driving *production* decision functions rather than a transcription of them
//! — the discipline `frogdb-cluster`'s models established and the
//! replication-correctness PRD (§3 W3) ports. Each model carries a smoke
//! configuration in the default suite plus exhaustive configurations behind
//! `#[ignore]`, run by `just replication-model-check` and nightly.
//!
//! * [`feed_gate`] — the slot-handoff replica-feed hold (FM-CLUSTER-097),
//!   against `crate::feed_gate`'s `decide_feed_hold_until` / `decide_publish` /
//!   `decide_hold`.
//! * [`promotion`] — the promotion / resume composite, against
//!   `plan_primary_stint`, `PartialSyncReplay::handle_partial_sync_request`
//!   and `select_psync_arm`.
//!
//! Test-only: the models are `#[cfg(test)]`, so nothing here is compiled into
//! the server binary.

pub(crate) mod feed_gate;
pub(crate) mod promotion;
