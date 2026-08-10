//! Explicit-state models of the replication protocol (`stateright`).
//!
//! One module per model, each against its own slice of the protocol and each
//! driving *production* decision functions rather than a transcription of them
//! — the discipline `frogdb-cluster`'s models established and the
//! replication-correctness PRD (§3 W3) ports.
//!
//! * [`feed_gate`] — the slot-handoff replica-feed hold (FM-CLUSTER-097),
//!   against `crate::feed_gate`'s `decide_feed_hold_until` / `decide_publish` /
//!   `decide_hold`.
//!
//! Test-only: the models are `#[cfg(test)]`, so nothing here is compiled into
//! the server binary.

pub(crate) mod feed_gate;
