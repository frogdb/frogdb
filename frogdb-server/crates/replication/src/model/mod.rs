//! Explicit-state models (`stateright`) of this crate's composites.
//!
//! One submodule per model, each named for the composite it checks. Every model
//! follows the same discipline, which is the point of collecting them here:
//! the transition function is *production code* — the model layer supplies only
//! what the network, the scheduler and the callers supply — and each model
//! carries a smoke configuration in the default suite plus exhaustive
//! configurations behind `#[ignore]`, run by `just replication-model-check` and
//! nightly.

pub(crate) mod promotion;
