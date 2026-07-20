//! Node-observable state aggregation shared across surfaces.
//!
//! Observable node state — memory, keyspace, per-shard stats, WAL lag,
//! replication identity, clients, cluster/slot topology — was historically
//! re-modeled and re-aggregated independently by INFO text
//! (`server/src/info`), the JSON `/status` endpoint (`telemetry/src/status`),
//! and the debug web UI (`debug/src/web_ui`). This module is the shared home
//! for the aggregation logic those surfaces render, so each observable is
//! folded exactly once.
//!
//! Migrated so far: WAL durability lag ([`WalLagAggregate`]), consumed by both
//! INFO and telemetry. Remaining observables are still modeled per-surface
//! pending the incremental `NodeStateSnapshot` migration.

mod wal;

pub use wal::{ShardWalLag, WalLagAggregate};
