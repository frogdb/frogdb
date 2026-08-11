//! Server-side replication runtime.
//!
//! `frogdb-replication` owns the *protocol*: frame encoding, handshake, offset
//! and WAIT coordination, the primary/replica session state machines. It owns no
//! shards and no store, so every place the protocol has to touch this node's
//! data is an injected seam. This crate is the other half — the shard-touching
//! implementations of those seams, plus the write fence that replica ACK
//! freshness drives:
//!
//! - [`ReplicaCommandExecutor`] — the [`ReplicaApplier`] seam: route a
//!   replicated command group to its tagged origin shard and check the result.
//! - [`live_snapshot_source`] — the `LiveSnapshotSource` seam: serialize this
//!   node's live keyspace for a full resync it must serve.
//! - [`LiveSnapshotInstaller`] — the `SnapshotInstaller` seam: install a
//!   received full-resync dataset (staged checkpoint or live blobs) into the
//!   live keyspace.
//! - [`ReplicationQuorumChecker`] — the `QuorumChecker`/`WriteFenceReporter`
//!   the write gate consults to fence a primary that has lost its replicas.
//!
//! Everything here talks to shards through `frogdb_core`'s plain-data channel
//! messages ([`CoreMsg`]/[`ReplicationMsg`]) and to the protocol through
//! `frogdb_replication`'s seam types. Nothing reaches `Server`, `Subsystems`, or
//! a connection, so — like `frogdb_recovery` (ADR 0003) and unlike `frogdb_txn`
//! (ADR 0002) — no host trait is needed to invert a dependency; the shard
//! senders are the whole surface. The parts that *are* connection-coupled stay
//! in the server crate: the `REPLICAOF`/`REPLCONF`/`PSYNC` command handlers, the
//! boot wiring in `server::replication_init`, and `role_manager`.
//!
//! [`CoreMsg`]: frogdb_core::CoreMsg
//! [`ReplicationMsg`]: frogdb_core::ReplicationMsg
//! [`ReplicaApplier`]: frogdb_replication::ReplicaApplier

pub mod executor;
pub mod export;
pub mod install;
pub mod quorum;

/// Stateful property generation over the self-fence checker (issue 05, R6).
/// Test-only: the harness drives production seams, it ships nothing.
#[cfg(test)]
mod properties;
#[cfg(test)]
mod test_shards;

pub use executor::ReplicaCommandExecutor;
pub use export::live_snapshot_source;
pub use install::LiveSnapshotInstaller;
pub use quorum::ReplicationQuorumChecker;
