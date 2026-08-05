//! Server-side cluster runtime.
//!
//! `frogdb-cluster` owns the metadata plane — the Raft state machine, the
//! replicated topology, the RPC envelopes — and owns no shards, no sockets and
//! no configuration. This crate is the other half: the four components that
//! turn that plane into a running node.
//!
//! - [`bus`] serves the cluster bus: it accepts peer connections, hands Raft
//!   RPCs to the Raft instance, and services the bus-local subset (pub/sub
//!   fan-out, HealthProbe) straight from the shard senders.
//! - [`pubsub`] is the outbound direction of the same wire: `PUBLISH` broadcast
//!   to every peer, `SPUBLISH` forwarded to the slot owner.
//! - [`flags`] holds the `[cluster]` knobs that steer decisions as shared
//!   atomics, read at the point of use so `CONFIG SET` needs no restart.
//! - [`failure_detector`] probes peers over TCP, keeps a local health table,
//!   and — on the leader — reconciles that view into the replicated topology.
//! - [`migration_events`] wakes the clients blocked on a slot that just moved
//!   away, translating each completion event into a per-shard notification.
//!
//! Nothing here reaches back into `frogdb-server`: the network primitives come
//! from [`frogdb_net`] (so the turmoil swap reaches this crate — see the
//! `turmoil` feature), and the one genuinely server-coupled dependency, TLS on
//! inbound bus connections, is injected as the [`bus::BusTlsAcceptor`] seam.

pub mod bus;
pub mod failure_detector;
pub mod flags;
pub mod migration_events;
pub mod pubsub;

pub use bus::{ClusterBusContext, run as run_cluster_bus};
pub use failure_detector::{
    DetectorRaft, FailureDetector, FailureDetectorConfig, spawn_failure_detector_task,
};
pub use flags::{ClusterRuntimeFlags, SelfFenceGate};
pub use migration_events::{
    MigrationNotice, plan_migration_notice, run_slot_migration_event_dispatcher,
};
pub use pubsub::{ClusterPubSubForwarder, ShardRoute, SpublishOutcome};

#[cfg(not(feature = "turmoil"))]
pub use bus::BusTlsAcceptor;
