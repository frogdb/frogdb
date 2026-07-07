//! The connection-command seam (core-side definition).
//!
//! A [`ConnectionCommand`] executes against a narrow [`ConnCtx`] view of the
//! connection — only the subsystems it declares — instead of taking
//! `&ConnectionHandler` (a large god object in the server crate). This is the
//! target shape for the connection-level command tree: it collapses the "spec
//! stub in a command module, logic as a method on `ConnectionHandler`" split
//! into one self-contained unit whose interface is its `ConnCtx`.
//!
//! # Why this lives in `core`
//!
//! The command registry ([`crate::registry`]) stores one executor per command
//! as a [`crate::registry::CommandImpl`] tagged union. For the registry to name
//! the connection executor as `CommandImpl::Connection(&'static dyn
//! ConnectionCommand)`, the trait must be defined in `core` (the server crate
//! cannot be named by `core`). The only server-specific dependency the original
//! `ConnCtx` had — the `ConfigManager` — is abstracted here behind the
//! [`ConfigProvider`] trait, which the server's `ConfigManager` implements.
//!
//! The trait is object-safe: `execute` returns a boxed future rather than being
//! an `async fn`, so a command can be stored and dispatched as `&'static dyn
//! ConnectionCommand` from the registry.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

use crate::client_registry::ClientRegistry;
use crate::command_spec::CommandSpec;
use crate::hotkeys::SharedHotkeySession;
use crate::keyspace_stats::KeyspaceStats;
use crate::latency_histogram::CommandLatencyHistograms;
use crate::shard::ShardSender;

/// A boxed, `Send` future — the object-safe return type for the async methods on
/// the connection-command seam.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runtime-configuration operations the CONFIG command needs, abstracted so the
/// connection-command seam can live in `core` without naming the server's
/// `ConfigManager`. The server implements this for `ConfigManager`.
pub trait ConfigProvider: Send + Sync {
    /// Return parameters matching `pattern` as `(name, value)` pairs
    /// (CONFIG GET).
    fn get(&self, pattern: &str) -> Vec<(String, String)>;

    /// Set a configuration parameter, propagating to shard workers as needed
    /// (CONFIG SET). Errors are surfaced as human-readable strings.
    fn set<'a>(&'a self, name: &'a str, value: &'a str) -> BoxFuture<'a, Result<(), String>>;

    /// Rewrite the on-disk config file with current runtime values
    /// (CONFIG REWRITE).
    fn rewrite(&self) -> Result<(), String>;

    /// Return CONFIG HELP text lines.
    fn help(&self) -> Vec<String>;
}

/// Cluster slot-ownership queries the HOTKEYS command needs to scope sampling to
/// the slots this node serves. Abstracted so the connection-command seam can live
/// in `core` without naming the server's cluster wiring; the server implements
/// this for its `ClusterDeps`.
pub trait HotkeyClusterProvider: Send + Sync {
    /// Whether the server is running in cluster mode.
    fn is_cluster_mode(&self) -> bool;

    /// Whether this node owns `slot` (always true in standalone mode).
    fn node_handles_slot(&self, slot: u16) -> bool;

    /// The slots this node serves (empty in standalone mode).
    fn node_slot_list(&self) -> Vec<u16>;
}

/// A narrow, per-command view of the connection: shared borrows of only the
/// subsystems the executing [`ConnectionCommand`] needs. This is the command's
/// test surface — a command is exercised by constructing a `ConnCtx` over
/// fixture dependencies, with no socket and no `ConnectionHandler`.
pub struct ConnCtx<'a> {
    /// Runtime configuration parameters (CONFIG GET/SET/REWRITE/HELP).
    pub config: &'a dyn ConfigProvider,
    /// Per-client registry: call counts, error stats (CONFIG RESETSTAT).
    pub client_registry: &'a ClientRegistry,
    /// `INFO commandstats`/`errorstats`/`latencystats` histograms (RESETSTAT).
    pub latency_histograms: &'a CommandLatencyHistograms,
    /// Operator-visible keyspace hit/miss counters (RESETSTAT rebase).
    pub keyspace_stats: &'a KeyspaceStats,
    /// Per-shard message channels, for broadcasts (RESETSTAT).
    pub shard_senders: &'a [ShardSender],
    /// Point-in-time snapshot coordinator (BGSAVE/LASTSAVE). Already an
    /// object-safe `core` trait, so it is named directly rather than behind a
    /// bespoke provider like [`ConfigProvider`].
    pub snapshot_coordinator: &'a dyn crate::persistence::SnapshotCoordinator,
    /// Server-wide hotkey sampling session (HOTKEYS START/STOP/RESET/GET).
    pub hotkey_session: &'a SharedHotkeySession,
    /// Cluster slot-ownership queries for scoping HOTKEYS sampling to this node.
    pub hotkey_cluster: &'a dyn HotkeyClusterProvider,
    /// Negotiated RESP protocol version, for RESP2/RESP3 reply shaping
    /// (HOTKEYS GET).
    pub protocol_version: ProtocolVersion,
}

/// A command handled at the connection level, executed against a narrow
/// [`ConnCtx`] rather than `&mut ConnectionHandler`.
///
/// Object-safe: dispatched as `&'static dyn ConnectionCommand` from the command
/// registry (see [`crate::registry::CommandImpl::Connection`]). Each command
/// carries its own [`CommandSpec`] via [`ConnectionCommand::spec`], exactly like
/// a shard [`crate::command::Command`] does — this is the single source of truth
/// for the command's metadata and for registry `strategy` ↔ variant validation.
pub trait ConnectionCommand: Send + Sync {
    /// Declarative specification of this command's mechanics. Its
    /// [`CommandSpec::strategy`] must be [`crate::command::ExecutionStrategy::
    /// ConnectionLevel`]; the registry enforces this.
    fn spec(&self) -> &'static CommandSpec;

    /// Execute the command against its connection view. Returns a boxed future
    /// (for object safety) resolving to the wire response.
    fn execute<'a>(&'a self, ctx: &'a ConnCtx<'a>, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
}
