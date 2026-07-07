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

use crate::{AclManager, AuthenticatedUser};
use crate::client_registry::ClientRegistry;
use crate::command_spec::CommandSpec;
use crate::hotkeys::SharedHotkeySession;
use crate::keyspace_stats::KeyspaceStats;
use crate::latency_histogram::CommandLatencyHistograms;
use crate::registry::CommandRegistry;
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

/// Memory-diagnostics report generation for MEMORY DOCTOR, abstracted so the
/// connection-command seam can live in `core` without naming the server's
/// `frogdb_debug` collector. The server implements this for a wrapper over its
/// `MemoryDiagConfig`.
pub trait MemoryDiagProvider: Send + Sync {
    /// Collect memory diagnostics across `shard_senders` and format the
    /// human-readable MEMORY DOCTOR report.
    fn doctor_report<'a>(&'a self, shard_senders: &'a [ShardSender]) -> BoxFuture<'a, String>;
}

/// A single aggregate-cursor result row: ordered `(field, value)` pairs.
pub type CursorRow = Vec<(String, String)>;

/// The result of an FT.CURSOR READ: a batch of rows plus the next cursor id
/// (`0` when the cursor is exhausted).
pub type CursorReadBatch = (Vec<CursorRow>, u64);

/// Aggregate-cursor paging operations the FT.CURSOR command needs, abstracted so
/// the connection-command seam can live in `core` without naming the server's
/// `AggregateCursorStore`. The server implements this for `AggregateCursorStore`.
pub trait CursorStoreProvider: Send + Sync {
    /// Read the next batch from cursor `id`, validating it belongs to
    /// `expected_index`. Returns `Some((batch, new_cursor_id))` with
    /// `new_cursor_id == 0` when the cursor is exhausted, or `None` if the cursor
    /// does not exist (or its index does not match). `count` overrides the
    /// cursor's stored batch size (FT.CURSOR READ ... COUNT).
    fn read_cursor(
        &self,
        id: u64,
        count: Option<usize>,
        expected_index: &str,
    ) -> Option<CursorReadBatch>;

    /// Delete cursor `id` (FT.CURSOR DEL). Returns `true` if it existed.
    fn delete_cursor(&self, id: u64) -> bool;
}

/// Full INFO rendering, abstracted so the connection-command seam can live in
/// `core` without naming the server's fleet-aggregation types — `InfoSources`,
/// `InfoBuilder`, the replication/cluster state, and the metrics recorder all
/// live in the server crate. The server implements this for its
/// `ConnectionHandler`: `render` gathers every INFO source (a single fleet
/// scatter plus the connection-level snapshots) and renders the sections the
/// arguments select, returning the wire bulk-string response.
///
/// INFO is pure read aggregation, so this provider is entirely read-only. It is
/// a single `render` method rather than one accessor per source because
/// `InfoSources` bundles server-only types that `core` cannot name; the whole
/// gather-and-render therefore happens in the server-side impl.
pub trait InfoProvider: Send + Sync {
    /// Gather all INFO sources and render the sections selected by `args`
    /// (empty `args` = the default section set). Returns a boxed future (for
    /// object safety) resolving to the wire bulk-string response.
    fn render<'a>(&'a self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
}

/// A no-op [`InfoProvider`] for `ConnCtx` fixtures whose command under test does
/// not read INFO. `render` returns an empty bulk string, so a fixture can supply
/// `info: &NoopInfoProvider` without wiring up the full INFO aggregation.
pub struct NoopInfoProvider;

impl InfoProvider for NoopInfoProvider {
    fn render<'a>(&'a self, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::bulk(Bytes::new()) })
    }
}

/// The connection-state *mutation* capability for connection commands that must
/// change per-connection auth/protocol state (AUTH, HELLO, and — following this
/// same pattern — the upcoming CLIENT and connection-state RESET/ASKING/READONLY
/// migrations).
///
/// # Why this exists (the mutable-`ConnCtx` mechanism)
///
/// The six connection commands migrated before AUTH/HELLO are pure reads: they
/// take `&ConnCtx` and only ever *read* their subsystems. AUTH/HELLO are the
/// first connection commands that must *write* connection state — authenticate a
/// user, switch RESP2/RESP3, set the client name, latch HELLO-received. The seam
/// exposes that as this object-safe trait, held on [`ConnCtx::conn_state`] as an
/// `Option<&mut dyn ConnStateMut>`:
///
/// * pure-read connection commands carry `conn_state: None` and ignore it;
/// * mutating connection commands (dispatched via a dedicated `&mut`-capable
///   `ConnCtx` builder) carry `conn_state: Some(&mut ..)` and drive state through
///   it, while [`ConnectionCommand::execute`] takes `&mut ConnCtx` so the inner
///   `&mut` can be reborrowed.
///
/// The trait lives in `core` (like [`ConfigProvider`]) so the seam never names
/// the server's `ConnectionState`; the server implements it for that type.
/// `Send + Sync` is required because the boxed `Send` future returned from
/// `execute` captures a `&ConnCtx` (pure-read commands) or `&mut ConnCtx`
/// (mutating commands), so `ConnCtx` — and therefore this trait object — must be
/// both `Send` and `Sync`.
pub trait ConnStateMut: Send + Sync {
    /// The connection id (`HELLO` reply `id` field, ACL-log correlation).
    fn id(&self) -> u64;

    /// The current negotiated protocol version. HELLO reads this *after*
    /// [`set_protocol_version`](Self::set_protocol_version) to shape its reply.
    fn protocol_version(&self) -> ProtocolVersion;

    /// The `id=.. addr=.. name=..` client descriptor recorded in the ACL log on
    /// an authentication attempt.
    fn client_info(&self) -> String;

    /// Record a successful authentication as `user` (AUTH / HELLO AUTH).
    fn authenticate(&mut self, user: AuthenticatedUser);

    /// Switch the negotiated RESP protocol version (HELLO protover).
    fn set_protocol_version(&mut self, version: ProtocolVersion);

    /// Set (`Some`) or clear (`None`/empty) the client name (HELLO SETNAME).
    fn set_name(&mut self, name: Option<Bytes>);

    /// Latch that HELLO has been received on this connection (and when).
    fn mark_hello_received(&mut self);
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
    /// Aggregate-cursor store for FT.CURSOR READ/DEL paging.
    pub cursor_store: &'a dyn CursorStoreProvider,
    /// Server metrics recorder for SLO latency bands (LATENCY BANDS).
    pub metrics_recorder: &'a dyn crate::MetricsRecorder,
    /// Memory-diagnostics report generator (MEMORY DOCTOR).
    pub memory_diag: &'a dyn MemoryDiagProvider,
    /// Total shard count for machine-readable status reporting (STATUS JSON).
    pub num_shards: usize,
    /// Configured maximum client connections (STATUS JSON).
    pub max_clients: u64,
    /// ACL user store for ACL SETUSER/GETUSER/DELUSER/LIST/USERS/CAT/LOG/SAVE/
    /// LOAD/etc. Named directly (a `core` type) rather than behind a bespoke
    /// provider like [`ConfigProvider`].
    pub acl_manager: &'a AclManager,
    /// The command registry, so ACL DRYRUN can resolve a target command's key
    /// spec and flags (its access type and key positions) for the simulated
    /// key-permission check. Named directly (a `core` type).
    pub command_registry: &'a CommandRegistry,
    /// The connection's authenticated username (ACL WHOAMI). Read-only view of
    /// the connection's auth identity.
    pub username: &'a str,
    /// Fleet-and-connection INFO aggregation/rendering (INFO). Read-only.
    pub info: &'a dyn InfoProvider,
    /// Mutable connection-state capability, present only for connection commands
    /// that change per-connection auth/protocol state (AUTH, HELLO). `None` for
    /// pure-read connection commands, which never touch it. See
    /// [`ConnStateMut`] for the full rationale of this mechanism.
    pub conn_state: Option<&'a mut dyn ConnStateMut>,
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
    ///
    /// `ctx` is `&mut` so a mutating connection command can reborrow
    /// [`ConnCtx::conn_state`] (`Some(&mut dyn ConnStateMut)`). Pure-read
    /// commands ignore it and use only the shared subsystem borrows.
    fn execute<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Response>;
}
