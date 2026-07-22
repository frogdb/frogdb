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
use crate::registry::CommandRegistry;
use crate::shard::ShardSender;
use crate::{AclManager, AuthenticatedUser};

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

/// What a RESET found active on the connection, returned by
/// [`ConnStateMut::reset`] so the executor can drive the I/O half of RESET (the
/// shard `ConnectionClosed` fan-out) that the pure state reset cannot reach.
///
/// The reset of the `ConnectionState` fields themselves is funnelled through the
/// type's own reset method; this only reports what needed the follow-up I/O.
#[derive(Debug, Clone, Copy)]
pub struct ResetOutcome {
    /// The connection had active pub/sub subscriptions before the reset.
    pub was_in_pubsub: bool,
    /// Client tracking was enabled before the reset.
    pub tracking_was_enabled: bool,
}

/// Metrics captured by a DISCARD that dropped an open transaction, returned by
/// [`ConnStateMut::discard`] so the executor can record the `discarded`
/// transaction metric. Mirrors the server's `TxnMetrics` in core-nameable terms
/// (the executor cannot name the server type).
#[derive(Debug, Clone, Copy)]
pub struct TxnDiscardOutcome {
    /// Number of commands that had been queued.
    pub queued_count: usize,
    /// When MULTI was issued, for duration metrics.
    pub start_time: Option<std::time::Instant>,
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

/// Machine-readable server-status rendering (STATUS JSON), abstracted so the
/// connection-command seam can live in `core` without naming the server's status
/// collector or telemetry's `ServerStatus` type. Like [`InfoProvider`], the whole
/// gather-and-render runs server-side behind one method: STATUS JSON and the HTTP
/// `/status` endpoint share the *same* collector, so the two surfaces can never
/// disagree. The server implements this for its `ConnectionHandler`.
pub trait StatusProvider: Send + Sync {
    /// Collect and render the current server status as the machine-readable JSON
    /// bulk-string response. Returns a boxed future (for object safety).
    fn status_json<'a>(&'a self) -> BoxFuture<'a, Response>;
}

/// A no-op [`StatusProvider`] for `ConnCtx` fixtures whose command under test is
/// not STATUS. `status_json` returns an error, so a fixture can rely on the
/// default without wiring up a status collector.
pub struct NoopStatusProvider;

impl StatusProvider for NoopStatusProvider {
    fn status_json<'a>(&'a self) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR status unavailable") })
    }
}

/// The scripting/function entry points a [`ConnectionCommand`] needs to drive
/// EVAL/EVALSHA/SCRIPT/FCALL/FUNCTION, abstracted so the connection-command seam
/// can live in `core` without naming the server's script executor, Lua VM,
/// function registry, or cross-shard EVAL coordinator. The server implements
/// this for its `ConnectionHandler`, delegating to the existing
/// `handlers/scripting/*` helpers (the eval engine and cross-shard VLL
/// continuation logic stay in the server crate — only the *entry point* moves
/// behind the seam).
///
/// Every method returns a boxed future (for object safety) resolving to the wire
/// response. `read_only` selects EVAL_RO/EVALSHA_RO/FCALL_RO semantics (write
/// rejection + replica eligibility).
pub trait ScriptingProvider: Send + Sync {
    /// EVAL / EVAL_RO — execute a Lua script (source), routing to the owning
    /// shard(s) with cross-shard continuation locks and CROSSSLOT enforcement.
    fn eval<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response>;

    /// EVALSHA / EVALSHA_RO — execute a cached Lua script by SHA1
    /// (NOSCRIPT when uncached).
    fn evalsha<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response>;

    /// SCRIPT LOAD / EXISTS / FLUSH / KILL / HELP — manage the script cache.
    fn script<'a>(&'a self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;

    /// FCALL / FCALL_RO — call a registered function on the owning shard.
    fn fcall<'a>(&'a self, args: &'a [Bytes], read_only: bool) -> BoxFuture<'a, Response>;

    /// FUNCTION LOAD/DELETE/FLUSH/LIST/DUMP/RESTORE/STATS/KILL/HELP — manage the
    /// function-library registry.
    fn function<'a>(&'a self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
}

/// A no-op [`ScriptingProvider`] for `ConnCtx` fixtures whose command under test
/// is not a scripting command. Every method returns an error response, so a
/// fixture can supply `scripting: &NoopScriptingProvider` without wiring up the
/// script executor / function registry.
pub struct NoopScriptingProvider;

impl ScriptingProvider for NoopScriptingProvider {
    fn eval<'a>(&'a self, _args: &'a [Bytes], _read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR scripting unavailable") })
    }
    fn evalsha<'a>(&'a self, _args: &'a [Bytes], _read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR scripting unavailable") })
    }
    fn script<'a>(&'a self, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR scripting unavailable") })
    }
    fn fcall<'a>(&'a self, _args: &'a [Bytes], _read_only: bool) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR scripting unavailable") })
    }
    fn function<'a>(&'a self, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
        Box::pin(async { Response::error("ERR scripting unavailable") })
    }
}

/// The tracking mode reported by CLIENT TRACKINGINFO, mirrored from the
/// server's `TrackingMode` so the connection-command seam need not name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingModeView {
    /// Default mode: all reads tracked.
    Default,
    /// Opt-in mode: reads tracked only after CLIENT CACHING YES.
    OptIn,
    /// Opt-out mode: reads tracked unless CLIENT CACHING NO.
    OptOut,
    /// Broadcast mode: prefix-based invalidation, no per-read tracking.
    Broadcast,
}

/// A read-only snapshot of a connection's client-tracking state for
/// CLIENT TRACKINGINFO / GETREDIR / CACHING gating (see
/// [`ConnStateMut::tracking_info`]). Mirrors the server's `TrackingState` in
/// core-nameable terms.
pub struct TrackingInfoView {
    /// Whether tracking is enabled.
    pub enabled: bool,
    /// The active tracking mode.
    pub mode: TrackingModeView,
    /// NOLOOP flag.
    pub noloop: bool,
    /// Per-command caching override (`Some(true)` = YES, `Some(false)` = NO).
    pub caching_override: Option<bool>,
    /// REDIRECT target connection id (0 = no redirect).
    pub redirect: u64,
    /// BCAST registered prefixes.
    pub prefixes: Vec<Bytes>,
}

/// The connection-level IO plumbing for CLIENT TRACKING that lives on the
/// server's `ConnectionHandler` rather than on `ConnectionState`: the
/// invalidation delivery channel (or REDIRECT forwarding task) and per-shard
/// tracking registration.
///
/// The `ConnectionState` transition ([`ConnStateMut::enable_tracking`] /
/// [`ConnStateMut::disable_tracking`]) runs first and hands the computed BCAST
/// prefixes to [`enable`](Self::enable); this trait performs only the IO half.
/// It is kept off [`ConnStateMut`] because it needs the handler's shard senders
/// and task/channel handles, which `ConnectionState` does not own. Named only in
/// core terms ([`Bytes`], [`ShardSender`]) so the seam stays server-agnostic.
pub trait ClientTrackingProvider: Send + Sync {
    /// Wire the invalidation delivery path (a REDIRECT forwarding task when
    /// `redirect > 0`, otherwise the connection's own invalidation channel) and
    /// register with every shard (broadcast registration when `bcast`).
    fn enable<'a>(
        &'a mut self,
        conn_id: u64,
        redirect: u64,
        bcast: bool,
        noloop: bool,
        prefixes: Vec<Bytes>,
        shard_senders: &'a [ShardSender],
    ) -> BoxFuture<'a, ()>;

    /// Unregister tracking from every shard and tear down local plumbing
    /// (invalidation channel + redirect forwarding task).
    fn disable<'a>(
        &'a mut self,
        conn_id: u64,
        shard_senders: &'a [ShardSender],
    ) -> BoxFuture<'a, ()>;
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

    /// Reset the connection to its initial server-side context (RESET command):
    /// exit pub/sub, clear tracking + transaction state, reset the protocol to
    /// RESP2, and clear the client name. Returns a [`ResetOutcome`] describing
    /// what was active so the executor can drive the I/O half (shard
    /// notifications). The whole state reset is funnelled through the connection
    /// type's own reset method rather than exposing each field.
    ///
    /// Per Redis `resetCommand`, RESET also reverts authentication to the
    /// default user — see [`revert_to_default_user`](Self::revert_to_default_user),
    /// which the executor drives after this returns (it needs the ACL manager,
    /// unavailable to the pure state reset, to re-evaluate the auth flag).
    fn reset(&mut self) -> ResetOutcome;

    /// Revert the connection's authentication to the default user and re-evaluate
    /// whether it counts as authenticated (RESET). Mirrors Redis `resetCommand`,
    /// which sets `c->user = DefaultUser` and marks the connection authenticated
    /// only when the default user is `nopass` and enabled. The executor computes
    /// `authenticated` from the ACL manager and passes it here.
    fn revert_to_default_user(&mut self, authenticated: bool);

    /// Set the one-shot cluster ASKING flag (ASKING command).
    fn set_asking(&mut self);

    /// Set (`true`) or clear (`false`) the READONLY replica-read flag
    /// (READONLY / READWRITE commands).
    fn set_readonly(&mut self, readonly: bool);

    // ---- CLIENT ----

    /// The current client name (CLIENT GETNAME), if set.
    fn name(&self) -> Option<Bytes>;

    /// Enable replies (CLIENT REPLY ON).
    fn reply_on(&mut self);

    /// Disable replies (CLIENT REPLY OFF).
    fn reply_off(&mut self);

    /// Suppress the next reply (CLIENT REPLY SKIP).
    fn reply_skip_next(&mut self);

    /// Set the one-shot per-command caching override (CLIENT CACHING YES/NO).
    fn set_caching_override(&mut self, track: bool);

    /// A read-only snapshot of the connection's client-tracking state
    /// (CLIENT TRACKINGINFO / GETREDIR / CACHING gating).
    fn tracking_info(&self) -> TrackingInfoView;

    /// Apply the CLIENT TRACKING ON state transition, returning the BCAST
    /// prefixes to register with the shards (empty for non-BCAST modes).
    /// Enforces Redis's flag-compatibility rules in Redis's check order; on
    /// rejection the `Err` string is the raw human-readable reason (the caller
    /// prefixes `ERR `). The IO half is driven by [`ClientTrackingProvider`].
    fn enable_tracking(
        &mut self,
        bcast: bool,
        optin: bool,
        optout: bool,
        noloop: bool,
        prefixes: Vec<Bytes>,
        redirect: u64,
    ) -> Result<Vec<Bytes>, String>;

    /// Apply the CLIENT TRACKING OFF state transition. Returns `true` if
    /// tracking had been enabled (so the caller must run the IO teardown via
    /// [`ClientTrackingProvider::disable`]).
    fn disable_tracking(&mut self) -> bool;

    /// Flush this connection's buffered per-client stats into `registry`
    /// (CLIENT STATS forces a sync before reading the registry).
    fn sync_stats_to_registry(&mut self, registry: &ClientRegistry);

    // ---- Transactions (MULTI / DISCARD / WATCH / UNWATCH) ----
    //
    // The transaction *state machine* mutations live here so the transaction
    // connection commands drive them through the seam. EXEC's orchestration
    // (draining the queue over shards + running deferred connection-level
    // commands) needs the whole `ConnectionHandler` and stays in the connection
    // layer; only the pure state transitions are on this trait.

    /// Begin a MULTI transaction. Returns `false` if a transaction is already
    /// open (nested MULTI), leaving the existing transaction untouched. Existing
    /// watches (WATCH before MULTI) are preserved.
    fn begin_multi(&mut self) -> bool;

    /// Whether a transaction block is currently open (MULTI issued, awaiting
    /// EXEC/DISCARD). Read by WATCH to reject `WATCH inside MULTI`.
    fn is_in_multi(&self) -> bool;

    /// Record a watched key with its watch-time version, owning shard, and
    /// liveness (WATCH). `live_at_watch` is the `wk->expired` inverse (present
    /// and unexpired when watched), carried per key into EXEC. The watched
    /// shards are folded into the transaction target at EXEC time (see
    /// `take_transaction`), from the live post-UNWATCH watch set.
    fn watch_key(&mut self, key: Bytes, shard_id: usize, version: u64, live_at_watch: bool);

    /// Forget all watched keys (UNWATCH).
    fn unwatch(&mut self);

    /// Drop an open transaction including its watches (DISCARD). Returns the
    /// discard metrics for the caller to record, or `None` for DISCARD without
    /// MULTI.
    fn discard(&mut self) -> Option<TxnDiscardOutcome>;
}

/// The connection-local pub/sub machinery for the SUBSCRIBE/UNSUBSCRIBE/PUBLISH
/// family, abstracted so the connection-command seam can live in `core` without
/// naming the server's `ConnectionHandler`, its cluster routing, its
/// scatter-gather introspection, or its lazily-created pub/sub channel.
///
/// # Why this is one method per command rather than a narrow accessor set
///
/// Pub/sub is the most deeply connection-entangled command group: a single
/// SUBSCRIBE interleaves per-connection subscription-set mutation, the lazy
/// pub/sub channel init (`pubsub_tx`/`pubsub_rx`), per-shard batched
/// registration, cluster slot-migration routing, and per-channel reply framing;
/// PUBSUB introspection fans out through scatter-gather. None of that is
/// expressible as a handful of `ConnCtx` field borrows without re-plumbing half
/// the handler. So — exactly like [`InfoProvider`] — the whole operation runs
/// server-side behind one method per command, and the executor
/// ([`crate::registry::CommandImpl::Connection`]) is a thin dispatch over the
/// command's kind. The server implements this over a narrow bundle of disjoint
/// `&mut`/`&` borrows of the handler (state + channel + shard senders + cluster),
/// held on [`ConnCtx::pubsub`] as `Some(&mut ..)` for the pub/sub executor only.
///
/// Each subscribe/publish method performs its own channel-access ACL check
/// (mirroring the pre-migration `dispatch_pubsub` gate) before touching state,
/// so the executor need not thread a separate permission seam. The
/// SUBSCRIBE/UNSUBSCRIBE family returns one reply per channel (a `Vec<Response>`,
/// surfaced through [`ConnectionCommand::execute_multi`]); PUBLISH/SPUBLISH and
/// PUBSUB return a single `Response`.
pub trait PubSubProvider: Send + Sync {
    /// SUBSCRIBE — validate channel access, then subscribe to each channel.
    fn subscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// UNSUBSCRIBE — unsubscribe from the given channels (all, if none given).
    fn unsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// PSUBSCRIBE — validate channel access, then subscribe to each pattern.
    fn psubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// PUNSUBSCRIBE — unsubscribe from the given patterns (all, if none given).
    fn punsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// SSUBSCRIBE — validate channel access, then subscribe to each sharded
    /// channel (with per-channel cluster slot-migration routing).
    fn ssubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// SUNSUBSCRIBE — unsubscribe from the given sharded channels.
    fn sunsubscribe<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Vec<Response>>;
    /// PUBLISH — validate channel access, then broadcast a message.
    fn publish<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
    /// SPUBLISH — validate channel access, then publish to a sharded channel.
    fn spublish<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
    /// PUBSUB — CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS/SHARDNUMSUB/HELP.
    fn pubsub<'a>(&'a mut self, args: &'a [Bytes]) -> BoxFuture<'a, Response>;
}

/// The handler-only capabilities the DEBUG command needs that a `ConnCtx` cannot
/// expose as plain field borrows: the `enable-debug-command` gate, the shared
/// tracer, per-shard round-trips (VLL / active-expire / keysizes / allocsize),
/// this connection's own pub/sub subscription counts, and diagnostic-bundle
/// generation. Abstracted so the connection-command seam can live in `core`
/// without naming the server's `ConnectionHandler`, its `SharedTracer`, or the
/// `frogdb_debug` bundle machinery. The server implements this for its
/// `ConnectionHandler`.
///
/// DEBUG's subcommand routing and argument parsing live in the executor
/// (`DebugConnCommand`); this trait carries only the per-subcommand I/O, exactly
/// mirroring how [`InfoProvider`] / [`ScriptingProvider`] keep server-only work
/// behind the seam. Methods return already-parsed values (or the raw shard data
/// the executor then formats) rather than re-parsing `args`.
pub trait DebugProvider: Send + Sync {
    /// Whether `server.enable-debug-command` permits DEBUG SLEEP.
    fn debug_command_enabled(&self) -> bool;

    /// DEBUG TRACING STATUS — render the tracer's status block.
    fn tracing_status(&self) -> Response;

    /// DEBUG TRACING RECENT [count] — render up to `count` recent traces.
    fn tracing_recent(&self, count: usize) -> Response;

    /// DEBUG VLL [shard_id] — gather VLL queue info from the selected shard, or
    /// every shard when `shard_filter` is `None`. The executor formats the reply.
    fn gather_vll<'a>(
        &'a self,
        shard_filter: Option<usize>,
    ) -> BoxFuture<'a, Vec<crate::shard::VllQueueInfo>>;

    /// DEBUG LOCKTABLE — the per-shard VLL lock-table snapshots (all shards).
    fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::LockTableInfo>>;

    /// DEBUG WAITQUEUE — the per-shard blocking-waiter snapshots (all shards).
    fn gather_wait_queue<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::WaitQueueInfo>>;

    /// DEBUG MEMORY-CHECK — per-shard tracked-vs-recomputed memory (all shards).
    fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::MemoryCheckInfo>>;

    /// DEBUG EXPIRY-INDEX-CHECK — per-shard index-vs-deadline audit (all shards).
    fn expiry_index_check<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::ExpiryIndexCheckInfo>>;

    /// DEBUG PUBSUB LIMITS — this connection's and the fleet's subscription usage
    /// against the configured maxima (a whole-reply subcommand: it reads
    /// connection-local subscription counts the executor cannot see).
    fn pubsub_limits<'a>(&'a self) -> BoxFuture<'a, Response>;

    /// DEBUG BUNDLE GENERATE [DURATION <seconds>] — collect and store a
    /// diagnostic bundle, returning its id.
    fn bundle_generate<'a>(&'a self, duration_secs: u64) -> BoxFuture<'a, Response>;

    /// DEBUG BUNDLE LIST — list stored diagnostic bundles.
    fn bundle_list(&self) -> Response;

    /// DEBUG SET-ACTIVE-EXPIRE 0|1 — toggle active expiration across all shards,
    /// waiting for each shard's acknowledgment.
    fn set_active_expire<'a>(&'a self, enabled: bool) -> BoxFuture<'a, ()>;

    /// DEBUG KEYSIZES-HIST-ASSERT — the keysize histograms merged across every
    /// shard. The executor resolves the requested type/bin and compares.
    fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, crate::KeysizeHistograms>;

    /// DEBUG ALLOCSIZE-SLOTS-ASSERT — total allocated memory for keys in `slot`,
    /// summed across every shard. The executor compares against the expectation.
    fn allocsize_in_slot<'a>(&'a self, slot: u16) -> BoxFuture<'a, usize>;
}

/// The connection-local MONITOR machinery, abstracted so the connection-command
/// seam can live in `core` without naming the server's `ConnectionHandler` or
/// its monitor broadcast channel.
///
/// MONITOR turns the connection into a monitor: it registers the connection to
/// receive the feed of all executed commands and replies `+OK`; the actual
/// streaming is driven by the connection run-loop (exactly like SUBSCRIBE — the
/// command only registers, the loop streams). Registration mutates a single
/// connection-local field (the monitor receiver), which is reachable neither
/// through [`ConnStateMut`] (it lives on the handler, not the connection state)
/// nor through a field borrow the seam could name without pulling the server's
/// broadcaster type into `core`. So — like [`PubSubProvider`] — it runs
/// server-side behind one method, held on [`ConnCtx::monitor`] as `Some(&mut ..)`
/// for the MONITOR executor only.
pub trait MonitorProvider: Send + Sync {
    /// Register this connection as a monitor: subscribe it to the executed-command
    /// feed so the run-loop begins streaming. Idempotent (re-running MONITOR
    /// re-subscribes).
    fn enable_monitor(&mut self);
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
    /// Whether cluster support is enabled (Redis `server.cluster_enabled`).
    /// READONLY/READWRITE/ASKING reject with
    /// `-ERR This instance has cluster support disabled` when this is `false`,
    /// matching Redis's `readonlyCommand`/`readwriteCommand`/`askingCommand`.
    pub cluster_enabled: bool,
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
    /// Machine-readable server-status rendering (STATUS JSON). Read-only; shares
    /// the same collector as the HTTP `/status` endpoint. Defaults to the no-op
    /// provider; the read-only dispatch path layers the live one via
    /// [`with_status`](Self::with_status).
    pub status: &'a dyn StatusProvider,
    /// Scripting/function entry points (EVAL/EVALSHA/SCRIPT/FCALL/FUNCTION).
    /// Read-only: the executor / function registry / cross-shard EVAL
    /// coordinator all live behind [`ScriptingProvider`].
    pub scripting: &'a dyn ScriptingProvider,
    /// Mutable connection-state capability, present only for connection commands
    /// that change per-connection auth/protocol state (AUTH, HELLO). `None` for
    /// pure-read connection commands, which never touch it. See
    /// [`ConnStateMut`] for the full rationale of this mechanism.
    pub conn_state: Option<&'a mut dyn ConnStateMut>,
    /// Client-tracking IO plumbing (CLIENT TRACKING ON/OFF): the invalidation
    /// delivery channel / REDIRECT forwarding task and per-shard registration.
    /// Present only for the CLIENT executor (a disjoint `&mut` borrow of the
    /// handler's tracking IO, alongside `conn_state`'s `&mut` of the state).
    /// `None` for every other connection command. See [`ClientTrackingProvider`].
    pub tracking: Option<&'a mut dyn ClientTrackingProvider>,
    /// Connection-local pub/sub machinery (SUBSCRIBE/UNSUBSCRIBE/PUBLISH family):
    /// the subscription set + lazy pub/sub channel + per-shard registration +
    /// cluster routing + scatter-gather introspection. Present only for the
    /// pub/sub executor (dispatched via a dedicated mutable builder that bundles
    /// the disjoint handler borrows the family needs); `None` for every other
    /// connection command. See [`PubSubProvider`].
    pub pubsub: Option<&'a mut dyn PubSubProvider>,
    /// Handler-only DEBUG capabilities (tracer, per-shard round-trips, bundle
    /// generation, connection subscription counts, the debug-command gate).
    /// Present only for the DEBUG executor, which dispatches through the read-only
    /// `conn_ctx` builder; `None` for every other connection command. See
    /// [`DebugProvider`].
    pub debug: Option<&'a dyn DebugProvider>,
    /// Connection-local MONITOR machinery (register this connection to receive
    /// the executed-command feed). Present only for the MONITOR executor
    /// (dispatched via a dedicated mutable builder that borrows the handler's
    /// monitor receiver + broadcaster); `None` for every other connection
    /// command. See [`MonitorProvider`].
    pub monitor: Option<&'a mut dyn MonitorProvider>,
}

impl<'a> ConnCtx<'a> {
    /// The one place the ambient-field list is authored: every production
    /// dispatch path and every test fixture constructs its `ConnCtx` through
    /// here, so adding a subsystem to the seam is a one-site edit (this
    /// signature plus the struct field), not a lockstep sweep over hand-copied
    /// literals.
    ///
    /// Capability slots (`conn_state`/`tracking`/`pubsub`/`debug`/`monitor`)
    /// default to absent; `info`/`scripting` default to the no-op providers and
    /// the connection identity (`protocol_version`/`username`) to placeholders.
    /// The read-only dispatch path overrides all five via
    /// [`with_full_reads`](Self::with_full_reads); a mutable dispatch path
    /// leaves them — its command reads identity through `conn_state`, never
    /// through the placeholder fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &'a dyn ConfigProvider,
        client_registry: &'a ClientRegistry,
        latency_histograms: &'a CommandLatencyHistograms,
        keyspace_stats: &'a KeyspaceStats,
        shard_senders: &'a [ShardSender],
        snapshot_coordinator: &'a dyn crate::persistence::SnapshotCoordinator,
        hotkey_session: &'a SharedHotkeySession,
        hotkey_cluster: &'a dyn HotkeyClusterProvider,
        cursor_store: &'a dyn CursorStoreProvider,
        metrics_recorder: &'a dyn crate::MetricsRecorder,
        memory_diag: &'a dyn MemoryDiagProvider,
        acl_manager: &'a AclManager,
        command_registry: &'a CommandRegistry,
        num_shards: usize,
        max_clients: u64,
        cluster_enabled: bool,
    ) -> Self {
        // The no-op defaults, authored once (the providers are zero-sized, so
        // the references promote to `'static`).
        const NOOP_INFO: &NoopInfoProvider = &NoopInfoProvider;
        const NOOP_SCRIPTING: &NoopScriptingProvider = &NoopScriptingProvider;
        const NOOP_STATUS: &NoopStatusProvider = &NoopStatusProvider;
        ConnCtx {
            config,
            client_registry,
            latency_histograms,
            keyspace_stats,
            shard_senders,
            snapshot_coordinator,
            hotkey_session,
            hotkey_cluster,
            cursor_store,
            metrics_recorder,
            memory_diag,
            acl_manager,
            command_registry,
            num_shards,
            max_clients,
            cluster_enabled,
            // Placeholders: a read-only caller overrides these via
            // `with_full_reads`; a mutable caller reads identity through
            // `conn_state` and never renders INFO or runs scripts.
            protocol_version: ProtocolVersion::default(),
            username: "",
            info: NOOP_INFO,
            status: NOOP_STATUS,
            scripting: NOOP_SCRIPTING,
            conn_state: None,
            tracking: None,
            pubsub: None,
            debug: None,
            monitor: None,
        }
    }

    /// Layer the mutable connection-state capability (AUTH/HELLO/CLIENT and the
    /// RESET/ASKING/READONLY/READWRITE family) onto the ambient view.
    pub fn with_conn_state(mut self, conn_state: &'a mut dyn ConnStateMut) -> Self {
        self.conn_state = Some(conn_state);
        self
    }

    /// Layer the client-tracking IO capability (CLIENT TRACKING) onto the
    /// ambient view.
    pub fn with_tracking(mut self, tracking: &'a mut dyn ClientTrackingProvider) -> Self {
        self.tracking = Some(tracking);
        self
    }

    /// Layer the pub/sub capability (SUBSCRIBE/UNSUBSCRIBE/PUBLISH family) onto
    /// the ambient view.
    pub fn with_pubsub(mut self, pubsub: &'a mut dyn PubSubProvider) -> Self {
        self.pubsub = Some(pubsub);
        self
    }

    /// Layer the MONITOR registration capability onto the ambient view.
    pub fn with_monitor(mut self, monitor: &'a mut dyn MonitorProvider) -> Self {
        self.monitor = Some(monitor);
        self
    }

    /// Override the authenticated-username placeholder (fixtures for commands
    /// that read identity without the `conn_state` capability, e.g. ACL WHOAMI).
    pub fn with_username(mut self, username: &'a str) -> Self {
        self.username = username;
        self
    }

    /// Layer the live server-status provider (STATUS JSON) onto the ambient view.
    /// The read-only dispatch path calls this so STATUS renders from the same
    /// collector as the HTTP `/status` endpoint; fixtures use it to inject a
    /// collector-backed provider.
    pub fn with_status(mut self, status: &'a dyn StatusProvider) -> Self {
        self.status = status;
        self
    }

    /// Read-only dispatch path only: replace every identity/read placeholder
    /// with the live values — the real INFO and scripting providers, the DEBUG
    /// capability, and the connection's negotiated protocol version and
    /// authenticated username. The placeholder defaults from [`new`](Self::new)
    /// must never leak into the read path; this is the override point.
    pub fn with_full_reads(
        mut self,
        info: &'a dyn InfoProvider,
        scripting: &'a dyn ScriptingProvider,
        debug: Option<&'a dyn DebugProvider>,
        protocol_version: ProtocolVersion,
        username: &'a str,
    ) -> Self {
        self.info = info;
        self.scripting = scripting;
        self.debug = debug;
        self.protocol_version = protocol_version;
        self.username = username;
        self
    }
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

    /// Extract this command's keys from `args` (the arguments *after* the command
    /// name), for the registry's key-based machinery (cluster slot validation,
    /// transaction key-folding, `COMMAND GETKEYSANDFLAGS`).
    ///
    /// The default derives keys from the declarative [`CommandSpec::keys`], which
    /// covers every keyless connection command and the static key patterns. A
    /// connection command whose spec is [`crate::command_spec::KeySpec::Dynamic`]
    /// (EVAL/EVALSHA/FCALL: keys located by a runtime `numkeys`) overrides this,
    /// mirroring [`crate::command::Command::dynamic_keys`] on the shard side.
    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        self.spec().keys.extract(args)
    }

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

    /// Execute the command, returning **one or more** wire responses.
    ///
    /// This is the multi-response seam for the pub/sub SUBSCRIBE/UNSUBSCRIBE
    /// family, which emits one confirmation frame *per channel* rather than a
    /// single reply. The default wraps the single-response [`execute`](Self::
    /// execute) in a one-element `Vec`, so every existing single-response
    /// connection command (CONFIG, AUTH, CLIENT, …) gets it for free with zero
    /// churn; only the pub/sub executor overrides it.
    ///
    /// Connection-level dispatch calls `execute_multi` uniformly and flattens
    /// the result onto the wire; the returned `Vec` is never empty for a
    /// well-formed command.
    fn execute_multi<'a>(
        &'a self,
        ctx: &'a mut ConnCtx<'a>,
        args: &'a [Bytes],
    ) -> BoxFuture<'a, Vec<Response>> {
        Box::pin(async move { vec![self.execute(ctx, args).await] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoopMetricsRecorder, new_shared_hotkey_session};

    struct StubConfig;
    impl ConfigProvider for StubConfig {
        fn get(&self, _pattern: &str) -> Vec<(String, String)> {
            Vec::new()
        }
        fn set<'a>(&'a self, _name: &'a str, _value: &'a str) -> BoxFuture<'a, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn rewrite(&self) -> Result<(), String> {
            Ok(())
        }
        fn help(&self) -> Vec<String> {
            Vec::new()
        }
    }

    struct StubCluster;
    impl HotkeyClusterProvider for StubCluster {
        fn is_cluster_mode(&self) -> bool {
            false
        }
        fn node_handles_slot(&self, _slot: u16) -> bool {
            true
        }
        fn node_slot_list(&self) -> Vec<u16> {
            Vec::new()
        }
    }

    struct StubCursorStore;
    impl CursorStoreProvider for StubCursorStore {
        fn read_cursor(
            &self,
            _id: u64,
            _count: Option<usize>,
            _expected_index: &str,
        ) -> Option<CursorReadBatch> {
            None
        }
        fn delete_cursor(&self, _id: u64) -> bool {
            false
        }
    }

    struct StubMemoryDiag;
    impl MemoryDiagProvider for StubMemoryDiag {
        fn doctor_report<'a>(&'a self, _shard_senders: &'a [ShardSender]) -> BoxFuture<'a, String> {
            Box::pin(async { String::new() })
        }
    }

    /// An [`InfoProvider`] whose render is distinguishable from the no-op
    /// default, so the test can prove `with_full_reads` replaced it.
    struct MarkerInfo;
    impl InfoProvider for MarkerInfo {
        fn render<'a>(&'a self, _args: &'a [Bytes]) -> BoxFuture<'a, Response> {
            Box::pin(async { Response::bulk(Bytes::from_static(b"marker")) })
        }
    }

    /// Slot-filler for the `debug` capability; the test never invokes it.
    struct StubDebug;
    impl DebugProvider for StubDebug {
        fn debug_command_enabled(&self) -> bool {
            true
        }
        fn tracing_status(&self) -> Response {
            unimplemented!()
        }
        fn tracing_recent(&self, _count: usize) -> Response {
            unimplemented!()
        }
        fn gather_vll<'a>(
            &'a self,
            _shard_filter: Option<usize>,
        ) -> BoxFuture<'a, Vec<crate::shard::VllQueueInfo>> {
            unimplemented!()
        }
        fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::LockTableInfo>> {
            unimplemented!()
        }
        fn gather_wait_queue<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::WaitQueueInfo>> {
            unimplemented!()
        }
        fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::MemoryCheckInfo>> {
            unimplemented!()
        }
        fn expiry_index_check<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<crate::shard::ExpiryIndexCheckInfo>> {
            unimplemented!()
        }
        fn pubsub_limits<'a>(&'a self) -> BoxFuture<'a, Response> {
            unimplemented!()
        }
        fn bundle_generate<'a>(&'a self, _duration_secs: u64) -> BoxFuture<'a, Response> {
            unimplemented!()
        }
        fn bundle_list(&self) -> Response {
            unimplemented!()
        }
        fn set_active_expire<'a>(&'a self, _enabled: bool) -> BoxFuture<'a, ()> {
            unimplemented!()
        }
        fn keysizes_snapshot<'a>(&'a self) -> BoxFuture<'a, crate::KeysizeHistograms> {
            unimplemented!()
        }
        fn allocsize_in_slot<'a>(&'a self, _slot: u16) -> BoxFuture<'a, usize> {
            unimplemented!()
        }
    }

    /// The one targeted assertion from the builder collapse: `ConnCtx::new`
    /// yields the documented placeholder/no-op/absent defaults, and the
    /// read-only dispatch override (`with_full_reads`) replaces every one of
    /// them — the placeholder defaults must never leak into the read path.
    #[tokio::test]
    async fn new_defaults_are_placeholders_and_with_full_reads_overrides_them() {
        let client_registry = ClientRegistry::new();
        let latency_histograms = CommandLatencyHistograms::new(true);
        let keyspace_stats = KeyspaceStats::new();
        let snapshot_coordinator = crate::persistence::NoopSnapshotCoordinator::new();
        let hotkey_session = new_shared_hotkey_session();
        let metrics_recorder = NoopMetricsRecorder::new();
        let acl_manager = AclManager::new(Default::default());
        let command_registry = CommandRegistry::new();

        let ctx = ConnCtx::new(
            &StubConfig,
            &client_registry,
            &latency_histograms,
            &keyspace_stats,
            &[],
            &snapshot_coordinator,
            &hotkey_session,
            &StubCluster,
            &StubCursorStore,
            &metrics_recorder,
            &StubMemoryDiag,
            acl_manager.as_ref(),
            &command_registry,
            0,
            10000,
            false,
        );

        // Defaults: placeholder identity, no-op read providers, no capabilities.
        assert_eq!(ctx.protocol_version, ProtocolVersion::default());
        assert_eq!(ctx.username, "");
        assert!(ctx.conn_state.is_none());
        assert!(ctx.tracking.is_none());
        assert!(ctx.pubsub.is_none());
        assert!(ctx.debug.is_none());
        assert!(ctx.monitor.is_none());
        // The default `info` is the no-op provider: an empty bulk string.
        assert_eq!(ctx.info.render(&[]).await, Response::bulk(Bytes::new()));
        // The default `scripting` is the no-op provider: an error response.
        assert_eq!(
            ctx.scripting.script(&[]).await,
            Response::error("ERR scripting unavailable")
        );

        // The read-only override replaces every placeholder.
        let info = MarkerInfo;
        let debug = StubDebug;
        let ctx = ctx.with_full_reads(
            &info,
            &NoopScriptingProvider,
            Some(&debug),
            ProtocolVersion::Resp3,
            "alice",
        );
        assert_eq!(ctx.protocol_version, ProtocolVersion::Resp3);
        assert_eq!(ctx.username, "alice");
        assert!(ctx.debug.is_some());
        assert_eq!(
            ctx.info.render(&[]).await,
            Response::bulk(Bytes::from_static(b"marker"))
        );
    }
}
