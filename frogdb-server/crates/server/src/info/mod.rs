//! Connection-level INFO section builder.
//!
//! Each INFO section is a deep module that owns *both* its data access and its
//! byte format: an [`InfoSection`] renders itself from an [`InfoSources`]
//! bundle that is gathered exactly once per INFO request. There is no
//! placeholder string, no `.replace`, no `.replace_range`, and no re-parsing of
//! a buffer the code just emitted — the stub-and-patch contract that used to
//! span `commands/info.rs` and `connection/scatter.rs` is gone.
//!
//! Layout invariants (`# Title\r\n`, `field:value\r\n`, trailing blank line)
//! live in [`SectionWriter`]; section selection (`default`/`all`/`everything`,
//! dedup, request order) lives in [`SectionSelector`]; assembly lives in
//! [`InfoBuilder`]. The single round of shard messaging lives in
//! [`gather_shard_snapshot`], so a section's `render` is a pure function of
//! already-collected data — it never scatters.
//!
//! The *shard-local* INFO in `crate::commands::info` remains only for scripts
//! (`redis.call('INFO')` executes on the shard); it reports the shard's own
//! view and no longer emits patch anchors.

mod sections;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{
    ClusterState, CommandLatencyHistograms, MetricsRecorder, ServerCommandStats, ShardSender,
};
use frogdb_protocol::Response;
use frogdb_replication::{
    BacklogGeometry, NetByteCountersSnapshot, Phase, ReplicaInfo, SyncCountersSnapshot,
};
use frogdb_telemetry::definitions::{CommandsTotal, WalBytes, WalWrites};
use frogdb_telemetry::{NodeStateSnapshot, ShardScatterError};
use tracing::warn;

// ============================================================================
// Section formatting
// ============================================================================

/// Owns the INFO text format invariants: `# Title\r\n` header, one
/// `field:value\r\n` line per field, and a trailing blank line (`\r\n`).
///
/// Every section writes through this type, so the CRLF/section-boundary
/// contract is one type's responsibility instead of an unwritten rule
/// re-derived by string patchers.
pub struct SectionWriter {
    buf: String,
    fields: usize,
}

impl SectionWriter {
    /// Start a section with its `# Title` header.
    pub fn new(title: &str) -> Self {
        Self {
            buf: format!("# {title}\r\n"),
            fields: 0,
        }
    }

    /// Emit `name:value`.
    pub fn field(&mut self, name: &str, value: impl std::fmt::Display) -> &mut Self {
        self.buf.push_str(name);
        self.buf.push(':');
        self.buf.push_str(&value.to_string());
        self.buf.push_str("\r\n");
        self.fields += 1;
        self
    }

    /// Emit `name:value` when `value` is `Some`, nothing when `None`.
    ///
    /// This is the honest rendering for sources that can be unavailable
    /// (e.g. counters living in a disabled metrics recorder): the field is
    /// *absent* rather than a plausible-looking stale `0`.
    pub fn field_opt(&mut self, name: &str, value: Option<impl std::fmt::Display>) -> &mut Self {
        if let Some(v) = value {
            self.field(name, v);
        }
        self
    }

    /// Emit a preformatted line (for non-`field:value` shapes like
    /// `db0:keys=N,expires=N` or `slave0:ip=...`).
    pub fn line(&mut self, line: &str) -> &mut Self {
        self.buf.push_str(line);
        self.buf.push_str("\r\n");
        self.fields += 1;
        self
    }

    /// Whether any field or line has been written after the header.
    pub fn has_fields(&self) -> bool {
        self.fields > 0
    }

    /// Finish the section: header + fields + trailing blank line.
    pub fn finish(mut self) -> String {
        self.buf.push_str("\r\n");
        self.buf
    }
}

// ============================================================================
// Section selection
// ============================================================================

/// Sections included in `INFO` with no args and `INFO default`.
const DEFAULT_SECTIONS: &[&str] = &[
    "server",
    "clients",
    "memory",
    "persistence",
    "stats",
    "replication",
    "cpu",
    "keyspace",
    "ratelimit",
];

/// Additional sections included only in `INFO all` / `INFO everything`.
const EXTRA_SECTIONS: &[&str] = &[
    "commandstats",
    "errorstats",
    "latencystats",
    "latency_baseline",
    "tiered",
    "keysizes",
];

/// Resolved, deduplicated, ordered list of requested section names.
///
/// Owns the `default`/`all`/`everything`/`""` alias expansion and the
/// first-mention-wins dedup of repeated section args.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionSelector {
    names: Vec<String>,
}

impl SectionSelector {
    /// Resolve raw INFO arguments into an ordered section list.
    pub fn from_args(args: &[Bytes]) -> Self {
        let mut names: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let push = |name: &str, names: &mut Vec<String>, seen: &mut HashSet<String>| {
            if seen.insert(name.to_string()) {
                names.push(name.to_string());
            }
        };
        let push_defaults = |names: &mut Vec<String>, seen: &mut HashSet<String>| {
            for name in DEFAULT_SECTIONS {
                push(name, names, seen);
            }
        };

        if args.is_empty() {
            push_defaults(&mut names, &mut seen);
        } else {
            for arg in args {
                let section = String::from_utf8_lossy(arg).to_ascii_lowercase();
                match section.as_str() {
                    "all" | "everything" => {
                        push_defaults(&mut names, &mut seen);
                        for name in EXTRA_SECTIONS {
                            push(name, &mut names, &mut seen);
                        }
                    }
                    // Empty string arg is treated like default in Redis.
                    "default" | "" => push_defaults(&mut names, &mut seen),
                    other => push(other, &mut names, &mut seen),
                }
            }
        }

        Self { names }
    }

    /// Whether `name` was requested.
    pub fn includes(&self, name: &str) -> bool {
        self.names.iter().any(|n| n == name)
    }

    /// Requested section names, in request order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.names.iter().map(String::as_str)
    }
}

// ============================================================================
// The section seam
// ============================================================================

/// One section = one owner of its data + its format. No stub, no anchor
/// string: `render` pulls every value from `src` and emits the final bytes,
/// including the `# Header\r\n` and trailing `\r\n`. It performs no I/O — the
/// sources are fully materialized by the time it runs.
pub trait InfoSection: Send + Sync {
    /// Stable section key (`"stats"`, `"commandstats"`, …) used for selection.
    fn name(&self) -> &'static str;

    /// Render the full section text from already-gathered sources.
    fn render(&self, src: &InfoSources) -> String;
}

/// Owns section registration and assembly.
pub struct InfoBuilder {
    sections: Vec<Box<dyn InfoSection>>,
}

impl InfoBuilder {
    /// The standard registry: every section FrogDB serves, in canonical order.
    pub fn standard() -> Self {
        Self {
            sections: sections::all_sections(),
        }
    }

    /// Render the requested sections, in request order. Unknown section names
    /// contribute nothing (matching Redis, which ignores unknown sections).
    pub fn render(&self, requested: &SectionSelector, src: &InfoSources) -> String {
        let mut out = String::new();
        for name in requested.iter() {
            if let Some(section) = self.sections.iter().find(|s| s.name() == name) {
                out.push_str(&section.render(src));
            }
        }
        out
    }
}

// ============================================================================
// Sources
// ============================================================================

/// Client-registry counts for the Clients section.
#[derive(Debug, Clone, Default)]
pub struct ClientsSnapshot {
    /// Currently connected clients.
    pub connected: usize,
    /// Clients blocked in BLPOP/WAIT/etc.
    pub blocked: usize,
    /// Configured maxclients.
    pub max_clients: u64,
}

/// ACL rate-limit aggregates for the Ratelimit section.
#[derive(Debug, Clone, Default)]
pub struct RateLimitSnapshot {
    /// Users with a rate limit configured.
    pub users: usize,
    /// Total commands rejected by rate limiting.
    pub commands_rejected: u64,
    /// Total bytes rejected by rate limiting.
    pub bytes_rejected: u64,
}

impl RateLimitSnapshot {
    /// Whether there is any rate-limit activity worth reporting.
    pub fn is_active(&self) -> bool {
        self.users > 0 || self.commands_rejected > 0 || self.bytes_rejected > 0
    }
}

/// What a `slaveN:` line's `state=` field reports.
///
/// Exactly Redis's three states (`wait_bgsave`, `send_bulk`, `online`). Redis
/// *omits* the whole line for a slave in none of them, and so does FrogDB: the
/// fourth phase FrogDB has (`Disconnecting`) maps to no state, so
/// [`ReplicaState::from_phase`] returns `None` and the replica is dropped at the
/// render boundary (FM-REPLICATION-060). `connected_slaves` is the count of the
/// lines that survive that boundary, so the count and the list still cannot
/// disagree — the invariant FM-REPLICATION-043 actually protects — while the
/// invented `offline` spelling no longer reaches a client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplicaState {
    /// Handshake accepted, checkpoint not started.
    WaitBgsave,
    /// Checkpoint is being transferred.
    SendBulk,
    /// Streaming the live WAL — the only state in which the replica is caught
    /// up enough to satisfy WAIT.
    #[default]
    Online,
}

impl ReplicaState {
    /// The wire spelling in the `slaveN:` line.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitBgsave => "wait_bgsave",
            Self::SendBulk => "send_bulk",
            Self::Online => "online",
        }
    }

    /// The state Redis would report for this phase, or `None` for a phase Redis
    /// has no word for — the caller must then render no line at all.
    pub fn from_phase(phase: Phase) -> Option<Self> {
        match phase {
            // A replica that has not yet been told which sync it is getting is
            // in the same position as a Redis replica waiting for the fork.
            Phase::Connecting | Phase::PreparingCheckpoint => Some(Self::WaitBgsave),
            Phase::StreamingCheckpoint => Some(Self::SendBulk),
            Phase::Streaming => Some(Self::Online),
            // Tearing down: Redis skips such a slave, and inventing a spelling
            // for it put a state on the wire no Redis client knows.
            Phase::Disconnecting => None,
        }
    }
}

/// One `slaveN:` line worth of replica state.
#[derive(Debug, Clone, Default)]
pub struct ReplicaLine {
    /// Replica IP address.
    pub ip: String,
    /// Replica listening port, as announced by `REPLCONF listening-port`.
    pub port: u16,
    /// Where the replica is in its lifecycle.
    pub state: ReplicaState,
    /// Replica acknowledged offset.
    pub offset: u64,
    /// Whole seconds since the replica's last ACK — Redis's `lag`.
    pub lag_secs: u64,
}

impl ReplicaLine {
    /// Project a live session snapshot onto the line INFO renders, or `None`
    /// for a replica in a phase Redis has no state for (FM-REPLICATION-060).
    ///
    /// Every field is read off the same [`ReplicaInfo`], so no two fields of a
    /// `slaveN:` line can describe different instants, and none of them is a
    /// literal. The `None` arm is the *only* filter between the registry and
    /// the wire: both renderers feed from every registered replica and let this
    /// projection decide, so the set of lines and `connected_slaves` are one
    /// decision made once.
    pub fn from_replica(replica: &ReplicaInfo) -> Option<Self> {
        Some(Self {
            ip: replica.address.ip().to_string(),
            port: replica.listening_port,
            state: ReplicaState::from_phase(replica.phase)?,
            offset: replica.acked_offset,
            lag_secs: replica.lag_secs(),
        })
    }

    /// Render this line as `slave<index>:…`, without a trailing terminator.
    ///
    /// THE single spelling of the `slaveN:` format. Both INFO renderers — the
    /// connection-level [`crate::info::sections`] one and the shard-local
    /// [`crate::commands::info`] one — call this, so the two cannot drift into
    /// reporting different fields for the same replica.
    pub fn render(&self, index: usize) -> String {
        format!(
            "slave{}:ip={},port={},state={},offset={},lag={}",
            index,
            self.ip,
            self.port,
            self.state.as_str(),
            self.offset,
            self.lag_secs
        )
    }
}

/// The three `sync_*` fields of `INFO stats`, in Redis's order.
///
/// Shared by both INFO renderers for the same reason [`ReplicaLine::render`] is:
/// one list, so neither renderer can omit or rename a counter the other reports.
pub fn sync_counter_fields(sync: SyncCountersSnapshot) -> [(&'static str, u64); 3] {
    [
        ("sync_full", sync.full),
        ("sync_partial_ok", sync.partial_ok),
        ("sync_partial_err", sync.partial_err),
    ]
}

/// The two `total_net_repl_*_bytes` fields of `INFO stats` (hardening issue
/// 29), in Redis's order.
///
/// Shared by both INFO renderers for the same reason [`sync_counter_fields`]
/// is: these two were hardcoded-zero literals in *both* of them, with nothing
/// in the codebase counting a real byte — a fix landed in one renderer and
/// forgotten in the other would silently reintroduce the fake-zero half of
/// the bug.
pub fn net_byte_fields(net_bytes: NetByteCountersSnapshot) -> [(&'static str, u64); 2] {
    [
        ("total_net_repl_input_bytes", net_bytes.input),
        ("total_net_repl_output_bytes", net_bytes.output),
    ]
}

/// The four `repl_backlog_*` fields of `INFO replication`, in Redis's order.
///
/// Shared by both INFO renderers for the same reason [`sync_counter_fields`] is:
/// these four were literals in *both* of them (`repl_backlog_size:1048576`,
/// `repl_backlog_first_byte_offset:0`), and a fix applied to one renderer and
/// forgotten in the other is the shape this list makes impossible
/// (FM-REPLICATION-059).
pub fn backlog_geometry_fields(geometry: BacklogGeometry) -> [(&'static str, u64); 4] {
    [
        ("repl_backlog_active", u64::from(geometry.active)),
        ("repl_backlog_size", geometry.size_bytes),
        ("repl_backlog_first_byte_offset", geometry.first_byte_offset),
        ("repl_backlog_histlen", geometry.histlen),
    ]
}

/// The replica-only link fields of `INFO replication`, in render order.
///
/// Two fields, one list, for the same reason [`backlog_geometry_fields`] is one
/// list: `master_link_status` was rendered from a duplicated literal in each
/// renderer, and `master_sync_error` is the field an operator is told to alert
/// on — a node that reported it to clients but not to `redis.call('INFO')`
/// (or the reverse) would be exactly the half-fix this campaign keeps finding
/// (FM-REPLICATION-061).
///
/// `master_sync_error` is emitted **only when present**: absent means "not
/// given up", which is the common case, and an empty-valued field would read
/// as an error that happened rather than as one that did not.
pub fn replica_link_fields(
    master_link_up: bool,
    sync_error: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut fields = vec![(
        "master_link_status",
        if master_link_up { "up" } else { "down" }.to_string(),
    )];
    if let Some(reason) = sync_error {
        fields.push(("master_sync_error", reason.to_string()));
    }
    fields
}

/// The `SnapshotCoordinator`-derived block of `INFO persistence` — save
/// outcome, counters, and durations — in Redis's order.
///
/// Shared by both INFO renderers for the same reason [`sync_counter_fields`]
/// is: the shard-local path (`redis.call('INFO')`, `commands::info::build_persistence_info`)
/// used to emit these eight fields as literals (`rdb_last_bgsave_status:ok`
/// always, `rdb_saves:0` always, ...) because a shard's `CommandContext` had
/// no handle on the coordinator — a script polling INFO to check save health
/// always read "healthy" even while saves were actively failing
/// (issue 10 / FM-PERSISTENCE-022). Every shard is constructed with the same
/// `Arc<dyn SnapshotCoordinator>` the connection-level path reads
/// (`server/init.rs` builds it once), so both renderers now read the
/// identical [`SnapshotStats`] and cannot drift into reporting different
/// save health for the same node.
pub fn persistence_snapshot_fields(
    stats: &frogdb_core::SnapshotStats,
    bgsave_in_progress: bool,
) -> Vec<(&'static str, String)> {
    use std::time::UNIX_EPOCH;

    let last_save_unix = stats.last_save_time.map(|saved_at| {
        saved_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    });
    let last_bgsave_secs = stats.last_duration.map(|d| d.as_secs());
    // Elapsed at *read* time, from the same stats value: a hung save reports
    // a growing number rather than a frozen one.
    let current_bgsave_secs = stats.current_save_elapsed().map(|d| d.as_secs());

    let mut fields = vec![
        (
            "rdb_bgsave_in_progress",
            u8::from(bgsave_in_progress).to_string(),
        ),
        (
            "rdb_last_save_time",
            last_save_unix.unwrap_or(0).to_string(),
        ),
        // Real save outcome, not a literal: `err` while the most recent
        // finished save failed, back to `ok` on the next success (Redis
        // semantics).
        (
            "rdb_last_bgsave_status",
            if stats.last_error.is_some() {
                "err"
            } else {
                "ok"
            }
            .to_string(),
        ),
    ];
    if let Some(err) = &stats.last_error {
        // INFO is a line-oriented format: fold any CR/LF in the cause into
        // spaces so one error cannot forge extra fields.
        fields.push(("rdb_last_bgsave_error", err.replace(['\r', '\n'], " ")));
    }
    fields.push((
        "rdb_last_bgsave_time_sec",
        last_bgsave_secs.map_or(-1, |s| s as i64).to_string(),
    ));
    fields.push((
        "rdb_current_bgsave_time_sec",
        current_bgsave_secs.map_or(-1, |s| s as i64).to_string(),
    ));
    fields.push(("rdb_saves", stats.saves.to_string()));
    fields.push(("rdb_bgsave_failures", stats.failures.to_string()));
    fields
}

/// The `slaveN:` feed both INFO renderers use: every registered replica, in
/// attach order, projected through [`ReplicaLine::from_replica`] — which drops
/// the ones in a phase Redis has no state for (FM-REPLICATION-060).
///
/// Both renderers used to call `get_streaming_replicas()` instead, so a replica
/// being fed its checkpoint right now — the case an operator opens `INFO
/// replication` to see — appeared in no `slaveN:` line and in no
/// `connected_slaves` count at all. That filter is *not* moved here as a
/// narrower one: it stays where it belongs, on the acked-offset projection
/// `WAIT`, `ROLE` and the quorum checker read, which is a different question
/// ("who has acknowledged this offset") that must not widen with the render.
pub fn rendered_replicas(tracker: &frogdb_replication::ReplicationTrackerImpl) -> Vec<ReplicaLine> {
    tracker
        .get_all_replicas()
        .iter()
        .filter_map(ReplicaLine::from_replica)
        .collect()
}

/// Primary-role replication state (present when this node tracks replicas).
#[derive(Debug, Clone, Default)]
pub struct PrimarySnapshot {
    /// The replicas this node renders, one per `slaveN:` line — every
    /// registered replica in a phase Redis names, not only the streaming ones
    /// (FM-REPLICATION-060).
    pub replicas: Vec<ReplicaLine>,
}

/// Live replication identity, materialized once per INFO request.
#[derive(Debug, Clone, Default)]
pub struct ReplicationSnapshot {
    /// Whether this server is a replica.
    pub is_replica: bool,
    /// This node's id (fallback replication id source).
    pub node_id: Option<u64>,
    /// The real replication id exchanged in PSYNC/FULLRESYNC, when present.
    /// Standalone and pure cluster mode have no PSYNC identity and fall back
    /// to the node id.
    pub replication_id: Option<String>,
    /// Primary-role state (replica tracking); `None` for replica/standalone.
    pub primary: Option<PrimarySnapshot>,
    /// Lifetime `sync_full` / `sync_partial_ok` / `sync_partial_err`, read as
    /// one triple so the three fields cannot describe different instants.
    /// Reported in `INFO stats`, not `INFO replication`, as in Redis.
    pub sync: SyncCountersSnapshot,
    /// Lifetime replication net-byte counters (hardening issue 29), read as
    /// one pair off the tracker so `total_net_repl_input_bytes` and
    /// `total_net_repl_output_bytes` describe the same instant. Reported in
    /// `INFO stats`, not `INFO replication`, matching where `sync` above is
    /// reported.
    pub net_bytes: NetByteCountersSnapshot,
    /// The replication backlog's live shape — capacity, whether a resume window
    /// is open, and where it starts. Read off the tracker (which the backlog is
    /// published to at construction) in *both* roles: the configured capacity is
    /// a property of the node, not of the role it happens to be running, and a
    /// replica reports it with `active:0` exactly as Redis does.
    pub backlog: BacklogGeometry,
    /// Live replication offset of this node, whatever role it is running.
    ///
    /// Rendered as `master_repl_offset`. On a primary this is the last offset
    /// stamped into the stream; on a replica it is the last offset applied from
    /// the primary — the same value the replica sends in `PSYNC`/`REPLCONF ACK`,
    /// and the value that becomes the failover boundary if it is promoted.
    /// Redis reports the same single counter (`server.master_repl_offset`) in
    /// both roles.
    pub repl_offset: u64,
    /// Primary host when running as a replica.
    pub master_host: Option<String>,
    /// Primary port when running as a replica.
    pub master_port: Option<u16>,
    /// Whether the replication link to the primary is connected and
    /// streaming, when running as a replica. Rendered as
    /// `master_link_status:up`/`down`.
    pub master_link_up: bool,
    /// Why the inbound stream **gave up**, when it did: the detail of a full
    /// resync this node can never install. Rendered as `master_sync_error`,
    /// and only on a replica.
    ///
    /// Absent while the link is up, and also while it is down but still
    /// retrying — `master_link_status:down` on its own means "no data arriving
    /// right now", which is usually transient. This field is what says the node
    /// has stopped trying and needs a human. FrogDB-specific: Redis has no
    /// equivalent because it has no structural refusal to report (its RDB is
    /// partitioning-agnostic).
    pub master_sync_error: Option<String>,
    /// Failover-continuity window: the previous primary's replication id
    /// (rendered as `master_replid2`) paired with the offset boundary up to
    /// which it stays valid for PSYNC (rendered as `second_repl_offset`).
    /// `None` before any failover — INFO then reports the all-zero
    /// `master_replid2` and `second_repl_offset:-1` that Redis uses for "no
    /// secondary window".
    ///
    /// The boundary is FrogDB's **inclusive** `ReplicationState::secondary_offset`:
    /// `ReplicationState::window_contains` continues a replica whose requested
    /// offset is `<= secondary_offset`.
    /// Redis instead reports `second_repl_offset = master_repl_offset + 1`, an
    /// **exclusive** one-past-the-end boundary. We deliberately render FrogDB's
    /// inclusive value verbatim rather than adding one to mimic Redis: the
    /// reported pair then matches FrogDB's own continuation predicate exactly,
    /// so an operator who reads `second_repl_offset:N` can trust that any
    /// replica at offset `<= N` is judged continuable via `master_replid2`.
    pub secondary_window: Option<(String, i64)>,
}

impl ReplicationSnapshot {
    /// The `master_replid` to report: the live PSYNC id when present,
    /// otherwise the node id rendered as 40-char hex.
    pub fn replid(&self) -> String {
        self.replication_id
            .clone()
            .unwrap_or_else(|| format!("{:040x}", self.node_id.unwrap_or(0)))
    }
}

/// Persistence configuration + snapshot-coordinator state.
#[derive(Debug, Clone, Default)]
pub struct PersistenceSnapshot {
    /// Configured WAL durability mode ("periodic", "sync", "async").
    pub durability_mode: String,
    /// Whether a background save is currently running.
    pub bgsave_in_progress: bool,
    /// Raw save history straight from the `SnapshotCoordinator` — the single
    /// source [`crate::info::persistence_snapshot_fields`] renders into
    /// `rdb_last_save_time`/`rdb_last_bgsave_status`/`rdb_saves`/etc, shared
    /// with the shard-local INFO renderer (issue 10 / FM-PERSISTENCE-022).
    pub snapshot_stats: frogdb_core::SnapshotStats,
    /// Keys restored by this boot's recovery (`rdb_last_load_keys_loaded`).
    ///
    /// This and the two below describe the *load*, so they are constants for the
    /// life of the process — Redis' semantics for the same fields.
    pub load_keys_loaded: u64,
    /// Keys this boot's recovery dropped because their TTL had already passed
    /// (`rdb_last_load_keys_expired`).
    pub load_keys_expired: u64,
    /// Keys this boot's recovery skipped because their stored value would not
    /// deserialize (`rdb_last_load_keys_failed`, a FrogDB extension). Non-zero
    /// means the keyspace came back smaller than what is on disk.
    pub load_keys_failed: u64,
}

/// Latency-histogram handles for the Latencystats section.
#[derive(Clone)]
pub struct LatencySnapshot {
    /// Server-wide per-command latency histograms.
    pub histograms: Arc<CommandLatencyHistograms>,
    /// Configured percentiles to report.
    pub percentiles: Vec<f64>,
}

/// Startup latency-baseline results for the Latency_Baseline section.
#[derive(Debug, Clone, Default)]
pub struct BaselineSnapshot {
    /// Test duration in seconds.
    pub duration_secs: u64,
    /// Number of samples taken.
    pub samples: u64,
    /// Minimum observed latency (µs).
    pub min_us: u64,
    /// Maximum observed latency (µs).
    pub max_us: u64,
    /// Average observed latency (µs).
    pub avg_us: f64,
    /// 99th percentile latency (µs).
    pub p99_us: u64,
    /// Threshold above which a warning was issued (µs).
    pub warning_threshold_us: u64,
}

/// Memory limits from configuration.
#[derive(Debug, Clone, Default)]
pub struct MemoryConfigSnapshot {
    /// Configured maxmemory in bytes (0 = unlimited).
    pub maxmemory: u64,
    /// Configured eviction policy, rendered (e.g. "noeviction").
    pub policy: String,
}

/// Everything a section can read, gathered once per INFO request.
///
/// The single round of shard messaging happens in
/// [`gather_shard_snapshot`] before this is constructed, so a section's
/// `render` is a pure function of already-collected data. Sections read
/// through the typed accessors; proposal 24 can relocate the
/// keyspace-hit/miss source behind [`InfoSources::keyspace_hits`] without
/// touching any section.
pub struct InfoSources {
    pub(crate) cluster_state: Option<Arc<ClusterState>>,
    pub(crate) clients: ClientsSnapshot,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    pub(crate) total_error_replies: u64,
    /// Per-command stats, merged with the calling connection's un-synced
    /// local stats and sorted by command name.
    pub(crate) command_stats: Vec<(String, ServerCommandStats)>,
    /// Per-error-prefix counts, sorted by prefix.
    pub(crate) error_types: Vec<(String, u64)>,
    pub(crate) latency: LatencySnapshot,
    pub(crate) rate_limit: RateLimitSnapshot,
    pub(crate) replication: ReplicationSnapshot,
    pub(crate) persistence: PersistenceSnapshot,
    pub(crate) memory_config: MemoryConfigSnapshot,
    pub(crate) baseline: Option<BaselineSnapshot>,
    pub(crate) key_memory_enabled: bool,
    pub(crate) shards: NodeStateSnapshot,
    /// The resettable keyspace hit/miss accumulator (proposal 24). Counted at
    /// the execution seam, so it is live even when metrics are disabled.
    pub(crate) keyspace_stats: Arc<frogdb_core::KeyspaceStats>,
}

impl InfoSources {
    /// Keyspace hits since the last `CONFIG RESETSTAT`, read from the
    /// [`frogdb_core::KeyspaceStats`] accumulator (cumulative − baseline).
    /// The Prometheus `_total` counter is a separate, strictly monotonic
    /// view; INFO reports the resettable value, matching Redis. Always
    /// `Some` — the seam counts lookups even with metrics disabled.
    pub fn keyspace_hits(&self) -> Option<u64> {
        Some(self.keyspace_stats.reported_hits())
    }

    /// Keyspace misses since the last `CONFIG RESETSTAT`; see
    /// [`Self::keyspace_hits`].
    pub fn keyspace_misses(&self) -> Option<u64> {
        Some(self.keyspace_stats.reported_misses())
    }

    /// Total commands processed, summed across the `command` label of the same
    /// `frogdb_commands_total` counter Prometheus scrapes and the `/status`
    /// endpoint reports — so INFO, `/metrics`, and `/status` never disagree.
    /// `None` when metrics are disabled (nothing to report).
    pub fn total_commands_processed(&self) -> Option<u64> {
        self.metrics.counter_value(CommandsTotal::NAME)
    }

    /// Total WAL writes, from the same counter Prometheus scrapes.
    pub fn wal_writes_total(&self) -> Option<u64> {
        self.metrics.counter_value(WalWrites::NAME)
    }

    /// Total WAL bytes written, from the same counter Prometheus scrapes.
    pub fn wal_bytes_total(&self) -> Option<u64> {
        self.metrics.counter_value(WalBytes::NAME)
    }

    /// Aggregated per-shard data (memory, keys, eviction, keysizes, WAL).
    pub fn shards(&self) -> &NodeStateSnapshot {
        &self.shards
    }

    /// Client-registry counts.
    pub fn clients(&self) -> &ClientsSnapshot {
        &self.clients
    }

    /// Total error replies served (from the client registry).
    pub fn total_error_replies(&self) -> u64 {
        self.total_error_replies
    }

    /// Per-command stats, merged and sorted.
    pub fn command_stats(&self) -> &[(String, ServerCommandStats)] {
        &self.command_stats
    }

    /// Per-error-prefix counts, sorted.
    pub fn error_types(&self) -> &[(String, u64)] {
        &self.error_types
    }

    /// Latency histograms + configured percentiles.
    pub fn latency(&self) -> &LatencySnapshot {
        &self.latency
    }

    /// ACL rate-limit aggregates.
    pub fn rate_limit(&self) -> &RateLimitSnapshot {
        &self.rate_limit
    }

    /// Live replication identity.
    pub fn replication(&self) -> &ReplicationSnapshot {
        &self.replication
    }

    /// Persistence config + snapshot-coordinator state.
    pub fn persistence(&self) -> &PersistenceSnapshot {
        &self.persistence
    }

    /// Memory limits from configuration.
    pub fn memory_config(&self) -> &MemoryConfigSnapshot {
        &self.memory_config
    }

    /// Startup latency-baseline results, if the test was run.
    pub fn baseline(&self) -> Option<&BaselineSnapshot> {
        self.baseline.as_ref()
    }

    /// Whether key-memory histograms are enabled in config.
    pub fn key_memory_enabled(&self) -> bool {
        self.key_memory_enabled
    }

    /// Cluster state, for version-gated Server section fields.
    pub fn cluster_state(&self) -> Option<&ClusterState> {
        self.cluster_state.as_deref()
    }
}

// ============================================================================
// The one shard round trip
// ============================================================================

/// The one place INFO talks to shards.
///
/// Delegates to the shared [`NodeStateSnapshot::collect`] — the single
/// `InfoSnapshot` fleet scatter that telemetry `/status` and the debug UI also
/// use — and maps its per-shard [`ShardScatterError`] to the INFO wire error and
/// warn log. INFO is strict: a missing shard is an error, never a silently
/// under-reported aggregate.
pub async fn gather_shard_snapshot(
    senders: &[ShardSender],
    timeout: Duration,
    conn_id: u64,
) -> Result<NodeStateSnapshot, Response> {
    NodeStateSnapshot::collect(senders, timeout)
        .await
        .map_err(|err| {
            let shard_id = err.shard_id();
            match err {
                ShardScatterError::Unavailable { .. } => {
                    warn!(conn_id, shard_id, cmd = "INFO", "shard unavailable");
                    Response::error("ERR shard unavailable")
                }
                ShardScatterError::Dropped { .. } => {
                    warn!(conn_id, shard_id, cmd = "INFO", "shard dropped request");
                    Response::error("ERR shard dropped request")
                }
                ShardScatterError::Timeout { .. } => {
                    warn!(conn_id, shard_id, cmd = "INFO", "scatter timeout");
                    Response::error("ERR timeout")
                }
            }
        })
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use frogdb_core::NoopMetricsRecorder;

    /// A minimal `InfoSources` for unit tests: metrics disabled, no
    /// replication, no persistence, empty shard aggregate.
    pub fn sources() -> InfoSources {
        InfoSources {
            cluster_state: None,
            clients: ClientsSnapshot::default(),
            metrics: Arc::new(NoopMetricsRecorder::new()),
            total_error_replies: 0,
            command_stats: Vec::new(),
            error_types: Vec::new(),
            latency: LatencySnapshot {
                histograms: Arc::new(CommandLatencyHistograms::new(false)),
                percentiles: Vec::new(),
            },
            rate_limit: RateLimitSnapshot::default(),
            replication: ReplicationSnapshot::default(),
            persistence: PersistenceSnapshot {
                durability_mode: "periodic".to_string(),
                ..Default::default()
            },
            memory_config: MemoryConfigSnapshot {
                maxmemory: 0,
                policy: "noeviction".to_string(),
            },
            baseline: None,
            key_memory_enabled: true,
            shards: NodeStateSnapshot::default(),
            keyspace_stats: Arc::new(frogdb_core::KeyspaceStats::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::sources;
    use super::*;
    use frogdb_core::{
        InfoShardSnapshot, KeysizeHistograms, ObservabilityMsg, ShardMemoryStats, ShardMessage,
        ShardReceiver, TieredCounts,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    // -------------------------------------------------------------------
    // SectionWriter format invariants
    // -------------------------------------------------------------------

    #[test]
    fn section_writer_header_fields_and_trailing_blank_line() {
        let mut w = SectionWriter::new("Stats");
        w.field("expired_keys", 3u64);
        w.field("evicted_keys", 0u64);
        assert_eq!(
            w.finish(),
            "# Stats\r\nexpired_keys:3\r\nevicted_keys:0\r\n\r\n"
        );
    }

    #[test]
    fn section_writer_field_opt_none_is_absent() {
        let mut w = SectionWriter::new("Stats");
        w.field_opt("keyspace_hits", None::<u64>);
        w.field_opt("keyspace_misses", Some(7u64));
        assert_eq!(w.finish(), "# Stats\r\nkeyspace_misses:7\r\n\r\n");
    }

    #[test]
    fn section_writer_empty_section_is_header_plus_blank_line() {
        let w = SectionWriter::new("Keyspace");
        assert!(!w.has_fields());
        assert_eq!(w.finish(), "# Keyspace\r\n\r\n");
    }

    #[test]
    fn section_writer_raw_line() {
        let mut w = SectionWriter::new("Keyspace");
        w.line("db0:keys=5,expires=0,avg_ttl=0");
        assert!(w.has_fields());
        assert_eq!(
            w.finish(),
            "# Keyspace\r\ndb0:keys=5,expires=0,avg_ttl=0\r\n\r\n"
        );
    }

    // -------------------------------------------------------------------
    // SectionSelector
    // -------------------------------------------------------------------

    fn args(names: &[&str]) -> Vec<Bytes> {
        names.iter().map(|n| Bytes::from(n.to_string())).collect()
    }

    #[test]
    fn selector_no_args_is_default() {
        let sel = SectionSelector::from_args(&[]);
        assert_eq!(sel.iter().collect::<Vec<_>>(), DEFAULT_SECTIONS);
    }

    #[test]
    fn selector_default_and_empty_string_aliases() {
        let by_name = SectionSelector::from_args(&args(&["default"]));
        let by_empty = SectionSelector::from_args(&args(&[""]));
        assert_eq!(by_name, by_empty);
        assert_eq!(by_name.iter().collect::<Vec<_>>(), DEFAULT_SECTIONS);
    }

    #[test]
    fn selector_all_and_everything_include_extras() {
        for alias in ["all", "everything", "ALL"] {
            let sel = SectionSelector::from_args(&args(&[alias]));
            let expected: Vec<&str> = DEFAULT_SECTIONS
                .iter()
                .chain(EXTRA_SECTIONS.iter())
                .copied()
                .collect();
            assert_eq!(sel.iter().collect::<Vec<_>>(), expected, "alias {alias}");
        }
    }

    #[test]
    fn selector_dedups_repeated_sections() {
        let sel = SectionSelector::from_args(&args(&["server", "SERVER", "default"]));
        // "server" appears once, then default's remaining sections follow.
        let names: Vec<&str> = sel.iter().collect();
        assert_eq!(names[0], "server");
        assert_eq!(names.iter().filter(|n| **n == "server").count(), 1);
        assert!(sel.includes("keyspace"));
    }

    #[test]
    fn selector_preserves_request_order() {
        let sel = SectionSelector::from_args(&args(&["cpu", "server"]));
        assert_eq!(sel.iter().collect::<Vec<_>>(), vec!["cpu", "server"]);
    }

    #[test]
    fn selector_unknown_name_is_carried_but_renders_nothing() {
        let sel = SectionSelector::from_args(&args(&["bogus"]));
        assert!(sel.includes("bogus"));
        let out = InfoBuilder::standard().render(&sel, &sources());
        assert_eq!(out, "");
    }

    // -------------------------------------------------------------------
    // Aggregation
    // -------------------------------------------------------------------

    fn shard_snap(shard_id: usize) -> InfoShardSnapshot {
        InfoShardSnapshot {
            shard_id,
            memory: ShardMemoryStats {
                shard_id,
                data_memory: 100,
                keys: 10,
                peak_memory: 200,
                memory_limit: 0,
                overhead_estimate: 0,
                evicted_keys: 1,
                expired_keys: 2,
                lazyfreed_objects: 3,
            },
            dirty: 5,
            tiered: TieredCounts {
                hot_keys: 4,
                warm_keys: 3,
                unspills: 2,
                spills: 1,
                expired_on_unspill: 1,
            },
            keysizes: KeysizeHistograms::new(),
            wal_lag: None,
            master_host: None,
            master_port: None,
            master_link_up: false,
            master_sync_error: None,
        }
    }

    // The per-shard fold (`absorb`) and WAL aggregation now live on
    // `frogdb_telemetry::NodeStateSnapshot` and are unit-tested there; INFO only
    // wraps `NodeStateSnapshot::collect` with its wire-error mapping, exercised
    // by the scatter tests below.

    // -------------------------------------------------------------------
    // Single-scatter invariant
    // -------------------------------------------------------------------

    /// Mock shards that answer `InfoSnapshot` and count every message they
    /// receive; guards against a section re-introducing its own scatter.
    fn mock_shards(n: usize) -> (Vec<ShardSender>, Arc<AtomicUsize>) {
        let messages = Arc::new(AtomicUsize::new(0));
        let mut senders = Vec::new();
        for shard_id in 0..n {
            let (tx, rx) = mpsc::channel(16);
            senders.push(ShardSender::new(tx));
            let messages = Arc::clone(&messages);
            let mut receiver = ShardReceiver::new(rx);
            tokio::spawn(async move {
                while let Some(env) = receiver.recv().await {
                    messages.fetch_add(1, Ordering::SeqCst);
                    if let ShardMessage::Observability(ObservabilityMsg::InfoSnapshot {
                        response_tx,
                    }) = env.message
                    {
                        let _ = response_tx.send(shard_snap(shard_id));
                    }
                }
            });
        }
        (senders, messages)
    }

    #[tokio::test]
    async fn gather_sends_exactly_one_message_per_shard() {
        let (senders, messages) = mock_shards(4);
        let agg = gather_shard_snapshot(&senders, Duration::from_secs(5), 0)
            .await
            .expect("gather succeeds");
        assert_eq!(agg.keys, 40, "all four shards folded");
        assert_eq!(
            messages.load(Ordering::SeqCst),
            4,
            "one INFO gather = one message per shard, no second scatter"
        );
    }

    #[tokio::test]
    async fn gather_errors_when_a_shard_drops_the_request() {
        // Shard 0 answers; shard 1 receives but drops the reply channel.
        let (tx0, rx0) = mpsc::channel(16);
        let (tx1, rx1) = mpsc::channel(16);
        let senders = vec![ShardSender::new(tx0), ShardSender::new(tx1)];
        let mut r0 = ShardReceiver::new(rx0);
        tokio::spawn(async move {
            while let Some(env) = r0.recv().await {
                if let ShardMessage::Observability(ObservabilityMsg::InfoSnapshot { response_tx }) =
                    env.message
                {
                    let _ = response_tx.send(shard_snap(0));
                }
            }
        });
        let mut r1 = ShardReceiver::new(rx1);
        tokio::spawn(async move {
            while let Some(env) = r1.recv().await {
                drop(env); // drops response_tx without replying
            }
        });
        let err = gather_shard_snapshot(&senders, Duration::from_secs(5), 0)
            .await
            .expect_err("missing shard must not silently under-report");
        assert!(
            matches!(err, Response::Error(ref e) if e.as_ref() == b"ERR shard dropped request")
        );
    }
}
