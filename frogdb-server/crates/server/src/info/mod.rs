//! Connection-level INFO section builder.
//!
//! Each INFO section is a deep module that owns *both* its data access and its
//! byte format: an [`InfoSection`] renders itself from an [`InfoSources`]
//! bundle that is gathered exactly once per INFO request. There is no
//! placeholder string, no `.replace`, no `.replace_range`, and no re-parsing of
//! a buffer the code just emitted — the stub-and-patch contract that used to
//! span `commands/info.rs` and `connection/handlers/scatter.rs` is gone.
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
use frogdb_core::persistence::WalLagStats;
use frogdb_core::{
    ClusterState, CommandLatencyHistograms, InfoShardSnapshot, KeysizeHistograms, MetricsRecorder,
    ServerCommandStats, ShardMessage, ShardSender, TieredCounts,
};
use frogdb_protocol::Response;
use frogdb_telemetry::definitions::{WalBytes, WalWrites};
use tokio::sync::oneshot;
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

/// Aggregated per-shard data, collected with a single fleet scatter
/// (see [`gather_shard_snapshot`]) that replaces the previous two passes
/// (`MemoryStats` + `KeysizesSnapshot`). Adding a future per-shard INFO field
/// is a new field here (and on `frogdb_core::InfoShardSnapshot`), not a new
/// round trip.
#[derive(Debug, Clone, Default)]
pub struct ShardInfoSnapshot {
    /// Total data memory across all shards (bytes).
    pub used_memory: usize,
    /// Sum of per-shard peak memory high-water marks (bytes).
    pub peak_memory: u64,
    /// Total number of keys across all shards.
    pub keys: usize,
    /// Total keys evicted across all shards.
    pub evicted_keys: u64,
    /// Total keys expired across all shards.
    pub expired_keys: u64,
    /// Total objects freed via lazyfree across all shards.
    pub lazyfreed_objects: u64,
    /// Total writes since last snapshot across all shards.
    pub dirty: u64,
    /// Summed tiered-storage counters.
    pub tiered: TieredCounts,
    /// Keysize histograms merged across all shards.
    pub keysizes: KeysizeHistograms,
    /// Aggregated WAL lag; `None` when persistence is disabled on every shard.
    pub wal: Option<WalAggregate>,
    /// Primary host when running as a replica (from shard identity).
    pub master_host: Option<String>,
    /// Primary port when running as a replica (from shard identity).
    pub master_port: Option<u16>,
}

impl ShardInfoSnapshot {
    /// Fold one shard's reply into the aggregate.
    pub fn absorb(&mut self, snap: InfoShardSnapshot) {
        self.used_memory += snap.memory.data_memory;
        self.peak_memory += snap.memory.peak_memory;
        self.keys += snap.memory.keys;
        self.evicted_keys += snap.memory.evicted_keys;
        self.expired_keys += snap.memory.expired_keys;
        self.lazyfreed_objects += snap.memory.lazyfreed_objects;
        self.dirty += snap.dirty;
        self.tiered.hot_keys += snap.tiered.hot_keys;
        self.tiered.warm_keys += snap.tiered.warm_keys;
        self.tiered.promotions += snap.tiered.promotions;
        self.tiered.demotions += snap.tiered.demotions;
        self.tiered.expired_on_promote += snap.tiered.expired_on_promote;
        self.keysizes.merge(&snap.keysizes);
        if let Some(lag) = snap.wal_lag {
            match &mut self.wal {
                Some(agg) => agg.absorb(&lag),
                None => self.wal = Some(WalAggregate::from_shard(&lag)),
            }
        }
        if self.master_host.is_none() {
            self.master_host = snap.master_host;
        }
        if self.master_port.is_none() {
            self.master_port = snap.master_port;
        }
    }
}

/// WAL lag aggregated across shards: pending work and failure counters sum,
/// lags take the worst (max) shard, the "last flush" timestamp takes the
/// oldest (min) shard — i.e. every shard has flushed at least as of the
/// reported time — and the flush status is ok only if every shard's last
/// flush attempt succeeded.
#[derive(Debug, Clone, Default)]
pub struct WalAggregate {
    /// Total operations buffered but not yet flushed.
    pub pending_ops: usize,
    /// Total bytes buffered but not yet flushed.
    pub pending_bytes: usize,
    /// Worst per-shard durability lag (ms since last flush).
    pub durability_lag_ms: u64,
    /// Total failed flush attempts across shards since startup.
    pub flush_failures: u64,
    /// Total WAL entries dropped in failed flushes across shards since
    /// startup. Losses are permanent; this never decreases.
    pub lost_ops: u64,
    /// False if any shard's most recent flush attempt failed.
    pub last_flush_ok: bool,
    /// Oldest per-shard last-flush wall-clock time (unix ms).
    pub last_flush_time_ms: u64,
}

impl WalAggregate {
    /// Seed the aggregate from the first shard's lag stats.
    fn from_shard(lag: &WalLagStats) -> Self {
        Self {
            pending_ops: lag.pending_ops,
            pending_bytes: lag.pending_bytes,
            durability_lag_ms: lag.durability_lag_ms,
            flush_failures: lag.flush_failures,
            lost_ops: lag.lost_ops,
            last_flush_ok: lag.last_flush_ok,
            last_flush_time_ms: lag.last_flush_timestamp_ms,
        }
    }

    /// Fold another shard's lag stats into the aggregate.
    fn absorb(&mut self, lag: &WalLagStats) {
        self.pending_ops += lag.pending_ops;
        self.pending_bytes += lag.pending_bytes;
        self.durability_lag_ms = self.durability_lag_ms.max(lag.durability_lag_ms);
        self.flush_failures += lag.flush_failures;
        self.lost_ops += lag.lost_ops;
        self.last_flush_ok &= lag.last_flush_ok;
        self.last_flush_time_ms = self.last_flush_time_ms.min(lag.last_flush_timestamp_ms);
    }
}

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

/// One `slaveN:` line worth of replica state.
#[derive(Debug, Clone, Default)]
pub struct ReplicaLine {
    /// Replica IP address.
    pub ip: String,
    /// Replica listening port.
    pub port: u16,
    /// Replica acknowledged offset.
    pub offset: u64,
}

/// Primary-role replication state (present when this node tracks replicas).
#[derive(Debug, Clone, Default)]
pub struct PrimarySnapshot {
    /// Streaming replicas, one per `slaveN:` line.
    pub replicas: Vec<ReplicaLine>,
    /// Current replication offset.
    pub repl_offset: u64,
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
    /// Primary host when running as a replica.
    pub master_host: Option<String>,
    /// Primary port when running as a replica.
    pub master_port: Option<u16>,
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
    /// Unix time (seconds) of the last successful save, if any.
    pub last_save_unix: Option<u64>,
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
    pub(crate) shards: ShardInfoSnapshot,
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

    /// Total WAL writes, from the same counter Prometheus scrapes.
    pub fn wal_writes_total(&self) -> Option<u64> {
        self.metrics.counter_value(WalWrites::NAME)
    }

    /// Total WAL bytes written, from the same counter Prometheus scrapes.
    pub fn wal_bytes_total(&self) -> Option<u64> {
        self.metrics.counter_value(WalBytes::NAME)
    }

    /// Aggregated per-shard data (memory, keys, eviction, keysizes, WAL).
    pub fn shards(&self) -> &ShardInfoSnapshot {
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
/// Sends a single combined [`ShardMessage::InfoSnapshot`] request to every
/// shard and folds the replies into a [`ShardInfoSnapshot`], so adding a
/// per-shard field never adds a round trip. All replies are awaited under a
/// single shared deadline; a missing shard is an error, never a silently
/// under-reported aggregate.
pub async fn gather_shard_snapshot(
    senders: &[ShardSender],
    timeout: Duration,
    conn_id: u64,
) -> Result<ShardInfoSnapshot, Response> {
    let mut rxs = Vec::with_capacity(senders.len());
    for (shard_id, sender) in senders.iter().enumerate() {
        let (response_tx, rx) = oneshot::channel();
        if sender
            .send(ShardMessage::InfoSnapshot { response_tx })
            .await
            .is_err()
        {
            warn!(conn_id, shard_id, cmd = "INFO", "shard unavailable");
            return Err(Response::error("ERR shard unavailable"));
        }
        rxs.push((shard_id, rx));
    }

    // One deadline for the whole gather, not one-per-receiver.
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let mut aggregate = ShardInfoSnapshot::default();
    for (shard_id, rx) in rxs {
        tokio::select! {
            reply = rx => match reply {
                Ok(snap) => aggregate.absorb(snap),
                Err(_) => {
                    warn!(conn_id, shard_id, cmd = "INFO", "shard dropped request");
                    return Err(Response::error("ERR shard dropped request"));
                }
            },
            _ = &mut deadline => {
                warn!(conn_id, shard_id, cmd = "INFO", "scatter timeout");
                return Err(Response::error("ERR timeout"));
            }
        }
    }
    Ok(aggregate)
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
            shards: ShardInfoSnapshot::default(),
            keyspace_stats: Arc::new(frogdb_core::KeyspaceStats::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::sources;
    use super::*;
    use frogdb_core::{ShardMemoryStats, ShardReceiver};
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
                promotions: 2,
                demotions: 1,
                expired_on_promote: 1,
            },
            keysizes: KeysizeHistograms::new(),
            wal_lag: None,
            master_host: None,
            master_port: None,
        }
    }

    #[test]
    fn shard_snapshot_absorb_sums_counters() {
        let mut agg = ShardInfoSnapshot::default();
        agg.absorb(shard_snap(0));
        agg.absorb(shard_snap(1));
        assert_eq!(agg.used_memory, 200);
        assert_eq!(agg.peak_memory, 400);
        assert_eq!(agg.keys, 20);
        assert_eq!(agg.evicted_keys, 2);
        assert_eq!(agg.expired_keys, 4);
        assert_eq!(agg.lazyfreed_objects, 6);
        assert_eq!(agg.dirty, 10);
        assert_eq!(agg.tiered.hot_keys, 8);
        assert_eq!(agg.tiered.demotions, 2);
        assert!(agg.wal.is_none());
    }

    #[test]
    fn wal_aggregate_sums_pending_maxes_lag_and_mins_timestamps() {
        let lag = |pending, dlag, flush, failures, lost, ok| WalLagStats {
            pending_ops: pending,
            pending_bytes: pending * 10,
            durability_lag_ms: dlag,
            sequence: 0,
            durable_sequence: 0,
            flush_failures: failures,
            lost_ops: lost,
            lost_bytes: lost * 100,
            last_flush_ok: ok,
            shard_id: 0,
            last_flush_timestamp_ms: flush,
        };
        let mut snap_a = shard_snap(0);
        snap_a.wal_lag = Some(lag(3, 50, 1_000, 2, 1, true));
        let mut snap_b = shard_snap(1);
        snap_b.wal_lag = Some(lag(4, 80, 800, 3, 2, false));

        let mut agg = ShardInfoSnapshot::default();
        agg.absorb(snap_a);
        agg.absorb(snap_b);
        let wal = agg.wal.expect("wal aggregate present");
        assert_eq!(wal.pending_ops, 7);
        assert_eq!(wal.pending_bytes, 70);
        assert_eq!(wal.durability_lag_ms, 80, "lag takes the worst shard");
        assert_eq!(wal.flush_failures, 5, "failures sum across shards");
        assert_eq!(wal.lost_ops, 3, "lost ops sum across shards");
        assert!(
            !wal.last_flush_ok,
            "one failing shard makes the aggregate status err"
        );
        assert_eq!(
            wal.last_flush_time_ms, 800,
            "flush time takes the oldest shard"
        );
    }

    #[test]
    fn wal_aggregate_none_when_no_shard_has_persistence() {
        let mut agg = ShardInfoSnapshot::default();
        agg.absorb(shard_snap(0));
        assert!(agg.wal.is_none());
    }

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
                    if let ShardMessage::InfoSnapshot { response_tx } = env.message {
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
                if let ShardMessage::InfoSnapshot { response_tx } = env.message {
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
