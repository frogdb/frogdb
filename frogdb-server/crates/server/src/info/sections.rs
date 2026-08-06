//! INFO section renderers.
//!
//! One type per section; each owns its data access (via [`InfoSources`]
//! accessors) and its byte format (via [`SectionWriter`]). Real values are
//! emitted at the point of writing — there is no `0` placeholder for another
//! module to patch, so "the patch silently no-oped" is unrepresentable.

use frogdb_core::clock;
use std::time::UNIX_EPOCH;

use frogdb_cluster::version_gate;
use frogdb_core::histogram::KeysizeType;

use super::{
    InfoSection, InfoSources, SectionWriter, backlog_geometry_fields, net_byte_fields,
    replica_link_fields, sync_counter_fields,
};

/// The standard section registry, in canonical order.
pub(super) fn all_sections() -> Vec<Box<dyn InfoSection>> {
    vec![
        Box::new(ServerSection),
        Box::new(ClientsSection),
        Box::new(MemorySection),
        Box::new(PersistenceSection),
        Box::new(StatsSection),
        Box::new(ReplicationSection),
        Box::new(CpuSection),
        Box::new(KeyspaceSection),
        Box::new(RatelimitSection),
        Box::new(CommandstatsSection),
        Box::new(ErrorstatsSection),
        Box::new(LatencystatsSection),
        Box::new(LatencyBaselineSection),
        Box::new(TieredSection),
        Box::new(KeysizesSection),
    ]
}

// ============================================================================
// Server
// ============================================================================

struct ServerSection;

impl InfoSection for ServerSection {
    fn name(&self) -> &'static str {
        "server"
    }

    fn render(&self, src: &InfoSources) -> String {
        let now = clock::system_now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();

        let mut w = SectionWriter::new("Server");
        w.field("frogdb_version", env!("CARGO_PKG_VERSION"))
            .field("redis_version", frogdb_core::ADVERTISED_REDIS_VERSION)
            .field("redis_git_sha1", "00000000")
            .field("redis_git_dirty", 0)
            .field("redis_build_id", 0)
            .field("redis_mode", "standalone")
            .field(
                "os",
                format!(
                    "{} {} {}",
                    std::env::consts::OS,
                    std::env::consts::FAMILY,
                    std::env::consts::ARCH
                ),
            )
            .field("arch_bits", std::mem::size_of::<usize>() * 8)
            .field("monotonic_clock", "POSIX clock_gettime")
            .field("multiplexing_api", "tokio")
            .field("atomicvar_api", "std")
            .field("gcc_version", "0.0.0")
            .field("process_id", std::process::id())
            .field("process_supervised", "no")
            .field("run_id", "frogdb0000000000000000000000000000000000")
            .field("tcp_port", 6379)
            .field("server_time_usec", now.as_micros())
            .field("uptime_in_seconds", 0)
            .field("uptime_in_days", 0)
            .field("hz", 10)
            .field("configured_hz", 10)
            .field("lru_clock", 0)
            .field("executable", "/usr/local/bin/frogdb")
            .field("config_file", "")
            .field("io_threads_active", 0);

        // Version-gated: include cluster version fields after finalization.
        if let Some(cs) = src.cluster_state() {
            let active = cs.active_version();
            if version_gate::is_gate_active("extended_info_fields", active.as_deref()) {
                let snapshot = cs.snapshot();
                let cluster_version = snapshot
                    .nodes
                    .values()
                    .filter(|n| !n.version.is_empty())
                    .map(|n| n.version.as_str())
                    .min()
                    .unwrap_or("");
                w.field("active_version", active.as_deref().unwrap_or(""))
                    .field("cluster_version", cluster_version);
            }
        }

        w.finish()
    }
}

// ============================================================================
// Clients
// ============================================================================

struct ClientsSection;

impl InfoSection for ClientsSection {
    fn name(&self) -> &'static str {
        "clients"
    }

    fn render(&self, src: &InfoSources) -> String {
        let c = src.clients();
        let mut w = SectionWriter::new("Clients");
        w.field("connected_clients", c.connected)
            .field("cluster_connections", 0)
            .field("maxclients", c.max_clients)
            .field("client_recent_max_input_buffer", 0)
            .field("client_recent_max_output_buffer", 0)
            .field("blocked_clients", c.blocked)
            .field("tracking_clients", 0)
            .field("clients_in_timeout_table", 0);
        w.finish()
    }
}

// ============================================================================
// Memory
// ============================================================================

struct MemorySection;

impl InfoSection for MemorySection {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let cfg = src.memory_config();
        let used = sh.used_memory;
        let peak = sh.peak_memory.max(used as u64);
        let peak_perc = if peak > 0 {
            used as f64 * 100.0 / peak as f64
        } else {
            100.0
        };

        let mut w = SectionWriter::new("Memory");
        w.field("used_memory", used)
            .field("used_memory_human", format!("{}K", used / 1024))
            .field("used_memory_rss", used)
            .field("used_memory_rss_human", format!("{}K", used / 1024))
            .field("used_memory_peak", peak)
            .field("used_memory_peak_human", format!("{}K", peak / 1024))
            .field("used_memory_peak_perc", format!("{peak_perc:.2}%"))
            .field("used_memory_overhead", 0)
            .field("used_memory_startup", 0)
            .field("used_memory_dataset", used)
            .field("used_memory_dataset_perc", "100.00%")
            .field("allocator_allocated", 0)
            .field("allocator_active", 0)
            .field("allocator_resident", 0)
            .field("total_system_memory", 0)
            .field("total_system_memory_human", "0K")
            .field("used_memory_lua", 0)
            .field("used_memory_lua_human", "0B")
            .field("used_memory_scripts", 0)
            .field("used_memory_scripts_human", "0B")
            .field("number_of_cached_scripts", 0)
            .field("maxmemory", cfg.maxmemory)
            .field(
                "maxmemory_human",
                if cfg.maxmemory == 0 {
                    "0B".to_string()
                } else {
                    format!("{}K", cfg.maxmemory / 1024)
                },
            )
            .field("maxmemory_policy", &cfg.policy)
            .field("allocator_frag_ratio", "1.00")
            .field("allocator_frag_bytes", 0)
            .field("allocator_rss_ratio", "1.00")
            .field("allocator_rss_bytes", 0)
            .field("rss_overhead_ratio", "1.00")
            .field("rss_overhead_bytes", 0)
            .field("mem_fragmentation_ratio", "1.00")
            .field("mem_fragmentation_bytes", 0)
            .field("mem_not_counted_for_evict", 0)
            .field("mem_replication_backlog", 0)
            .field("mem_clients_slaves", 0)
            .field("mem_clients_normal", 0)
            .field("mem_aof_buffer", 0)
            .field("mem_allocator", "rust")
            .field("active_defrag_running", 0)
            .field("lazyfree_pending_objects", 0)
            .field("lazyfreed_objects", sh.lazyfreed_objects);
        w.finish()
    }
}

// ============================================================================
// Persistence
// ============================================================================

struct PersistenceSection;

impl InfoSection for PersistenceSection {
    fn name(&self) -> &'static str {
        "persistence"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let p = src.persistence();

        let mut w = SectionWriter::new("Persistence");
        w.field("loading", 0)
            .field("async_loading", 0)
            .field("persistence_enabled", u8::from(sh.wal.is_some()))
            .field("durability_mode", &p.durability_mode);

        // Real aggregated WAL lag — the fields are present only when
        // persistence is actually enabled, never placeholder zeros.
        if let Some(wal) = &sh.wal {
            w.field("wal_pending_ops", wal.pending_ops)
                .field("wal_pending_bytes", wal.pending_bytes)
                .field("wal_durability_lag_ms", wal.max_durability_lag_ms)
                .field(
                    "wal_last_flush_status",
                    if wal.last_flush_ok { "ok" } else { "err" },
                )
                .field("wal_flush_failures", wal.flush_failures)
                .field("wal_lost_ops", wal.lost_ops)
                .field("wal_last_flush_time", wal.last_flush_time_ms() / 1000)
                .field_opt("wal_writes_total", src.wal_writes_total())
                .field_opt("wal_bytes_total", src.wal_bytes_total());
        }

        w.field("current_cow_peak", 0)
            .field("current_cow_size", 0)
            .field("current_cow_size_age", 0)
            .field("current_fork_perc", "0.00")
            .field("current_save_keys_processed", 0)
            .field("current_save_keys_total", 0)
            .field("rdb_changes_since_last_save", sh.dirty);
        // The save outcome, counters, and durations: one field list shared
        // with the shard-local `redis.call('INFO')` renderer
        // (`persistence_snapshot_fields`, issue 10 / FM-PERSISTENCE-022) so
        // the two cannot report different save health for the same node.
        for (name, value) in
            crate::info::persistence_snapshot_fields(&p.snapshot_stats, p.bgsave_in_progress)
        {
            w.field(name, value);
        }
        w.field("rdb_last_cow_size", 0)
            // What this boot's recovery did, fixed for the life of the process
            // (Redis reports the same two about its last RDB load). The failed
            // count is a FrogDB extension: an undecodable value is skipped
            // rather than fatal, so without a count a boot can lose keys and
            // look identical to one that never had them.
            .field("rdb_last_load_keys_expired", p.load_keys_expired)
            .field("rdb_last_load_keys_loaded", p.load_keys_loaded)
            .field("rdb_last_load_keys_failed", p.load_keys_failed)
            .field("aof_enabled", 0)
            .field("aof_rewrite_in_progress", 0)
            .field("aof_rewrite_scheduled", 0)
            .field("aof_last_rewrite_time_sec", -1)
            .field("aof_current_rewrite_time_sec", -1)
            .field("aof_last_bgrewrite_status", "ok")
            .field("aof_rewrites", 0)
            .field("aof_rewrites_consecutive_failures", 0)
            .field("aof_last_write_status", "ok")
            .field("aof_last_cow_size", 0)
            .field("module_fork_in_progress", 0)
            .field("module_fork_last_cow_size", 0);
        w.finish()
    }
}

// ============================================================================
// Stats
// ============================================================================

struct StatsSection;

impl InfoSection for StatsSection {
    fn name(&self) -> &'static str {
        "stats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let sh = src.shards();
        let mut w = SectionWriter::new("Stats");
        w.field("total_connections_received", 1)
            // Real total from the shared `frogdb_commands_total` counter (0 when
            // metrics are disabled), matching `/status` `commands.total_processed`.
            .field(
                "total_commands_processed",
                src.total_commands_processed().unwrap_or(0),
            )
            // No instantaneous-rate sampler exists yet; kept as a Redis-compat
            // stub rather than a fabricated value (see `/status` `ops_per_sec`,
            // which is omitted for the same reason).
            .field("instantaneous_ops_per_sec", 0)
            .field("total_net_input_bytes", 0)
            .field("total_net_output_bytes", 0);
        // The real replication transfer-byte counters (hardening issue 29),
        // from the one shared field list, so this renderer and the
        // shard-local one in `crate::commands::info` cannot report a
        // different set — no longer the hardcoded-zero literals both used to
        // emit.
        for (name, value) in net_byte_fields(src.replication().net_bytes) {
            w.field(name, value);
        }
        w.field("instantaneous_input_kbps", "0.00")
            .field("instantaneous_output_kbps", "0.00")
            .field("instantaneous_input_repl_kbps", "0.00")
            .field("instantaneous_output_repl_kbps", "0.00")
            .field("rejected_connections", 0);
        // The PSYNC-outcome counters, from the one shared field list, so this
        // renderer and the shard-local one in `crate::commands::info` cannot
        // report a different set (FM-REPLICATION-050).
        for (name, value) in sync_counter_fields(src.replication().sync) {
            w.field(name, value);
        }
        w.field("expired_keys", sh.expired_keys)
            .field("expired_stale_perc", "0.00")
            .field("expired_time_cap_reached_count", 0)
            .field("expire_cycle_cpu_milliseconds", 0)
            .field("evicted_keys", sh.evicted_keys)
            .field("evicted_clients", 0)
            .field("total_eviction_exceeded_time", 0)
            .field("current_eviction_exceeded_time", 0)
            // Resettable reported values from the KeyspaceStats accumulator
            // (CONFIG RESETSTAT advances the baseline; the Prometheus _total
            // counters stay monotonic).
            .field_opt("keyspace_hits", src.keyspace_hits())
            .field_opt("keyspace_misses", src.keyspace_misses())
            .field("pubsub_channels", 0)
            .field("pubsub_patterns", 0)
            .field("pubsubshard_channels", 0)
            .field("latest_fork_usec", 0)
            .field("total_forks", 0)
            .field("migrate_cached_sockets", 0)
            .field("slave_expires_tracked_keys", 0)
            .field("active_defrag_hits", 0)
            .field("active_defrag_misses", 0)
            .field("active_defrag_key_hits", 0)
            .field("active_defrag_key_misses", 0)
            .field("total_active_defrag_time", 0)
            .field("current_active_defrag_time", 0)
            // Client-side-caching tracking-table size; FrogDB does not yet
            // count tracked keys (previously this misreported the db size).
            .field("tracking_total_keys", 0)
            .field("tracking_total_items", 0)
            .field("tracking_total_prefixes", 0)
            .field("unexpected_error_replies", 0)
            .field("total_error_replies", src.total_error_replies())
            .field("dump_payload_sanitizations", 0)
            .field("total_reads_processed", 0)
            .field("total_writes_processed", 0)
            .field("io_threaded_reads_processed", 0)
            .field("io_threaded_writes_processed", 0);
        w.finish()
    }
}

// ============================================================================
// Replication
// ============================================================================

struct ReplicationSection;

/// The all-zero `master_replid2` Redis reports when no failover window exists.
const ZERO_REPLID: &str = "0000000000000000000000000000000000000000";

impl InfoSection for ReplicationSection {
    fn name(&self) -> &'static str {
        "replication"
    }

    fn render(&self, src: &InfoSources) -> String {
        let r = src.replication();
        let replid = r.replid();
        // Failover-continuity pair, shared by both role arms below. When a
        // previous-primary window exists we surface it verbatim: replid2 is the
        // old id and second_repl_offset is FrogDB's inclusive boundary (the last
        // offset `window_contains` will still continue via replid2). No window
        // yet -> the all-zero id and the -1 sentinel Redis uses.
        let (replid2, second_repl_offset) = match &r.secondary_window {
            Some((prev_id, boundary)) => (prev_id.as_str(), *boundary),
            None => (ZERO_REPLID, -1),
        };
        let mut w = SectionWriter::new("Replication");

        if let Some(primary) = &r.primary {
            w.field("role", "master")
                .field("connected_slaves", primary.replicas.len());
            for (i, replica) in primary.replicas.iter().enumerate() {
                w.line(&replica.render(i));
            }
            w.field("master_failover_state", "no-failover")
                .field("master_replid", &replid)
                .field("master_replid2", replid2)
                .field("master_repl_offset", r.repl_offset)
                .field("second_repl_offset", second_repl_offset);
            // Geometry, not literals, and through the one shared list the
            // shard-local renderer also uses (FM-REPLICATION-059).
            for (name, value) in backlog_geometry_fields(r.backlog) {
                w.field(name, value);
            }
        } else {
            w.field("role", if r.is_replica { "slave" } else { "master" });
            if r.is_replica {
                w.field_opt("master_host", r.master_host.as_deref())
                    .field_opt("master_port", r.master_port);
                // The link status and, only when the stream gave up, why —
                // through the one shared list the shard-local renderer also
                // uses (FM-REPLICATION-061).
                for (name, value) in
                    replica_link_fields(r.master_link_up, r.master_sync_error.as_deref())
                {
                    w.field(name, value);
                }
            }
            w.field("connected_slaves", 0)
                .field("master_failover_state", "no-failover")
                .field("master_replid", &replid)
                .field("master_replid2", replid2)
                // A replica reports the offset it has applied, not zero: it is
                // the offset it would resume from, and the boundary it would
                // freeze if promoted. Matches Redis, which keeps one
                // `master_repl_offset` counter across both roles.
                .field("master_repl_offset", r.repl_offset)
                .field("second_repl_offset", second_repl_offset);
            // Same list on the replica branch: the capacity is this node's
            // config whatever role it runs, and an unarmed window renders
            // `active:0` with a first byte offset and histlen of 0 by
            // projection rather than by literal.
            for (name, value) in backlog_geometry_fields(r.backlog) {
                w.field(name, value);
            }
        }
        w.finish()
    }
}

// ============================================================================
// CPU
// ============================================================================

struct CpuSection;

impl InfoSection for CpuSection {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn render(&self, _src: &InfoSources) -> String {
        let mut w = SectionWriter::new("CPU");
        w.field("used_cpu_sys", "0.000000")
            .field("used_cpu_user", "0.000000")
            .field("used_cpu_sys_children", "0.000000")
            .field("used_cpu_user_children", "0.000000")
            .field("used_cpu_sys_main_thread", "0.000000")
            .field("used_cpu_user_main_thread", "0.000000");
        w.finish()
    }
}

// ============================================================================
// Keyspace
// ============================================================================

struct KeyspaceSection;

impl InfoSection for KeyspaceSection {
    fn name(&self) -> &'static str {
        "keyspace"
    }

    fn render(&self, src: &InfoSources) -> String {
        let keys = src.shards().keys;
        let mut w = SectionWriter::new("Keyspace");
        if keys > 0 {
            w.line(&format!("db0:keys={keys},expires=0,avg_ttl=0"));
        }
        w.finish()
    }
}

// ============================================================================
// Ratelimit
// ============================================================================

struct RatelimitSection;

impl InfoSection for RatelimitSection {
    fn name(&self) -> &'static str {
        "ratelimit"
    }

    fn render(&self, src: &InfoSources) -> String {
        let rl = src.rate_limit();
        if !rl.is_active() {
            return String::new();
        }
        let mut w = SectionWriter::new("Ratelimit");
        w.field("ratelimit_users_configured", rl.users)
            .field("ratelimit_total_commands_rejected", rl.commands_rejected)
            .field("ratelimit_total_bytes_rejected", rl.bytes_rejected);
        w.finish()
    }
}

// ============================================================================
// Commandstats
// ============================================================================

struct CommandstatsSection;

impl InfoSection for CommandstatsSection {
    fn name(&self) -> &'static str {
        "commandstats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Commandstats");
        for (cmd, stats) in src.command_stats() {
            let usec_per_call = if stats.calls > 0 {
                stats.usec as f64 / stats.calls as f64
            } else {
                0.0
            };
            w.line(&format!(
                "cmdstat_{cmd}:calls={},usec={},usec_per_call={usec_per_call:.2},rejected_calls={},failed_calls={}",
                stats.calls, stats.usec, stats.rejected_calls, stats.failed_calls,
            ));
        }
        w.finish()
    }
}

// ============================================================================
// Errorstats
// ============================================================================

struct ErrorstatsSection;

impl InfoSection for ErrorstatsSection {
    fn name(&self) -> &'static str {
        "errorstats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Errorstats");
        for (prefix, count) in src.error_types() {
            w.line(&format!("errorstat_{prefix}:count={count}"));
        }
        w.finish()
    }
}

// ============================================================================
// Latencystats
// ============================================================================

struct LatencystatsSection;

impl InfoSection for LatencystatsSection {
    fn name(&self) -> &'static str {
        "latencystats"
    }

    fn render(&self, src: &InfoSources) -> String {
        let lt = src.latency();
        let mut w = SectionWriter::new("Latencystats");
        if lt.histograms.is_enabled() && !lt.percentiles.is_empty() {
            let mut cmds = lt.histograms.all_commands();
            cmds.sort();
            for cmd in cmds {
                if let Some(pvals) = lt.histograms.percentiles_for(&cmd, &lt.percentiles) {
                    let parts: Vec<String> = pvals
                        .iter()
                        .map(|(p, us)| {
                            // Microseconds to milliseconds, 3 decimal places.
                            format!("p{}={:.3}", format_percentile(*p), us / 1000.0)
                        })
                        .collect();
                    w.line(&format!("latencystats_{cmd}:{}", parts.join(",")));
                }
            }
        }
        w.finish()
    }
}

/// Format a percentile value for display (e.g. 99.9 -> "99.9", 50.0 -> "50").
fn format_percentile(p: f64) -> String {
    if p == p.floor() {
        format!("{}", p as u64)
    } else {
        format!("{p}")
    }
}

// ============================================================================
// Latency_Baseline
// ============================================================================

struct LatencyBaselineSection;

impl InfoSection for LatencyBaselineSection {
    fn name(&self) -> &'static str {
        "latency_baseline"
    }

    fn render(&self, src: &InfoSources) -> String {
        let mut w = SectionWriter::new("Latency_Baseline");
        match src.baseline() {
            Some(b) => {
                w.field("baseline_test_run", 1)
                    .field("baseline_duration_secs", b.duration_secs)
                    .field("baseline_samples", b.samples)
                    .field("baseline_min_us", b.min_us)
                    .field("baseline_max_us", b.max_us)
                    .field("baseline_avg_us", format!("{:.1}", b.avg_us))
                    .field("baseline_p99_us", b.p99_us)
                    .field("baseline_warning_threshold_us", b.warning_threshold_us)
                    .field(
                        "baseline_exceeded_threshold",
                        u8::from(b.max_us > b.warning_threshold_us),
                    );
            }
            None => {
                w.field("baseline_test_run", 0);
            }
        }
        w.finish()
    }
}

// ============================================================================
// Tiered
// ============================================================================

struct TieredSection;

impl InfoSection for TieredSection {
    fn name(&self) -> &'static str {
        "tiered"
    }

    fn render(&self, src: &InfoSources) -> String {
        let t = &src.shards().tiered;
        let enabled = t.warm_keys > 0 || t.spills > 0;
        let mut w = SectionWriter::new("Tiered");
        w.field("tiered_enabled", u8::from(enabled))
            .field("tiered_hot_keys", t.hot_keys)
            .field("tiered_warm_keys", t.warm_keys)
            .field("tiered_unspills", t.unspills)
            .field("tiered_spills", t.spills)
            .field("tiered_expired_on_unspill", t.expired_on_unspill);
        w.finish()
    }
}

// ============================================================================
// Keysizes
// ============================================================================

struct KeysizesSection;

impl InfoSection for KeysizesSection {
    fn name(&self) -> &'static str {
        "keysizes"
    }

    fn render(&self, src: &InfoSources) -> String {
        let keysizes = &src.shards().keysizes;
        let mut w = SectionWriter::new("Keysizes");
        for ty in KeysizeType::ALL {
            let hist = keysizes.get(*ty);
            if !hist.is_empty() {
                w.line(&format!("{}:{}", ty.info_field_name(), hist.format_bins()));
            }
        }
        if src.key_memory_enabled() && !keysizes.key_memory.is_empty() {
            w.line(&format!(
                "distrib_key_sizes:{}",
                keysizes.key_memory.format_bins()
            ));
        }
        w.finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::test_support::sources;
    use super::super::{
        InfoBuilder, PrimarySnapshot, RateLimitSnapshot, ReplicaLine, ReplicaState, SectionSelector,
    };
    use super::*;
    use frogdb_core::{ServerCommandStats, WalLagAggregate};

    fn render(section: &dyn InfoSection, src: &InfoSources) -> String {
        section.render(src)
    }

    #[test]
    fn stats_renders_shard_sums_and_real_error_count() {
        let mut src = sources();
        src.shards.expired_keys = 12;
        src.shards.evicted_keys = 7;
        src.total_error_replies = 3;
        let out = render(&StatsSection, &src);
        assert!(out.contains("expired_keys:12\r\n"), "{out}");
        assert!(out.contains("evicted_keys:7\r\n"), "{out}");
        assert!(out.contains("total_error_replies:3\r\n"), "{out}");
    }

    #[test]
    fn stats_total_commands_processed_reflects_the_shared_counter() {
        use frogdb_telemetry::PrometheusRecorder;
        use frogdb_telemetry::definitions::CommandsTotal;

        // With metrics disabled the field is an honest 0, never absent.
        let src = sources();
        let out = render(&StatsSection, &src);
        assert!(out.contains("total_commands_processed:0\r\n"), "{out}");

        // Wired to the same `frogdb_commands_total` counter `/status` reads, so
        // INFO and `/status` cannot disagree.
        let recorder = std::sync::Arc::new(PrometheusRecorder::new());
        CommandsTotal::inc_by(&*recorder, 5, "GET");
        CommandsTotal::inc_by(&*recorder, 2, "SET");
        let mut src = sources();
        src.metrics = recorder;
        let out = render(&StatsSection, &src);
        assert!(out.contains("total_commands_processed:7\r\n"), "{out}");
    }

    #[test]
    fn stats_renders_keyspace_counts_even_with_metrics_disabled() {
        // The accumulator counts at the execution seam, independent of the
        // metrics recorder — a fresh server honestly reports 0, never an
        // absent field (proposal 24).
        let src = sources();
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:0\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:0\r\n"), "{out}");
    }

    #[test]
    fn stats_renders_reported_keyspace_values_and_reset_rebaselines() {
        let src = sources();
        src.keyspace_stats.record(42, 5);
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:42\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:5\r\n"), "{out}");

        // CONFIG RESETSTAT advances the baseline: reported values return to
        // zero while the cumulative view stays monotonic.
        src.keyspace_stats.reset();
        let out = render(&StatsSection, &src);
        assert!(out.contains("keyspace_hits:0\r\n"), "{out}");
        assert!(out.contains("keyspace_misses:0\r\n"), "{out}");
        assert_eq!(src.keyspace_stats.cumulative_hits(), 42);
        assert_eq!(src.keyspace_stats.cumulative_misses(), 5);
    }

    #[test]
    fn persistence_disabled_reports_zero_and_omits_wal_fields() {
        let src = sources();
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("persistence_enabled:0\r\n"), "{out}");
        assert!(!out.contains("wal_pending_ops"), "{out}");
        assert!(!out.contains("wal_durability_lag_ms"), "{out}");
        assert!(out.contains("rdb_changes_since_last_save:0\r\n"), "{out}");
    }

    #[test]
    fn persistence_enabled_renders_aggregated_wal_lag() {
        let mut src = sources();
        src.shards.dirty = 9;
        src.shards.wal = Some(WalLagAggregate {
            pending_ops: 4,
            pending_bytes: 128,
            max_durability_lag_ms: 55,
            flush_failures: 2,
            lost_ops: 1,
            last_flush_ok: false,
            last_flush_time_ms: 1_700_000_000_500,
            per_shard: Vec::new(),
        });
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("persistence_enabled:1\r\n"), "{out}");
        assert!(out.contains("wal_pending_ops:4\r\n"), "{out}");
        assert!(out.contains("wal_pending_bytes:128\r\n"), "{out}");
        assert!(out.contains("wal_durability_lag_ms:55\r\n"), "{out}");
        assert!(out.contains("wal_last_flush_status:err\r\n"), "{out}");
        assert!(out.contains("wal_flush_failures:2\r\n"), "{out}");
        assert!(out.contains("wal_lost_ops:1\r\n"), "{out}");
        assert!(out.contains("wal_last_flush_time:1700000000\r\n"), "{out}");
        assert!(out.contains("rdb_changes_since_last_save:9\r\n"), "{out}");
        // Metrics disabled: totals are honestly absent.
        assert!(!out.contains("wal_writes_total"), "{out}");
    }

    // FM-PERSISTENCE-022
    /// The save-outcome fields report the coordinator's real state: `err` with
    /// the cause while the last save failed, `ok` once one succeeds, and a
    /// failure counter that a later success does not erase.
    #[test]
    fn persistence_renders_the_real_bgsave_outcome() {
        // No save attempted: `ok`, no cause, zero counters (Redis' initial state).
        let src = sources();
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_status:ok\r\n"), "{out}");
        assert!(!out.contains("rdb_last_bgsave_error"), "{out}");
        assert!(out.contains("rdb_saves:0\r\n"), "{out}");
        assert!(out.contains("rdb_bgsave_failures:0\r\n"), "{out}");
        assert!(out.contains("rdb_last_save_time:0\r\n"), "{out}");

        // Last save failed: `err`, the cause, and the failure counted. The
        // save time still reports the last *successful* save.
        let mut src = sources();
        src.persistence.snapshot_stats.saves = 2;
        src.persistence.snapshot_stats.failures = 1;
        src.persistence.snapshot_stats.last_save_time =
            Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000));
        src.persistence.snapshot_stats.last_error =
            Some("IO error: No space left\non device".to_string());
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_status:err\r\n"), "{out}");
        assert!(
            out.contains("rdb_last_bgsave_error:IO error: No space left on device\r\n"),
            "the cause is reported with CR/LF folded to spaces: {out}"
        );
        assert!(out.contains("rdb_saves:2\r\n"), "{out}");
        assert!(out.contains("rdb_bgsave_failures:1\r\n"), "{out}");
        assert!(out.contains("rdb_last_save_time:1700000000\r\n"), "{out}");

        // Recovered: status back to `ok`, cause gone, failure count retained.
        src.persistence.snapshot_stats.last_error = None;
        src.persistence.snapshot_stats.saves = 3;
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_status:ok\r\n"), "{out}");
        assert!(!out.contains("rdb_last_bgsave_error"), "{out}");
        assert!(out.contains("rdb_saves:3\r\n"), "{out}");
        assert!(
            out.contains("rdb_bgsave_failures:1\r\n"),
            "a success must not erase the failure history: {out}"
        );
    }

    // FM-PERSISTENCE-033
    /// The three load fields report what *this boot's* recovery actually did,
    /// including the FrogDB-only failed count — the only positive signal that a
    /// boot dropped keys, since a smaller keyspace cannot say so by itself.
    #[test]
    fn persistence_renders_the_real_load_stats() {
        // A boot that loaded nothing reports zeros — the same rendering as the
        // old hardcoded fields, which is why nobody noticed they were fake.
        let src = sources();
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_load_keys_loaded:0\r\n"), "{out}");
        assert!(out.contains("rdb_last_load_keys_expired:0\r\n"), "{out}");
        assert!(out.contains("rdb_last_load_keys_failed:0\r\n"), "{out}");

        let mut src = sources();
        src.persistence.load_keys_loaded = 4_200;
        src.persistence.load_keys_expired = 17;
        src.persistence.load_keys_failed = 3;
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_load_keys_loaded:4200\r\n"), "{out}");
        assert!(out.contains("rdb_last_load_keys_expired:17\r\n"), "{out}");
        assert!(
            out.contains("rdb_last_load_keys_failed:3\r\n"),
            "skipped-undecodable keys must be reportable: {out}"
        );
    }

    // FM-PERSISTENCE-022
    /// The two save-duration fields carry Redis' `-1` sentinel only where Redis
    /// means it — "no save has completed" and "no save is running" — and a real
    /// second count everywhere else.
    #[test]
    fn persistence_renders_save_durations_with_redis_sentinels() {
        let src = sources();
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_time_sec:-1\r\n"), "{out}");
        assert!(out.contains("rdb_current_bgsave_time_sec:-1\r\n"), "{out}");

        let mut src = sources();
        src.persistence.bgsave_in_progress = true;
        src.persistence.snapshot_stats.last_duration = Some(std::time::Duration::from_secs(7));
        src.persistence.snapshot_stats.current_started_at =
            Some(std::time::Instant::now() - std::time::Duration::from_secs(2));
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_time_sec:7\r\n"), "{out}");
        assert!(out.contains("rdb_current_bgsave_time_sec:2\r\n"), "{out}");
        assert!(out.contains("rdb_bgsave_in_progress:1\r\n"), "{out}");

        // A save that finished in under a second reports 0, not the -1 that
        // would claim no save ever ran.
        let mut src = sources();
        src.persistence.snapshot_stats.last_duration = Some(std::time::Duration::from_secs(0));
        let out = render(&PersistenceSection, &src);
        assert!(out.contains("rdb_last_bgsave_time_sec:0\r\n"), "{out}");
        assert!(out.contains("rdb_current_bgsave_time_sec:-1\r\n"), "{out}");
    }

    #[test]
    fn clients_renders_registry_counts() {
        let mut src = sources();
        src.clients.connected = 3;
        src.clients.blocked = 1;
        src.clients.max_clients = 500;
        let out = render(&ClientsSection, &src);
        assert!(out.contains("connected_clients:3\r\n"), "{out}");
        assert!(out.contains("blocked_clients:1\r\n"), "{out}");
        assert!(out.contains("maxclients:500\r\n"), "{out}");
    }

    #[test]
    fn memory_renders_fleet_wide_sums() {
        let mut src = sources();
        src.shards.used_memory = 2048;
        src.shards.peak_memory = 4096;
        src.shards.lazyfreed_objects = 6;
        let out = render(&MemorySection, &src);
        assert!(out.contains("used_memory:2048\r\n"), "{out}");
        assert!(out.contains("used_memory_human:2K\r\n"), "{out}");
        assert!(out.contains("used_memory_peak:4096\r\n"), "{out}");
        assert!(out.contains("used_memory_peak_perc:50.00%\r\n"), "{out}");
        assert!(out.contains("lazyfreed_objects:6\r\n"), "{out}");
        assert!(out.contains("maxmemory_policy:noeviction\r\n"), "{out}");
    }

    #[test]
    fn keyspace_empty_and_populated() {
        let mut src = sources();
        assert_eq!(
            render(&KeyspaceSection, &src),
            "# Keyspace\r\n\r\n",
            "no keys: header only"
        );
        src.shards.keys = 42;
        assert_eq!(
            render(&KeyspaceSection, &src),
            "# Keyspace\r\ndb0:keys=42,expires=0,avg_ttl=0\r\n\r\n"
        );
    }

    // FM-REPLICATION-023
    #[test]
    fn replication_standalone_renders_node_id_replid() {
        let mut src = sources();
        src.replication.node_id = Some(0xabc);
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid:{:040x}\r\n", 0xabc)),
            "{out}"
        );
        assert!(out.contains("connected_slaves:0\r\n"), "{out}");
    }

    // FM-REPLICATION-023
    #[test]
    fn replication_live_replid_overrides_node_id() {
        let mut src = sources();
        src.replication.node_id = Some(0xabc);
        src.replication.replication_id = Some("f00f".repeat(10));
        let out = render(&ReplicationSection, &src);
        assert!(
            out.contains(&format!("master_replid:{}\r\n", "f00f".repeat(10))),
            "{out}"
        );
        // Default (no failover): the distinct master_replid2 line is all-zero
        // and second_repl_offset is the -1 sentinel Redis reports for "no
        // secondary window".
        assert!(
            out.contains("master_replid2:0000000000000000000000000000000000000000\r\n"),
            "{out}"
        );
        assert!(out.contains("second_repl_offset:-1\r\n"), "{out}");
    }

    // FM-REPLICATION-023
    #[test]
    fn replication_primary_renders_secondary_window_after_failover() {
        // Primary arm: a promoted node exposes the previous primary's id as
        // master_replid2 and its inclusive boundary as second_repl_offset.
        let mut src = sources();
        src.replication.primary = Some(PrimarySnapshot { replicas: vec![] });
        src.replication.repl_offset = 5000;
        let prev_id = "abcd".repeat(10);
        src.replication.secondary_window = Some((prev_id.clone(), 4096));
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid2:{prev_id}\r\n")),
            "{out}"
        );
        // Rendered verbatim from FrogDB's inclusive secondary_offset, not
        // Redis's +1 exclusive convention.
        assert!(out.contains("second_repl_offset:4096\r\n"), "{out}");
    }

    // FM-REPLICATION-023
    #[test]
    fn replication_replica_renders_secondary_window_after_failover() {
        // Else arm (replica / standalone-master): the same window pair renders
        // when there is no primary tracker.
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        let prev_id = "1234".repeat(10);
        src.replication.secondary_window = Some((prev_id.clone(), 0));
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(
            out.contains(&format!("master_replid2:{prev_id}\r\n")),
            "{out}"
        );
        // Boundary of 0 is a real (inclusive) window, distinct from the -1
        // no-failover sentinel.
        assert!(out.contains("second_repl_offset:0\r\n"), "{out}");
    }

    // FM-REPLICATION-043
    #[test]
    fn replication_replica_renders_master_link_up_when_streaming() {
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        src.replication.master_link_up = true;
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(out.contains("master_host:10.0.0.1\r\n"), "{out}");
        assert!(out.contains("master_port:6380\r\n"), "{out}");
        assert!(out.contains("master_link_status:up\r\n"), "{out}");
    }

    // FM-REPLICATION-043
    #[test]
    fn replication_replica_renders_master_link_down_when_not_streaming() {
        // The link starts (and stays) down until the replica connection
        // machinery reaches the Streaming state — must never default to "up".
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        src.replication.master_link_up = false;
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(out.contains("master_link_status:down\r\n"), "{out}");
    }

    // FM-REPLICATION-043
    #[test]
    fn replication_replica_renders_its_applied_offset() {
        // A replica's master_repl_offset is the offset it has applied — the
        // value it would resume from and freeze as the failover boundary if
        // promoted. Reporting a hardcoded 0 made a promoted node's
        // second_repl_offset look like it came out of nowhere.
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        src.replication.repl_offset = 172;
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(out.contains("master_repl_offset:172\r\n"), "{out}");
    }

    /// A snapshot shaped like one a completed handshake produces.
    fn streaming_replica(listening_port: u16) -> frogdb_replication::ReplicaInfo {
        use frogdb_replication::{Phase, ReplicaCapabilities, ReplicaInfo, ReplicaSession};
        let session = ReplicaSession::new(1, "127.0.0.1:54321".parse().unwrap());
        ReplicaInfo {
            listening_port,
            acked_offset: 99,
            phase: Phase::Streaming,
            capabilities: ReplicaCapabilities::default(),
            ..session.snapshot()
        }
    }

    // FM-REPLICATION-043, FM-REPLICATION-049
    /// Every field of a `slaveN:` line is projected off the replica's own
    /// snapshot. `state` and `lag` used to be string constants in the format
    /// call (GAP-2 / GAP-3): `state=online` was true only because the caller
    /// pre-filtered to streaming replicas — two facts in two files with nothing
    /// tying them — and `lag=0` was a literal even though the tracker already
    /// computed the number the lag-disconnect policy acts on.
    #[test]
    fn a_slave_line_is_projected_from_the_replica_not_from_literals() {
        use frogdb_replication::Phase;

        let line = ReplicaLine::from_replica(&streaming_replica(7001))
            .expect("a streaming replica renders a line");
        assert_eq!(line.port, 7001, "the announced port, not 0");
        assert_eq!(line.state, ReplicaState::Online);
        assert_eq!(
            line.render(0),
            "slave0:ip=127.0.0.1,port=7001,state=online,offset=99,lag=0"
        );

        // The state now follows the phase rather than the caller's filter: a
        // replica still receiving its checkpoint reports `send_bulk`, which is
        // what tells an operator the link is not yet usable for WAIT.
        let mut mid_sync = streaming_replica(7001);
        mid_sync.phase = Phase::StreamingCheckpoint;
        let line = ReplicaLine::from_replica(&mid_sync).expect("a syncing replica renders a line");
        assert_eq!(line.state, ReplicaState::SendBulk);
        assert!(line.render(2).contains("state=send_bulk"), "{line:?}");

        // Every phase maps to a state or to no line at all; none may render as
        // `online` by default.
        for (phase, expected) in [
            (Phase::Connecting, Some(ReplicaState::WaitBgsave)),
            (Phase::PreparingCheckpoint, Some(ReplicaState::WaitBgsave)),
            (Phase::StreamingCheckpoint, Some(ReplicaState::SendBulk)),
            (Phase::Streaming, Some(ReplicaState::Online)),
            // No Redis spelling: the line is dropped (FM-REPLICATION-060).
            (Phase::Disconnecting, None),
        ] {
            assert_eq!(ReplicaState::from_phase(phase), expected, "{phase:?}");
        }
    }

    /// A tracker holding one replica per requested phase, registered in the
    /// order given so the ids ascend with the phases.
    fn tracker_with_phases(
        phases: &[frogdb_replication::Phase],
    ) -> std::sync::Arc<frogdb_replication::ReplicationTrackerImpl> {
        let tracker = frogdb_replication::ReplicationTrackerImpl::new_arc();
        for (i, phase) in phases.iter().enumerate() {
            let address = format!("127.0.0.1:{}", 54321 + i as u16).parse().unwrap();
            let session = tracker.register_announced_replica(
                address,
                frogdb_replication::ReplicaAnnouncement {
                    listening_port: 7000 + i as u16,
                    ..Default::default()
                },
            );
            session.force_phase_for_test(*phase);
        }
        tracker
    }

    // FM-REPLICATION-060
    /// A replica being fed its checkpoint is *rendered*, as `send_bulk`. It is
    /// the situation an operator opens `INFO replication` to see, and both
    /// renderers used to feed from `get_streaming_replicas()`, so such a
    /// replica appeared in no `slaveN:` line and in no `connected_slaves` count
    /// — absent, not misreported (issue 21).
    #[test]
    fn a_replica_being_fed_a_checkpoint_is_rendered_as_send_bulk() {
        use frogdb_replication::Phase;

        let tracker = tracker_with_phases(&[Phase::StreamingCheckpoint]);
        let lines = crate::info::rendered_replicas(&tracker);
        assert_eq!(lines.len(), 1, "a syncing replica must be rendered");
        assert_eq!(lines[0].state, ReplicaState::SendBulk);
        assert!(lines[0].render(0).contains("state=send_bulk"), "{lines:?}");
    }

    // FM-REPLICATION-043
    // FM-REPLICATION-060
    /// `connected_slaves` is the count of rendered lines over the wider feed:
    /// every replica in a phase Redis names, whatever its phase. The count and
    /// the line list are one projection (FM-REPLICATION-043), so they cannot
    /// drift regardless of which phases the registry holds.
    #[test]
    fn connected_slaves_counts_every_rendered_line() {
        use frogdb_replication::Phase;

        let tracker = tracker_with_phases(&[
            Phase::Connecting,
            Phase::PreparingCheckpoint,
            Phase::StreamingCheckpoint,
            Phase::Streaming,
        ]);
        let replicas = crate::info::rendered_replicas(&tracker);
        assert_eq!(replicas.len(), 4, "every named phase is rendered");
        assert_eq!(
            replicas.iter().map(|l| l.state).collect::<Vec<_>>(),
            vec![
                ReplicaState::WaitBgsave,
                ReplicaState::WaitBgsave,
                ReplicaState::SendBulk,
                ReplicaState::Online,
            ],
            "registry order is attach order, and each line states its own phase"
        );

        let mut src = sources();
        src.replication.primary = Some(PrimarySnapshot { replicas });
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("connected_slaves:4\r\n"), "{out}");
        let rendered = out.lines().filter(|l| l.starts_with("slave")).count();
        assert_eq!(
            rendered, 4,
            "the count and the line count are one projection: {out}"
        );
    }

    // FM-REPLICATION-060
    /// The one phase that stays filtered. Redis's `genInfoSectionDict` emits a
    /// line only for `wait_bgsave` / `send_bulk` / `online` and skips a slave in
    /// any other state; `Disconnecting` is FrogDB's fourth phase and has no
    /// Redis spelling, so it is dropped at the render boundary rather than
    /// given the invented `offline` state it used to carry.
    #[test]
    fn a_disconnecting_replica_is_not_rendered() {
        use frogdb_replication::Phase;

        let tracker = tracker_with_phases(&[Phase::Streaming, Phase::Disconnecting]);
        let lines = crate::info::rendered_replicas(&tracker);
        assert_eq!(lines.len(), 1, "the tearing-down replica is dropped");
        assert_eq!(lines[0].state, ReplicaState::Online);
        assert_eq!(
            lines[0].port, 7000,
            "the surviving line is the first replica"
        );
    }

    // FM-REPLICATION-060
    /// The feed is ordered, so two consecutive `INFO` calls against an
    /// unchanged registry render the same `slaveN:` indices. The registry is a
    /// `HashMap`, whose iteration order is not stable across renders — an
    /// operator watching `slave0:` would otherwise see it hop between replicas.
    #[test]
    fn rendered_replicas_are_ordered_by_attach() {
        use frogdb_replication::Phase;

        let tracker = tracker_with_phases(&[Phase::Streaming; 8]);
        let ports = |lines: &[ReplicaLine]| lines.iter().map(|l| l.port).collect::<Vec<_>>();
        let first = crate::info::rendered_replicas(&tracker);
        assert_eq!(ports(&first), (7000..7008).collect::<Vec<u16>>());
        assert_eq!(
            ports(&crate::info::rendered_replicas(&tracker)),
            ports(&first),
            "the order must not depend on hash iteration"
        );
    }

    // FM-REPLICATION-043, FM-REPLICATION-049
    /// `lag` is the age of the replica's last ACK in whole seconds, so a
    /// replica that has gone quiet renders a growing number instead of a
    /// permanent `0` (GAP-3).
    #[test]
    fn a_slave_line_reports_the_real_ack_age() {
        use std::time::{Duration, Instant};

        let mut replica = streaming_replica(7001);
        replica.last_ack_time = Instant::now() - Duration::from_secs(4);
        let line = ReplicaLine::from_replica(&replica).expect("a streaming replica renders");
        assert_eq!(line.lag_secs, 4);
        assert!(line.render(0).ends_with(",lag=4"), "{line:?}");
    }

    // FM-REPLICATION-050
    /// The two INFO renderers report the same three PSYNC counters for the same
    /// state. They are separate code paths over separate data sources
    /// (connection-level sections vs. the shard-local builder scripts see), and
    /// the duplication is exactly how both ended up hardcoding zeros.
    #[test]
    fn both_info_renderers_report_the_same_sync_counters() {
        use frogdb_replication::SyncCountersSnapshot;

        let sync = SyncCountersSnapshot {
            full: 3,
            partial_ok: 4,
            partial_err: 5,
        };
        let mut src = sources();
        src.replication.sync = sync;

        let connection_level = render(&StatsSection, &src);
        let shard_local = crate::commands::info::build_stats_info(sync, src.replication.net_bytes);

        let sync_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| line.starts_with("sync_"))
                .map(str::to_string)
                .collect()
        };

        assert_eq!(
            sync_lines(&connection_level),
            vec![
                "sync_full:3".to_string(),
                "sync_partial_ok:4".to_string(),
                "sync_partial_err:5".to_string(),
            ],
            "{connection_level}"
        );
        assert_eq!(
            sync_lines(&connection_level),
            sync_lines(&shard_local),
            "the two INFO renderers must agree for the same state"
        );
    }

    // FM-REPLICATION-058
    /// After `CONFIG RESETSTAT` both renderers report zeros — read through the
    /// tracker, the way the live server reads them, so the reset is observed at
    /// the wire format and not only at the counter.
    #[test]
    fn both_info_renderers_report_zeros_after_the_counters_are_reset() {
        use frogdb_core::ReplicationTrackerImpl;
        use frogdb_replication::SyncOutcome;

        let tracker = ReplicationTrackerImpl::new();
        tracker.record_sync_outcome(SyncOutcome::PartialRefused);
        tracker.record_sync_outcome(SyncOutcome::PartialOk);
        tracker.record_sync_outcome(SyncOutcome::FullResyncRequested);
        tracker.reset_sync_counters();

        let sync = tracker.sync_counters();
        let mut src = sources();
        src.replication.sync = sync;

        let expected = vec![
            "sync_full:0".to_string(),
            "sync_partial_ok:0".to_string(),
            "sync_partial_err:0".to_string(),
        ];
        let sync_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| line.starts_with("sync_"))
                .map(str::to_string)
                .collect()
        };

        let connection_level = render(&StatsSection, &src);
        assert_eq!(
            sync_lines(&connection_level),
            expected,
            "{connection_level}"
        );
        assert_eq!(
            sync_lines(&crate::commands::info::build_stats_info(
                sync,
                src.replication.net_bytes
            )),
            expected
        );
    }

    // FM-REPLICATION-063
    /// The two INFO renderers report the same net-byte counters for the same
    /// state (hardening issue 29): both used to hardcode `0` independently,
    /// which is exactly the shape that lets one renderer "fix" a field the
    /// other keeps faking.
    #[test]
    fn both_info_renderers_report_the_same_repl_byte_counters() {
        use frogdb_replication::NetByteCountersSnapshot;

        let net_bytes = NetByteCountersSnapshot {
            input: 12_345,
            output: 67_890,
        };
        let mut src = sources();
        src.replication.net_bytes = net_bytes;

        let connection_level = render(&StatsSection, &src);
        let shard_local = crate::commands::info::build_stats_info(src.replication.sync, net_bytes);

        let net_byte_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| line.starts_with("total_net_repl_"))
                .map(str::to_string)
                .collect()
        };

        assert_eq!(
            net_byte_lines(&connection_level),
            vec![
                "total_net_repl_input_bytes:12345".to_string(),
                "total_net_repl_output_bytes:67890".to_string(),
            ],
            "{connection_level}"
        );
        assert_eq!(
            net_byte_lines(&connection_level),
            net_byte_lines(&shard_local),
            "the two INFO renderers must agree for the same state"
        );
    }

    // FM-REPLICATION-063
    /// After `CONFIG RESETSTAT` both renderers report zero net-repl-bytes,
    /// read through the tracker the way the live server does (hardening
    /// issue 29), mirroring
    /// `both_info_renderers_report_zeros_after_the_counters_are_reset` for
    /// `sync_*` above. The end-to-end RESETSTAT dispatch itself is covered by
    /// `config_resetstat_zeroes_the_repl_byte_counters` in `conn_command.rs`.
    #[test]
    fn both_info_renderers_report_zeros_after_the_repl_byte_counters_are_reset() {
        use frogdb_core::ReplicationTrackerImpl;

        let tracker = ReplicationTrackerImpl::new();
        tracker.net_bytes_handle().record_output(500);
        tracker.net_bytes_handle().record_input(200);
        tracker.reset_net_bytes();

        let net_bytes = tracker.net_bytes();
        let mut src = sources();
        src.replication.net_bytes = net_bytes;

        let expected = vec![
            "total_net_repl_input_bytes:0".to_string(),
            "total_net_repl_output_bytes:0".to_string(),
        ];
        let net_byte_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| line.starts_with("total_net_repl_"))
                .map(str::to_string)
                .collect()
        };

        let connection_level = render(&StatsSection, &src);
        assert_eq!(
            net_byte_lines(&connection_level),
            expected,
            "{connection_level}"
        );
        assert_eq!(
            net_byte_lines(&crate::commands::info::build_stats_info(
                src.replication.sync,
                net_bytes
            )),
            expected
        );
    }

    // FM-REPLICATION-050
    /// A primary that has served no PSYNC reports all three at zero, and the
    /// zero is live rather than printed: nudging one counter away from zero
    /// moves only that line. A renderer with `sync_full` etc. hardcoded to `0`
    /// (the pre-fix state) would render this baseline correctly too — zero and
    /// a literal `0` are the same bytes — and only the perturbation below can
    /// tell them apart.
    #[test]
    fn an_untouched_primary_reports_zero_sync_counters() {
        let out = render(&StatsSection, &sources());
        assert!(out.contains("sync_full:0\r\n"), "{out}");
        assert!(out.contains("sync_partial_ok:0\r\n"), "{out}");
        assert!(out.contains("sync_partial_err:0\r\n"), "{out}");

        let mut src = sources();
        src.replication.sync.full = 7;
        let out = render(&StatsSection, &src);
        assert!(out.contains("sync_full:7\r\n"), "{out}");
        assert!(out.contains("sync_partial_ok:0\r\n"), "{out}");
        assert!(out.contains("sync_partial_err:0\r\n"), "{out}");
    }

    // FM-REPLICATION-059
    /// The size reported is the one the backlog was built with, in both
    /// renderers. The pre-fix render printed Redis's 1 MiB default regardless,
    /// so an operator could not use the field to confirm a tuned
    /// `backlog-max-mb` had landed — the only reason to read it.
    #[test]
    fn the_backlog_size_reported_is_the_configured_one() {
        use frogdb_replication::primary::ReplicationRingBuffer;

        // A cap that is neither the Redis default nor the FrogDB default, so a
        // literal of either shape fails this.
        let ring = ReplicationRingBuffer::new(64, 3_145_728);
        let geometry = ring.geometry(0);

        let mut src = sources();
        src.replication.backlog = geometry;
        let connection_level = render(&ReplicationSection, &src);
        assert!(
            connection_level.contains("repl_backlog_size:3145728\r\n"),
            "{connection_level}"
        );
        assert!(
            !connection_level.contains("repl_backlog_size:1048576\r\n"),
            "the 1 MiB literal must be gone: {connection_level}"
        );

        let shard_local = crate::commands::info::build_backlog_info(geometry);
        assert!(
            shard_local.contains("repl_backlog_size:3145728\r\n"),
            "{shard_local}"
        );
    }

    // FM-REPLICATION-059
    /// The reported first byte offset is the armed eviction floor — the offset
    /// below which a partial resync is refused (FM-REPLICATION-014). The
    /// pre-fix `0` said the backlog serves from the beginning of history, which
    /// is the opposite of what the floor means, and it is the field an operator
    /// reads when diagnosing "why is every reconnect full-resyncing".
    #[test]
    fn the_first_byte_offset_reported_is_the_armed_floor() {
        use bytes::Bytes;
        use frogdb_replication::primary::ReplicationRingBuffer;

        // Two entries of capacity; a third evicts the first and raises the
        // floor to where the evicted entry ended.
        let ring = ReplicationRingBuffer::new(2, 1024 * 1024);
        ring.push(10, 0, Bytes::from("0123456789"));
        ring.push(20, 0, Bytes::from("0123456789"));
        ring.push(30, 0, Bytes::from("0123456789"));
        let floor = ring.start_offset().expect("pushes arm the window");
        assert_eq!(floor, 10, "eviction raised the floor");

        let mut src = sources();
        src.replication.backlog = ring.geometry(30);
        src.replication.repl_offset = 30;
        let out = render(&ReplicationSection, &src);
        assert!(
            out.contains(&format!("repl_backlog_first_byte_offset:{floor}\r\n")),
            "{out}"
        );
        assert!(out.contains("repl_backlog_active:1\r\n"), "{out}");
        // The window covers `(floor, head]`, so the two numbers must add up to
        // the head rather than each being reported from a different instant.
        assert!(out.contains("repl_backlog_histlen:20\r\n"), "{out}");
    }

    // FM-REPLICATION-059
    /// The twin of `both_info_renderers_report_the_same_sync_counters`: the
    /// four `repl_backlog_*` fields were literals in *both* renderers, so the
    /// check that matters is that one state renders identically through both.
    #[test]
    fn both_info_renderers_report_the_same_backlog_geometry() {
        use frogdb_replication::BacklogGeometry;

        let geometry = BacklogGeometry {
            active: true,
            size_bytes: 5_242_880,
            first_byte_offset: 4096,
            histlen: 512,
        };
        let mut src = sources();
        src.replication.backlog = geometry;

        let backlog_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| line.starts_with("repl_backlog_"))
                .map(str::to_string)
                .collect()
        };

        let connection_level = render(&ReplicationSection, &src);
        assert_eq!(
            backlog_lines(&connection_level),
            vec![
                "repl_backlog_active:1".to_string(),
                "repl_backlog_size:5242880".to_string(),
                "repl_backlog_first_byte_offset:4096".to_string(),
                "repl_backlog_histlen:512".to_string(),
            ],
            "{connection_level}"
        );
        assert_eq!(
            backlog_lines(&connection_level),
            backlog_lines(&crate::commands::info::build_backlog_info(geometry)),
            "the two INFO renderers must agree for the same state"
        );
    }

    // FM-REPLICATION-059
    /// A replica reports its node's configured capacity with no window open —
    /// the replica branch had the same two literals as the primary one, and it
    /// is the branch an operator reads while deciding whether a reconnect will
    /// be partial.
    #[test]
    fn a_replica_reports_its_configured_capacity_with_no_window() {
        use frogdb_replication::BacklogGeometry;

        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.backlog = BacklogGeometry {
            active: false,
            size_bytes: 2_097_152,
            first_byte_offset: 0,
            histlen: 0,
        };
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:slave\r\n"), "{out}");
        assert!(out.contains("repl_backlog_active:0\r\n"), "{out}");
        assert!(out.contains("repl_backlog_size:2097152\r\n"), "{out}");
        assert!(
            out.contains("repl_backlog_first_byte_offset:0\r\n"),
            "{out}"
        );
    }

    // FM-REPLICATION-043
    #[test]
    fn replication_primary_renders_slave_lines() {
        let mut src = sources();
        src.replication.primary = Some(PrimarySnapshot {
            replicas: vec![ReplicaLine {
                ip: "127.0.0.1".to_string(),
                port: 7001,
                state: ReplicaState::Online,
                offset: 99,
                lag_secs: 0,
            }],
        });
        src.replication.repl_offset = 100;
        // `repl_backlog_active` follows the backlog's own window, not the
        // replica count: a primary can have a replica attached and no window
        // (the backlog disabled or TTL-freed), and a window with no replica
        // attached (FM-REPLICATION-059).
        src.replication.backlog = frogdb_replication::BacklogGeometry {
            active: true,
            size_bytes: 1024,
            first_byte_offset: 0,
            histlen: 100,
        };
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("role:master\r\n"), "{out}");
        assert!(out.contains("connected_slaves:1\r\n"), "{out}");
        assert!(
            out.contains("slave0:ip=127.0.0.1,port=7001,state=online,offset=99,lag=0\r\n"),
            "{out}"
        );
        assert!(out.contains("master_repl_offset:100\r\n"), "{out}");
        assert!(out.contains("repl_backlog_active:1\r\n"), "{out}");
        // No failover window on this primary: zero replid2 and -1 sentinel.
        assert!(
            out.contains("master_replid2:0000000000000000000000000000000000000000\r\n"),
            "{out}"
        );
        assert!(out.contains("second_repl_offset:-1\r\n"), "{out}");
    }

    #[test]
    fn commandstats_renders_sorted_lines() {
        let mut src = sources();
        src.command_stats = vec![
            (
                "get".to_string(),
                ServerCommandStats {
                    calls: 2,
                    usec: 10,
                    rejected_calls: 0,
                    failed_calls: 1,
                },
            ),
            (
                "set".to_string(),
                ServerCommandStats {
                    calls: 1,
                    usec: 7,
                    rejected_calls: 2,
                    failed_calls: 0,
                },
            ),
        ];
        let out = render(&CommandstatsSection, &src);
        assert_eq!(
            out,
            "# Commandstats\r\n\
             cmdstat_get:calls=2,usec=10,usec_per_call=5.00,rejected_calls=0,failed_calls=1\r\n\
             cmdstat_set:calls=1,usec=7,usec_per_call=7.00,rejected_calls=2,failed_calls=0\r\n\r\n"
        );
    }

    #[test]
    fn errorstats_renders_prefix_counts() {
        let mut src = sources();
        src.error_types = vec![("ERR".to_string(), 4), ("WRONGTYPE".to_string(), 1)];
        let out = render(&ErrorstatsSection, &src);
        assert_eq!(
            out,
            "# Errorstats\r\nerrorstat_ERR:count=4\r\nerrorstat_WRONGTYPE:count=1\r\n\r\n"
        );
    }

    #[test]
    fn latencystats_disabled_renders_header_only() {
        let src = sources();
        assert_eq!(render(&LatencystatsSection, &src), "# Latencystats\r\n\r\n");
    }

    #[test]
    fn ratelimit_inactive_renders_nothing() {
        let src = sources();
        assert_eq!(render(&RatelimitSection, &src), "");
    }

    #[test]
    fn ratelimit_active_renders_fields() {
        let mut src = sources();
        src.rate_limit = RateLimitSnapshot {
            users: 2,
            commands_rejected: 10,
            bytes_rejected: 0,
        };
        let out = render(&RatelimitSection, &src);
        assert_eq!(
            out,
            "# Ratelimit\r\nratelimit_users_configured:2\r\n\
             ratelimit_total_commands_rejected:10\r\n\
             ratelimit_total_bytes_rejected:0\r\n\r\n"
        );
    }

    #[test]
    fn tiered_renders_summed_counters() {
        let mut src = sources();
        src.shards.tiered.hot_keys = 5;
        src.shards.tiered.warm_keys = 2;
        src.shards.tiered.spills = 1;
        let out = render(&TieredSection, &src);
        assert!(out.contains("tiered_enabled:1\r\n"), "{out}");
        assert!(out.contains("tiered_hot_keys:5\r\n"), "{out}");
        assert!(out.contains("tiered_warm_keys:2\r\n"), "{out}");
    }

    #[test]
    fn latency_baseline_not_run() {
        let src = sources();
        assert_eq!(
            render(&LatencyBaselineSection, &src),
            "# Latency_Baseline\r\nbaseline_test_run:0\r\n\r\n"
        );
    }

    #[test]
    fn builder_renders_default_sections_in_order() {
        let src = sources();
        let sel = SectionSelector::from_args(&[]);
        let out = InfoBuilder::standard().render(&sel, &src);
        let server = out.find("# Server\r\n").expect("server section");
        let clients = out.find("# Clients\r\n").expect("clients section");
        let keyspace = out.find("# Keyspace\r\n").expect("keyspace section");
        assert!(server < clients && clients < keyspace, "{out}");
        // Extras excluded from default.
        assert!(!out.contains("# Commandstats"), "{out}");
        assert!(!out.contains("# Keysizes"), "{out}");
        // Rendered exactly once, straight from the accumulator — a duplicate
        // line would mean a stub anchor plus a patched copy survived.
        assert_eq!(out.matches("keyspace_hits:").count(), 1, "{out}");
    }
    // FM-REPLICATION-061
    /// A stream that gave up says so in the field an operator reads, and names
    /// both sides of the disagreement. Before this the only record was a log
    /// line printed once per reconnect attempt: `INFO` showed
    /// `master_link_status:down`, indistinguishable from a primary that is
    /// merely restarting, so the node looked like it was catching up when it
    /// had in fact stopped trying (issue 23).
    #[test]
    fn a_replica_that_gave_up_names_the_mismatch_in_info() {
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        src.replication.master_link_up = false;
        src.replication.master_sync_error = Some(
            "shard-count mismatch: the primary's checkpoint was written with 4 shard(s), \
             this node is configured for 2"
                .to_string(),
        );
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("master_link_status:down\r\n"), "{out}");
        let line = out
            .lines()
            .find(|l| l.starts_with("master_sync_error:"))
            .unwrap_or_else(|| panic!("no master_sync_error line: {out}"));
        assert!(
            line.contains("with 4 shard(s)") && line.contains("configured for 2"),
            "the field must name both sides — neither node's own config shows the \
             disagreement: {line}"
        );
    }

    // FM-REPLICATION-061
    /// A link that is down but still retrying renders **no** `master_sync_error`
    /// at all. The field's presence is the whole signal ("this needs a human"),
    /// so rendering it empty, or with a placeholder, on every transient
    /// disconnect would make it worthless for alerting.
    #[test]
    fn a_link_that_is_merely_down_renders_no_sync_error() {
        let mut src = sources();
        src.replication.is_replica = true;
        src.replication.master_host = Some("10.0.0.1".to_string());
        src.replication.master_port = Some(6380);
        src.replication.master_link_up = false;
        src.replication.master_sync_error = None;
        let out = render(&ReplicationSection, &src);
        assert!(out.contains("master_link_status:down\r\n"), "{out}");
        assert!(
            !out.contains("master_sync_error"),
            "an absent refusal must render no line at all: {out}"
        );
    }

    // FM-REPLICATION-061
    /// Both renderers report the same link block for the same state — including
    /// the refusal. A node whose clients saw `master_sync_error` but whose
    /// `redis.call('INFO')` did not (or the reverse) is the split this repo has
    /// now hit three times (FM-REPLICATION-043, -059, -060).
    #[test]
    fn both_renderers_report_the_same_replica_link_block() {
        let link_lines = |section: &str| -> Vec<String> {
            section
                .split("\r\n")
                .filter(|line| {
                    line.starts_with("master_link_status") || line.starts_with("master_sync_error")
                })
                .map(str::to_string)
                .collect()
        };

        for (link_up, refusal) in [
            (true, None),
            (false, None),
            (
                false,
                Some("warm-tier mismatch: this node has tiered-storage.enabled = false"),
            ),
        ] {
            let mut src = sources();
            src.replication.is_replica = true;
            src.replication.master_host = Some("10.0.0.1".to_string());
            src.replication.master_port = Some(6380);
            src.replication.master_link_up = link_up;
            src.replication.master_sync_error = refusal.map(str::to_string);

            let connection_level = render(&ReplicationSection, &src);
            let shard_local = crate::commands::info::build_replica_link_info(link_up, refusal);
            assert_eq!(
                link_lines(&connection_level),
                link_lines(&shard_local),
                "the two INFO renderers must agree for link_up={link_up}, refusal={refusal:?}"
            );
        }
    }
}
